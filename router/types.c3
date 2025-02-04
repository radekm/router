module router;

import std::collections;
import std::net::os;

// Login message is smaller than other messages
// to prevent exhausting memory by clients that aren't logged in.
const int MAX_LOGIN_MSG_SIZE = 512;
const int MAX_MSG_SIZE = 10_000_000;
// Maximum number of services a single client can subscribe to.
const int MAX_SUBSCRIPTIONS = 32;
const int MAX_SERVICE_NAME_LEN = 64;
// Maximum number of distinct service names that can be interned.
const int MAX_SERVICE_NAMES = 65_535;
// Maximum number of clients (including services) that can connect to router at the same time.
// Further connections are not accepted.
const int MAX_CLIENTS = 512;
// Maximum number of messages that can be waiting in a queue to be sent to a single client.
// Trying to enqueue an additional message causes disconnect of the client.
const int MAX_PENDING_MESSAGES = 5000;
// Maximum number of responses that can be awaited from a single client.
// Trying to await an additional response causes disconnect of the client.
const int MAX_AWAITED_RESPONSES = 128;
const int INTERVAL_BETWEEN_PINGS_US = 5_000_000;

enum ClientState
{
    NOT_INITIALIZED,

    // The client must send a login message.
    // Sending anything else causes disconnect of the client.
    // Nothing is sent to clients in this state.
    WAITING_FOR_LOGIN,

    // The client has successfully logged in, and router
    // has determined whether it's an ordinary client, a service or a recorder.
    READY,

    // The client will be disconnected from router.
    // When in this state, the client's socket is still open and registered in epoll.
    // We must notify other clients and services that are not `WAITING_FOR_CLOSE`
    // that this client no longer exists.
    // Nothing is sent to clients in this state.
    WAITING_FOR_CLOSE,
}

// Useful for broadcasting messages to multiple clients.
// `ref_count` must be decremented when all bytes of the message have been sent.
struct SharedPendingMessage
{
    uint ref_count;
    MsgHeader header;
    char[?] body;
}

// Represents a subscription of a client to a service.
struct Subscription
{
    // Which service.
    ServiceId service;
    // Current instance id of `service`. Or 0 if the client is not connected to `service`.
    ClientInstanceId instance_id;
}

struct AwaitedResponse
{
    // One of: LOGIN, CLIENT_ADDED_TO_SERVICE, CLIENT_REMOVED_FROM_SERVICE, PONG.
    MsgType msg_type;
    // When the original request has been enqueued by router.
    // This is also used to determine when the response should arrive.
    // In the case of PONG, its `header.enqueued_by_router_us` must be equal to this timestamp.
    ulong request_enqueued_by_router_us;
    // Instance id of the added or removed client or 0.
    ClientInstanceId instance_id;
}

struct Client
{
    // TODO: Add context for TLS encryption.

    ClientInstanceId instance_id;
    ClientState state;
    NativeSocket socket;
    String addr;  // TODO: Consider `char[128]`.

    // Services to which this client is subscribed.
    // This client may receive messages from these services and send requests to these services.
    // Doesn't contain duplicate services.
    ElasticArray(<Subscription, MAX_SUBSCRIPTIONS>) subscriptions;

    // Ensures that the client is alive and responds promptly.
    // Responses must arrive in the same order as the requests.
    RingBuf(<AwaitedResponse, MAX_AWAITED_RESPONSES>) awaited_responses;

    // For recieving messages from this client.

    int received_from_header;
    MsgHeader header;
    int received_from_body;
    char* body;

    // For sending messages to this client.

    // `sent_from_message` is the number of bytes from the first message
    // in `pending_messages` queue that have been sent.
    int sent_from_message;
    RingBuf(<SharedPendingMessage*, MAX_PENDING_MESSAGES>) pending_messages;

    // --------------------------------------------------------------
    // Following fields are used only if this client is a service.
    // --------------------------------------------------------------

    ServiceId service_id;

    // This service may receive requests from these clients and send messages to them.
    // This is primarily useful when the service broadcasts a message and
    // also for notifying clients when the service terminates
    // Doesn't contain duplicate clients.
    ElasticArray(<ClientInstanceId, MAX_CLIENTS>) subscribers;
}

// When initialized != 0.
distinct ServiceId = ushort;

// When initialized `unique` != 0.
bitstruct ClientInstanceId : ulong
{
    // Index into `Router.clients` array.
    ushort idx : 0 .. 15;
    ulong unique : 16 .. 63;
}

struct Router
{
    // Listening socket.
    NativeSocket server_socket;

    EPollFd epfd;

    // Interning service names.

    // Owns service names.
    List(<String>) service_id_to_name;
    // Maps service names to service ids.
    HashMap(<String, ServiceId>) service_name_to_id;

    // Currently connected clients.

    Client[MAX_CLIENTS] clients;
    int active_clients_count;
    ulong total_clients_connected;  // Used for generating instance ids.

    // The last timestamp when router sent pings.
    ulong pings_sent_us;

    // Queue of clients in `WAITING_FOR_CLOSE` state.
    // Closing one client may switch another client to `WAITING_FOR_CLOSE` state.
    RingBuf(<ClientInstanceId, MAX_CLIENTS>) clients_waiting_for_close;
}

enum MsgType : ushort
{
    // Sent as the first message from each client to router.
    // The message body contains the client's username, password and service name.
    // Service name is empty for non-service clients.
    LOGIN,

    // Sent from a client to router when the client
    // wants to receive messages from a service.
    // The message body contains the service name.
    SUBSCRIBE_TO_SERVICE,

    // Sent from router to a client when the client is connected to or disconnected from a service.
    // The header contains `instance_id` of the service.
    // The message body contains the service name.
    CONNECTED_TO_SERVICE,
    DISCONNECTED_FROM_SERVICE,

    // Sent from router to a service to add or remove a client.
    // The header contains `instance_id` of the client.
    ADD_CLIENT_TO_SERVICE,
    REMOVE_CLIENT_FROM_SERVICE,

    // Sent from a service to router as confirmation
    // that the service has added or removed a client.
    // The header contains `instance_id` of the client.
    CLIENT_ADDED_TO_SERVICE,
    CLIENT_REMOVED_FROM_SERVICE,

    // Sent from a service to router and from router to all clients connected to the service.
    // When sent from a service to router, the header contains `instance_id` 0.
    // When sent from router to clients, the header contains `instance_id` of the service.
    MSG_TO_ALL_CLIENTS,
    // Sent from a service to router and from router to a single client connected to the service.
    // When sent from a service to router, the header contains `instance_id` of the client.
    // When sent from router to the client, the header contains `instance_id` of the service.
    MSG_TO_ONE_CLIENT,

    // Sent from a client to router and from router to a service.
    // When sent from a client, the header contains `instance_id` of the sending service.
    // When sent from router, the header contains `instance_id` of the originating client.
    REQUEST,

    // Sent from router to a client.
    PING,
    // Sent from a client to router.
    // The header contains `enqueued_by_router_us` from PING.
    PONG,
}

struct MsgHeader @packed
{
    // For messages from router, `enqueued_by_router_us` contains the timestamp when router
    // enqueued the message for sending.
    // For messages from clients, it is zero except for PONG.
    ulong enqueued_by_router_us;  // Since 1970.
    ushort msg_type;
    ushort flags;
    uint body_size;
    ClientInstanceId instance_id;  // 0 when not used.
}

struct Config
{
    ZString host;
    int port;
}
