module router;

import std::collections;
import std::encoding::json;
import std::io;
import std::net::os;
import std::time::os;
import libc;

macro void abort_with_errno(String string, ...) @noreturn
{
    CInt errno = libc::errno();
	io::eprintfn("Last errno %d: %s", errno, libc::strerror(errno));
    abort(string, $vasplat);
}

fn void Config.destroy(&self)
{
    free(self.host);
    *self = {};
}

fn Config parse_config()
{
    @pool()
    {
        char[] bytes = file::load_temp("config.json") ?? abort("Unable to open config.json file");
        Object* obj = json::temp_parse_string((String)bytes) ?? abort("Unable to parse config.json file");

        String host = obj.get_string("host") ?? abort("Unable to read host");
        int port = obj.get_int("port") ?? abort("Unable to read port");

        Config config;
        config.host = host.zstr_copy();  // Call copy after processing entire config.
        config.port = port;

        return config;
    };
}

// Must not be on stack because it's huge.
Router router;

fn void main()
{
    Config config = parse_config();
    defer config.destroy();

    // Ignore SIGPIPE to prevent `send` from generating SIGPIPE if the peer
    // on a stream-oriented socket has closed the connection.
    // Alternatively we can pass flag `MSG_NOSIGNAL` to every `send` call.
    if (libc::signal(libc::SIGPIPE, SIG_IGN) == SIG_ERR) abort_with_errno("Failed to ignore SIGPIPE");

    AddrInfo hints;
    hints.ai_family = os::AF_INET;
    hints.ai_socktype = os::SOCK_STREAM;
    hints.ai_flags = os::AI_PASSIVE;
    AddrInfo* ai;

    @stack_mem(128; Allocator allocator)
    {
        DString port_str;
        port_str.init(allocator: allocator);
        port_str.appendf("%d", config.port);

        CInt error = os::getaddrinfo(config.host, port_str.zstr_view(), &hints, &ai);
        if (error == EAI_SYSTEM)
        {
            // `EAI_SYSTEM` means that error is in `errno`.
            abort_with_errno("getaddrinfo failed for host '%s' and port '%s'", config.host, port_str);
        }
        else if (error != 0)
        {
        	io::eprintfn("Error %d from getaddrinfo: %s", error, gai_strerror(error));
            abort("getaddrinfo failed for host '%s' and port '%s'", config.host, port_str);
        }
    };
    defer os::freeaddrinfo(ai);

    if (ai == null) abort("getaddrinfo returned nothing");
    if (ai.ai_next != null) abort("getaddrinfo returned more than one address");

    NativeSocket server_socket = os::socket(os::AF_INET, os::SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (server_socket == -1) abort_with_errno("Server socket wasn't created");
    defer (void)server_socket.close();

    CInt optval = 1;
    if (os::setsockopt(server_socket, os::SOL_SOCKET, os::SO_REUSEADDR, &optval, $sizeof(optval)) != 0)
    {
        abort_with_errno("setsockopt failed when setting SO_REUSEADDR");
    }

    if (os::bind(server_socket, ai.ai_addr, ai.ai_addrlen) == -1) abort_with_errno("bind failed");

    if (os::listen(server_socket, backlog: 32) == -1) abort_with_errno("listen failed");

    EPollFd epfd = epoll_create(1);
    if (epfd == -1) abort_with_errno("epoll_create failed");
    defer epfd.close();

    // `EPollEvent.data.u64` contains `ClientInstanceId`.
    // This `ClientInstanceId` is `0` for server socket.
    if (epoll_ctl(epfd, EPOLL_CTL_ADD, server_socket, &&EPollEvent { .events = EPOLLIN }) == -1)
    {
        abort_with_errno("Server socket wasn't added to epoll");
    }

    router.init(server_socket, epfd);
    defer router.destroy();

    io::eprintfn("Listening on %s:%d", config.host, config.port);

    // Event loop.
    while (true)
    {
        EPollEvent[32] events;
        CInt n = epoll_wait(epfd, &events[0], events.len, 1000);
        if (n == -1) abort_with_errno("epoll_wait failed");

        foreach (EPollEvent event : events[:n])
        {
            bool error = event.events & (EPOLLERR | EPOLLHUP) != 0;

            ClientInstanceId instance_id = (ClientInstanceId)event.data.u64;

            // Event on server socket.
            if ((ulong)instance_id == 0)
            {
                EPoll expected_events = EPOLLIN | EPOLLERR | EPOLLHUP;
                if (event.events & ~expected_events != 0)
                {
                    io::eprintfn("Unexpected event on server socket %d", event.events);
                }

                if (error) abort("Error on server socket");

                // No new connection to accept.
                if (event.events & EPOLLIN == 0) continue;

                // For client's address.
                char[128] addr_storage;
                Socklen_t addr_len = addr_storage.len;

                // Try to accept one connection. Remaining connections will be extracted
                // in the next iterations of event loop.
                NativeSocket client_socket = accept4(
                    server_socket,
                    (SockAddrPtr)&addr_storage,
                    &addr_len,
                    SOCK_NONBLOCK);
                if (client_socket == -1)
                {
                    CInt errno = libc::errno();
                	io::eprintfn("Failed to accept connection. Errno %d: %s", errno, libc::strerror(errno));
                }
                else
                {
                    // Connection has been accepted.
                    char[128] host;
                    char[128] serv;
                    CInt name_error = getnameinfo(
                        (SockAddrPtr)&addr_storage, addr_len,
                        &host[0], host.len,
                        &serv[0], serv.len,
                        0);
                    if (name_error != 0)
                    {
                        io::eprintfn("Disconnecting newly accepted client - its address can't be resolved");
                        (void)client_socket.close();
                    }
                    else
                    {
                        io::eprintfn("Accepted client from '%s:%s'", (ZString)&host[0], (ZString)&serv[0]);
                        router.add_client(client_socket, (ZString)&host[0], (ZString)&serv[0]);
                    }
                }
            }
            // Event on client socket.
            else
            {
                EPoll expected_events = EPOLLIN | EPOLLOUT | EPOLLERR | EPOLLHUP;
                if (event.events & ~expected_events != 0)
                {
                    io::eprintfn("Unexpected event on client socket %d", event.events);
                }

                Client* client = &router.clients[instance_id.idx];

                if (client.state == NOT_INITIALIZED)
                {
                    abort("Got event for client which is not initialized (instance %d)", (ulong)instance_id);
                }
                if (client.instance_id != instance_id) abort("Actual instance id differs from epoll instance id");

                // Ignore event if client is `WAITING_FOR_CLOSE`.
                if (client.state == WAITING_FOR_CLOSE) continue;

                if (error)
                {
                    router.mark_client_for_closing(client, "Error on client socket");
                    continue;
                }

                // Our strategy is to prefer writing over reading
                // to free memory consumed by `pending_messages`.
                if (event.events & EPOLLOUT != 0)
                {
                    router.send_pending_msg(client);
                }
                else if (event.events & EPOLLIN != 0)
                {
                    if (router.receive_msg(client))
                    {
                        router.process_msg(client);
                    }
                }
            }
        }

        router.enqueue_pings();
        router.check_awaited_responses();  // Check if responses aren't too delayed.
        router.close_clients_waiting_for_it();  // Close clients `WAITING_FOR_CLOSE`.
    }
}

fn void Router.init(&self, NativeSocket server_socket, EPollFd epfd)
{
    self.server_socket = server_socket;
    self.epfd = epfd;
}

fn void Router.destroy(&self)
{
    *self = {};
}

fn void Router.add_client(&self, NativeSocket client_socket, ZString host, ZString port)
{
    // Find free slot in `self.clients` array.
    if (self.active_clients_count == self.clients.len)
    {
        io::eprintfn("No free slot for client from '%s:%s'", host, port);
        (void)client_socket.close();
        return;
    }
    int client_index = 0;
    for (; client_index < self.clients.len; client_index++)
    {
        if (self.clients[client_index].state == NOT_INITIALIZED) break;
    }
    // We haven't found a free slot
    // but `self.active_clients_count != self.clients.len`
    // means that there should be a free slot.
    if (client_index == self.clients.len) abort("Router has inconsistent state: No free slot for client");

    ClientInstanceId instance_id;
    instance_id.idx = (ushort)client_index;
    instance_id.unique = self.total_clients_connected + 1;

    // Try to add client to epoll.
    EPollEvent new_event = { .events = EPOLLIN, .data.u64 = (ulong)instance_id };
    if (epoll_ctl(self.epfd, EPOLL_CTL_ADD, client_socket, &new_event) == -1)
    {
        abort_with_errno("Client socket of new client wasn't added to epoll");
    }

    DString addr = dstring::new();
    defer addr.free();
    addr.appendf("%s:%s", host, port);

    self.total_clients_connected++;
    self.active_clients_count++;

    Client* client = &self.clients[client_index];
    client.instance_id = instance_id;
    client.state = WAITING_FOR_LOGIN;
    client.socket = client_socket;
    client.addr = addr.copy_str();
    client.awaited_responses.enqueue({
        .msg_type = LOGIN,
        .request_enqueued_by_router_us = get_monotonic_event_time_us(),
    });
}

// For zero `service_id` returns empty string.
fn String Router.get_service_name(&self, ServiceId service_id)
{
    if ((uint)service_id == 0) return "";
    if ((uint)service_id > self.service_id_to_name.len()) abort("Service id out of range");
    String name = self.service_id_to_name[(usz)service_id - 1];
    if (name == "") abort("Empty service name");
    return name;
}

macro void Router.mark_client_for_closing(&self, Client* client, String $reason, ...)
{
    io::eprintfn(
        "CLIENT ERROR: Marking client for closing " +++
            "(instance %d, state '%s', addr '%s', service: '%s'): " +++
            $reason,
        (ulong)client.instance_id,
        client.state,
        client.addr,
        self.get_service_name(client.service_id),
        $vasplat);

    if (client.state == NOT_INITIALIZED) abort("Cannot mark uninitialized client for closing");
    if (client.state != WAITING_FOR_CLOSE)
    {
        client.state = WAITING_FOR_CLOSE;
        self.clients_waiting_for_close.enqueue(client.instance_id);
    }
}

// Returns `true` if the entire message has been received and can be processed.
fn bool Router.receive_msg(&self, Client* client)
{
    // Receive rest of the header.
    if (client.received_from_header < MsgHeader.sizeof)
    {
        void* buffer = &client.header;
        isz n = libc::recv(
            client.socket,
            buffer + client.received_from_header,
            MsgHeader.sizeof - client.received_from_header,
            0);

        // Handle error.
        if (n == -1)
        {
            self.handle_error_when_receiving_or_sending(client, receiving: true);
            return false;
        }

        client.received_from_header += (int)n;
    }

    // Read rest of the body.
    if (client.received_from_header == MsgHeader.sizeof && client.header.body_size > 0)
    {
        // Allocate buffer for the body.
        if (client.body == null)
        {
            uint max_body_size = client.state == WAITING_FOR_LOGIN ? MAX_LOGIN_MSG_BODY_SIZE : MAX_MSG_BODY_SIZE;
            if (client.header.body_size > max_body_size)
            {
                self.mark_client_for_closing(client, "Client sent too big body %d", client.header.body_size);
                return false;
            }

            client.body = malloc(client.header.body_size);
        }

        // Receive rest of the body.
        if (client.received_from_body < client.header.body_size)
        {
            isz n = libc::recv(
                client.socket,
                client.body + client.received_from_body,
                (usz)client.header.body_size - client.received_from_body,
                0);

            // Handle error.
            if (n == -1)
            {
                self.handle_error_when_receiving_or_sending(client, receiving: true);
                return false;
            }

            client.received_from_body += (int)n;
        }
    }

    bool received_everything =
        client.received_from_header == MsgHeader.sizeof &&
        client.received_from_body == client.header.body_size;
    return received_everything;
}

// Returns `true` if the message has been successfully enqueued.
fn bool Router.enqueue_pending_msg(&self, Client* client, SharedPendingMessage* msg)
{
    if (!client.pending_messages.can_enqueue())
    {
        self.mark_client_for_closing(client, "Too many pending messages");
        return false;
    }

    client.pending_messages.enqueue(msg);
    msg.ref_count++;

    // We enqueued the first message.
    if (client.pending_messages.written == 1)
    {
        // Configure epoll for sending - include `EPOLLOUT`.
        EPollEvent mod_event = { .events = EPOLLIN | EPOLLOUT, .data.u64 = (ulong)client.instance_id };
        if (epoll_ctl(self.epfd, EPOLL_CTL_MOD, client.socket, &mod_event) == -1)
        {
            abort_with_errno("Failed to configure epoll for both reading and writing");
        }
    }

    return true;
}

fn void Router.send_pending_msg(&self, Client* client)
{
    while (true)
    {
        // There are no more messages waiting to be sent.
        if (client.pending_messages.written == 0)
        {
            // Configure epoll only for receiving - exclude `EPOLLOUT`.
            EPollEvent mod_event = { .events = EPOLLIN, .data.u64 = (ulong)client.instance_id };
            if (epoll_ctl(self.epfd, EPOLL_CTL_MOD, client.socket, &mod_event) == -1)
            {
                abort_with_errno("Failed to configure epoll for reading only");
            }
            return;
        }

        SharedPendingMessage* msg = client.pending_messages.peek();
        int msg_size = MsgHeader.sizeof + msg.header.body_size;
        int remaining_size = msg_size - client.sent_from_message;
        if (remaining_size < 0) abort("Router has inconsistent state: Sent more bytes than message size");
        if (remaining_size == 0) abort("Router has inconsistent state: Sent message not removed");

        // Sent the header.
        if (client.sent_from_message < MsgHeader.sizeof)
        {
            void* buffer = &msg.header;
            isz n = libc::send(
                client.socket,
                buffer + client.sent_from_message,
                MsgHeader.sizeof - client.sent_from_message,
                0);

            // Handle error.
            if (n == -1)
            {
                self.handle_error_when_receiving_or_sending(client, receiving: false);
                return;
            }

            client.sent_from_message += (int)n;
            // Even though we tried to send the entire header, it still hasn't been sent.
            // Let's wait for the next event loop iteration.
            if (client.sent_from_message != MsgHeader.sizeof) return;
        }

        // Update remaining size.
        remaining_size = msg_size - client.sent_from_message;

        // At this point the entire header has been sent.
        if (remaining_size > 0)
        {
            // We have to send the body.
            isz n = libc::send(
                client.socket,
                &msg.body + client.sent_from_message - MsgHeader.sizeof,
                remaining_size,
                0);

            // Handle error.
            if (n == -1)
            {
                self.handle_error_when_receiving_or_sending(client, receiving: false);
                return;
            }

            client.sent_from_message += (int)n;
            if (client.sent_from_message != msg_size) return;
        }

        // At this point the entire message has been sent.
        msg.ref_count--;
        if (msg.ref_count == 0)
        {
            free(msg);
        }
        client.pending_messages.dequeue();
        client.sent_from_message = 0;  // Nothing has been sent from the next message.
    }
}

fn void Router.handle_error_when_receiving_or_sending(&self, Client* client, bool receiving)
{
    CInt errno = libc::errno();

    // Recoverable error.
    if (errno == errno::EAGAIN || errno == errno::EWOULDBLOCK || errno == errno::EINTR) return;

    String action = receiving ? "receiving" : "sending";
    self.mark_client_for_closing(client, "Error %d when %s: %s", errno, action, libc::strerror(errno));
}

// Returns `true` if the response has been successfully enqueued.
fn bool Router.enqueue_awaited_response(&self, Client* client, AwaitedResponse response)
{
    if (client.awaited_responses.can_enqueue())
    {
        client.awaited_responses.enqueue(response);
        return true;
    }
    else
    {
        self.mark_client_for_closing(client, "Too many awaited responses");
        return false;
    }
}

fn void Router.enqueue_pings(&self)
{
    ulong now = get_monotonic_event_time_us();
    // Too early to enqueue pings.
    if (now < self.pings_sent_us + INTERVAL_BETWEEN_PINGS_US) return;

    SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, 0);
    msg.header = {
        .enqueued_by_router_us = now,
        .msg_type = MsgType.PING.ordinal,
    };

    foreach (&client : self.clients)
    {
        if (client.state != READY) continue;
        if (self.enqueue_pending_msg(client, msg))
        {
            // PING has been enqueued. Add PONG to awaited responses.
            self.enqueue_awaited_response(client, {
                .msg_type = PONG,
                .request_enqueued_by_router_us = now,
            });
        }
    }

    // Ping hasn't been enqueued.
    if (msg.ref_count == 0)
    {
        free(msg);
    }

    self.pings_sent_us = now;
}

fn void Router.check_awaited_responses(&self)
{
    ulong now = get_monotonic_event_time_us();
    // Too early to check responses.
    if (now < self.responses_checked_us + INTERVAL_BETWEEN_CHECKING_RESPONSES_US) return;

    foreach (&client : self.clients)
    {
        if (client.state != READY) continue;
        if (client.awaited_responses.written > 0)
        {
            AwaitedResponse resp = client.awaited_responses.peek();
            if (resp.request_enqueued_by_router_us + MAX_RESPONSE_TIME_US < now)
            {
                self.mark_client_for_closing(
                    client,
                    "Missing awaited response %s with request time %d and instance id %d",
                    resp.msg_type, resp.request_enqueued_by_router_us, (ulong)resp.instance_id);
            }
        }
    }

    self.responses_checked_us = now;
}

fn void Router.close_clients_waiting_for_it(&self)
{
    while (self.clients_waiting_for_close.written > 0)
    {
        ulong now = get_monotonic_event_time_us();

        ClientInstanceId instance_id = self.clients_waiting_for_close.dequeue();
        Client* client = &self.clients[instance_id.idx];
        io::eprintfn(
            "Closing client (instance %d, state '%s', addr '%s', service: '%s')",
            (ulong)client.instance_id,
            client.state,
            client.addr,
            self.get_service_name(client.service_id));

        if (client.state != WAITING_FOR_CLOSE) abort("Client waiting for close has invalid state");
        if (client.instance_id != instance_id) abort("Actual instance id differs from instance id to be closed");

        if (epoll_ctl(self.epfd, EPOLL_CTL_DEL, client.socket, null) == -1)
        {
            abort_with_errno("Failed to remove client socket from epoll");
        }
        (void)client.socket.close();

        // Notify services to which the client is subscribed and remove the subscriber.
        //
        // We don't use `enqueue_msg_and_response_remove_client_from_service` because
        // we want to reuse a single message and send it to all services.
        {
            SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, 0);
            msg.header = {
                .enqueued_by_router_us = now,
                .msg_type = MsgType.REMOVE_CLIENT_FROM_SERVICE.ordinal,
                .instance_id = client.instance_id,
            };
            foreach (&subscription : client.subscriptions.entries[:client.subscriptions.size])
            {
                // Client is not connected to any instance of the service.
                if ((ulong)subscription.instance_id == 0) continue;

                Client* service = &self.clients[subscription.instance_id.idx];
                // Service doesn't exist anymore.
                if (service.state != READY || service.instance_id != subscription.instance_id) continue;
                if (self.enqueue_pending_msg(service, msg))
                {
                    AwaitedResponse response = {
                        .msg_type = CLIENT_REMOVED_FROM_SERVICE,
                        .request_enqueued_by_router_us = now,
                        .instance_id = client.instance_id,
                    };
                    if (self.enqueue_awaited_response(service, response))
                    {
                        // Everything has been enqueued, remove the subscriber.
                        if (!service.subscribers.remove_first_item(client.instance_id))
                        {
                            abort("Router has inconsistent state: Subscriber not found");
                        }
                    }
                }
            }
            if (msg.ref_count == 0)
            {
                free(msg);
            }
        }

        // If the client is a service, then notify the clients that are subscribed to it.
        if ((uint)client.service_id != 0)
        {
            String service_name = self.get_service_name(client.service_id);
            uint body_size = 1 + service_name.len;
            SharedPendingMessage* msg = mem::new_with_padding(SharedPendingMessage, body_size);
            msg.header = {
                .enqueued_by_router_us = now,
                .msg_type = MsgType.DISCONNECTED_FROM_SERVICE.ordinal,
                .body_size = body_size,
                .instance_id = client.instance_id,
            };
            msg.body[0] = (char)service_name.len;
            mem::copy(&msg.body[1], &service_name[0], service_name.len);
            foreach (ClientInstanceId subscriber : client.subscribers.entries[:client.subscribers.size])
            {
                Client* subscriber_client = &self.clients[subscriber.idx];
                // Subscriber doesn't exist anymore.
                if (subscriber_client.state != READY || subscriber_client.instance_id != subscriber) continue;

                // Notify the subscriber that it's no longer connected to service
                // and zero instance id of the service.
                if (self.enqueue_pending_msg(subscriber_client, msg))
                {
                    if (try subscription = self.find_subscription_by_service_id(subscriber_client, client.service_id))
                    {
                        if (subscription.instance_id != client.instance_id)
                        {
                            abort("Router has inconsistent state: Subscription to different instance of service");
                        }
                        subscription.instance_id = (ClientInstanceId)(ulong)0;
                    }
                    else
                    {
                        abort("Router has inconsistent state: Missing subscription to service");
                    }
                }
            }
            if (msg.ref_count == 0)
            {
                free(msg);
            }
        }

        client.destroy();
    }
}

// Free memory.
fn void Client.destroy(&self)
{
    self.addr.free();
    free(self.body);
    while (self.pending_messages.written > 0)
    {
        SharedPendingMessage* msg = self.pending_messages.dequeue();
        msg.ref_count--;
        if (msg.ref_count == 0)
        {
            free(msg);
        }
    }
    *self = {};
}

fn ulong get_monotonic_event_time_us()
{
	TimeSpec ts;
	if (os::clock_gettime(os::CLOCK_REALTIME, &ts) != 0)
    {
        abort_with_errno("clock_gettime failed");
    }

    static ulong time_us = 0;  // Ensure that time is monotonic.
    time_us = max(time_us, ts.s * 1_000_000u64 + ts.ns / 1_000u64);
	return time_us;
}
