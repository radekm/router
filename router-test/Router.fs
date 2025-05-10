module Router

open System
open System.IO
open System.Net.Sockets
open System.Runtime.ExceptionServices
open System.Runtime.InteropServices
open System.Text
open System.Threading.Tasks

type MsgType =
    // Sent as the first message from each client to router.
    // The message body contains the client's username, password and service name.
    // Service name is empty for non-service clients.
    | Login = 0us

    // Sent from a client to router when the client
    // wants to receive messages from a service.
    // The message body contains the service name.
    | SubscribeToService = 1us

    // Sent from router to a client when the client is connected to or disconnected from a service.
    // The header contains `instance_id` of the service.
    // The message body contains the service name.
    | ConnectedToService = 2us
    | DisconnectedFromService = 3us

    // Sent from router to a service to add or remove a client.
    // The header contains `instance_id` of the client.
    | AddClientToService = 4us
    | RemoveClientFromService = 5us

    // Sent from a service to router as confirmation
    // that the service has added or removed a client.
    // The header contains `instance_id` of the client.
    | ClientAddedToService = 6us
    | ClientRemovedFromService = 7us

    // Sent from a service to router and from router to all clients connected to the service.
    // When sent from a service to router, the header contains `instance_id` 0.
    // When sent from router to clients, the header contains `instance_id` of the service.
    | MsgToAllClients = 8us
    // Sent from a service to router and from router to a single client connected to the service.
    // When sent from a service to router, the header contains `instance_id` of the client.
    // When sent from router to the client, the header contains `instance_id` of the service.
    | MsgToOneClient = 9us

    // Sent from a client to router and from router to a service.
    // When sent from a client, the header contains `instance_id` of the sending service.
    // When sent from router, the header contains `instance_id` of the originating client.
    | Request = 10us

    // Sent from router to a client.
    | Ping = 11us
    // Sent from a client to router.
    // The header contains `enqueued_by_router_us` from PING.
    | Pong = 12us

type ServiceName = string

[<Struct; StructLayout(LayoutKind.Sequential, Pack = 1)>]
type ClientInstanceId = { Id : uint64 }

[<Struct; StructLayout(LayoutKind.Sequential, Pack = 1)>]
type UsSinceEpoch =
    { Us : uint64 }

    member me.ToDateTimeOffset() =
        DateTimeOffset
            .FromUnixTimeMilliseconds(int64 (me.Us / 1000UL))
            .AddTicks(int64 (me.Us % 1000UL * 100UL))

[<Struct; StructLayout(LayoutKind.Sequential, Pack = 1)>]
type MsgHeader =
    { EnqueuedByRouterUs : UsSinceEpoch  // Since 1970.
      MsgType : MsgType
      Flags : uint16
      BodySize : uint
      InstanceId : ClientInstanceId  // 0 when not used.
    }

type OutgoingMsg =
    | Login of username:string * password:string * serviceName:ServiceName
    | SubscribeToService of ServiceName
    | Request of destService:ClientInstanceId * data:byte[]
    | Pong of timeFromPing:UsSinceEpoch
    // Only services send following messages:
    | ClientAddedToService of client:ClientInstanceId
    | ClientRemovedFromService of client:ClientInstanceId
    | MsgToAllClients of data:byte[]
    | MsgToOneClient of destClient:ClientInstanceId * data:byte[]

type IncomingMsg =
    | ConnectedToService of time:UsSinceEpoch * service:ClientInstanceId * serviceName:ServiceName
    | DisconnectedFromService of time:UsSinceEpoch * service:ClientInstanceId * serviceName:ServiceName
    | MsgToAllClients of time:UsSinceEpoch * srcService:ClientInstanceId * data:byte[]
    | MsgToOneClient of time:UsSinceEpoch * srcService:ClientInstanceId * data:byte[]
    | Ping of time:UsSinceEpoch
    // Only services receive following messages:
    | AddClientToService of time:UsSinceEpoch * client:ClientInstanceId
    | RemoveClientFromService of time:UsSinceEpoch * client:ClientInstanceId
    | Request of time:UsSinceEpoch * srcClient:ClientInstanceId * data:byte[]

// Because `reraise` cannot be called from `backgroundTask`.
let inline reraiseAnywhere<'a> (e: exn) : 'a =
    ExceptionDispatchInfo.Capture(e).Throw()
    Unchecked.defaultof<'a>

// Raised from `readingTask` when `ReceiveAsync` receives 0 bytes
// or from `write` when `SendAsync` sends 0 bytes.
exception ConnectionClosed

let MaxMsgBodySize = 10_000_000u
let MaxServiceNameLen = 64

// Returns `TcpClient` after it successfully connects or raises.
// It's up to the caller to dispose the returned `TcpClient`.
// If it raises then `onMsg` and `onStop` are not called.
//
// After `TcpClient` successfully connects `readingTask` is started.
// `onMsg` is called for each read message.
// `onStop` is called when `readingTask` stops with the reason why it stopped.
//
// If `onMsg` raises then the exception interrupts `readingTask`
// and `onStop` is called with it. If `onStop` raises nothing happens.
let connectAndStartReading
    (host : string)
    (port : int)
    (onMsg : IncomingMsg -> unit)
    (onStop : exn -> unit) : Task<TcpClient> = backgroundTask {

    let client = new TcpClient()

    try
        do! client.ConnectAsync(host, port)  // CONSIDER: Use cancellation token here?
        let readingTask = backgroundTask {
            let headerBuffer = Array.zeroCreate (Marshal.SizeOf<MsgHeader>())
            while true do
                let mutable receivedFromHeader = 0
                while receivedFromHeader < headerBuffer.Length do
                    let segment =
                        ArraySegment<byte>(headerBuffer, receivedFromHeader, headerBuffer.Length - receivedFromHeader)
                    let! n = client.Client.ReceiveAsync(segment)
                    if n <= 0
                    then raise ConnectionClosed
                    else receivedFromHeader <- receivedFromHeader + n

                let header = MemoryMarshal.Read<MsgHeader>(ReadOnlySpan headerBuffer)
                if header.BodySize > MaxMsgBodySize then
                    failwithf "Message body too big: %d" header.BodySize

                let mutable receivedFromBody = 0
                let bodyBuffer = Array.zeroCreate (int header.BodySize)
                while receivedFromBody < int header.BodySize do
                    let segment =
                        ArraySegment<byte>(bodyBuffer, receivedFromBody, bodyBuffer.Length - receivedFromBody)
                    let! n = client.Client.ReceiveAsync(segment)
                    if n <= 0
                    then raise ConnectionClosed
                    else receivedFromBody <- receivedFromBody + n

                let readServiceNameFromBody () =
                    if bodyBuffer.Length = 0 then
                        failwithf "Body of %A must contain service name but it's empty" header.MsgType
                    let len = int bodyBuffer[0]
                    if len = 0 then
                        failwithf "Service name of %A must be non-empty" header.MsgType
                    if bodyBuffer.Length <> len + 1 then
                        failwithf "Service name doesn't exactly fit into body of %A" header.MsgType
                    // We assume that service names use UTF-8 encoding.
                    Encoding.UTF8.GetString(bodyBuffer, 1, len)

                match header.MsgType with
                | MsgType.ConnectedToService ->
                    let msg = IncomingMsg.ConnectedToService ( header.EnqueuedByRouterUs,
                                                               header.InstanceId,
                                                               readServiceNameFromBody () )
                    onMsg msg
                | MsgType.DisconnectedFromService ->
                    let msg = IncomingMsg.DisconnectedFromService ( header.EnqueuedByRouterUs,
                                                                    header.InstanceId,
                                                                    readServiceNameFromBody () )
                    onMsg msg
                | MsgType.AddClientToService ->
                    let msg = IncomingMsg.AddClientToService ( header.EnqueuedByRouterUs,
                                                               header.InstanceId )
                    onMsg msg
                | MsgType.RemoveClientFromService ->
                    let msg = IncomingMsg.RemoveClientFromService ( header.EnqueuedByRouterUs,
                                                                    header.InstanceId )
                    onMsg msg
                | MsgType.MsgToAllClients ->
                    let msg = IncomingMsg.MsgToAllClients ( header.EnqueuedByRouterUs,
                                                            header.InstanceId,
                                                            bodyBuffer )
                    onMsg msg
                | MsgType.MsgToOneClient ->
                    let msg = IncomingMsg.MsgToOneClient ( header.EnqueuedByRouterUs,
                                                           header.InstanceId,
                                                           bodyBuffer )
                    onMsg msg
                | MsgType.Request ->
                    let msg = IncomingMsg.Request ( header.EnqueuedByRouterUs,
                                                    header.InstanceId,
                                                    bodyBuffer )
                    onMsg msg
                | MsgType.Ping -> onMsg (Ping header.EnqueuedByRouterUs)
                | MsgType.Login
                | MsgType.SubscribeToService
                | MsgType.ClientAddedToService
                | MsgType.ClientRemovedFromService
                | MsgType.Pong -> failwithf "Received message which can be only sent: %A" header.MsgType
                | t -> failwithf "Received message with unknown type: %d" (uint16 t)
        }
        let _completeTask = backgroundTask {
            try
                do! readingTask
                onStop (Exception "Reading task should never stop without exception")
            with e ->
                onStop e
        }
        return client
    with e ->
        // In case of error while connecting `onStop` is not called.
        client.Dispose()
        return (reraiseAnywhere e)
}

let write (client : TcpClient) (msg : OutgoingMsg) = backgroundTask {
    let header, bodyBuffer =
        match msg with
        | Login(username, password, serviceName) ->
            if username.Length = 0 || password.Length = 0 then
                failwith "Username and password must not be empty"
            let username = Encoding.UTF8.GetBytes username
            let password = Encoding.UTF8.GetBytes password
            let serviceName = Encoding.UTF8.GetBytes serviceName
            if username.Length > 255 || password.Length > 255 || serviceName.Length > MaxServiceNameLen then
                failwith "Username or password or service name is too long"
            let bodySize = 3 + username.Length + password.Length + serviceName.Length
            let bodyBuffer = Array.zeroCreate bodySize
            use ms = new MemoryStream(bodyBuffer)
            use writer = new BinaryWriter(ms)
            writer.Write(byte username.Length)
            writer.Write(username)
            writer.Write(byte password.Length)
            writer.Write(password)
            writer.Write(byte serviceName.Length)
            writer.Write(serviceName)
            let header : MsgHeader = { EnqueuedByRouterUs = { Us = 0UL }
                                       MsgType = MsgType.Login
                                       Flags = 0us
                                       BodySize = uint bodySize
                                       InstanceId = { Id = 0UL } }
            header, bodyBuffer
        | SubscribeToService serviceName ->
            if serviceName.Length = 0 then
                failwith "Service name must not be empty"
            let serviceName = Encoding.UTF8.GetBytes serviceName
            if serviceName.Length > MaxServiceNameLen then
                failwith "Service name is too long"
            let bodySize = 1 + serviceName.Length
            let bodyBuffer = Array.zeroCreate bodySize
            use ms = new MemoryStream(bodyBuffer)
            use writer = new BinaryWriter(ms)
            writer.Write(byte serviceName.Length)
            writer.Write(serviceName)
            let header : MsgHeader = { EnqueuedByRouterUs = { Us = 0UL }
                                       MsgType = MsgType.SubscribeToService
                                       Flags = 0us
                                       BodySize = uint bodySize
                                       InstanceId = { Id = 0UL } }
            header, bodyBuffer
        | OutgoingMsg.Request(destService, data) ->
            if uint data.Length > MaxMsgBodySize then
                failwith "Request data too big"
            let header : MsgHeader = { EnqueuedByRouterUs = { Us = 0UL }
                                       MsgType = MsgType.Request
                                       Flags = 0us
                                       BodySize = uint data.Length
                                       InstanceId = destService }
            header, data
        | Pong timeFromPing ->
            let header : MsgHeader = { EnqueuedByRouterUs = timeFromPing
                                       MsgType = MsgType.Pong
                                       Flags = 0us
                                       BodySize = 0u
                                       InstanceId = { Id = 0UL } }
            header, [||]
        | ClientAddedToService client ->
            let header : MsgHeader = { EnqueuedByRouterUs = { Us = 0UL }
                                       MsgType = MsgType.ClientAddedToService
                                       Flags = 0us
                                       BodySize = 0u
                                       InstanceId = client }
            header, [||]
        | ClientRemovedFromService client ->
            let header : MsgHeader = { EnqueuedByRouterUs = { Us = 0UL }
                                       MsgType = MsgType.ClientRemovedFromService
                                       Flags = 0us
                                       BodySize = 0u
                                       InstanceId = client }
            header, [||]
        | OutgoingMsg.MsgToAllClients data ->
            if uint data.Length > MaxMsgBodySize then
                failwith "Message data too big"
            let header : MsgHeader = { EnqueuedByRouterUs = { Us = 0UL }
                                       MsgType = MsgType.MsgToAllClients
                                       Flags = 0us
                                       BodySize = uint data.Length
                                       InstanceId = { Id = 0UL } }
            header, data
        | OutgoingMsg.MsgToOneClient(destClient, data) ->
            if uint data.Length > MaxMsgBodySize then
                failwith "Message data too big"
            let header : MsgHeader = { EnqueuedByRouterUs = { Us = 0UL }
                                       MsgType = MsgType.MsgToOneClient
                                       Flags = 0us
                                       BodySize = uint data.Length
                                       InstanceId = destClient }
            header, data
    let headerBuffer = Array.zeroCreate (Marshal.SizeOf<MsgHeader>())
    MemoryMarshal.Write(Span headerBuffer, &header)
    let mutable written = 0
    while written < headerBuffer.Length do
        let segment = ArraySegment(headerBuffer, written, headerBuffer.Length - written)
        let! n = client.Client.SendAsync(segment)
        if n <= 0
        then raise ConnectionClosed
        else written <- written + n

    written <- 0
    while written < bodyBuffer.Length do
        let segment = ArraySegment(bodyBuffer, written, bodyBuffer.Length - written)
        let! n = client.Client.SendAsync(segment)
        if n <= 0
        then raise ConnectionClosed
        else written <- written + n
}

type Config =
    { Host : string
      Port : int
      Username : string
      Password : string
    }
