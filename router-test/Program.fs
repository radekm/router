
open System
open System.Threading.Channels
open System.Threading.Tasks

// TODO: Fill config.
let config : Router.Config =
    { Host = "10.10.10.10"
      Port = 8000
      Username = "u"
      Password = "p" }

let numberOfMessages = 4000

// Channel for messages coming to consumer.
let consumerChannel = Channel.CreateUnbounded<Router.IncomingMsg>()

let runConsumer () = backgroundTask {
    use! cl =
        Router.connectAndStartReading
            config.Host config.Port
            // Exception in `onMsg` results in calling `onStop`.
            (fun msg -> consumerChannel.Writer.TryWrite(msg) |> ignore)
            (failwithf "Consumer disconnected from router: %A")
    do! Router.write cl (Router.Login (config.Username, config.Password, "test-consumer"))
    do! Router.write cl (Router.SubscribeToService "test-producer")

    let mutable producerInstanceId : Router.ClientInstanceId = { Id = 0UL }
    let mutable i = 0

    try
        while true do
            let! incomingMsg = consumerChannel.Reader.ReadAsync()
            match incomingMsg with
            | Router.ConnectedToService(_, instanceId, "test-producer") ->
                producerInstanceId <- instanceId
                Console.WriteLine("Consumer connected to producer")
            | Router.DisconnectedFromService(_, _, "test-producer") ->
                failwith "Disconnected from producer"
            | Router.IncomingMsg.MsgToAllClients (_time, instanceId, data) when instanceId = producerInstanceId ->
                if data.Length <> i + 1 then
                    failwith "Received wrong len"
                data
                |> Array.iteri (fun j b ->
                    if b <> byte (j % 53) then
                        failwithf "Invalid byte in message %d at index %d, size %d" i j data.Length)
                if i % 100 = 0 then
                    Console.WriteLine("Received {0}", i)
                i <- i + 1
            | Router.Ping time ->
                do! Router.write cl (Router.Pong time)
            | _ -> failwithf "Unexpected message from router: %A" incomingMsg

            if i = numberOfMessages then
                Console.WriteLine("Received correct number of messages")
            elif i > numberOfMessages then
                failwith "Received too many messages"

    with e -> Console.WriteLine("Error when consuming: {0}", e)
}

// Channel for messages coming to producer.
let producerChannel = Channel.CreateUnbounded<Router.IncomingMsg>()

let runProducer () = backgroundTask {
    use! cl =
        Router.connectAndStartReading
            config.Host config.Port
            // Exception in `onMsg` results in calling `onStop`.
            (fun msg -> producerChannel.Writer.TryWrite(msg) |> ignore)
            (failwithf "Producer disconnected from router: %A")
    do! Router.write cl (Router.Login (config.Username, config.Password, "test-producer"))

    try
        while true do
            let! incomingMsg = producerChannel.Reader.ReadAsync()
            match incomingMsg with
            | Router.AddClientToService (_time, client) ->
                do! Router.write cl (Router.ClientAddedToService client)
                Console.WriteLine("Producer starts sending messages")
                for i = 0 to numberOfMessages - 1 do
                    // Message body must not be empty, so we add 1.
                    let data = Array.zeroCreate (i + 1)
                    data |> Array.iteri (fun j _ -> data[j] <- byte (j % 53))
                    do! Router.write cl (Router.MsgToAllClients data)
                    if i % 100 = 0 then
                        Console.WriteLine("Sent {0}", i)
                Console.WriteLine("Producer sent all messages")
            | Router.RemoveClientFromService(_time, client) ->
                do! Router.write cl (Router.ClientRemovedFromService client)
                failwith "Client from producer removed"
            | Router.Ping time ->
                do! Router.write cl (Router.Pong time)
            | _ -> failwithf "Unexpected message from router: %A" incomingMsg
    with e -> Console.WriteLine("Error when producing: {0}", e)
}

[<EntryPoint>]
let main args =
    let consumerTask = runConsumer ()
    let producerTask = runProducer ()
    Task.WaitAny(consumerTask, producerTask) |> printfn "Task which ended first: %d"
    Console.WriteLine("Consumer exception: {0}", consumerTask.Exception)
    Console.WriteLine("Producer exception: {0}", producerTask.Exception)
    0
