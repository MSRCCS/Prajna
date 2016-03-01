namespace Prajna.Nano

open System
open System.Collections.Generic
open System.Threading
open System.Collections.Concurrent

open Prajna.Tools.FSharp
open Prajna.Tools.Network

type ConcreteNetwork() = 
    inherit Network()

/// Multiplexes multiple queues so nobody has to call GetConsumingEnumerable, which blocks threads
/// We use one thread for each 60 queues, which is a limitation of BlockingCollection.TryTakeFromAny
type QueueMultiplexer<'T> private () =

    [<Literal>]
    static let MaxSocketsPerThread = 60
    static let mutable allInstances = new List<QueueMultiplexer<'T>>()
    
    let mutable readQueues : BlockingCollection<'T>[] = Array.zeroCreate 0
    let mutable callbacks : List<'T -> unit> = new List<_>()
    let mutable shutdowns : List<unit -> unit> = new List<_>()

    member private this.StartNewThreadLoop() =
        let start = new ThreadStart(fun _ ->
            let mutable stop = false
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "New multiplexer thread running")
            while not stop do
                try
                    let mutable buffer = Unchecked.defaultof<'T>
                    let queueIndex = BlockingCollection<'T>.TryTakeFromAny(readQueues, &buffer, 200)
                    if queueIndex <> -1 then
                        callbacks.[queueIndex] buffer
                with
                    | :? ArgumentException ->
                        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Multiplexer thread stopping")
                        stop <- true
            shutdowns |> Seq.iter (fun f -> f()))
        let thread = new Thread(start)
        thread.IsBackground <- true
        thread.Start()

    member private this.Count = readQueues.Length
    
    member private this.AddQueue(bufferQueue: BlockingCollection<'T>, callback: 'T -> unit, shutdown: Option<unit -> unit>) =
        let newQueues : BlockingCollection<'T>[] = 
            Array.init (readQueues.Length + 1) (fun i -> if i < readQueues.Length then readQueues.[i] else bufferQueue)
        callbacks.Add callback
        shutdowns.Add <| defaultArg shutdown (fun() -> ())
        readQueues <- newQueues
        if readQueues.Length = 1 then
            this.StartNewThreadLoop()

    member private this.Shutdown() = 
        for q in readQueues do
            q.CompleteAdding()
        readQueues <- Array.zeroCreate 0
        callbacks <- new List<_>()

    static member AddQueue(bufferQueue: BlockingCollection<'T>, callback: 'T -> unit, ?shutdown: unit -> unit) =
        lock allInstances (fun _ ->
            let sharedInstance =
                if allInstances.Count = 0 || allInstances.[allInstances.Count - 1].Count >= MaxSocketsPerThread then
                    let newInstance = new QueueMultiplexer<'T>()
                    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Created new QueueMultiplexer")
                    allInstances.Add newInstance
                    newInstance
                else
                    allInstances.[allInstances.Count - 1]
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "Adding new queue to multiplexer")
            sharedInstance.AddQueue(bufferQueue, callback, shutdown)
        ) 

    static member Shutdown() =
        lock allInstances (fun _ ->
            for sc in allInstances do
                sc.Shutdown()
            allInstances <- new List<QueueMultiplexer<'T>>()
        )


