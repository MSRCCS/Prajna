(*---------------------------------------------------------------------------
    Copyright 2015 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.                                                      

    File: 
        distributedfunction.fs
  
    Description: 
        DistributedFunctions are functions being exported to/imported from a Prajna service. 
    Prajna service exports DistributedFunctions as either Action<_>, Func<_,_> or FuncTask<_,_>. 
    They can be imported by other service, a regular Prajna program, or 
    a data anlytical jobs. There is overhead in setting up distributed function, but once setup, the 
    contract can be consumed efficiently, as a function call. 

    Message Used:
        Register, Contract:     Register a contract at daemon (no feedback on whether registration succeeds)
        Ready, Contract:        Registration status. 
        Error, Contract:        Certain service fails. 
        Get, Contract:          Lookfor a contract with a specific name 
        Set, Contract:          Returned a list of contracts with name, contract type, input and output type
        Close, Contract:        There is no valid contract for the name
        Request, Contract:      Initiate a call to a contract
        FailedRequest, Contract:    Error in servicing the request   
        Reply, Contract         Return a call to a contract
        FailedReply, Contract:      Error in deliverying the reply
    Author:																	
        Jin Li, Partner Researcher Manager
        Microsoft 
        Email: jinl at microsoft dot com
    Date:
        Oct. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service
open System
open System.IO
open System.Net
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open System.Runtime.Serialization.Json
open Prajna.Tools
open Prajna.Tools.Network
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Core

// The class DistributedFunctionProvider is public to allow the programmer to provider information

/// Infromation of distributed Function Provider 
/// To use distributed function, 
type DistributedFunctionProvider() = 
    /// Provider version information, in a.b.c.d. Each of a, b, c, d should be from 0..255, and a should be from 0 to 255 the provider version is represented as an unsigned integer. 
    /// Version should be comparable 
    member val Version = 0u with get, set
    /// Use a string of "a.b.c.d" to express the versin information
    member x.VersionString with get() = let arr = System.BitConverter.GetBytes(x.Version) |> Array.rev
                                        let addr = arr |> Array.map ( fun b -> b.ToString() ) |> String.concat "."
                                        addr.ToString()
                            and set( str: string ) = let arr = str.Split([|'.'|]) |> Array.map( System.Byte.Parse ) |> Array.rev
                                                     x.Version <- System.BitConverter.ToUInt32( arr, 0 )
    /// Name of the provider 
    member val Name = "" with get, set
    /// Name of the provider 
    member val Institution = "" with get, set
    /// Name of the provider 
    member val Email = "" with get, set
    /// Public ID of the provider, the consumer will use this to reference the provider to be used. 

    member val PublicID = Guid.Empty with get, set
    /// Private ID of the provider, the service provider should use this ID to register for service 
    member val PrivateID = Guid.Empty with get, set 

/// For built in Prajna functions. 
type DistributedFunctionBuiltInProvider() = 
    inherit DistributedFunctionProvider( VersionString = "0.0.0.1", 
                                         Name = "Prajna.Service", 
                                         Institution = "MSRCCS", 
                                         Email = "Prajna@github.com", 
                                         PublicID = Guid("{66241EF4-9B6C-4511-9D3B-4712856D69D8}"), 
                                         PrivateID = Guid("{0CCBDDEC-A6A6-423C-9261-622D325DFC69}") ) 

/// DistributedFunctionPerformance provides a performance statistics instance for the underlying operation. 
/// Whenever a request is queued, a timestamp is registered in the class. We will wait for the 
/// operation to complete to unqueue the request, and register a finished timestamp of the request. 
/// From both, we can calculate the timespan that the request complete, and compute execution statistics. 
type internal DistributedFunctionPerformance(info: unit -> string) = 
    member val internal OperationTimeCollection = ConcurrentDictionary<_,_>()
    member val internal OperationTimeArray = Array.zeroCreate<_> NetworkPerformance.RTTSamples with get
    member val internal nOperation = ref 0L with get
    member val internal nValidOperation = ref -1L with get
    /// Number of operation queued
    member x.NumOperations with get() = !x.nOperation
    /// Number of valid operation completed
    member x.NumValidOperations with get() = !x.nValidOperation
    /// This function when the timed operation starts
    member x.RegisterStart( reqId: Guid ) = 
        let ticks = (PerfADateTime.UtcNowTicks())
        Interlocked.Increment( x.nOperation ) |> ignore
        x.OperationTimeCollection.GetOrAdd( reqId, ticks ) 
    /// This function is called when the timed operation ends. bSuccess=true, operation completes. bSuccess=false, operation fails (timeout), and is not timed. 
    member x.RegisterEnd( reqId, bSuccess ) = 
        let bRemove, ticks = x.OperationTimeCollection.TryRemove( reqId ) 
        if bRemove then 
            if bSuccess then 
                let idx = int (Interlocked.Increment( x.nValidOperation ))
                let ticksCur = (PerfADateTime.UtcNowTicks())
                let elapse = float ( ticksCur - ticks ) / float TimeSpan.TicksPerMillisecond // convert to millisecond
                x.OperationTimeArray.[ idx % NetworkPerformance.RTTSamples ] <- elapse
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "req %A completes successfully in %f ms" reqId elapse))
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  let ticksCur = (PerfADateTime.UtcNowTicks())
                                                               let elapse = float ( ticksCur - ticks ) / float TimeSpan.TicksPerMillisecond 
                                                               sprintf "req %A timedout in %f ms" reqId elapse))
        else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ ->  sprintf "fail to locate req %A ... " reqId))
    /// average operation time in milliseconds. 
    member x.GetAvgOperationTime( ) = 
        let sum = Array.sum x.OperationTimeArray
        let num = Math.Min( (!x.nValidOperation)+1L, int64 NetworkPerformance.RTTSamples )
        if num <= 0L then 
            0.
        else
            sum / float num

/// SingleRequestPerformance gives out the performance of a single query. 
[<AllowNullLiteral>]
type SingleRequestPerformance() = 
    /// Ticks processing start
    member val private TickStart = DateTime.UtcNow.Ticks with get, set
    /// Time spent in assignment stage, before the request is queued to network 
    member val InAssignment = 0 with get, set
    /// Time spent in network (including network stack)
    member val InNetwork = 0 with get, set
    /// Time spent in queue of the query engine 
    member val InQueue = 0 with get, set
    /// Time spent in processing 
    member val InProcessing = 0 with get, set
    /// Number of Pending request in queue
    member val Capacity = 0 with get, set
    /// Number of Slots Available
    member val NumSlotsAvailable = 0 with get, set
    /// Additional Message
    member val Message : string = null with get, set
    /// Milliseconds since this structure is created. 
    member x.Elapse with get() = int (( DateTime.UtcNow.Ticks - x.TickStart ) / TimeSpan.TicksPerMillisecond)
    /// Serialize 
    static member Pack( x:SingleRequestPerformance, ms:Stream ) = 
        let inQueue = if x.InQueue < 0 then 0 else if x.InQueue > 65535 then 65535 else x.InQueue
        let inProc = if x.InProcessing < 0 then 0 else if x.InProcessing > 65535 then 65535 else x.InProcessing
        ms.WriteUInt16( uint16 inQueue )
        ms.WriteUInt16( uint16 inProc )
        ms.WriteVInt32( x.Capacity )
        ms.WriteVInt32( x.NumSlotsAvailable )
    /// Deserialize 
    static member Unpack( ms:Stream ) = 
        let inQueue = int (ms.ReadUInt16())
        let inProcessing = int (ms.ReadUInt16())
        let capacity = ms.ReadVInt32()
        let numSlots = ms.ReadVInt32()
        SingleRequestPerformance( InQueue = inQueue, InProcessing = inProcessing, 
                                    Capacity = capacity, NumSlotsAvailable=numSlots )
    /// Show string that can be used to monitor backend performance 
    abstract BackEndInfo: unit -> string
    override x.BackEndInfo() = 
        sprintf "queue: %dms, proc: %dms, slot: %d/%d" x.InQueue x.InProcessing x.NumSlotsAvailable x.Capacity
    /// Show string that can be used to monitor frontend performance 
    abstract FrontEndInfo: unit -> string
    override x.FrontEndInfo() = 
        sprintf "assign: %dms, network %dms, queue: %dms, proc: %dms, slot: %d/%d" x.InAssignment x.InNetwork x.InQueue x.InProcessing x.NumSlotsAvailable x.Capacity
    member x.CalculateNetworkPerformance( perfLocal: SingleRequestPerformance, ticksSent ) = 
        x.TickStart <- ticksSent
        x.InAssignment <- perfLocal.InAssignment
        let elapse = x.Elapse
        let inNetwork = elapse - x.InProcessing - x.InQueue
        if inNetwork < 0 then 
            Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Calculated in network is %d ms (negative): elapse = %d ms, assignment = %d ms, queue = %d ms, process = %d ms"
                                                                inNetwork elapse x.InAssignment x.InQueue x.InProcessing ) 
        x.InNetwork <- Math.Max( 0, inNetwork )
        ()

/// Helper class to generate schema ID
type internal SchemaJSonHelper<'T>() = 
    /// Get a GUID that representing the coding of a type. Note that this binds to the 
    /// fullname of the type with assembly (AssemblyQualifiedName) so if the assembly 
    /// of the type changes, the schemaID will change. 
    static member SchemaID() = 
        let schemaStr = "System.Runtime.Serialization.Json.DataContractJsonSerializer:" + typeof<'T>.AssemblyQualifiedName
        let hash = HashStringToGuid(schemaStr)
        hash, schemaStr
    static member Encoder(o:'T, ms:Stream) = 
        let fmt = DataContractJsonSerializer( typeof<'T> )
        use intermediaStream = new MemoryStream()
        fmt.WriteObject( intermediaStream, o )
        let buf = intermediaStream.GetBuffer()
        let len = intermediaStream.Length
        /// Wrap JSon stream 
        ms.WriteVInt32( int len )
        ms.WriteBytesWithOffset( buf, 0, int len)
    static member Decoder(ms:Stream) = 
        let fmt = DataContractJsonSerializer( typeof<'T> )
        /// Unwrap JSon stream 
        let len = ms.ReadVInt32()
        let buf = ms.ReadBytes( len )
        use intermediaStream = new MemoryStream( buf )
        let str = System.Text.UTF8Encoding.UTF8.GetString( buf )
        let o = fmt.ReadObject( intermediaStream ) 
        if Utils.IsNotNull o then o :?> 'T else Unchecked.defaultof<_>

/// Helper class to generate schema ID
type internal SchemaBinaryFormatterHelper<'T>() = 
    /// Get a GUID that representing the coding of a type. Note that this binds to the 
    /// fullname of the type with assembly (AssemblyQualifiedName) so if the assembly 
    /// of the type changes, the schemaID will change. 
    static member SchemaID() = 
        let schemaStr = "System.Runtime.Serialization.Formatters.Binary.BinaryFormatter:" + typeof<'T>.AssemblyQualifiedName
        let hash = HashStringToGuid(schemaStr)
        hash, schemaStr
    static member Encoder(o:'T, ms:Stream) = 
        let fmt = Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        fmt.Serialize( ms, o )
    static member Decoder(ms:Stream) = 
        let fmt = Runtime.Serialization.Formatters.Binary.BinaryFormatter()
        let o = fmt.Deserialize( ms ) 
        if Utils.IsNotNull o then o :?> 'T else Unchecked.defaultof<_>

/// Helper class to generate schema ID
type internal SchemaPrajnaFormatterHelper<'T>() =
    /// Get a GUID that representing the coding of a type. Note that this binds to the
    /// fullname of the type with assembly (AssemblyQualifiedName) so if the assembly
    /// of the type changes, the schemaID will change.
    static member SchemaID() =
        let schemaStr = "Prajna.Tools.BinarySerializer:" + typeof<'T>.AssemblyQualifiedName
        let hash = HashStringToGuid(schemaStr)
        hash, schemaStr
    static member Encoder(o:'T, ms:Stream) =
        let fmt = Prajna.Tools.BinarySerializer() :> IFormatter
        fmt.Serialize( ms, o )
    static member Decoder(ms:Stream) =
        let fmt = Prajna.Tools.BinarySerializer() :> IFormatter
        let o = fmt.Deserialize( ms )
        if Utils.IsNotNull o then o :?> 'T else Unchecked.defaultof<_>

/// Govern the behavior of the default serialization to be used 
type DefaultSerializerForDistributedFunction =
    /// Default Serializer implementation in Prajna
    | PrajnaSerializer = 0
    /// System.Runtime.Serialization.Formatters.Binary.BinaryFormatter
    | BinarySerializer = 1
    /// System.Runtime.Serialization.Json.DataContractJsonSerializer
    | JSonSerializer = 2
    /// Customized
    | Customized = 3

/// A registered distributed function
/// A disposing interface, when called

/// Identity of a distributed function
type private DistributedFunctionID( providerID: Guid, domainID: Guid, schemaIn: Guid, schemaOut: Guid ) = 
    /// Guid of the provider
    member val ProviderID = providerID with get
    /// Guid of the domain 
    member val DomainID = domainID with get
    /// Guid of the input schema
    member val SchemaIn = schemaIn with get
    /// Guid of the output schema
    member val SchemaOut = schemaOut with get




/// Distributed function holder 
/// Govern the execution cycle of a distributed function. 
[<Serializable>]
type internal DistributedFunctionHolderByLock(name:string, capacity:int, executor: (Guid * int * Object * CancellationToken * IObserver<Object> -> unit) ) = 
    let capacityControl = ( capacity > 0 )
    let currentItems = ref 0 
    let totalItems = ref 0L 
    let cts = new CancellationTokenSource()
    let lock = if not capacityControl then null else new ManualResetEvent(true)
    let refCnt = ref 0 
    member x.AddRef() = Interlocked.Increment( refCnt ) 
    member x.DecRef() = Interlocked.Decrement( refCnt ) 
    /// Execute a distributed function with a certain time budget. 
    /// The execution is an observer object. 
    member x.ExecuteWithTimebudget( jobID: Guid, timeBudget: int, input: Object, token: CancellationToken, observer:IObserver<Object>) = 
        let noToken = ( token = Unchecked.defaultof<CancellationToken>)
        let isCancelled() = 
            cts.IsCancellationRequested ||
                if noToken then false else token.IsCancellationRequested
        let mutable bCancelled = isCancelled() 
        if bCancelled then 
            // Operation has already been cancelled. 
            observer.OnCompleted()
        else
            let useTimeBudget = if timeBudget <=0 then Int32.MaxValue else timeBudget 
            let mutable elapse = 0 
            if capacityControl then 
                let launchTicks = DateTime.UtcNow.Ticks 
                let cnt = Interlocked.Increment( currentItems )
                let mutable bCanExecute = ( cnt <= capacity )
                let waitArr = 
                    if not capacityControl then 
                        if noToken then 
                            [| cts.Token.WaitHandle |]
                        else
                            [| cts.Token.WaitHandle; token.WaitHandle |]
                    else
                        if noToken then 
                            [| cts.Token.WaitHandle; lock |]
                        else
                            [| cts.Token.WaitHandle; token.WaitHandle; lock |]
                while not bCancelled && not bCanExecute && elapse < useTimeBudget do
                    bCancelled <- isCancelled()
                    if not bCancelled then 
                        lock.Reset() |> ignore 
                        let cnt = Interlocked.Decrement( currentItems )
                        if cnt >= capacity then 
                            // Only go into wait if cnt >= capacity, at least one job is still executing, 
                            // (the decrement come before lock.Set() below), 
                            // Should have no deadlock. 
                            let nRet = ThreadPoolWaitHandles.safeWaitAny( waitArr, useTimeBudget-elapse )
                            elapse <- if nRet = WaitHandle.WaitTimeout then 
                                            // Get out 
                                            useTimeBudget 
                                        else 
                                            int (( DateTime.UtcNow.Ticks - launchTicks ) / TimeSpan.TicksPerMillisecond) 
                        // Does this thread win the lock? 
                        let cnt = Interlocked.Increment( currentItems )
                        bCanExecute <- ( cnt <= capacity )
            if ( elapse >= useTimeBudget ) then 
                let ex = TimeoutException( sprintf "Function %s, Time budget %d ms has been exhausted" name timeBudget )
                observer.OnError(ex)
            else
                try 
                    if bCancelled then 
                        observer.OnCompleted()
                    else
                        executor( jobID, useTimeBudget - elapse, input, token, observer )
                with 
                | ex -> 
                    observer.OnError( ex )
            if capacityControl then 
                Interlocked.Decrement( currentItems ) |> ignore 
                lock.Set() |> ignore
    override x.ToString() = sprintf "Distributed function %s:%d %d/%d" name (!totalItems) (!currentItems) (capacity)
    /// Information on how many threads can enter semaphore, and the total capacity of the semaphore 
    member x.GetCount() = 
        if capacityControl then 
            capacity - !refCnt, capacity 
        else
            0, Int32.MaxValue

    /// Cancel all jobs related to this distributed function. 
    member x.Cancel() = 
        // This process is responsible for the disposing routine
        cts.Cancel()
    /// Can we dispose this job holder?
    member x.CanDispose() = 
        if capacityControl then 
            Volatile.Read( currentItems ) <= 0 
        else 
            true 
    member x.CleanUp() = 
        if capacityControl then 
            // Dispose CancellationTokenSource and lock
            if Utils.IsNotNull lock then 
                lock.Dispose()
            cts.Dispose() 
        else
            // When not in capacity control, there is no lock. 
            // However, we may not be able to dispose CancellationTokenSource as there may still be job executing, and need to 
            // check the state of the cancellation Token. We will let the System garbage collection CancellationTokenSource
            cts.Cancel()
    override x.Finalize() =
        /// Close All Active Connection, to be called when the program gets shutdown.
        x.CleanUp()
    interface IDisposable with
        /// Close All Active Connection, to be called when the program gets shutdown.
        member x.Dispose() = 
                x.CleanUp()
                GC.SuppressFinalize(x)
#if false
            try 
                x.CleanUp()
                GC.SuppressFinalize(x)
            with 
            | :? ObjectDisposedException as ex -> 
                // https://msdn.microsoft.com/en-us/library/system.idisposable.dispose.aspx
                // If an object's Dispose method is called more than once, the object must ignore all calls after the first one. The object must not throw an exception if its Dispose method is called multiple times.                
                ()
            | ex -> 
                reraise()
#endif

/// Distributed function holder 
/// Govern the execution cycle of a distributed function. 
[<Serializable>]
type internal DistributedFunctionHolderBySemaphore(name:string, capacity:int, executor: (Guid * int * Object * CancellationToken * IObserver<Object> -> unit) ) = 
    let capacityControl = ( capacity > 0 )
    let semaphore = if capacityControl then new SemaphoreSlim( capacity, capacity) else null
    let cts = new CancellationTokenSource()
    let ctsStatus = ref 0   // 0: CTS active, 1: cancelled, 2: disposed. 
    let refCnt = ref 0 
    member x.AddRef() = Interlocked.Increment( refCnt ) 
    member x.DecRef() = Interlocked.Decrement( refCnt ) 
    /// Execute a distributed function with a certain time budget. 
    /// The execution is an observer object. 
    member x.ExecuteWithTimebudgetAndPerf( jobID: Guid, timeBudget: int, input: Object, perf: SingleRequestPerformance, token: CancellationToken, observer:IObserver<Object>) = 
        let noToken = ( token = Unchecked.defaultof<CancellationToken>)
        let isCancelled() = 
            cts.IsCancellationRequested ||
                if noToken then false else token.IsCancellationRequested
        let mutable bCancelled = isCancelled() 
        if bCancelled then 
            // Operation has already been cancelled. 
            observer.OnCompleted()
        else
            let useTimeBudget = if timeBudget <=0 then Int32.MaxValue else timeBudget 
            let launchTicks = DateTime.UtcNow.Ticks 
            let chainedCTS = 
                if noToken then 
                    cts
                else
                    CancellationTokenSource.CreateLinkedTokenSource( token, cts.Token )
            let chainedToken = chainedCTS.Token
            if capacityControl then 
                let semaTask = semaphore.WaitAsync(useTimeBudget, chainedToken )
                let wrapperExecute (taskEnter:Task<bool>) = 
                    let elapse = int (( DateTime.UtcNow.Ticks - launchTicks ) / TimeSpan.TicksPerMillisecond )
                    if Utils.IsNotNull perf then  
                        perf.InQueue <- elapse 
                    if taskEnter.Result then 
                        // Entered
                        try 
                            if useTimeBudget > elapse then 
                                if not chainedCTS.Token.IsCancellationRequested then 
                                    executor( jobID, useTimeBudget - elapse, input, chainedToken, observer )
                                else
                                    // Operation cancelled. 
                                    observer.OnCompleted()
                            else
                                let ex = TimeoutException( sprintf "ExecuteWithTimebudget: Function %s, time budget %d ms has been exhausted (%d ms)" name timeBudget elapse)
                                observer.OnError(ex)
                            if not noToken then 
                                chainedCTS.Dispose()
                            semaphore.Release() |> ignore
                        with 
                        | ex -> 
                            Logger.LogF( LogLevel.Warning, fun _ -> sprintf "ExecuteWithTimebudget, Functioon %s, exception in main loop of %A" name ex )
                            observer.OnError( ex )
                            if not noToken then 
                                chainedCTS.Dispose()
                            semaphore.Release() |> ignore
                    else
                        try 
                            if useTimeBudget <= elapse then 
                                let ex = OperationCanceledException(sprintf "ExecuteWithTimebudget: Function %s, semaphore task is entered as cancelled" name)
                                observer.OnError(ex)
                            else
                                let ex = TimeoutException( sprintf "ExecuteWithTimebudget: Function %s, semaphore task is entered in %d ms(budget %d ms)" name elapse timeBudget )
                                observer.OnError(ex)
                            if not noToken then 
                                chainedCTS.Dispose()
                            semaphore.Release() |> ignore 
                        with
                        | ex -> 
                            Logger.LogF( LogLevel.Warning, fun _ -> sprintf "ExecuteWithTimebudget, Functioon %s, exception while not enter, of %A" name ex )
                            observer.OnError( ex )
                            if not noToken then 
                                chainedCTS.Dispose()
                let cancelledExecute (taskEnter:Task<bool>) = 
                    try 
                        if not noToken then 
                            chainedCTS.Dispose()
                        semaphore.Release() |> ignore 
                    with
                    | ex -> 
                        Logger.LogF( LogLevel.Warning, fun _ -> sprintf "ExecuteWithTimebudget, Functioon %s, exception when dispose of %A" name ex )
                semaTask.ContinueWith( wrapperExecute, TaskContinuationOptions.None ) |> ignore 
                semaTask.ContinueWith( cancelledExecute, TaskContinuationOptions.OnlyOnCanceled ) |> ignore
            else
                try 
                    executor( jobID, useTimeBudget, input, chainedCTS.Token, observer )
                    if not noToken then 
                        chainedCTS.Dispose()
                with 
                | ex -> 
                    if not noToken then 
                        chainedCTS.Dispose()
                    observer.OnError( ex )
    /// Execute a distributed function with a certain time budget. 
    /// The execution is an observer object. 
    member x.ExecuteWithTimebudget( jobID: Guid, timeBudget: int, input: Object, token: CancellationToken, observer:IObserver<Object>) = 
        x.ExecuteWithTimebudgetAndPerf( jobID, timeBudget, input, null, token, observer )
    override x.ToString() = sprintf "Distributed function %s:%s/%d" name (if Utils.IsNull semaphore then "Unknown" else (capacity-semaphore.CurrentCount).ToString() ) (capacity)
    /// Information on how many threads can enter semaphore, and the total capacity of the semaphore 
    /// Return 0, 0 if no capacity control is enforced. 
    member x.GetCount() = 
        if capacityControl then 
            if Volatile.Read( ctsStatus ) = 0 then
                semaphore.CurrentCount, capacity 
            else
                0, capacity
        else
            0, 0
    /// Cancel all jobs related to this distributed function. 
    /// ctsStatus will be 1 or higher after Cancel is called. 
    member x.Cancel() = 
        // This process is responsible for the disposing routine
        Logger.LogF( LogLevel.ExtremeVerbose, fun _ -> sprintf "Function holder %s cancelled" name)
        if Interlocked.CompareExchange( ctsStatus, 1, 0 ) = 0 then 
            // The only case that we will transition cts to cancel is that: 
            // 1) It hasn't been cancelled before, and 
            // 2) The current thread succeed in the Exchange, and execute the cancel operation 
            Logger.LogF ( LogLevel.MildVerbose, fun _ -> sprintf "%A is cancelled" x)
            cts.Cancel() 
    /// Can we dispose this job holder?
    member x.CanDispose() = 
        // Wait for all job in semaphore to exit
        if capacityControl then 
            semaphore.CurrentCount = capacity 
        else 
            true 
    member x.CleanUp() = 
        Logger.LogF( LogLevel.ExtremeVerbose, fun _ -> sprintf "Function holder %s cleaned. " name)
        x.Cancel()
        if capacityControl && x.CanDispose() then 
            // Dispose CancellationTokenSource and lock
            if Interlocked.CompareExchange( ctsStatus, 2, 1 ) = 1 then 
                // The only case that we will transition cts to dispose is that: 
                // 1) It hasn't been disposed before, and 
                // 2) The current thread succeed in the Exchange, and execute the dispose operation 
                Logger.LogF ( LogLevel.MildVerbose, fun _ -> sprintf "%A is disposed" x)
                cts.Dispose() 
                semaphore.Dispose() 
        else
            // When not in capacity control, there is no lock. 
            // However, we may not be able to dispose CancellationTokenSource as there may still be job executing, and need to 
            // check the state of the cancellation Token. We will let the System garbage collection CancellationTokenSource
            ()
    override x.Finalize() =
        /// Close All Active Connection, to be called when the program gets shutdown.
        x.CleanUp()
    interface IDisposable with
        /// Close All Active Connection, to be called when the program gets shutdown.
        member x.Dispose() = 
            x.CleanUp()
            GC.SuppressFinalize(x)

type internal DistributedFunctionHolder = DistributedFunctionHolderBySemaphore

       
/// <summary> 
/// Service endpoint performance tracking, if the programer intend to track more statistics, additional
/// information may be included. 
/// </summary>
type ServiceEndpointPerformance (slot) = 
    inherit NetworkPerformance() 
    let mutable totalRequest = 0L
    let mutable completedReq = 0L
    let mutable failedReq = 0L
    let mutable outstandingRequest = 0
    let mutable avgNetworkRtt=300
    let mutable avgQueuePerSlot = 100
    let mutable avgProcessing = 1000 
    let mutable slotsOnBackend = 0
    static member val DefaultNumberOfPerformanceSlot = 32 with get, set
    member val internal QueryPerformanceCollection = Array.zeroCreate<_> slot with get, set
    /// Maximum number of request that can be served.
    member val internal MaxSlots = 0 with get, set
    /// Current number of request that is under service 
    member val internal Curslots = 0 with get, set
    /// Number of completed queries 
    member x.NumCompletedQuery with get() = (completedReq)
    /// Number of failed queries
    member x.NumFailedQuery with get() = failedReq
    /// Total number of queries issued 
    member x.TotalQuery with get() = totalRequest
    /// Number of queries to be serviced. 
    member x.OutstandingRequests with get() = outstandingRequest
    /// Default Expected Latency value before a reply is received 
    static member val DefaultExpectedLatencyInMS = 100 with get, set
    /// <summary>
    /// If we send request to this Backend, the expected processing latency (in millisecond)
    /// It is calculated by avgNetworkRtt + avgProcessing + ( avgQueuePerSlot * itemsInQueue )
    /// </summary>
    member val ExpectedLatencyInMS=ServiceEndpointPerformance.DefaultExpectedLatencyInMS with get, set

    /// <summary> 
    /// This function is called before each request is sent to backend for statistics 
    /// </summary>
    member internal x.RegisterRequest() = 
        Interlocked.Increment( &totalRequest ) |> ignore
        Interlocked.Increment( &outstandingRequest ) |> ignore
        x.ExpectedLatencyInMS <- avgNetworkRtt + avgProcessing * ( slotsOnBackend + Math.Max( outstandingRequest, 0) )
    /// This function is called when a request/reply arrives 
    member private x.ReplyArrived() = 
        let mutable cnt = Interlocked.Decrement( &outstandingRequest )
        while cnt < 0 do
            cnt <- Interlocked.Increment( &outstandingRequest )    
    /// This function is called when a request fails. 
    member internal x.FailedRequest( ) = 
        x.ReplyArrived()
        Interlocked.Increment( &failedReq ) |> ignore 
    /// Show the expected latency of the backend in string 
    member x.ExpectedLatencyInfo() = 
        sprintf "exp %dms=%d+%d*(%d+%d)"
                    x.ExpectedLatencyInMS
                    avgNetworkRtt avgProcessing 
                    slotsOnBackend outstandingRequest
    /// Show the backend queue status in string 
    member x.QueueInfo() = 
        sprintf "total: %d completed:%d, outstanding: %d, failed:%d" totalRequest completedReq outstandingRequest (failedReq)
    /// <summary> 
    /// This function is called whenever a reply is received. For error/timeout, put a non-empty message in perfQ.Message, and call this function. 
    /// If perQ.Message is not null, the execution fails. 
    /// </summary> 
    member internal x.DepositReply( perfQ:SingleRequestPerformance ) = 
        x.ReplyArrived()
        if Utils.IsNull perfQ.Message then 
            let items = Interlocked.Increment( &completedReq ) 
            /// ToDo: The following logic needs scrutization. 
            let idx = int (( items - 1L ) % int64 x.QueryPerformanceCollection.Length)
            x.QueryPerformanceCollection.[idx] <- perfQ
            // Calculate statistics of performance
            let mutable sumNetwork = 0
            let mutable sumQueue = 0 
            let mutable sumProcessing = 0 
            let slotsAvailable = perfQ.NumSlotsAvailable
            let mutable sumQueueSlots = 0
            let maxItems = int (Math.Min( items, x.QueryPerformanceCollection.LongLength ) )
            for i = 0 to maxItems - 1 do 
                let pQ = x.QueryPerformanceCollection.[i]
                sumNetwork <- sumNetwork + pQ.InNetwork
                sumQueue <- sumQueue + pQ.InQueue
                sumProcessing <- sumProcessing + pQ.InProcessing
                sumQueueSlots <- sumQueueSlots + ( pQ.Capacity - pQ.NumSlotsAvailable )
            avgNetworkRtt <- sumNetwork / maxItems
            avgProcessing <- sumProcessing / maxItems
            avgQueuePerSlot <- sumQueue / Math.Max( sumQueueSlots, 1) // Queue is usually proportional to the # of items in queue. 
            x.MaxSlots <- perfQ.Capacity
            x.Curslots <- Math.Min( 0, perfQ.NumSlotsAvailable - outstandingRequest ) 
            x.ExpectedLatencyInMS <- avgNetworkRtt + avgProcessing + avgQueuePerSlot * ( x.Curslots + Math.Min( outstandingRequest, 0) )
        else
            Interlocked.Increment( &failedReq ) |> ignore 
    /// A functional variable used to construct ServiceEndpointPerformance
    static member val ConstructFunc = fun _ -> ( new ServiceEndpointPerformance( ServiceEndpointPerformance.DefaultNumberOfPerformanceSlot ) ) with get, set


/// This class store the signature of remote endpoints that are providing service. 
type internal RemoteDistributedFunctionProviderSignatureStore() = 
    static let importedCollection = ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,ConcurrentDictionary<int64,ServiceEndpointPerformance>>>>>()
    /// Collection of imported distributed functions, indexed by providerID, domainID, schemaIn and schemaOut
    static member ImportedCollections with get() = importedCollection
    /// Find an array of network signatures that are associated with distributed function with 
    /// signature publicID, domainID, schemaIn, schemaOut. 
    static member TryDiscoverConnectedClients( publicID, domainID, schemaIn, schemaOut ) = 
        let retList = List<_>()
        let searchIDs = 
            if publicID = Guid.Empty then 
                // To array to release lock on ExportedCollections
                importedCollection.Keys :> seq<Guid>
            else
                let bExist, providerStore = importedCollection.TryGetValue( publicID )
                if not bExist then 
                    Seq.empty
                else
                    Seq.singleton publicID 
        for searchID in searchIDs do
            let bExist, providerStore = importedCollection.TryGetValue( searchID ) 
            if bExist then 
                let bExist, domainStore = providerStore.TryGetValue( domainID )
                if bExist then 
                    // We may search for alternate schemas here. 
                    let useSchemaIn = CustomizedSerialization.AlternateDeserializerID( schemaIn )
                    let bExist, schemaInStore = domainStore.TryGetValue( useSchemaIn )
                    if bExist then 
                        let useSchemaOut = CustomizedSerialization.AlternateSerializerID( schemaOut )
                        let bExist, signatureStores = schemaInStore.TryGetValue( useSchemaOut )
                        if bExist then 
                            for pair in signatureStores do 
                                let signature = pair.Key
                                let perf = pair.Value
                                retList.Add( signature, searchID, domainID, schemaIn, schemaOut, perf  )
        retList.ToArray()
    /// Register a remote function service by a four tuple
    static member Register( providerID, domainID, schemaIn, schemaOut, signature ) = 
        let providerCollection = importedCollection.GetOrAdd( providerID, fun _ -> ConcurrentDictionary<_,_>())
        let domainCollection = providerCollection.GetOrAdd( domainID, fun _ -> ConcurrentDictionary<_,_>() )
        let schemaInCollection = domainCollection.GetOrAdd( schemaIn, fun _ -> ConcurrentDictionary<_,_>() )
        let schemaOutCollection = schemaInCollection.GetOrAdd( schemaOut, fun _ -> ConcurrentDictionary<_,_>())
        schemaOutCollection.GetOrAdd( signature, fun _ -> ServiceEndpointPerformance.ConstructFunc()  ) |> ignore
        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register Distributed Function for provider %A, domain %A, schema %A, %A (from %s), endpoints = %d"
                                                                providerID domainID schemaIn schemaOut (LocalDNS.GetHostInfoInt64(signature))
                                                                schemaOutCollection.Count
                                                    )
    /// A certain provider has an unrecoverable error, we will need to unregister all distributed function by that provider. 
    static member ErrorAt( signature, ex: Exception ) = 
        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "RemoteDistributedFunctionProviderSignatureStore.Error, message from client at %s cause exception: %A"
                                                                (LocalDNS.GetHostInfoInt64(signature)) ex
                                                    )
    /// A certain provider has an unrecoverable error, we will need to unregister all distributed function by that provider. 
    static member ReceiveErrorAt( signature, ex: Exception ) = 
        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Receiving Error, DistributedFunction message from client at %s, exception: %A"
                                                                (LocalDNS.GetHostInfoInt64(signature)) ex
                                                    )

    /// A certain provider has disconnected, we will need to unregister all distributed function serviced by that provider.
    static member Disconnect( signature ) = 
        let clientName = (LocalDNS.GetHostInfoInt64(signature))
        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "RemoteDistributedFunctionProviderSignatureStore.Disconnect, client from %s is disconnected "
                                                                (LocalDNS.GetHostInfoInt64(signature))
                                                    )
        for pair0 in importedCollection do
            let providerCollection = pair0.Value
            for pair1 in providerCollection do 
                let domainCollection = pair1.Value
                for pair2 in domainCollection do 
                    let schemaInCollection = pair2.Value
                    for pair3 in schemaInCollection do 
                        let schemaOutCollection = pair3.Value
                        let bRemove, _ = schemaOutCollection.TryRemove( signature )
                        if bRemove then 
                            Logger.LogF( LogLevel.MildVerbose, fun _ -> let providerID = pair0.Key
                                                                        let domainID = pair1.Key
                                                                        let schemaIn = pair2.Key
                                                                        let schemaOut = pair3.Key
                                                                        sprintf "From %s remove distributed function from provider %A, domain %A, schema %A, %A "
                                                                                  clientName  providerID domainID schemaIn schemaOut
                                                                        )
                            
        






/// <summary> 
/// A distribution Policy delegate select 1 to N backend to service a particular request. 
/// Return:
///     1 to N remote signature that is used to serve the current request. 
/// </summary>
type internal DistributionPolicyFunction = Func<OneRemoteDistributedFunctionRequest, int64[]>

/// How distributed function deal with failover. 
and internal FailoverExecution = 
    | Resend of int64[]     // Resend to a number of peers 
    | CancelAll             // Cancel all existing requests 

/// <summary> 
/// A failover policy delegate select 1 to N backend to re-service a particular request, whenever one request fails 
/// Return:
///     1 to N remote signature that is used to serve the current request. 
/// </summary>
and internal FailoverPolicyFunction = Func<OneRemoteDistributedFunctionRequest*int64*Exception, FailoverExecution>

/// <summary> 
/// An aggregation policy delegate determines the action to perform when it receives the reply (OnNext) from a particular service endpoints.  
/// Return:
///     true: the life cycle of the RemoteDistributedFunction has been completed, and the function holder may terminates. 
///     false: the life cycle fo the RemoteDistributedFunction can still continues. 
/// </summary>
and internal AggregationPolicyFunction = Func<OneRemoteDistributedFunctionRequest*int64*Object, bool>

/// <summary> 
/// An completion policy delegate determines the action to perform when it receives the completion (OnComplete) from a particular service endpoints.  
/// Return:
///     true: the life cycle of the RemoteDistributedFunction has been completed, and the function holder may terminates. 
///     false: the life cycle fo the RemoteDistributedFunction can still continues. 
/// </summary>
and internal CompletionPolicyFunction = Func<OneRemoteDistributedFunctionRequest*int64, bool>


/// This class encapsulate a single distributed function request. 
and internal OneRemoteDistributedFunctionRequest( stub: DistributedFunctionClientStub, 
                                                    serverInfoLocal: ContractServerInfoLocal, 
                                                    name: string, 
                                                    jobID: Guid, 
                                                    timeoutInMilliseconds: int, 
                                                    replyAction:IObserver<Object>,
                                                    distributionPolicyID: Guid, 
                                                    aggregationPolicyID: Guid, 
                                                    failoverPolicyID: Guid, 
                                                    completionPolicyID: Guid ) as thisInstance = 
    /// Cancellation Token source 
    let ctsSource = if timeoutInMilliseconds > 0 then new CancellationTokenSource(timeoutInMilliseconds) else new CancellationTokenSource()
    let cts = ctsSource.Token
    let nCancelled = ref 0 
    let rgs = cts.Register( Action( thisInstance.CallbackForCancellation) )
    let mutable exInExecution : Exception = null 
    let perfLocal = SingleRequestPerformance( )
    member internal x.PerfLocal with get() = perfLocal
    member val ConnectedClients = ConcurrentDictionary<int64,int64*Guid*Guid*Guid*Guid*ServiceEndpointPerformance>() with get, set
    /// JobID 
    member val JobID = jobID with get
    /// ID of the service provider to be performed 
    member val ProviderID = Guid.Empty with get, set
    /// ID that represents the coding of the input 
    member val InputSchemaCollection: Guid[] = null with get, set
    /// ID that represents the coding of the input 
    member val OutputSchemaCollection: Guid[] = null with get, set
    /// ID of the service to be performed 
    member val DomainID = Guid.Empty with get, set
    /// Guid governs which service endpoint(s) are selected to service the request. 
    member val DistributionPolicy = distributionPolicyID with get
    /// Guid governs how to aggregate multiple requests  
    member val AggregationPolicy = aggregationPolicyID with get, set
    /// Guid governs the behavior of failover 
    member val FailoverPolicy = failoverPolicyID with get, set
    /// Request object (input)
    member val ReqObjects = ConcurrentQueue<_>() with get
    /// We will register an outstanding request if SentSignatureCollections contains the network signature 
    /// Objects are sent to these locations. 
    member val SentSignatureCollections = ConcurrentDictionary<int64,int64>() with get
    /// Objects are sent to these locations. 
    member val CompletedSignatureCollections = ConcurrentDictionary<int64,bool>() with get
    /// Timestamp that the request is received. 
    member val TicksRequestReceived = DateTime.UtcNow.Ticks 
        with get
    /// Timestamp that the request will timeout 
    member val TicksRequestTimeout = 
        if timeoutInMilliseconds <= 0 then DateTime.MaxValue.Ticks else DateTime.UtcNow.Ticks + int64 timeoutInMilliseconds * TimeSpan.TicksPerMillisecond 
        with get, set
    /// Cancellation Token
    member x.Token with get() = cts
    /// Cancel after a certain time interval
    member x.CancelAfter( timeInMilliseconds: int ) =
        if timeInMilliseconds <= 0 then 
            x.TicksRequestTimeout <- DateTime.MaxValue.Ticks 
        else
            ctsSource.CancelAfter( timeInMilliseconds )
            x.TicksRequestTimeout <- DateTime.UtcNow.Ticks + int64 timeInMilliseconds * TimeSpan.TicksPerMillisecond 
    /// Distribution Function 
    member val DistributionFuncLazy = lazy( DistributionPolicyCollection.FindDistributionPolicy(distributionPolicyID) ) with get
    /// Failover policy function
    member val FailoverFuncLazy = lazy( FailoverPolicyCollection.FindFailoverPolicy( failoverPolicyID ) ) with get
    /// Aggregation Function 
    member val AggregationFuncLazy = lazy( AggregationPolicyCollection.FindAggregationPolicy( aggregationPolicyID) ) with get
    /// Completion Function 
    member val CompletionFuncLazy = lazy( CompletionPolicyCollection.FindCompletionPolicy( completionPolicyID) ) with get
    /// Execued when the function starts
    /// For failing to start (return false), the caller will remove and dispose the resource associated with the 
    /// OneRemoteDistributedFunctionRequest
    member x.BaseFunctionOnStart() = 
        if Utils.IsNotNull x.InputSchemaCollection && Utils.IsNotNull x.OutputSchemaCollection then 
            let dic = stub.GetServiceEndPoints()
            x.ConnectedClients <- dic
            if x.ConnectedClients.IsEmpty then 
                // Function can not be executed as there are no service endpoints. 
                let msg = sprintf "Can't find any service endpoint for distributed function call of %s, job %A" name jobID
                let ex = System.Runtime.Remoting.RemotingException( msg )
                x.Cancel( ex )
                false
            else
                true
        else
            let msg = sprintf "For distributed function call of %s, job %A, schemaIn %A or schemaOut %A is empty " name jobID x.InputSchemaCollection x.OutputSchemaCollection
            let ex = System.Runtime.Remoting.RemotingException( msg )
            x.Cancel( ex )
            false       
    /// Send to a certain remote service endpoints
    member x.SendTo( signatures: int64[] ) = 
        try 
            // Get a snapshot of the objects to be sent out 
            let objarr = x.ReqObjects |> Seq.toArray 
            if objarr.Length > 0 then 
                if signatures.Length > 0 then 
                    /// The function is not executed if there is no object to be sent out.
                    /// For Action or Function without parameter, please use null object as input 
                    for signature in signatures do 
                        let nNew = ref 0 
                        let ticksSent = x.SentSignatureCollections.GetOrAdd( signature, fun _ -> nNew := 1
                                                                                                 DateTime.UtcNow.Ticks )
                        if !nNew = 1 then 
                            /// Timing this request. 
                            // New signature 
                            let queue = NetworkConnections.Current.LookforConnectBySignature( signature )
                            let health = queue.Performance
                            let bExist, tuple = x.ConnectedClients.TryGetValue( signature )
                            let _, providerID, domainID, schemaIn, schemaOut, perf = tuple
                            if bExist then 
                                perf.RegisterRequest()
                            if bExist && Utils.IsNotNull queue && queue.CanSend then
                                use ms = new MemStream()
                                health.WriteHeader(ms)
                                ms.WriteGuid( x.JobID )
                                ms.WriteGuid( providerID )
                                ms.WriteGuid( domainID )
                                ms.WriteGuid( schemaIn )
                                ms.WriteGuid( schemaOut )
                                ms.WriteVInt32( objarr.Length )
                                for i = 0 to objarr.Length - 1 do 
                                    let obj = objarr.[i]
                                    ms.SerializeObjectWithSchema( obj, schemaIn )
                                let ticksCur = DateTime.UtcNow.Ticks
                                let elapse = int ( ( ticksCur - x.TicksRequestReceived ) / TimeSpan.TicksPerMillisecond )
                                let bTimeout, timeBudgetInMilliseconds = 
                                    if timeoutInMilliseconds <= 0 then 
                                        false, 0 
                                    else 
                                        if elapse >= timeoutInMilliseconds then 
                                            true, 0 
                                        else
                                            false, timeoutInMilliseconds - elapse
                                ms.WriteInt32( timeBudgetInMilliseconds )
                                health.WriteEndMark(ms)
                                if not bTimeout then 
                                    Logger.LogF( jobID, RemoteDistributedFunctionRequestStore.TraceDistributedFunctionLevel, 
                                                                fun _ -> sprintf "Send Request, DistributedFunction (%dB) to %s"
                                                                                        ms.Length
                                                                                        (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                )
                                    /// Timing this request
                                    queue.ToSend( ControllerCommand( ControllerVerb.Request, ControllerNoun.DistributedFunction ), ms )
                                else
                                    let msg = sprintf "Distributed function of %s, job %A times out after %d ms." name jobID elapse
                                    let ex = System.TimeoutException( msg )
                                    x.Cancel( ex )
                            else
                                x.Failed( signature )
                else
                    let msg = sprintf "For distributed function call of %s, job %A, there is no valid service endpoints to send request" name jobID
                    let ex = System.Runtime.Remoting.RemotingException( msg )
                    x.Cancel( ex )
        with 
        | ex -> 
            ex.Data.Add( "@FirstLocation", "___ OneRemoteDistributedFunctionRequest.SendTo __")
            x.Cancel( ex )
    /// Common processing of a failed endpoint
    member x.FailedCommon( failedSignature:int64 ) = 
        let bConnected, tuple = x.ConnectedClients.TryRemove( failedSignature )
        let bSent, ticksSent = x.SentSignatureCollections.TryRemove( failedSignature )
        if bSent && bConnected then 
            /// If we have register a request (in SentSignatureCollections)
            let _, providerID, domainID, schemaIn, schemaOut, perf = tuple
            perf.FailedRequest()
        bConnected, bSent
    /// A certain queue has failed 
    /// Need to trigger failover policy 
    member x.Failed(failedSignature:int64 ) = 
        // The reason to duplicate code from x.Failed( failedSignature, ex ) is so that 
        // if the request is not associated with the service endpoint that fails, very little operation is executed. 
        let bConnected, bSent = x.FailedCommon( failedSignature ) 
        if bSent then 
            let msg = sprintf "Socket %s fails to send a request" (LocalDNS.GetHostInfoInt64(failedSignature))
            let ex = System.Net.Sockets.SocketException( )
            ex.Data.Add( "FirstLocation", msg )
            x.ExecuteFailoverPollicy( failedSignature, ex )
    /// A certain service endpoint has failed with an exception. 
    member x.Failed(failedSignature:int64, ex: Exception ) = 
        let bConnected, bSent = x.FailedCommon( failedSignature ) 
        if bSent then 
            // We have find a certain service endpoint has failed. 
            x.ExecuteFailoverPollicy( failedSignature, ex )    
    /// Get reply schema
    member x.GetReplySchema( signature:int64 ) = 
        let bExist, tuple = x.ConnectedClients.TryGetValue( signature )
        if bExist then 
            let _, _, _, _, schemaOut, _ = tuple 
            Some schemaOut
        else
            None
    /// On receiving a reply 
    member x.OnRemoteReply( signature:int64, o: Object, perfNetwork: SingleRequestPerformance ) = 
        let bConnected, tuple = x.ConnectedClients.TryGetValue( signature )
        let bSent, ticksSent = x.SentSignatureCollections.TryGetValue( signature )
        if bConnected && bSent then 
            /// When we have both, we can tried to time the performance 
            let _, providerID, domainID, schemaIn, schemaOut, perfService = tuple
            perfNetwork.CalculateNetworkPerformance( x.PerfLocal, ticksSent )
            perfService.DepositReply( perfNetwork )
        let func : AggregationPolicyFunction = x.AggregationFuncLazy.Value
        let bDispose = func.Invoke( x, signature, o )
        if bDispose then 
            x.CallbackOnCompletion()
    /// On receiving a completion from a certain peer
    member x.OnRemoteCompletion( signature:int64 ) = 
        // This needs to happen before the signature is removed from SentSignatureCollections
        x.CompletedSignatureCollections.GetOrAdd( signature, true ) |> ignore 
        let bRemove, _ = x.SentSignatureCollections.TryRemove( signature )
        if bRemove then 
            let func : CompletionPolicyFunction = x.CompletionFuncLazy.Value
            let bDispose = func.Invoke( x, signature )
            if bDispose then 
                x.CallbackOnCompletion()
        else
            Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "OnRemoteCompletion by %s, but can't find signature in SentSignatureCollections "
                                                                (LocalDNS.GetHostInfoInt64( signature ))
                        )
    /// Are there any requests to peers still outstanding?
    member x.SentSignatureIsEmpty() = 
        x.SentSignatureCollections.IsEmpty
    /// Passback reply 
    member x.ReplyActionOnNext( o: Object ) = 
        replyAction.OnNext( o )
    /// Passback OnComplete
    member x.ReplyActionOnCompleted( ) = 
        replyAction.OnCompleted( )
    /// Process failover logic 
    member x.ExecuteFailoverPollicy( failedSignature, ex: Exception ) = 
        let func : FailoverPolicyFunction = x.FailoverFuncLazy.Value
        let ret = func.Invoke( x, failedSignature, ex )
        match ret with 
        | Resend signatures -> 
            x.SendTo( signatures )
        | CancelAll -> 
            x.Cancel( ex )
    /// Send cancellation to live service endpoint
    member x.SendCancellationToServiceEndpoints( ) = 
        let arr = x.SentSignatureCollections |> Seq.map( fun pair -> pair.Key ) |> Seq.toArray
        for signature in arr do 
            x.SendCancellationToServiceEndpoint( signature )
    member x.SendCancellationToServiceEndpoint( signature ) = 
        let bRemove, _ = x.SentSignatureCollections.TryRemove( signature )
        if bRemove then 
            let queue = NetworkConnections.Current.LookforConnectBySignature( signature )
            if Utils.IsNotNull queue && queue.CanSend then 
                let health = queue.Performance
                use ms = new MemStream()
                health.WriteHeader(ms)
                ms.WriteGuid( x.JobID )
                health.WriteEndMark(ms)
                queue.ToSend( ControllerCommand( ControllerVerb.Cancel, ControllerNoun.DistributedFunction ), ms )
    /// Get Distribution function signatures
    member x.BaseGetDistributionPolicySignatures() = 
        let func : DistributionPolicyFunction = x.DistributionFuncLazy.Value
        let signatures = func.Invoke( x )
        signatures 
    /// Execute Distribution function
    member x.BaseExecuteDistributionPolicy() = 
        let signatures = x.BaseGetDistributionPolicySignatures() 
        
        x.SendTo( signatures )
    /// Executed when the function receives a new object 
    member x.BaseFunctionOnNext( o: Object ) = 
        x.ReqObjects.Enqueue(o)
        x.BaseExecuteDistributionPolicy()
    /// Execued by App when the function starts
    /// For failing to start (return false), the caller will remove and dispose the resource associated with the 
    /// OneRemoteDistributedFunctionRequest
    abstract FunctionOnStart: unit -> bool
    default x.FunctionOnStart() = 
        x.BaseFunctionOnStart() 
    /// Execued by App when the function encounter an error 
    /// Note: external function should inform that the function is failing via OnError( ex ) or Cancel( ex ), 
    /// which will call FunctionOnError, but only once. 
    abstract FunctionOnError: Exception -> unit
    default x.FunctionOnError( ex : Exception ) = 
        ()
    /// Execued by App when the function receives a new object 
    abstract FunctionOnNext: Object -> unit 
    default x.FunctionOnNext( o: Object ) = 
        x.BaseFunctionOnNext( o)
    /// Execued by App when the function does not have additional input variable
    abstract FunctionOnCompleted: unit -> unit 
    default x.FunctionOnCompleted() = 
        ()
    /// Cancel actions 
    member x.Cancel( ex:Exception ) = 
        exInExecution <- ex
        ctsSource.Cancel() 
        x.CallbackForCancellation() 
    /// External Cancellation
    member x.CancelledByExternalToken() = 
        let ticks = DateTime.UtcNow.Ticks 
        let msg = sprintf "Operation cancelled by external token after %d ms" (( ticks - x.TicksRequestReceived ) / TimeSpan.TicksPerMillisecond)
        exInExecution <- System.OperationCanceledException( msg )
        x.CallbackForCancellation()
    /// On Cancellation
    member x.CallbackForCancellation() = 
        if !nCancelled = 0 then 
            // The semantics here is to deliver the first encoutered exception, 
            // future exception will be supressed
            // We could use this lockfree because: 1) the end condition transitions one way, and 2) we can return and wait for the end processing to be 
            // executed (by other thread if so happens)
            if Interlocked.Increment( nCancelled ) = 1 then 
                let ex = 
                    if Utils.IsNull exInExecution then 
                        let ticks = DateTime.UtcNow.Ticks
                        if ticks > x.TicksRequestTimeout then 
                            let msg = sprintf "Operation cancelled for timeout after %d ms" (( ticks - x.TicksRequestReceived ) / TimeSpan.TicksPerMillisecond)
                            Logger.Log( jobID, LogLevel.MediumVerbose, msg)
                            ( new System.TimeoutException( msg ) ) :> System.Exception
                        else
                            let msg = sprintf "For unknown reason that the operation is cancelled after %d ms" (( ticks - x.TicksRequestReceived ) / TimeSpan.TicksPerMillisecond)
                            Logger.Log( jobID, LogLevel.MediumVerbose, msg)
                            new System.Exception( msg )
                    else
                        exInExecution
                x.SendCancellationToServiceEndpoints() 
                replyAction.OnError( ex )
                x.FunctionOnError( ex )
                /// resource will be disposed 
                RemoteDistributedFunctionRequestStore.Current.RemoveRequest(jobID) 
    /// On Completion. 
    member x.CallbackOnCompletion() = 
        if !nCancelled = 0 then 
            // The semantics here is to deliver the first encoutered exception, 
            // future exception will be supressed
            // We could use this lockfree because: 1) the end condition transitions one way, and 2) we can return and wait for the end processing to be 
            // executed (by other thread if so happens)
            if Interlocked.Increment( nCancelled ) = 1 then 
                x.ReplyActionOnCompleted( )
                RemoteDistributedFunctionRequestStore.Current.RemoveRequest(jobID) 
    interface IObserver<Object> with 
        member this.OnCompleted() = 
            this.FunctionOnCompleted() 
        member this.OnError( ex ) = 
            this.Cancel( ex )
        member this.OnNext( o ) = 
            // Don't expect return value 
            this.FunctionOnNext( o )
    /// Internal implementation of Dispose
    member x.Dispose(disposing: bool ) = 
        if disposing then 
            ctsSource.Dispose() 
    override x.Finalize() =
        x.Dispose(false)
    interface IDisposable with
        member x.Dispose() = 
            x.Dispose(true)
            GC.SuppressFinalize(x)

/// This class store all distributed function request that is pending of execution
and internal RemoteDistributedFunctionRequestStore private () = 
    static member val TraceDistributedFunctionLevel = LogLevel.Info with get, set
    static member val Current : RemoteDistributedFunctionRequestStore = RemoteDistributedFunctionRequestStore() with get
    member val CollectionOfRemoteDistributedFunctionRequest = ConcurrentDictionary<_,_>() with get 
    member x.AddRequest( jobID: Guid, request: OneRemoteDistributedFunctionRequest, rgsOption: CancellationTokenRegistration option) = 
        x.CollectionOfRemoteDistributedFunctionRequest.GetOrAdd( jobID, (request,rgsOption) ) |> ignore 
    member x.RemoveRequest( jobID : Guid ) = 
        let bRemove, tuple = x.CollectionOfRemoteDistributedFunctionRequest.TryRemove( jobID )
        if bRemove then 
            /// Proper disposing of all IDisposable resources. 
            let oneRequest, rgsOption = tuple 
            match rgsOption with 
            | Some rgs -> 
                rgs.Dispose() 
            | _ -> 
                ()
            Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "RemoteDistributedFunctionRequestStore.RemoveRequest, successful remove job")
            ( oneRequest :> IDisposable ).Dispose()
        else
            Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "RemoteDistributedFunctionRequestStore.RemoveRequest, can't find the request in store, already removed? ")
    member x.TryGetRequestSchema( jobID: Guid, signature:int64 ) = 
        let bExist, tuple = x.CollectionOfRemoteDistributedFunctionRequest.TryGetValue( jobID )       
        if bExist then 
            let oneRequest, _ = tuple 
            oneRequest.GetReplySchema( signature )
        else
            None
    /// On exception (when a exception has been received) . 
    member x.RequestException( jobID: Guid, signature:int64, ex: Exception ) = 
        Logger.LogF( jobID, LogLevel.Info, fun _ -> sprintf "Exception encountered: %A" ex )
        let bExist, tuple = x.CollectionOfRemoteDistributedFunctionRequest.TryGetValue( jobID )
        if bExist then 
            let oneRequest, _ = tuple 
            oneRequest.Failed( signature, ex )
    /// OnNext (when a reply has been received ) 
    member x.OnNext( jobID, signature, o: Object, perfNetwork: SingleRequestPerformance ) = 
        let bExist, tuple = x.CollectionOfRemoteDistributedFunctionRequest.TryGetValue( jobID )
        if bExist then 
            let oneRequest, _ = tuple 
            /// To perform the statistics
            Logger.LogF( jobID, RemoteDistributedFunctionRequestStore.TraceDistributedFunctionLevel, fun _ -> sprintf "RemoteDistributedFunctionRequestStore.OnNext, process remote reply ")
            oneRequest.OnRemoteReply( signature, o, perfNetwork )
        else
            Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "RemoteDistributedFunctionRequestStore.OnNext, can't find the request in store, already removed? ")
    /// OnNext (when a reply has been received ) 
    member x.OnCompletion( jobID, signature ) = 
        let bExist, tuple = x.CollectionOfRemoteDistributedFunctionRequest.TryGetValue( jobID )
        if bExist then 
            let oneRequest, _ = tuple 
            Logger.LogF( jobID, RemoteDistributedFunctionRequestStore.TraceDistributedFunctionLevel, fun _ -> sprintf "RemoteDistributedFunctionRequestStore.OnCompletion, process remote completion ")
            oneRequest.OnRemoteCompletion( signature )
        else
            Logger.LogF( jobID, RemoteDistributedFunctionRequestStore.TraceDistributedFunctionLevel, fun _ -> sprintf "RemoteDistributedFunctionRequestStore.OnCompletion, can't find the request in store, already removed? ")



    /// Called when a client disconnects.
    member x.OnDisconnect( signature ) = 
        for pair in x.CollectionOfRemoteDistributedFunctionRequest do 
            let request, _ = pair.Value
            request.Failed( signature )


/// Collection of distribution policy for distributed function
and internal DistributionPolicyCollection private () = 
    /// <summary> 
    /// Collection of distribution Policy
    /// </summary> 
    static let distributionPolicyCollection = ConcurrentDictionary<Guid,string * DistributionPolicyFunction>() 
    static member RegisterDefault() = 
        DistributionPolicyCollection.Register( DistributionPolicyCollection.DistributionRandomGuid, 
                                                "RoundRobinOne", 
                                                Func<_,_>(DistributionPolicyCollection.DistributionRandom)
                                             )
        DistributionPolicyCollection.Register( DistributionPolicyCollection.DistributionToFastestGuid, 
                                                "FastestOne", 
                                                Func<_,_>(DistributionPolicyCollection.DistributionToFastest)
                                             )
        DistributionPolicyCollection.Register( DistributionPolicyCollection.DistributionRoundRobinGuid, 
                                                "RoundRobinOne", 
                                                Func<_,_>(DistributionPolicyCollection.DistributionRoundRobin)
                                             )
        DistributionPolicyCollection.Register( DistributionPolicyCollection.DistributionAllGuid, 
                                                "ToAll", 
                                                Func<_,_>(DistributionPolicyCollection.DistributionAll)
                                             )
    /// Guid of policy to select the backend with the lowest expected latency to serve the request 
    static member val DistributionToFastestGuid = Guid( "5308962E-91F0-439C-AB72-6B2180837433" ) with get
    /// Guid of policy to roundrobinly select a service endpoint
    static member val DistributionRoundRobinGuid = Guid ("A5000012-4F64-4E9C-B07C-4D984B1D260D" ) with get
    /// Guid of policy to select a service endpoint in a roundrobin way
    static member val DistributionRandomGuid = Guid ("5AB6F3F8-BE33-4342-962C-0F442008FF20" )with get
    /// Guid of policy to select all service endpoint
    static member val DistributionAllGuid = Guid ("02033161-7A82-417A-B7CB-53F9F61B776D" )with get

    /// Guid of the default distribution policy 
    static member val DistributionPolicyDefaultGuid = DistributionPolicyCollection.DistributionRandomGuid with get, set
    /// Guid of policy to randomly select a service endpoint
    /// Register additional distribution policy 
    static member private Register( policyGuid:Guid, name, del:DistributionPolicyFunction ) = 
        distributionPolicyCollection.Item( policyGuid ) <- ( name, del )
    /// Return a service endpoint with the lowest ExpectedLatencyInMS
    static member private DistributionToFastest( req: OneRemoteDistributedFunctionRequest ) = 
        let dic = req.ConnectedClients
        if Utils.IsNull dic then 
            Array.empty
        else
            let mutable queueSignature = 0L
            let mutable score = Int32.MaxValue          
            for pair in dic do 
                let signature = pair.Key
                let _, _, _, _, _, perfQ = pair.Value
                if perfQ.ExpectedLatencyInMS < score then 
                    score <- perfQ.ExpectedLatencyInMS
                    queueSignature <- signature
            if queueSignature <>0L then
                [| queueSignature |]
            else
                Array.empty
    /// Return a service endpoint using TotalQuery to determine which service endpoint will service the requests. 
    /// Note that if multiple requests are issued at almost the same time, it is possible that a service endpoint
    /// will receive multiple request 
    static member private DistributionRoundRobin( req: OneRemoteDistributedFunctionRequest ) = 
        let dic = req.ConnectedClients
        if Utils.IsNull dic then 
            Array.empty
        else
            let mutable score = Int64.MaxValue  
            for pair in dic do 
                let signature = pair.Key
                let _, _, _, _, _, perfQ = pair.Value
                let curScore = perfQ.TotalQuery
                if curScore < score then 
                    score <- curScore
            let lst = List<_>()
            for pair in dic do 
                let signature = pair.Key
                let _, _, _, _, _, perfQ = pair.Value
                let curScore = perfQ.TotalQuery
                if curScore = score then 
                    lst.Add( signature )
            if lst.Count = 0 then 
                /// The only reason that lst.Count = 0 is the entry with the lowest TotalQuery
                /// has been removed for failure, in such a case, we will rerun the function to find a valid service endpoint
                DistributionPolicyCollection.DistributionRoundRobin( req )
            else
                // Use a random generator seeded by jobID, so that different job will get different 
                // service endpoint assignment. 
                let rnd = System.Random( req.JobID.GetHashCode() )
                let idx = rnd.Next( lst.Count )
                [| lst.[idx] |]
    /// Return a random service endpoint 
    static member private DistributionAll( req: OneRemoteDistributedFunctionRequest ) = 
        let dic = req.ConnectedClients
        if Utils.IsNull dic then 
            Array.empty
        else
            /// The logic below should not send a request to a peer twice, 
            /// It is possible though that requests are directed to peer which is in the process of failing. 
            let arr = dic |> Seq.map( fun pair -> pair.Key ) 
                          |> Seq.filter( fun signature -> (not (req.SentSignatureCollections.ContainsKey(signature)) ) && 
                                                            (not (req.CompletedSignatureCollections.ContainsKey(signature))) 
                                                            )
                          |> Seq.toArray
            arr
    /// Return a random service endpoint 
    static member private DistributionRandom( req: OneRemoteDistributedFunctionRequest ) = 
        let dic = req.ConnectedClients
        if Utils.IsNull dic then 
            Array.empty
        else
            let arr = dic |> Seq.map( fun pair -> pair.Key ) |> Seq.toArray
            if arr.Length = 0 then 
                Array.empty 
            else 
                // Use a random generator seeded by jobID, so that different job will get different 
                // service endpoint assignment. 
                let rnd = System.Random( req.JobID.GetHashCode() )
                let idx = rnd.Next( arr.Length )
                [| arr.[idx] |]

    /// Find the distribution policy, if the request distribution policy doesn't exist, the policy DistributionToFastestBackend will be applied. 
    static member internal FindDistributionPolicy( distributionPolicyID ) = 
        let bExist, tuple = distributionPolicyCollection.TryGetValue( distributionPolicyID ) 
        if bExist then 
            let _, del = tuple
            del
        else
            let bExist, tuple = distributionPolicyCollection.TryGetValue( DistributionPolicyCollection.DistributionPolicyDefaultGuid ) 
            if bExist then 
                let _, del = tuple
                del
            else
                DistributionPolicyFunction( DistributionPolicyCollection.DistributionRandom )

/// Collection of distribution policy for distributed function
and internal FailoverPolicyCollection private () = 
    /// <summary> 
    /// Collection of distribution Policy
    /// </summary> 
    static let failoverPolicyCollection = ConcurrentDictionary<Guid,string * FailoverPolicyFunction>() 
    static member RegisterDefault() = 
        FailoverPolicyCollection.Register( FailoverPolicyCollection.FailoverRerunDistributionGuid, 
                                                "Rerun distribution", 
                                                Func<_,_>(FailoverPolicyCollection.FailoverRerunDistribution )
                                             )
        FailoverPolicyCollection.Register( FailoverPolicyCollection.FailoverByCancelGuid, 
                                                "Rerun distribution", 
                                                Func<_,_>(FailoverPolicyCollection.FailoverByCancel )
                                             )

    /// Guid of policy to re-run Distribution policy when any service endpoint fails 
    static member val FailoverRerunDistributionGuid = Guid( "B7D381EE-CC4F-4368-8628-238F7CC16838" ) with get
    /// Guid of policy to cancel all distributed function when any service endpoint fails with exception
    static member val FailoverByCancelGuid = Guid( "B7D381EE-CC4F-4368-8628-238F7CC16838" ) with get


    /// Guid of the default distribution policy 
    static member val FailoverPolicyDefaultGuid = FailoverPolicyCollection.FailoverByCancelGuid with get, set
    /// Guid of policy to randomly select a service endpoint
    /// Register additional distribution policy 
    static member private Register( policyGuid:Guid, name, del:FailoverPolicyFunction ) = 
        failoverPolicyCollection.Item( policyGuid ) <- ( name, del )
    /// Return a service endpoint with the lowest ExpectedLatencyInMS
    static member private FailoverRerunDistribution( req: OneRemoteDistributedFunctionRequest, signature, ex:Exception  ) = 
        // Re-execute distribution policy
        // The failed queue has already been removed from the connection
        Resend ( req.BaseGetDistributionPolicySignatures() )
    /// Return a service endpoint with the lowest ExpectedLatencyInMS
    static member private FailoverByCancel( req: OneRemoteDistributedFunctionRequest, signature, ex:Exception  ) = 
        // Re-execute distribution policy
        // The failed queue has already been removed from the connection
        match ex with 
        | :? System.Net.Sockets.SocketException as exSocket -> 
            Resend ( req.BaseGetDistributionPolicySignatures() )
        | _ -> 
            CancelAll

    /// Find the distribution policy, if the request distribution policy doesn't exist, the policy DistributionToFastestBackend will be applied. 
    static member internal FindFailoverPolicy( failoverPolicyID ) = 
        let bExist, tuple = failoverPolicyCollection.TryGetValue( failoverPolicyID ) 
        if bExist then 
            let _, del = tuple
            del
        else
            let bExist, tuple = failoverPolicyCollection.TryGetValue( FailoverPolicyCollection.FailoverPolicyDefaultGuid ) 
            if bExist then 
                let _, del = tuple
                del
            else
                FailoverPolicyFunction( FailoverPolicyCollection.FailoverByCancel )

/// Collection of aggregation policy for distributed function
and internal AggregationPolicyCollection private () = 
    /// <summary> 
    /// Collection of distribution Policy
    /// </summary> 
    static let aggregationPolicyCollection = ConcurrentDictionary<Guid,string * AggregationPolicyFunction>() 
    static member RegisterDefault() = 
        AggregationPolicyCollection.Register( AggregationPolicyCollection.AggregationByFirstReplyGuid, 
                                                "Aggregation by first reply", 
                                                Func<_,_>(AggregationPolicyCollection.AggregationByFirstReply )
                                             )
        AggregationPolicyCollection.Register( AggregationPolicyCollection.AggregationByAllReplyGuid, 
                                                "Aggregation by all replies", 
                                                Func<_,_>(AggregationPolicyCollection.AggregationByAllReply )
                                             )

    /// Guid of policy to return the first object of the reply
    static member val AggregationByFirstReplyGuid = Guid( "442C9DB9-6AC3-4132-9FB6-956F5BED8FCC" ) with get
    /// Guid of policy to return all object after completion is signalled
    static member val AggregationByAllReplyGuid = Guid( "A82C04E8-F187-4272-9817-98CFC4C4431E" ) with get
    /// Guid of the default distribution policy 
    static member val AggregationPolicyDefaultGuid = AggregationPolicyCollection.AggregationByFirstReplyGuid with get, set
    /// Guid of policy to randomly select a service endpoint
    /// Register additional distribution policy 
    static member private Register( policyGuid:Guid, name, del:AggregationPolicyFunction ) = 
        aggregationPolicyCollection.Item( policyGuid ) <- ( name, del )
    /// Return the first object of the reply, and then signals that the operation is completed
    static member private AggregationByFirstReply( req: OneRemoteDistributedFunctionRequest, signature, o:Object) = 
        req.ReplyActionOnNext( o )
        true
    /// Return the an object of the reply, but continuous execution
    static member private AggregationByAllReply( req: OneRemoteDistributedFunctionRequest, signature, o:Object) = 
        req.ReplyActionOnNext( o ) 
        false
    /// Find aggregation policy, if the request policy doesn't exist, the policy AggregationByFirstReplyGuid will be applied. 
    static member internal FindAggregationPolicy( aggregationPolicyID ) = 
        let bExist, tuple = aggregationPolicyCollection.TryGetValue( aggregationPolicyID ) 
        if bExist then 
            let _, del = tuple
            del
        else
            let bExist, tuple = aggregationPolicyCollection.TryGetValue( AggregationPolicyCollection.AggregationPolicyDefaultGuid ) 
            if bExist then 
                let _, del = tuple
                del
            else
                AggregationPolicyFunction( AggregationPolicyCollection.AggregationByFirstReply )

/// Collection of completion policy for distributed function
and internal CompletionPolicyCollection private () = 
    /// <summary> 
    /// Collection of distribution Policy
    /// </summary> 
    static let completionPolicyCollection = ConcurrentDictionary<Guid,string * CompletionPolicyFunction>() 
    static member RegisterDefault() = 
        CompletionPolicyCollection.Register( CompletionPolicyCollection.CompletionByFirstPeerGuid, 
                                                "Completion by first peer", 
                                                Func<_,_>(CompletionPolicyCollection.CompletionByFirstPeer )
                                             )
        CompletionPolicyCollection.Register( CompletionPolicyCollection.CompletionByAllPeersGuid, 
                                                "Aggregation by all replies", 
                                                Func<_,_>(CompletionPolicyCollection.CompletionByAllPeers )
                                             )

    /// Guid of completion policy which is done after first peer completes
    static member val CompletionByFirstPeerGuid = Guid( "7B0AE6C7-1029-4C65-A496-C5DB7A0B4B26" ) with get
    /// Guid of completion policy which is done after all peer complete
    static member val CompletionByAllPeersGuid   = Guid( "1710A165-A0E2-4991-B93A-8A4CCE855AB2" ) with get
    /// Guid of the default distribution policy 
    static member val CompletionPolicyDefaultGuid = CompletionPolicyCollection.CompletionByFirstPeerGuid with get, set
    /// Guid of policy to randomly select a service endpoint
    /// Register additional distribution policy 
    static member private Register( policyGuid:Guid, name, del:CompletionPolicyFunction ) = 
        completionPolicyCollection.Item( policyGuid ) <- ( name, del )
    /// Return the first object of the reply, and then signals that the operation is completed
    static member private CompletionByFirstPeer( req: OneRemoteDistributedFunctionRequest, signature ) = 
        true
    /// Return the an object of the reply, but continuous execution
    static member private CompletionByAllPeers( req: OneRemoteDistributedFunctionRequest, signature  ) = 
        req.SentSignatureIsEmpty()
    /// Find aggregation policy, if the request policy doesn't exist, the policy AggregationByFirstReplyGuid will be applied. 
    static member internal FindCompletionPolicy( completionPolicyID ) = 
        let bExist, tuple = completionPolicyCollection.TryGetValue( completionPolicyID ) 
        if bExist then 
            let _, del = tuple
            del
        else
            let bExist, tuple = completionPolicyCollection.TryGetValue( CompletionPolicyCollection.CompletionPolicyDefaultGuid ) 
            if bExist then 
                let _, del = tuple
                del
            else
                CompletionPolicyFunction( CompletionPolicyCollection.CompletionByFirstPeer )


/// This class encapsulate a DistributedFunctionHolder for a remote function call. 
/// The main reason of wrapping around DistributedFunctionHolder is capacity control (we can control 
/// how many service call can be accepted per Distributed Function) 
and internal DistributedFunctionClientStub(serverInfoLocal: ContractServerInfoLocal, 
                                            name:string, 
                                            providerID: Guid, 
                                            domainID: Guid, 
                                            inputSchemaCollection: Guid[], 
                                            outputSchemaCollection: Guid[], 
                                            distributionPolicy: Guid, 
                                            aggregationPolicy: Guid, 
                                            failoverPolicy: Guid, 
                                            completionPolicy: Guid ) = 
    static do 
        // Find a place to setup up the policy colleciton 
        DistributionPolicyCollection.RegisterDefault()
        AggregationPolicyCollection.RegisterDefault()
        FailoverPolicyCollection.RegisterDefault()
        CompletionPolicyCollection.RegisterDefault()
    /// Information of the servers/clusters that we tried use this distributed function upon.
    member val ServerInfo = serverInfoLocal with get
    /// ID of the service provider to be performed 
    member val ProviderID = providerID with get
    /// ID that represents the coding of the input 
    member val InputSchemaCollection = inputSchemaCollection with get
    /// ID that represents the coding of the input 
    member val OutputSchemaCollection = outputSchemaCollection with get
    /// ID of the service to be performed 
    member val DomainID = domainID with get
    /// Guid governs which service endpoint(s) are selected to service the request. 
    member val DistributionPolicy = distributionPolicy with get
    /// Guid governs how to aggregate multiple requests  
    member val AggregationPolicy = aggregationPolicy with get
    /// Guid governs the behavior of failover 
    member val FailoverPolicy = failoverPolicy with get
    /// Guid governs the behavior of completion
    member val CompletionPolicy = completionPolicy with get
    /// Request object (input)
    member val ReqObject : Object = null with get
    /// Pending Distributed Functions. 
    /// Execute a distributed function with a certain time budget. 
    /// The execution is an observer object. 
    member x.ExecutorForClientStub( timeBudgetInMilliseconds: int, input: Object, token: CancellationToken, replyAction:IObserver<Object>) = 
        /// Generate a new job ID
        let jobID = Guid.NewGuid() 
        let oneRequest = new OneRemoteDistributedFunctionRequest( x, 
                             serverInfoLocal, name, jobID, timeBudgetInMilliseconds, replyAction,
                             x.DistributionPolicy, 
                             x.AggregationPolicy, 
                             x.FailoverPolicy,
                             x.CompletionPolicy,
                             ProviderID = x.ProviderID, 
                             DomainID = x.DomainID, 
                             InputSchemaCollection = x.InputSchemaCollection, 
                             OutputSchemaCollection = x.OutputSchemaCollection
                             )

        let rgsOption = 
            if token<>Unchecked.defaultof<CancellationToken> then 
                // Has cancellation token source
                Some (token.Register( Action(oneRequest.CancelledByExternalToken)))
            else
                None
        RemoteDistributedFunctionRequestStore.Current.AddRequest( jobID, oneRequest, rgsOption)
        if token.IsCancellationRequested then 
            oneRequest.CancelledByExternalToken( )    
            // This additional call ensures even a cancellation is inserted any time 
            // The resource relaed to the job will always be released
            RemoteDistributedFunctionRequestStore.Current.RemoveRequest( jobID )
        else
            let bStart = oneRequest.FunctionOnStart() 
            if bStart then 
                let observer = ( oneRequest :> IObserver<Object> )
                observer.OnNext( input )
                observer.OnCompleted() 
            else
                // For failling to start, replyAction.OnError( ex ) will already be called 
                RemoteDistributedFunctionRequestStore.Current.RemoveRequest( jobID )
    /// Helper function to contruct ClientStub
    static member Construct( serverInfo, name, providerID, domainID, inputSchemaID, outputSchemaID ) = 
        DistributedFunctionClientStub( serverInfo, name, providerID, domainID, inputSchemaID, outputSchemaID, Guid.Empty, Guid.Empty, Guid.Empty, Guid.Empty )
    /// Helper function to contruct ClientStub
    static member ConstructAggregateAllPeers( serverInfo, name, providerID, domainID, inputSchemaID, outputSchemaID ) = 
        DistributedFunctionClientStub( serverInfo, name, providerID, domainID, inputSchemaID, outputSchemaID, 
                                        DistributionPolicyCollection.DistributionAllGuid, 
                                        AggregationPolicyCollection.AggregationByAllReplyGuid, 
                                        FailoverPolicyCollection.FailoverRerunDistributionGuid, 
                                        CompletionPolicyCollection.CompletionByAllPeersGuid )
    /// Get a list of service endpoints
    member x.GetServiceEndPoints() = 
        let dic = ConcurrentDictionary<int64,_>() 
        for schemaIn in x.InputSchemaCollection do 
            for schemaOut in x.OutputSchemaCollection do 
                let lst = RemoteDistributedFunctionProviderSignatureStore.TryDiscoverConnectedClients( x.ProviderID, x.DomainID, schemaIn, schemaOut )
                for tuple in lst do 
                    let signature, _, _, _, _, _ = tuple
                    if Utils.IsNull serverInfoLocal then
                        dic.Item( signature ) <- tuple
                    elif serverInfoLocal.ConnectedServerCollection.ContainsKey( signature ) then 
                        dic.Item( signature ) <- tuple
        dic
    /// Get the performance of the service
    member x.GetServicePerformance() = 
        let dic = x.GetServiceEndPoints()
        dic |> Seq.map( fun pair -> let signature = pair.Key 
                                    let _, _, _, _, _, perf = pair.Value 
                                    signature, perf ) 
        |> Seq.toArray

/// A DistributedFunctionHolder can be reference with multiple schema (e.g., different input/output coding type, etc..)
/// DistributedFunctionHolderRef allows reference counting on the DistributedFunctionHolder. 
/// When the reference count goes to zero, the enclosed DistributedFunctionHolder can be deferenced. 
type internal DistributedFunctionHolderRef( holder: DistributedFunctionHolder) = 
    let nDisposed = ref 0 
    do 
        holder.AddRef() |> ignore 
        ()
    member x.Holder with get() = holder 
    /// Execute a distributed function with a certain time budget. 
    /// The execution is an observer object. 
    member x.ExecuteWithTimebudget( jobID, timeBudget, input, token, observer ) = 
        holder.ExecuteWithTimebudget( jobID, timeBudget, input, token, observer )
    /// Execute a distributed function with a certain time budget. 
    /// The execution is an observer object. 
    member x.ExecuteWithTimebudgetAndPerf( jobID, timeBudget, input, perf, token, observer ) = 
        holder.ExecuteWithTimebudgetAndPerf( jobID, timeBudget, input, perf, token, observer )
    /// Get number of jobs that can be executed by the job holder and capacity
    member x.GetCount() = 
        holder.GetCount()
    /// Cancel all jobs related to this distributed function. 
    member x.Cancel() = 
        holder.Cancel()
    member x.CanDispose() = 
        holder.CanDispose()
    member x.CleanUp() = 
        if Interlocked.Increment( nDisposed ) = 1 then 
            let cnt = holder.DecRef() 
            if cnt = 0 then 
                ( holder :> IDisposable).Dispose() |> ignore 
    override x.Finalize() =
        /// Close All Active Connection, to be called when the program gets shutdown.
        x.CleanUp()
    interface IDisposable with
        /// Close All Active Connection, to be called when the program gets shutdown.
        member x.Dispose() = 
            x.CleanUp()
            GC.SuppressFinalize(x)


/// Option for DistributedFunctionHolder with a string to hold information on why fails to find
type internal DistributedFunctionHolderImporter = 
    | FoundFolder of DistributedFunctionHolderRef
    | NotFound of string


/// Safe Remvoal of an entry of ConcurrentDictionary. 
type internal SafeConcurrentDictionary =
    static member TryRemoveIfEmpty<'K, 'V, 'A, 'B when 'V :> ConcurrentDictionary<'A,'B> >( dic: ConcurrentDictionary<'K,'V>, key: 'K, entry: 'V) = 
        if entry.IsEmpty then 
            let bRemove, removedEntry = dic.TryRemove( key )
            if not (removedEntry.IsEmpty) then 
                // An entry may be inserted by another thread, needs to add the entry back 
                dic.GetOrAdd( key, removedEntry ) |> ignore 
                false
            else
                true
        else
            false

/// Representing a single registered distributed function. Two key functions of the RegisteredDistributedFunction class are:
/// 1. Implement IDisposable interface, when disposed, the registered function no longer respond to the distributed Function call. 
///    Note if registered distributed function goes out of scope, the disposeFunc is called with (false) input, 
/// 2. GetSchemas, return a 4-tuple that identified the provider, domain, schemaIn and schemaOut of the function registered. 
type RegisteredDistributedFunction internal ( disposeFunc: bool -> unit, schemas: seq<Guid*Guid*Guid*Guid> ) = 
    /// Get all schemas in the form of a provideID, domainID, schemaInID, schemaOutID tuple of the particular distributed function
    /// These schemas can be used by a remote App (potentially on different platform) to invoke the distributed function 
    member x.GetSchemas() = 
        schemas
    /// To be extended
    interface IDisposable with 
        member this.Dispose() = 
            disposeFunc( true )
            GC.SuppressFinalize( this )
    override x.Finalize() = 
        disposeFunc( false )

/// Store that holds executed remote function 
type internal RemoteFunctionExecutor private () as thisInstance = 
    /// A collection of remote function that is being executed 
    let inExecutionCollection = ConcurrentDictionary<Guid, DistributedFunctionHolderRef>()
    let toDisposeCollection =  ConcurrentDictionary<Guid, DistributedFunctionHolderRef*int64>()
    let timer = new System.Threading.Timer(TimerCallback( thisInstance.CheckForDisposal ), null, RemoteFunctionExecutor.CheckForDisposalIntervalInMilliseconds, RemoteFunctionExecutor.CheckForDisposalIntervalInMilliseconds)
    /// Maximum wait to try to dispose object that are still waiting 
    member val MaxWaitTimeForDisposingInMilliseconds = 600000L with get, set
    /// After time expires, whether we will forcefully disposing the DistributedFunctionHolder 
    member val ForceDisposing = false with get, set
    /// Interval to check for disposal
    static member val CheckForDisposalIntervalInMilliseconds : int = 500 with get, set 
    /// Access the common DistributedFunctionStore for the address space. 
    static member val Current = new RemoteFunctionExecutor() with get
    /// Register a job, 
    /// return bool * holder, 
    ///     true: a new job has been created
    ///     false: a existing job holder with the same job ID exists
    member x.RegisterHolder( jobID:Guid , constructHolderFunc: unit -> DistributedFunctionHolderImporter ) = 
        let bNew = ref false
        let wrappedAddFunc _ = 
            bNew := true
            Logger.LogF( jobID, LogLevel.ExtremeVerbose, fun _ -> "Begin Construct Holder Function ")
            let importer = constructHolderFunc()
            Logger.LogF( jobID, LogLevel.ExtremeVerbose, fun _ -> "End Construct Holder Function ")
            match importer with 
            | NotFound( msg ) -> 
                let ex = System.Exception( msg )
                raise( ex )
            | FoundFolder( holder ) -> 
                holder
        let holder = inExecutionCollection.GetOrAdd( jobID, wrappedAddFunc )
        !bNew, holder
    /// Cancel a job 
    member x.Cancel( jobID ) = 
        let bRemove, holder = inExecutionCollection.TryRemove( jobID )
        if bRemove then
            holder.Cancel()
            toDisposeCollection.GetOrAdd( jobID, (holder, DateTime.UtcNow.Ticks) ) |> ignore 
    /// Check whether job has stopped execution and can be disposed. 
    member x.CheckForDisposal( o: Object) = 
        for pair in toDisposeCollection do 
            let holder, ticks = pair.Value
            let bDisposing = holder.CanDispose() 
            let bRemove = bDisposing || ( DateTime.UtcNow.Ticks - ticks ) / TimeSpan.TicksPerMillisecond > x.MaxWaitTimeForDisposingInMilliseconds 
            if bRemove then 
                let bExist, pair = toDisposeCollection.TryRemove( pair.Key )
                if bExist then 
                    let holder, _ = pair
                    if holder.CanDispose() then 
                        ( holder :> IDisposable ).Dispose()
                    elif x.ForceDisposing then 
                        ( holder :> IDisposable ).Dispose()
                    else
                        // Not disposed 
                        ()
    static member RegisterHolder( jobID, constructHolderFunc ) = 
        RemoteFunctionExecutor.Current.RegisterHolder( jobID, constructHolderFunc )    
    static member Cancel( jobID ) = 
        RemoteFunctionExecutor.Current.Cancel( jobID )
    member x.Dispose( bDisposing ) = 
        if bDisposing then 
            timer.Dispose()
    /// To be extended
    interface IDisposable with 
        member this.Dispose() = 
            this.Dispose( true )
            GC.SuppressFinalize( this )
    override x.Finalize() = 
        x.Dispose( false )

/// Representing a single registered distributed function
type internal RegisteredDistributedFunctionOne ( publicID, domainID, schemaIn, schemaOut ) = 
    inherit RegisteredDistributedFunction( 
        ( fun disposing -> if disposing then 
                              // Only unregister function from DistributedFunctionStore if the Disposing API 
                              DistributedFunctionStore.Unregister( publicID, domainID, schemaIn, schemaOut ) ),
        ( [| (publicID, domainID, schemaIn, schemaOut) |] )
       )

/// Representing multiple registered distributed function
and internal RegisteredDistributedFunctionMultiple ( schemas: seq<Guid*Guid*Guid*Guid> ) = 
    inherit RegisteredDistributedFunction( 
        ( fun disposing -> if disposing then 
                              // Only unregister function from DistributedFunctionStore if the Disposing API 
                              for tuple in schemas do 
                                let publicID, domainID, schemaIn, schemaOut = tuple
                                DistributedFunctionStore.Unregister( publicID, domainID, schemaIn, schemaOut ) 
        ),
        schemas
       )

        
/// DistributedFunctionStore provides a central location for handling distributed functions. 
and DistributedFunctionStore internal () as thisStore = 
    /// A collection of all distributed stores 
    do 
        // Clean up registration 
        CleanUp.Current.Register( 700, thisStore, (fun _ -> thisStore.CleanUp(1000)), fun _ -> "DistributedFunctionStore" ) |> ignore 
    /// Capacity of the DistributedFunctionStore
    member val ConcurrentCapacity = 1 with get, set
    /// Current provider, private ID
    member val internal CurrentProviderID = Guid.Empty with get, set
    /// Default Provider used for import (default = all provider ) 
    member val internal DefaultImportProviderID = Guid.Empty with get, set
    /// Current provider, public ID
    member val internal CurrentProviderPublicID = Guid.Empty with get, set

    /// Hold all servers that we expect the registered distributed function to be called from
    member val internal ExportedServerCollection = ContractServerInfoLocalRepeatable() with get
    /// Access the common DistributedFunctionStore for the address space. 
    static member val Current = DistributedFunctionStore() with get
    /// Default tag for the codec
    static member val DefaultSerializerTag = DefaultSerializerForDistributedFunction.JSonSerializer with get, set
    /// Install Serializer, only one serializer should be installed per type. 
    /// User should call this to supply its own serializer/deserializer if desired. 
    static member InstallCustomizedSerializer<'T>( fmt: string, serializeFunc, deserializeFunc, bInstallByDefault ) = 
        let ty = typeof<'T>
        let info = fmt + ":" + ty.FullName
        let typeID = HashStringToGuid( fmt + ":" + ty.FullName )
        JobDependencies.InstallSerializer<'T>( typeID, serializeFunc, info, bInstallByDefault )
        JobDependencies.InstallDeserializer<'T>( typeID, deserializeFunc, info )
    /// Install Default Serializer
    static member InstallDefaultSerializer<'T>() = 
        let typeIDPrajna, strPrajna = SchemaPrajnaFormatterHelper<'T>.SchemaID()
        JobDependencies.InstallSerializer<'T>( typeIDPrajna, SchemaPrajnaFormatterHelper<'T>.Encoder, strPrajna, false )
        let typeIDBinary, strBinary = SchemaBinaryFormatterHelper<'T>.SchemaID()
        JobDependencies.InstallSerializer<'T>( typeIDBinary, SchemaBinaryFormatterHelper<'T>.Encoder, strBinary, false )
        let typeIDJSon, strJSon = SchemaJSonHelper<'T>.SchemaID()
        JobDependencies.InstallSerializer<'T>( typeIDJSon, SchemaJSonHelper<'T>.Encoder, strJSon, false )
        let mutable schemas = [| typeIDPrajna; typeIDBinary; typeIDJSon |] 
        let schemaID = 
            match DistributedFunctionStore.DefaultSerializerTag with 
            | DefaultSerializerForDistributedFunction.PrajnaSerializer ->               
                typeIDPrajna
            | DefaultSerializerForDistributedFunction.BinarySerializer ->                 
                typeIDBinary
            | DefaultSerializerForDistributedFunction.JSonSerializer -> 
                typeIDJSon
            | DefaultSerializerForDistributedFunction.Customized -> 
                // Programmer will implement serializer themselves. 
                let customizedSchemaID = CustomizedSerialization.GetInstalledSchemaID<'T>()
                if customizedSchemaID<>Guid.Empty then 
                    if not (Array.Exists( schemas, fun id -> id = customizedSchemaID )) then 
                        schemas <- Array.append schemas [| customizedSchemaID |]
                customizedSchemaID
            | _ -> 
                failwith (sprintf "DistributedFunctionStore: unrecognized default serializer tag %A" DistributedFunctionStore.DefaultSerializerTag )
        // Install all possible deserializer
        JobDependencies.InstallDeserializer<'T>( typeIDPrajna, SchemaPrajnaFormatterHelper<'T>.Decoder, strPrajna )
        JobDependencies.InstallDeserializer<'T>( typeIDBinary, SchemaBinaryFormatterHelper<'T>.Decoder, strBinary )
        JobDependencies.InstallDeserializer<'T>( typeIDJSon, SchemaJSonHelper<'T>.Decoder, strJSon )
        schemaID, schemas
    /// Collection of providers 
    member val internal ProviderCollection = ConcurrentDictionary<Guid, DistributedFunctionProvider>() with get
    /// Convert public ID to provider
    member val internal ProviderCollectionByPublicID = ConcurrentDictionary<Guid, DistributedFunctionProvider>() with get
    /// Register a provider, use this provider as the default. 
    member x.RegisterProvider( provider: DistributedFunctionProvider ) =
        x.ProviderCollection.Item( provider.PrivateID ) <- provider
        x.ProviderCollectionByPublicID.Item( provider.PublicID ) <- provider
        x.CurrentProviderID <- provider.PrivateID
        x.CurrentProviderPublicID <- provider.PublicID
    /// force other function to register its own provider
    member internal x.NullifyProvider( ) =
        x.CurrentProviderID <- Guid.Empty
        x.CurrentProviderPublicID <- Guid.Empty
    /// Collection of exported distributed functions, indexed by providerID, domainID, schemaIn and schemaOut
    /// This set is used 
    member val internal ExportedCollections = ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,DistributedFunctionHolderRef>>>>() with get
    /// <summary>
    /// Export an action or function object. 
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="act"> An action of type Action&lt;'T> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member internal x.RegisterInternal( privateID:Guid, domainID: Guid, schemaIn: Guid, schemaOut: Guid, obj: DistributedFunctionHolder, bReload) = 
        let bExist, provider = x.ProviderCollection.TryGetValue( privateID )
        if not bExist then 
            let ex = ArgumentException( sprintf "Please register provider with private ID %A first." privateID )
            raise( ex )
        else
            let publicID = provider.PublicID
            /// privateID is used in ExportedStore 
            let providerStore = x.ExportedCollections.GetOrAdd( publicID, fun _ -> ConcurrentDictionary<_,_>())
            let domainStore = providerStore.GetOrAdd( domainID, fun _ -> ConcurrentDictionary<_,_>() )
            let schemaInStore = domainStore.GetOrAdd( schemaIn, fun _ -> ConcurrentDictionary<_,_>() )
            if bReload then 
                // bReload is true, always overwrite the content in store. 
                schemaInStore.Item( schemaOut ) <- new DistributedFunctionHolderRef( obj )
            else
                // bReload is false, operation will fail if an item of the same name exists in DistributedFunctionStore
                let insertedObj = new DistributedFunctionHolderRef( obj )
                let existingObj = schemaInStore.GetOrAdd( schemaOut, insertedObj )
                if not(Object.ReferenceEquals( insertedObj, existingObj )) then 
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "DistributedFunctionStore, export of function failed, with provider privateID %A, domain %A, schema %A and %A, item of same signature already exists " 
                                                                        privateID domainID schemaIn schemaOut ))
            let ret = new RegisteredDistributedFunctionOne( publicID, domainID, schemaIn, schemaOut )
            // Return a disposable interface if the caller wants to unregister 
            ret :> RegisteredDistributedFunction
    member internal x.Unregister( publicID: Guid, domainID: Guid, schemaIn: Guid, schemaOut:Guid ) = 
            let bExist, providerStore = x.ExportedCollections.TryGetValue( publicID ) 
            if bExist then 
                let bExist, domainStore = providerStore.TryGetValue( domainID )
                if bExist then 
                    // We may search for alternate schemas here. 
                    let useSchemaIn = CustomizedSerialization.AlternateDeserializerID( schemaIn )
                    let bExist, schemaInStore = domainStore.TryGetValue( useSchemaIn )
                    if bExist then 
                        let useSchemaOut = CustomizedSerialization.AlternateSerializerID( schemaOut )
                        let bExist, holder = schemaInStore.TryRemove( useSchemaOut )
                        if bExist then 
                            if holder.CanDispose() then 
                                (holder :> IDisposable ).Dispose() 
                            /// Remove inserted entry of the function holder, use SafeConcurrentDictionary.TryRemoveIfEmpty
                            /// to avoid concurrent insert of a same entry
                            let bRemove = SafeConcurrentDictionary.TryRemoveIfEmpty( domainStore, useSchemaIn, schemaInStore )
                            if bRemove then 
                                let bRemove = SafeConcurrentDictionary.TryRemoveIfEmpty( providerStore, domainID, domainStore )
                                if bRemove then 
                                    SafeConcurrentDictionary.TryRemoveIfEmpty( x.ExportedCollections, publicID, providerStore ) |> ignore  
    /// Unregister a function
    static member Unregister( publicID: Guid, domainID: Guid, schemaIn: Guid, schemaOut:Guid ) = 
        DistributedFunctionStore.Current.Unregister( publicID, domainID, schemaIn, schemaOut )        
    /// Clean Up 
    member internal x.CleanUp(timeOut:int) = 
        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "DistructionFunctionStore.Cleanup")
        for pair0 in x.ExportedCollections do 
            for pair1 in pair0.Value do 
                for pair2 in pair1.Value do 
                    for pair3 in pair2.Value do 
                        let holder = pair3.Value
                        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Cancelling distributed function %A, %A, %A, %A"
                                                                            pair0.Key pair1.Key pair2.Key pair3.Key
                                        )
                        holder.Cancel() 
        let mutable bAllDisposed = false 
        let ticksStart = DateTime.UtcNow.Ticks
        while not bAllDisposed do 
            bAllDisposed <- true
            let elapse = ( DateTime.UtcNow.Ticks - ticksStart ) /TimeSpan.TicksPerMillisecond
            let timeout = (int elapse ) > timeOut
            for pair0 in x.ExportedCollections do 
                for pair1 in pair0.Value do 
                    for pair2 in pair1.Value do 
                        for pair3 in pair2.Value do 
                            let holder = pair3.Value
                            if timeout || holder.CanDispose() then 
                                Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Unregister distributed function %A, %A, %A, %A"
                                                                                    pair0.Key pair1.Key pair2.Key pair3.Key
                                                )
                                x.Unregister( pair0.Key, pair1.Key, pair2.Key, pair3.Key )
                            else
                                bAllDisposed <- false 
            if not bAllDisposed then 
                // Wait for some job to finish
                Thread.Sleep(1)
    /// Number of registered distributed functions. 
    member x.NumberOfRegistered() = 
        let mutable cnt = 0 
        for pair0 in x.ExportedCollections do 
            for pair1 in pair0.Value do 
                for pair2 in pair1.Value do 
                    for pair3 in pair2.Value do 
                        cnt <- cnt + 1
        cnt
    /// <summary>
    /// Register an action without input and output parameter. 
    /// <param name="name"> name of the action </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="act"> An action of type to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterUnitAction( name, capacity, privateID, domainID, act:unit -> unit, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            act()
            observer.OnCompleted()
        let obj = new DistributedFunctionHolder( name, capacity, executor )
        let schemaIn = Guid.Empty
        let schemaOut = Guid.Empty
        x.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
    /// <summary>
    /// Register an action without input and output parameter.
    /// <param name="name"> name of the action </param>
    /// <param name="act"> An action to be registered </param>
    /// </summary>
    member x.RegisterUnitAction( name, act:unit -> unit) = 
        x.RegisterUnitAction( name, x.ConcurrentCapacity, x.CurrentProviderID, HashStringToGuid( name ), 
            act, false ) 
    /// <summary>
    /// Register an action Action&lt;'T>
    /// <param name="name"> name of the action, for debugging purpose </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="act"> An action of type Action&lt;'T> to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterAction<'T>( name, capacity, privateID, domainID, act:'T -> unit, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            let runObject = if Utils.IsNull o then Unchecked.defaultof<'T> else o :?> 'T 
            act( runObject)
            observer.OnCompleted()
        let obj = new DistributedFunctionHolder( name, capacity, executor )
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let schemaOut = Guid.Empty
        let lst = List<_>()
        /// For export, we will install all possible schemas 
        for schemaIn in schemaInCollection do 
            let disposeInterface = x.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
            lst.AddRange( disposeInterface.GetSchemas() ) 
        let ret = new RegisteredDistributedFunctionMultiple( lst )
        // Return a disposable interface if the caller wants to unregister 
        ret :> RegisteredDistributedFunction
    /// <summary>
    /// Register an action Action&lt;'T>
    /// <param name="name"> name of the action, for debugging purpose </param>
    /// <param name="act"> An action of type Action&lt;'T> to be registered </param>
    /// </summary>
    member x.RegisterAction<'T>( name, act:'T -> unit) = 
        x.RegisterAction<'T>( name, x.ConcurrentCapacity, x.CurrentProviderID, HashStringToGuid( name ), 
            act, false ) 
    /// <summary>
    /// Register as a function Func&lt;'T,'TResult>
    /// <param name="name"> name of the action </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterFunction<'T,'TResult>( name, capacity, privateID, domainID, func:'T -> 'TResult, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            let runObject = if Utils.IsNull o then Unchecked.defaultof<'T> else o :?> 'T 
            let ret = func( runObject) :> Object
            observer.OnNext( ret )
            observer.OnCompleted()
        let obj = new DistributedFunctionHolder( name, x.ConcurrentCapacity, executor )
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let schemaOut, _ = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let lst = List<_>()
        for schemaIn in schemaInCollection do 
            let disposeInterface = x.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
            lst.AddRange( disposeInterface.GetSchemas() ) 
        let ret = new RegisteredDistributedFunctionMultiple( lst )
        // Return a disposable interface if the caller wants to unregister 
        ret :> RegisteredDistributedFunction
    /// <summary>
    /// Register as a function Func&lt;'T,'TResult>
    /// <param name="name"> name of the action </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be registered </param>
    /// </summary>
    member x.RegisterFunction<'T,'TResult>( name, func:'T -> 'TResult) = 
        x.RegisterFunction<'T, 'TResult>( name, x.ConcurrentCapacity, x.CurrentProviderID, HashStringToGuid( name ), 
            func, false ) 
    /// <summary>
    /// Register a function Func&lt;'TResult>
    /// <param name="name"> name of the action </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterFunction<'TResult>( name, capacity, privateID, domainID, func:unit -> 'TResult, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            let ret = func() :> Object
            observer.OnNext( ret )
            observer.OnCompleted()
        let obj = new DistributedFunctionHolder( name, x.ConcurrentCapacity, executor )
        let schemaIn = Guid.Empty
        let schemaOut, _ = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        x.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
    /// <summary>
    /// Register a function Func&lt;'TResult>
    /// <param name="name"> name of the function </param>
    /// <param name="func"> A function of type Func&lt;'TResult> to be registered </param>
    /// </summary>
    member x.RegisterFunction<'TResult>( name, func:unit -> 'TResult ) = 
        x.RegisterFunction<'TResult>( name, x.ConcurrentCapacity, x.CurrentProviderID, HashStringToGuid( name ), 
            func, false ) 
    /// Import a particular action/function
    member internal x.TryFindInternalLocal( publicID, domainID, schemaIn, schemaOut ) = 
        let mutable retFunc = NotFound( "Unknown Execution Path")
        let searchIDs = 
            if publicID = Guid.Empty then 
                // To array to release lock on ExportedCollections
                x.ExportedCollections.Keys |> Seq.toArray
            else
                let bExist, providerStore = x.ExportedCollections.TryGetValue( publicID )
                if not bExist then 
                    retFunc <- NotFound( sprintf "Provider ID of %A cannot be found" publicID)
                    Array.empty
                else
                    [| publicID |]
        for searchID in searchIDs do
            let bAssignWhenNotFound = ( searchID = publicID && publicID<> Guid.Empty )
            let bExist, providerStore = x.ExportedCollections.TryGetValue( searchID ) 
            if bExist then 
                let bExist, domainStore = providerStore.TryGetValue( domainID )
                if not bExist then 
                    if bAssignWhenNotFound then 
                        retFunc <- NotFound( sprintf "Provider ID %A, domain ID %A cannot be found" publicID domainID)
                if bExist then 
                    // We may search for alternate schemas here. 
                    let useSchemaIn = CustomizedSerialization.AlternateDeserializerID( schemaIn )
                    let bExist, schemaInStore = domainStore.TryGetValue( useSchemaIn )
                    if not bExist then 
                        if bAssignWhenNotFound then 
                            retFunc <- NotFound( sprintf "Provider ID %A, domain ID %A, does not support schema of input type %A" publicID domainID schemaIn)
                    else
                        let useSchemaOut = CustomizedSerialization.AlternateSerializerID( schemaOut )
                        let bExist, holder = schemaInStore.TryGetValue( useSchemaOut )
                        if not bExist then 
                            if bAssignWhenNotFound then 
                                retFunc <- NotFound( sprintf "Provider ID %A, domain ID %A, does not support schema of input type %A" publicID domainID schemaIn)
                        else
                            
                            retFunc <- FoundFolder( holder )
        retFunc
    /// <summary>
    /// Try find an action to execute without input and output parameter 
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindUnitActionLocal( publicID, domainID ) = 
        let schemaIn = Guid.Empty
        let schemaOut = Guid.Empty
        let retFunc = x.TryFindInternalLocal( publicID, domainID, schemaIn, schemaOut ) 
        retFunc 
    /// Try import an action
    member x.TryImportUnitActionLocal( name ) = 
        let retFunc = x.TryFindUnitActionLocal( x.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find unit action %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedAction() = 
                let exRet = ref null
                use cts = new CancellationTokenSource()
                use doneAction = new ManualResetEventSlim(false)
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                doneAction.Set() |> ignore 
                            member this.OnError( ex ) = 
                                doneAction.Set() |> ignore 
                                exRet := ex
                            member this.OnNext( o ) = 
                                // Don't expect return value 
                                ()
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, null, cts.Token, observer )
                doneAction.Wait() |> ignore 
                if Utils.IsNotNull !exRet then 
                    raise( !exRet ) 
            wrappedAction
    /// <summary>
    /// Try find an action to execute without input and output parameter 
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindActionLocal<'T>( publicID, domainID ) = 
        let schemaOut = Guid.Empty
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let mutable retFunc = (NotFound(sprintf "Not found"))
        for schemaIn in Array.rev schemaInCollection do 
            match retFunc with 
            | NotFound _ -> 
                retFunc <- x.TryFindInternalLocal( publicID, domainID, schemaIn, schemaOut )
            | _ -> 
                ()
        retFunc 

    /// Try import an action
    member x.TryImportActionLocal<'T>( name ) =
        let retFunc = x.TryFindActionLocal<'T>( x.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find action %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedAction(param:'T) = 
                let exRet = ref null
                use cts = new CancellationTokenSource()
                use doneAction = new ManualResetEventSlim(false)
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                doneAction.Set() |> ignore 
                            member this.OnError( ex ) = 
                                doneAction.Set() |> ignore 
                                exRet := ex
                            member this.OnNext( o ) = 
                                // Don't expect return value 
                                ()
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, param, cts.Token, observer )
                doneAction.Wait() |> ignore
                if Utils.IsNotNull !exRet then 
                    raise( !exRet ) 
            wrappedAction

    /// <summary>
    /// Try find function to execute with no input parameter, but with output. 
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindFunctionLocal<'TResult>( publicID, domainID ) = 
        let schemaIn = Guid.Empty
        let _, schemaOutCollection = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let mutable retFunc = (NotFound(sprintf "Not found"))
        for schemaOut in Array.rev schemaOutCollection do 
            match retFunc with 
            | NotFound _ -> 
                retFunc <- x.TryFindInternalLocal( publicID, domainID, schemaIn, schemaOut )
            | _ -> 
                ()
        retFunc 

    /// Try import a function with no input parameter
    member x.TryImportFunctionLocal<'TResult>( name ) =
        let retFunc = x.TryFindFunctionLocal<'TResult>( x.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find function %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedFunction() = 
                let exRet = ref null
                let res = ref Unchecked.defaultof<'TResult>
                use cts = new CancellationTokenSource()
                use doneAction = new ManualResetEventSlim(false)
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                doneAction.Set() |> ignore 
                            member this.OnError( ex ) = 
                                doneAction.Set() |> ignore 
                                exRet := ex
                            member this.OnNext( o ) = 
                                // Don't expect return value 
                                if Utils.IsNotNull o then 
                                    try 
                                        res := o :?> 'TResult
                                    with 
                                    | ex -> 
                                        doneAction.Set() |> ignore 
                                        exRet := ex
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, null, cts.Token, observer )
                doneAction.Wait() |> ignore 
                if Utils.IsNotNull !exRet then 
                    raise( !exRet ) 
                else 
                    !res
            wrappedFunction


    /// <summary>
    /// Try find a function to execute
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindFunctionLocal<'T,'TResult>( publicID, domainID ) = 
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let _, schemaOutCollection = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let mutable retFunc = (NotFound(sprintf "Not found"))
        for schemaIn in Array.rev schemaInCollection do 
            for schemaOut in Array.rev schemaOutCollection do 
                match retFunc with 
                | NotFound _ -> 
                    retFunc <- x.TryFindInternalLocal( publicID, domainID, schemaIn, schemaOut )
                | _ -> 
                    ()
        retFunc 

    /// Try import a function
    member x.TryImportFunctionLocal<'T,'TResult>( name ) =
        let retFunc = x.TryFindFunctionLocal<'T, 'TResult>( x.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find function(IO) %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedFunction(param:'T) = 
                let exRet = ref null
                let res = ref Unchecked.defaultof<'TResult>
                use cts = new CancellationTokenSource()
                use doneAction = new ManualResetEventSlim(false)
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                doneAction.Set() |> ignore 
                            member this.OnError( ex ) = 
                                doneAction.Set() |> ignore 
                                exRet := ex
                            member this.OnNext( o ) = 
                                // Don't expect return value 
                                if Utils.IsNotNull o then 
                                    try 
                                        res := o :?> 'TResult
                                    with 
                                    | ex -> 
                                        this.OnError( ex )
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, param, cts.Token, observer )
                doneAction.Wait() |> ignore 
                if Utils.IsNotNull !exRet then 
                    raise( !exRet ) 
                else 
                    !res
            wrappedFunction
    /// Get all current distributed function in a list
    member x.GetAllExportedFunctions() = 
        let lst = List<_>()
        for pair0 in x.ExportedCollections do 
            let providerID = pair0.Key
            let bExist, provider = x.ProviderCollectionByPublicID.TryGetValue( providerID )
            if bExist then 
                let privateID = provider.PrivateID
                for pair1 in pair0.Value do 
                    let domainID = pair1.Key
                    for pair2 in pair1.Value do 
                        let schemaIn = pair2.Key
                        for pair3 in pair2.Value do 
                            let schemaOut = pair3.Key
                            /// Exported Distributed Function uses privateID
                            lst.Add( privateID, domainID, schemaIn, schemaOut )
            else
                /// Logic error 
                let msg = sprintf "GetAllExportedFunctions: can't find the provider for the provider publicID %A" providerID
                failwith msg
        lst.ToArray()
    /// Export function to peers 
    member x.ExportToOneServer( signature:int64 ) = 
        let queue = NetworkConnections.Current.LookforConnectBySignature( signature )
        use token = x.ExportedServerCollection.CTS.Token
        if Utils.IsNotNull token then 
            if Utils.IsNotNull queue && queue.CanSend then 
                let health = queue.Performance
                let arr = x.GetAllExportedFunctions() 
                use ms = new MemStream() 
                health.WriteHeader( ms )
                ms.WriteVInt32( arr.Length )
                for i = 0 to arr.Length - 1 do 
                    let providerID, domainID, schemaIn, schemaOut = arr.[i]
                    ms.WriteGuid( providerID )
                    ms.WriteGuid( domainID )
                    ms.WriteGuid( schemaIn )
                    ms.WriteGuid( schemaOut )
                health.WriteEndMark( ms )
                Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register, %d Distributed Function (%dB) to %s"
                                                                arr.Length ms.Length
                                                                (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                )
                queue.ToSend( ControllerCommand( ControllerVerb.Register, ControllerNoun.DistributedFunction ), ms)

    /// Allow the registered distributed function to be called by a set of servers/server groups
    member x.ExportTo(serversInfo:ContractServersInfo) = 
        x.ExportedServerCollection.Add(serversInfo)
        x.ExportedServerCollection.OnConnect( x.ExportToOneServer, fun _ -> "Export distributed function" )
        x.ExportedServerCollection.RepeatedDNSResolve(true)
    /// Parser for Distributed Function
    static member ParseDistributedFunction = 
        DistributedFunctionStore.Current.DoParseDistributedFunction
    /// SendException 
    member x.SendAggregateException (queue:NetworkCommandQueue, verb:ControllerVerb, exCollection:ConcurrentDictionary<_,Exception>) = 
        if exCollection.IsEmpty then 
            false
        else
            let ex = AggregateException(exCollection |> Seq.map( fun pair -> pair.Value))
            Logger.LogF( LogLevel.Info, fun _ -> sprintf "Send (%A,DistributedFunction) to node %A of AggregatedException %A" 
                                                            verb (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                            ex
                                                            )
            let ms = new MemStream()
            let health = queue.Performance
            health.WriteHeader(ms)
            ms.WriteException( ex )
            health.WriteEndMark(ms)
            queue.ToSend( ControllerCommand(verb, ControllerNoun.DistributedFunction), ms)
            true
    /// SendException 
    member x.SendException (queue:NetworkCommandQueue, verb:ControllerVerb, ex:Exception) = 
            Logger.LogF( LogLevel.Info, fun _ -> sprintf "Send (%A,DistributedFunction) to node %A of Exception %A" 
                                                            verb (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                            ex
                                                            )
            let ms = new MemStream()
            let health = queue.Performance
            health.WriteHeader(ms)
            ms.WriteException( ex )
            health.WriteEndMark(ms)
            queue.ToSend( ControllerCommand(verb, ControllerNoun.DistributedFunction), ms)
    /// SendException in which the exception can be linked with a particular job
    member x.SendExceptionWithJobID (queue:NetworkCommandQueue, jobID: Guid, verb:ControllerVerb, ex:Exception) = 
            Logger.LogF( jobID, LogLevel.Info, fun _ -> sprintf "Send (%A,DistributedFunction) to node %A of Exception %A" 
                                                            verb (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                            ex
                                                            )
            let ms = new MemStream()
            let health = queue.Performance
            health.WriteHeader(ms)
            ms.WriteGuid( jobID )
            ms.WriteException( ex )
            health.WriteEndMark(ms)
            queue.ToSend( ControllerCommand(verb, ControllerNoun.DistributedFunction), ms)
    /// Operations to be executed when an unrecoverable error occurs 
    member x.ErrorAt( queue: NetworkCommandQueue, ex: Exception) = 
        RemoteDistributedFunctionProviderSignatureStore.ErrorAt( queue.RemoteEndPointSignature, ex )
        RemoteDistributedFunctionRequestStore.Current.OnDisconnect( queue.RemoteEndPointSignature )
    member x.ReceiveErrorAt( queue: NetworkCommandQueue, ex: Exception) = 
        RemoteDistributedFunctionProviderSignatureStore.ReceiveErrorAt( queue.RemoteEndPointSignature, ex )
        RemoteDistributedFunctionRequestStore.Current.OnDisconnect( queue.RemoteEndPointSignature )

    /// Exception during reply 
    static member ExceptionWhenReply( jobID: Guid, signature: int64, ex: Exception ) = 
        let queue = NetworkConnections.Current.LookforConnectBySignature( signature )
        if Utils.IsNotNull queue && queue.CanSend then 
            let health = queue.Performance
            use ms = new MemStream()
            health.WriteHeader( ms )
            ms.WriteGuid( jobID )
            ms.WriteException( ex )
            health.WriteEndMark( ms )
            queue.ToSend( ControllerCommand(ControllerVerb.FailedReply, ControllerNoun.DistributedFunction ), ms ) 
            Logger.LogF( jobID, LogLevel.Info, fun _ -> sprintf "FailedReply sent to %s, with exception %A" 
                                                                    (LocalDNS.GetHostInfoInt64(signature))
                                                                    ex )
        else
            Logger.LogF( jobID, LogLevel.Info, fun _ -> sprintf "FailedReply, unable to send to %s (queue closed), with exception %A" 
                                                                    (LocalDNS.GetHostInfoInt64(signature))
                                                                    ex )
    static member val TraceDistributedFunctionExecutionLevel = LogLevel.Info with get, set
    /// Parser for Distributed Function
    member x.DoParseDistributedFunction (queue:NetworkCommandQueue) (nc:NetworkCommand) =
        let cmd = nc.cmd
        let ms = nc.ms
        match cmd.Verb, cmd.Noun with 
        // called when the client receives registration
        | ( ControllerVerb.Register, ControllerNoun.DistributedFunction ) -> 
            try 
                let health = queue.Performance
                health.ReadHeader( ms )
                let arrlen = ms.ReadVInt32()
                let arr = Array.zeroCreate<_> arrlen 
                let exCollection = ConcurrentDictionary<_,Exception>()
                let lst = List<_>()
                for i = 0 to arr.Length - 1 do 
                    let providerID = ms.ReadGuid() 
                    let domainID = ms.ReadGuid() 
                    let schemaIn = ms.ReadGuid() 
                    let schemaOut = ms.ReadGuid() 
                    let bExist, provider = x.ProviderCollection.TryGetValue( providerID )
                    if not bExist then 
                        exCollection.GetOrAdd( providerID, ArgumentException(sprintf "Register, DistributedFunction: Provider %A doesn't exist" providerID)) |> ignore 
                    else
                        let publicID = provider.PublicID
                        // PublicID will be used during invokation. 
                        lst.Add( publicID, domainID, schemaIn, schemaOut )
                let bValid = health.ReadEndMark( ms ) 
                if not bValid then 
                    let msg = sprintf "Register, DistributedFunction:from gateway %s fails integrity check (%dB)" 
                                        (LocalDNS.GetShowInfo( queue.RemoteEndPoint )) ms.Length
                    exCollection.GetOrAdd( Guid.Empty, InvalidDataException(msg) ) |> ignore 
                if not (x.SendAggregateException( queue, ControllerVerb.FailedRegister, exCollection )) then 
                    for tuple in lst do 
                        let publicID, domainID, schemaIn, schemaOut = tuple 
                        Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Register, DistributedFunction at App of %A, %A, %A, %A from %s"
                                                                            publicID domainID schemaIn schemaOut
                                                                            (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                    )
                        RemoteDistributedFunctionProviderSignatureStore.Register( publicID, domainID, schemaIn, schemaOut, queue.RemoteEndPointSignature )
            with 
            | ex -> 
                Logger.Log( LogLevel.Error, sprintf "Exception@DoParseDistributedFunction(Register,DistributedFunction) of %A, exception swallowed" ex)
                // Option: the exception may be swallowed, if the code stablize
                reraise()
        // Called when the service endpoint receives a request. 
        | ( ControllerVerb.Request, ControllerNoun.DistributedFunction ) -> 
            try 
                let health = queue.Performance
                health.ReadHeader( ms )
                let jobID = ms.ReadGuid( )
                try 
                    Logger.LogF( jobID, DistributedFunctionStore.TraceDistributedFunctionExecutionLevel, fun _ -> sprintf "Start to parse Request, DistributedFunction")
                    let providerID = ms.ReadGuid()
                    let domainID = ms.ReadGuid( )
                    let schemaIn = ms.ReadGuid( ) 
                    let schemaOut = ms.ReadGuid( ) 
                    let objarrLength = ms.ReadVInt32( )
                    let objarr = Array.zeroCreate<Object> objarrLength
                    Logger.LogF( jobID, LogLevel.ExtremeVerbose, fun _ -> sprintf "Parse job metadata of Request, DistributedFunction")
                    for i = 0 to objarrLength - 1 do 
                        let obj = ms.DeserializeObjectWithSchema( schemaIn )
                        objarr.[i] <- obj
                    /// Timebudget 
                    let timeBudgetInMilliseconds = ms.ReadInt32()
                    let bValid = health.ReadEndMark( ms )
                    Logger.LogF( jobID, LogLevel.ExtremeVerbose, fun _ -> sprintf "Done parsing Request, DistributedFunction")
                    if not bValid then 
                        let msg = sprintf "Request, DistributedFunction failed to properly process the EndMarker for payload %dB " ms.Length
                        let ex = System.ArgumentException( msg )
                        x.ErrorAt( queue, ex )
                        x.SendExceptionWithJobID( queue, jobID, ControllerVerb.FailedRequest, ex )
                    else
                        /// Do function call 
                        try 
                            let constructFunc () = 
                                x.TryFindInternalLocal( providerID, domainID, schemaIn, schemaOut )
                            Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "Start to construct job holder of Request, DistributedFunction")
                            // Get job holder, register the job folder in 
                            // RemoteFunctionExecutor. If there is no imported function, i.e., 
                            // TryFindInternalLocal fails, an exception will be thrown, and be caught 
                            // and sent back to caller 
                            let bNew, holder = RemoteFunctionExecutor.RegisterHolder( jobID, constructFunc )
                            Logger.LogF( jobID, LogLevel.ExtremeVerbose, fun _ -> sprintf "Done construction job holder of Request, DistributedFunction")
                            if not bNew then 
                                // Place validation code if necessary. 
                                // We currently simply trust jobID to be unique 
                                ()
                            // Try not to take queue in closure
                            let signature = queue.RemoteEndPointSignature
                            let perf = SingleRequestPerformance( )
                            Logger.LogF( jobID, LogLevel.ExtremeVerbose, fun _ -> sprintf "Start to observer function")
                            let observerToExecuteDistributedFunction = 
                                {
                                    new IObserver<Object> with 
                                        member this.OnCompleted() = 
                                            try 
                                                let queueReply = NetworkConnections.Current.LookforConnectBySignature( signature )
                                                if Utils.IsNotNull queueReply && queueReply.CanSend then 
                                                    let healthReply = queueReply.Performance
                                                    use ms = new MemStream()
                                                    healthReply.WriteHeader( ms )
                                                    ms.WriteGuid( jobID )
                                                    healthReply.WriteEndMark( ms )
                                                    Logger.LogF( jobID, DistributedFunctionStore.TraceDistributedFunctionExecutionLevel, 
                                                                                      fun _ -> sprintf "Send ReportClose, DistributedFunction to %s" 
                                                                                                        (LocalDNS.GetShowInfo(queueReply.RemoteEndPoint))
                                                                     )
                                                    queueReply.ToSend( ControllerCommand(ControllerVerb.ReportClose, ControllerNoun.DistributedFunction ), ms ) 
                                                else
                                                    Logger.LogF( jobID, DistributedFunctionStore.TraceDistributedFunctionExecutionLevel, 
                                                                                    fun _ -> sprintf "ReportClose, Distributed Function, queue %s related to job has already been closed"
                                                                                                        (LocalDNS.GetHostInfoInt64(signature))
                                                                )
                                            with
                                            | ex -> 
                                                ex.Data.Add( "Location", "___ Reply, DistributedFunction (OnCompleted) ___")
                                                DistributedFunctionStore.ExceptionWhenReply( jobID, signature, ex )                                                        
                                        member this.OnError( ex ) = 
                                            ex.Data.Add( "Location", "___ Reply, DistributedFunction (OnError) ___")
                                            DistributedFunctionStore.ExceptionWhenReply( jobID, signature, ex )
                                        member this.OnNext( o ) = 
                                            // Don't expect return value 
                                            try
                                                let queueReply = NetworkConnections.Current.LookforConnectBySignature( signature )
                                                if Utils.IsNotNull queueReply && queueReply.CanSend then 
                                                    let healthReply = queueReply.Performance
                                                    use ms = new MemStream()
                                                    healthReply.WriteHeader( ms )
                                                    ms.WriteGuid( jobID )
                                                    ms.SerializeObjectWithSchema( o, schemaOut )
                                                    let elapse = perf.Elapse
                                                    perf.InProcessing <- Math.Min( 0, elapse - perf.InQueue)
                                                    let curCount, capacity = holder.GetCount() 
                                                    perf.Capacity <- capacity
                                                    perf.NumSlotsAvailable <- curCount
                                                    SingleRequestPerformance.Pack( perf, ms )
                                                    healthReply.WriteEndMark( ms )
                                                    Logger.LogF( jobID, DistributedFunctionStore.TraceDistributedFunctionExecutionLevel, 
                                                                                fun _ -> sprintf "Send Reply, DistributedFunction to %s" 
                                                                                                        (LocalDNS.GetShowInfo(queueReply.RemoteEndPoint))
                                                                     )
                                                    queueReply.ToSend( ControllerCommand(ControllerVerb.Reply, ControllerNoun.DistributedFunction ), ms ) 
                                                else
                                                    Logger.LogF( jobID, DistributedFunctionStore.TraceDistributedFunctionExecutionLevel, 
                                                                                fun _ -> sprintf "Exception, Distributed Function, queue %s related to job has already been closed"
                                                                                                        (LocalDNS.GetHostInfoInt64(signature))
                                                                )
                                            with
                                            | ex -> 
                                                ex.Data.Add( "Location", "___ Reply, DistributedFunction (OnNext) ___")
                                                DistributedFunctionStore.ExceptionWhenReply( jobID, signature, ex )                                                        
                                }
                            // Local action is not identified with a guid in execution
                            for obj in objarr do
                                Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "Rcvd Request, DistributedFunction of %A, %A, %A, %A from %s" providerID schemaIn domainID schemaOut 
                                                                                    (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                 )
                                holder.ExecuteWithTimebudgetAndPerf( jobID, timeBudgetInMilliseconds, obj, perf, Unchecked.defaultof<_>, observerToExecuteDistributedFunction )
                        with 
                        | ex -> 
                            /// The particular request fails, but it doesn't affect other request from that queue
                            x.SendExceptionWithJobID( queue, jobID, ControllerVerb.FailedRequest, ex )
                with 
                | ex -> 
                    // We have different exception processing for Distributed Function Request.
                    let msg = sprintf "Request, DistributedFunction failed to properly parse the payload of %dB " ms.Length
                    let ex = System.ArgumentException( msg )
                    x.ErrorAt( queue, ex )
                    x.SendExceptionWithJobID( queue, jobID, ControllerVerb.FailedRequest, ex )
            with 
            | ex -> 
                ex.Data.Add( "@FirstLocation", "___ Request, DistributedFunction ___")
                x.ErrorAt( queue, ex )
                x.SendException( queue, ControllerVerb.Error, ex)
                // Option: the exception may be swallowed, if the code stablize  
        // Canceling a distributed function
        | ( ControllerVerb.Cancel, ControllerNoun.DistributedFunction ) ->    
            try 
                let health = queue.Performance
                health.ReadHeader( ms )
                let jobID = ms.ReadGuid( )
                let bValid = health.ReadEndMark( ms )
                if not bValid then 
                    let msg = sprintf "Cancel, DistributedFunction failed to properly process the EndMarker for payload %dB " ms.Length
                    let ex = System.ArgumentException( msg )
                    x.ErrorAt( queue, ex )
                    x.SendExceptionWithJobID( queue, jobID, ControllerVerb.FailedRequest, ex )
                else
                    RemoteFunctionExecutor.Cancel( jobID )
            with 
            | ex -> 
                ex.Data.Add( "@FirstLocation", "___ Cancel, DistributedFunction ___")
                x.ErrorAt( queue, ex )
                x.SendException( queue, ControllerVerb.Error, ex)
                // Option: the exception may be swallowed, if the code stablize  
        // Called when we receive a reply to a distributed function call     
        | ( ControllerVerb.Reply, ControllerNoun.DistributedFunction ) -> 
            try 
                let health = queue.Performance
                health.ReadHeader( ms )
                let jobID = ms.ReadGuid()
                let schemaOption = RemoteDistributedFunctionRequestStore.Current.TryGetRequestSchema( jobID, queue.RemoteEndPointSignature )
                let replyObject, ex, perfRemote = 
                    match schemaOption with 
                    | Some schema -> 
                        let o = ms.DeserializeObjectWithSchema( schema )
                        let perf = SingleRequestPerformance.Unpack( ms )
                        let bValid = health.ReadEndMark( ms ) 
                        if not bValid then 
                            let msg = sprintf "Reply, DistributedFunction failed to properly process the EndMarker for payload %dB " ms.Length
                            let ex = System.ArgumentException( msg )
                            o, ex, null
                        else
                            o, null, perf
                    | None -> 
                        let msg = sprintf "Reply, DistributedFunction failed to find schema for reply from %s of payload %dB " (LocalDNS.GetShowInfo( queue.RemoteEndPoint)) ms.Length
                        let ex = System.ArgumentException( msg )
                        null, ex, null
                if Utils.IsNotNull ex then 
                    RemoteDistributedFunctionRequestStore.Current.RequestException( jobID, queue.RemoteEndPointSignature, ex )
                else
                    Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "Rcvd Reply, DistributedFunction from %s" 
                                                                        (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                        )
                    RemoteDistributedFunctionRequestStore.Current.OnNext( jobID, queue.RemoteEndPointSignature, replyObject, perfRemote )
            with 
            | ex ->
                ex.Data.Add( "@DoParseDistributedFunction(FailedRegister,DistributedFunction)", "exception at processing FailedReply function")
                // failure to correctly parse FailedReply
                raise( ex ) 
        // Called when all reply to a distributed function call has been serviced by that endpoint
        | ( ControllerVerb.ReportClose, ControllerNoun.DistributedFunction ) -> 
            try 
                let health = queue.Performance
                health.ReadHeader( ms )
                let jobID = ms.ReadGuid() 
                let bValid = health.ReadEndMark( ms ) 
                if not bValid then 
                    let msg = sprintf "ReportClose, DistributedFunction failed to properly process the EndMarker for payload %dB " ms.Length
                    let ex = System.ArgumentException( msg )
                    RemoteDistributedFunctionRequestStore.Current.RequestException( jobID, queue.RemoteEndPointSignature, ex )
                else
                    Logger.LogF( jobID, LogLevel.MildVerbose, fun _ -> sprintf "Rcvd ReportClose, DistributedFunction from %s" 
                                                                        (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                        )
                    RemoteDistributedFunctionRequestStore.Current.OnCompletion( jobID, queue.RemoteEndPointSignature )
            with 
            | ex ->
                ex.Data.Add( "@DoParseDistributedFunction(FailedRegister,DistributedFunction)", "exception at processing FailedReply function")
                // failure to correctly parse FailedReply
                raise( ex ) 
        // Called when a particular request fails to execution
        | ( ControllerVerb.FailedReply, ControllerNoun.DistributedFunction ) -> 
            try 
                let health = queue.Performance
                health.ReadHeader( ms )
                let jobID = ms.ReadGuid() 
                let exToThrow = 
                    try
                        let ex = ms.ReadException() 
                        let bValid = health.ReadEndMark( ms ) 
                        if not bValid then 
                            ex.Data.Add( "@DoParseDistributedFunction(FailedRegister,DistributedFunction)", "Failed to parse the end mark")
                        ex
                    with 
                    | ex -> 
                        ex
                RemoteDistributedFunctionRequestStore.Current.RequestException( jobID, queue.RemoteEndPointSignature, exToThrow )
            with 
            | ex ->
                ex.Data.Add( "@DoParseDistributedFunction(FailedRegister,DistributedFunction)", "exception at processing FailedReply function")
                // failure to correctly parse FailedReply
                raise( ex ) 
        // Called@service endpoint when a registration fails.
        | ( ControllerVerb.FailedRegister, ControllerNoun.DistributedFunction ) 
        // Called when we receive an error that is not tight to a particular request
        | ( ControllerVerb.Error, ControllerNoun.DistributedFunction ) -> 
            let exToThrow = 
                try
                    let health = queue.Performance
                    health.ReadHeader( ms )
                    let ex = ms.ReadException() 
                    let bValid = health.ReadEndMark( ms ) 
                    if not bValid then 
                        ex.Data.Add( "@DoParseDistributedFunction(FailedRegister,DistributedFunction)", "Failed to parse the end mark")                                    
                    ex
                with 
                | ex -> 
                    ex     
            match cmd.Verb, cmd.Noun with 
            | ( ControllerVerb.Error, ControllerNoun.DistributedFunction ) -> 
                /// Should fail all distributed function to run from that client. 
                /// Should deregister all distributed function from that client
                x.ReceiveErrorAt( queue, exToThrow )
            | _ -> 
                /// Exeption is thrown for FailedRegister out of band, which may crash the program (which is fine, the user's distributed function will not work)
                raise( exToThrow )
        | _ -> 
            ()
        (null: ManualResetEvent)
    /// Disconnect Processor 
    static member DisconnectProcessor = 
        DistributedFunctionStore.Current.DoDisconnect
    /// Disconnect Processor 
    member x.DoDisconnect (queue: NetworkCommandQueue) = 
        RemoteDistributedFunctionProviderSignatureStore.Disconnect( queue.RemoteEndPointSignature )
        RemoteDistributedFunctionRequestStore.Current.OnDisconnect( queue.RemoteEndPointSignature )
    /// Parser for Distributed Function (at Daemon)
    static member ParseDistributedFunctionCrossBar= 
        DistributedFunctionStore.Current.DoParseDistributedFunctionCrossBar
    static member val TraceDistributedFunctionCrossBarLevel = LogLevel.Info with get, set
    member x.DoParseDistributedFunctionCrossBar (queue:NetworkCommandQueue) (nc:NetworkCommand) =
        let cmd = nc.cmd
        let ms = nc.ms
        /// Daemon is simply in charge of forwarding the DistributedFunction message to client
        match cmd.Verb, cmd.Noun with 
        | ( _, ControllerNoun.DistributedFunction ) -> 
            try 
                /// Get a set of signatures to be used, use array so that we don't spend time to parse the signature 
                /// multiple times.  
                let signatures = NetworkCorssBarAtDaemon.GetMappedEntry( queue.RemoteEndPointSignature ) |> Seq.toArray
                if signatures.Length > 0 then 
                    for signature in signatures do 
                        let queueReroute = NetworkConnections.Current.LookforConnectBySignature( signature )
                        if queueReroute.CanSend then 
                            queueReroute.ToSend( cmd, ms) 
                            Logger.LogF( DistributedFunctionStore.TraceDistributedFunctionCrossBarLevel, 
                                                    fun _ -> sprintf "Reroute %A (%dB) from %s to %s"
                                                                            cmd ms.Length 
                                                                            (LocalDNS.GetShowInfo(queue.RemoteEndPoint))
                                                                            (LocalDNS.GetShowInfo(queueReroute.RemoteEndPoint))
                                        )
                        else
                            let ex = System.ArgumentException( sprintf "@DoParseDistributedFunctionCrossBar, Daemon at %s:%s can't reroute to endpoint %s of command (%A,%A) of %dB"
                                                                        RemoteExecutionEnvironment.MachineName RemoteExecutionEnvironment.ContainerName
                                                                        (LocalDNS.GetShowInfo(queueReroute.RemoteEndPoint))
                                                                        cmd.Verb cmd.Noun ms.Length
                                                            )
                            match cmd.Verb with 
                            // ToDo: when distributed function call is implemented, additional error message
                            // of ControllerVerb.Exception will be implemented, allowed the thrown exception to be catched by the calling function.                    
                            | _ -> 
                                // Sending ControllerVerb.Error will fail all distributed function from the message node. 
                                x.SendException( queue, ControllerVerb.Error, ex)
                else
                    /// Can't find any end point to delivery 
                    let ex = System.ArgumentException( sprintf "@DoParseDistributedFunctionCrossBar, Daemon at %s:%s can't find any endpoint to route this command (%A,%A) of %dB"
                                                                RemoteExecutionEnvironment.MachineName RemoteExecutionEnvironment.ContainerName
                                                                cmd.Verb cmd.Noun ms.Length
                                                    )
                    match cmd.Verb with 
                    // ToDo: when distributed function call is implemented, additional error message
                    // of ControllerVerb.Exception will be implemented, allowed the thrown exception to be catched by the calling function.                    
                    | _ -> 
                        // Sending ControllerVerb.Error will fail all distributed function from the message node. 
                        x.SendException( queue, ControllerVerb.Error, ex)
            with 
            | ex -> 
                let msg = sprintf "@DoParseDistributedFunctionCrossBar, Daemon at %s:%s can't find any endpoint to route this command (%A,%A) of %dB"
                                                                RemoteExecutionEnvironment.MachineName RemoteExecutionEnvironment.ContainerName
                                                                cmd.Verb cmd.Noun ms.Length
                ex.Data.Add( "FirstFailure", msg)                                   
                // Sending ControllerVerb.Error will fail all distributed function call from the message node. 
                x.SendException( queue, ControllerVerb.Error, ex)
        | _ -> 
            ()
        (null: ManualResetEvent)
    /// If true, every execution, the imported function will attempt to discover containers (connected ServiceEndpoints) which export the current function. 
    /// This is a better behavior, however, there is a cost of 4 concurrent dictionary lookup everytime an imported function executes.
    /// The cost may be justifable as each imported function execution involves network anyway.  
    /// If false, the discover of connected ServiceEndpoints only happens at the time of function import. 
    member val internal RediscoverConnectedClientEachExecution = true with get, set
    /// <summary>
    /// Try find an action to execute without input and output parameter 
    /// <param name="name"> name of the distributed function (for debug purpose only) </param>
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindUnitAction( serverInfo, name, publicID, domainID ) = 
        let schemaInCollection = [| Guid.Empty |]
        let schemaOutCollection = [| Guid.Empty |]
        let stub = DistributedFunctionClientStub.Construct( serverInfo, name, publicID, domainID, schemaInCollection, schemaOutCollection ) 
        stub 
    /// Try import an action
    member x.TryImportUnitAction( serverInfo, name ) = 
        let stub = x.TryFindUnitAction( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedAction() = 
            let exRet = ref null
            use cts = new CancellationTokenSource()
            use doneAction = new ManualResetEventSlim(false)
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            doneAction.Set() |> ignore 
                        member this.OnError( ex ) = 
                            doneAction.Set() |> ignore 
                            exRet := ex
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            ()
                }
            //  action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, null, cts.Token, observer )
            doneAction.Wait() |> ignore 
            if Utils.IsNotNull !exRet then 
                raise( !exRet ) 
        wrappedAction
    /// Try import an action
    member x.TryImportUnitAction( name ) = 
        x.TryImportUnitAction( null, name )
    /// Get performance of the unit action
    member x.GetPerformanceUnitAction( serverInfo, name ) = 
        let stub = x.TryFindUnitAction( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance()
    /// Get performance of the unit action
    member x.GetPerformanceUnitAction( name ) = 
        x.GetPerformanceUnitAction( null, name )
    /// <summary>
    /// Try find an action to execute without input and output parameter 
    /// Note that if there are multiple action with the same domain name, but with variation of the scema, the action will bind 
    /// to the any remote service endpoint that matches at least one input/output schema pair 
    /// <param name="name"> name of the distributed function (for debug purpose only) </param>
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindAction<'T>( serverInfo, name, publicID, domainID ) = 
        let schemaOutCollection = [| Guid.Empty |]
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let stub = DistributedFunctionClientStub.Construct( serverInfo, name, publicID, domainID, schemaInCollection, schemaOutCollection ) 
        stub
    /// Try import an action
    member x.TryImportAction<'T>( serverInfo, name ) =
        let stub = x.TryFindAction<'T>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedAction(param:'T) = 
            let exRet = ref null
            use cts = new CancellationTokenSource()
            use doneAction = new ManualResetEventSlim(false)
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            doneAction.Set() |> ignore 
                        member this.OnError( ex ) = 
                            doneAction.Set() |> ignore 
                            exRet := ex
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            ()
                }
            //  action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, param, cts.Token, observer )
            doneAction.Wait() |> ignore
            if Utils.IsNotNull !exRet then 
                raise( !exRet ) 
        wrappedAction
    /// Try import an action
    member x.TryImportAction<'T>( name ) =
        x.TryImportAction<'T>( null, name )
    /// Get performance of imported action
    member x.GetPerformanceAction<'T>( serverInfo, name ) =
        let stub = x.TryFindAction<'T>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance()
    /// Get performance of imported action
    member x.GetPerformanceAction<'T>( name ) =
        x.GetPerformanceAction<'T>( null, name )
    /// <summary>
    /// Try find function to execute with no input parameter, but with output. 
    /// <param name="name"> name of the distributed function (for debug purpose only) </param>
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindFunction<'TResult>( serverInfo, name, publicID, domainID ) = 
        let schemaInCollection = [| Guid.Empty |]
        let _, schemaOutCollection = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let stub = DistributedFunctionClientStub.Construct( serverInfo, name, publicID, domainID, schemaInCollection, schemaOutCollection ) 
        stub
    /// Try import a function with no input parameter
    member x.TryImportFunction<'TResult>( serverInfo, name ) =
        let stub = x.TryFindFunction<'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedFunction() = 
            let exRet = ref null
            let res = ref Unchecked.defaultof<'TResult>
            use cts = new CancellationTokenSource()
            use doneAction = new ManualResetEventSlim(false)
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            doneAction.Set() |> ignore 
                        member this.OnError( ex ) = 
                            doneAction.Set() |> ignore 
                            exRet := ex
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            if Utils.IsNotNull o then 
                                try 
                                    res := o :?> 'TResult
                                with 
                                | ex -> 
                                    doneAction.Set() |> ignore 
                                    exRet := ex
                }
            //  action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, null, cts.Token, observer )
            doneAction.Wait() |> ignore 
            if Utils.IsNotNull !exRet then 
                raise( !exRet ) 
            else 
                !res
        wrappedFunction
    /// Try import a function with no input parameter
    member x.TryImportFunction<'TResult>( name ) =
        x.TryImportFunction<'TResult>( null, name ) 
    /// Get performance of a function with no input parameter
    member x.GetPerformanceFunction<'TResult>( serverInfo, name ) =
        let stub = x.TryFindFunction<'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance()
    /// Get performance of a function with no input parameter
    member x.GetPerformanceFunction<'TResult>( name ) =
        x.GetPerformanceFunction<'TResult>( null, name )
    /// <summary>
    /// Try find a function to execute
    /// <param name="name"> name of the distributed function (for debug purpose only) </param>
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindFunction<'T,'TResult>( serverInfo, name, publicID, domainID ) = 
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let _, schemaOutCollection = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let stub = DistributedFunctionClientStub.Construct( serverInfo, name, publicID, domainID, schemaInCollection, schemaOutCollection ) 
        stub
    /// Try import a function
    member x.TryImportFunction<'T,'TResult>( serverInfo, name ) =
        let stub = x.TryFindFunction<'T, 'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedFunction(param:'T) = 
            let exRet = ref null
            let res = ref Unchecked.defaultof<'TResult>
            use cts = new CancellationTokenSource()
            use doneAction = new ManualResetEventSlim(false)
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            doneAction.Set() |> ignore 
                        member this.OnError( ex ) = 
                            doneAction.Set() |> ignore 
                            exRet := ex
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            if Utils.IsNotNull o then 
                                try 
                                    res := o :?> 'TResult
                                with 
                                | ex -> 
                                    this.OnError( ex )
                }
            //  action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, param, cts.Token, observer )
            doneAction.Wait() |> ignore 
            if Utils.IsNotNull !exRet then 
                raise( !exRet ) 
            else 
                !res
        wrappedFunction
    /// Try import a function
    member x.TryImportFunction<'T,'TResult>( name ) =
        x.TryImportFunction<'T,'TResult>( null, name )
    /// Get performance of a function
    member x.GetPerformanceFunction<'T,'TResult>( serverInfo, name ) =
        let stub = x.TryFindFunction<'T, 'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance()
    /// Get performance of a function
    member x.GetPerformanceFunction<'T,'TResult>( name ) =
        x.GetPerformanceFunction<'T,'TResult>( null, name ) 

    /// <summary>
    /// Try find a function to execute
    /// <param name="name"> name of the distributed function (for debug purpose only) </param>
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindSequenceFunction<'T,'TResult>( serverInfo, name, publicID, domainID ) = 
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let _, schemaOutCollection = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let stub = DistributedFunctionClientStub.ConstructAggregateAllPeers( serverInfo, name, publicID, domainID, schemaInCollection, schemaOutCollection ) 
        stub
    static member TakeHelper (collection:BlockingCollection<'TResult>, token:CancellationToken ) = 
        try 
            let retItem = collection.Take( token )
            Some retItem
        with 
        | :? InvalidOperationException as ex -> 
            // Completion called and collection as empty
            None
        | _ -> 
            reraise()
    /// Try import a function to execute results in a data sequence  
    member x.TryImportSequenceFunction<'T,'TResult>( serverInfo, name, capacity ) =
        let stub = x.TryFindSequenceFunction<'T, 'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedFunction(param:'T) = 
            let exRet = ref null
            let cts = new CancellationTokenSource()
            let nEnd = ref 0 
            // We can look later if the code will be more efficiently implemented via a ConcurrentQueue alone. 
            let collection = if capacity <=0 || capacity = Int32.MaxValue then new BlockingCollection<'TResult>() else new BlockingCollection<'TResult>( capacity )
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            collection.CompleteAdding()
                        member this.OnError( ex ) = 
                            Volatile.Write( exRet, ex )
                            collection.CompleteAdding()
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            if Utils.IsNotNull o then 
                                try 
                                    let res = o :?> 'TResult
                                    collection.Add( res )
                                with 
                                | ex -> 
                                    this.OnError( ex )
                            else
                                collection.Add( Unchecked.defaultof<_> )
                }
            //  action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, param, cts.Token, observer )
            seq {
                try
                    while not (collection.IsCompleted) do
                        // collection.Take may file exception InvalidOperationException, which will be swallowed at finally, 
                        // try .. with is not allows. 
                        let itemOpt = DistributedFunctionStore.TakeHelper( collection, cts.Token )
                        match itemOpt with
                        | Some retItem -> 
                            yield retItem
                        | None -> 
                            ()
                finally 
                    cts.Dispose()
                    collection.Dispose()
                let ex = Volatile.Read( exRet )
                if Utils.IsNotNull( ex ) then 
                    /// Raise exception
                    raise( ex  )
                }
        wrappedFunction
    /// Try import a function to execute results in a data sequence 
    member x.TryImportSequenceFunction<'T,'TResult>( name, capacity ) =
        x.TryImportSequenceFunction<'T,'TResult>( null, name, capacity )
    /// Try import a function sequence 
    member x.TryImportSequenceFunction<'T,'TResult>( name ) =
        x.TryImportSequenceFunction<'T,'TResult>( null, name, -1 )
    /// Get performance of a function to execute results in a data sequence 
    member x.GetPerformanceSequenceFunction<'T,'TResult>( serverInfo, name ) =
        let stub = x.TryFindSequenceFunction<'T, 'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance()
    /// Get performance of a function to execute results in a data sequence 
    member x.GetPerformanceSequenceFunction<'T,'TResult>( name ) =
        x.GetPerformanceSequenceFunction<'T,'TResult>( null, name ) 

    /// <summary>
    /// Try find a function to execute
    /// <param name="name"> name of the distributed function (for debug purpose only) </param>
    /// <param name="publicID"> public ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// </summary>
    member internal x.TryFindSequenceFunction<'TResult>( serverInfo, name, publicID, domainID ) = 
        let schemaInCollection = [| Guid.Empty |]
        let _, schemaOutCollection = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let stub = DistributedFunctionClientStub.ConstructAggregateAllPeers( serverInfo, name, publicID, domainID, schemaInCollection, schemaOutCollection ) 
        stub

    /// Try import a function to execute results in a data sequence 
    member x.TryImportSequenceFunction<'TResult>( serverInfo, name, capacity ) =
        let stub = x.TryFindSequenceFunction<'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedFunction() = 
            let exRet = ref null
            let cts = new CancellationTokenSource()
            let nEnd = ref 0 
            // We can look later if the code will be more efficiently implemented via a ConcurrentQueue alone. 
            let collection = if capacity <=0 || capacity = Int32.MaxValue then new BlockingCollection<'TResult>() else new BlockingCollection<'TResult>( capacity )
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            collection.CompleteAdding()
                        member this.OnError( ex ) = 
                            Volatile.Write( exRet, ex )
                            collection.CompleteAdding()
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            if Utils.IsNotNull o then 
                                try 
                                    let res = o :?> 'TResult
                                    collection.Add( res )
                                with 
                                | ex -> 
                                    this.OnError( ex )
                            else
                                collection.Add( Unchecked.defaultof<_> )
                }
            //  action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, null, cts.Token, observer )
            seq {
                try
                    while not (collection.IsCompleted) do
                        // collection.Take may file exception InvalidOperationException, which will be swallowed at finally, 
                        // try .. with is not allows. 
                        let itemOpt = DistributedFunctionStore.TakeHelper( collection, cts.Token )
                        match itemOpt with
                        | Some retItem -> 
                            yield retItem
                        | None -> 
                            ()
                finally 
                    cts.Dispose()
                    collection.Dispose()
                let ex = Volatile.Read( exRet )
                if Utils.IsNotNull( ex ) then 
                    /// Raise exception
                    raise( ex  )
                }
        wrappedFunction
    /// Try import a function to execute results in a data sequence 
    member x.TryImportSequenceFunction<'TResult>( name, capacity ) =
        x.TryImportSequenceFunction<'TResult>( null, name, capacity )
    /// Try import a function to execute results in a data sequence 
    member x.TryImportSequenceFunction<'TResult>( name ) =
        x.TryImportSequenceFunction<'TResult>( null, name, -1 )
    /// Get performance of a function to execute results in a data sequence 
    member x.GetPerformanceSequenceFunction<'TResult>( serverInfo, name ) =
        let stub = x.TryFindSequenceFunction<'TResult>( serverInfo, name, x.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance()
    /// Get performance of a function to execute results in a data sequence  
    member x.GetPerformanceSequenceFunction<'TResult>( name ) =
        x.GetPerformanceSequenceFunction<'TResult>( null, name )

/// Distributed function store with Async interface. 
type DistributedFunctionStoreAsync private( store:DistributedFunctionStore) =
    /// Access the common DistributedFunctionStore for the address space. 
    static member val Current = DistributedFunctionStoreAsync( DistributedFunctionStore.Current ) with get
    /// <summary>
    /// Register a task to be executed when called upon. 
    /// <param name="name"> name of the action </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="act"> An action of type to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterUnitAction( name, capacity, privateID, domainID, act:unit -> Task, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            let task = act()
            let wrappedFunc ( ta: Task)  = 
                if ta.IsCompleted then 
                    observer.OnCompleted()
                else
                    let ex = ta.Exception
                    if Utils.IsNotNull ex then 
                        observer.OnError(ex)
                    else
                        observer.OnCompleted()
            task.ContinueWith( Action<_>(wrappedFunc), token ) |> ignore
        let obj = new DistributedFunctionHolder( name, capacity, executor )
        let schemaIn = Guid.Empty
        let schemaOut = Guid.Empty
        store.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
    /// <summary>
    /// Register an action without input and output parameter.
    /// <param name="name"> name of the action </param>
    /// <param name="act"> An action to be registered </param>
    /// </summary>
    member x.RegisterUnitAction( name, act:unit -> Task) = 
        x.RegisterUnitAction( name, store.ConcurrentCapacity, store.CurrentProviderID, HashStringToGuid( name ), 
            act, false ) 
    /// <summary>
    /// Register an action Action&lt;'T>
    /// <param name="name"> name of the action, for debugging purpose </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="act"> An action of type Action&lt;'T> to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterAction<'T>( name, capacity, privateID, domainID, act:'T -> Task, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            let runObject = if Utils.IsNull o then Unchecked.defaultof<'T> else o :?> 'T 
            let task = act(runObject)
            let wrappedFunc ( ta: Task)  = 
                if ta.IsCompleted then 
                    observer.OnCompleted()
                else
                    let ex = ta.Exception
                    if Utils.IsNotNull ex then 
                        observer.OnError(ex)
                    else
                        observer.OnCompleted()
            task.ContinueWith( Action<_>(wrappedFunc), token ) |> ignore
        let obj = new DistributedFunctionHolder( name, capacity, executor )
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let schemaOut = Guid.Empty
        let lst = List<_>()
        /// For export, we will install all possible schemas 
        for schemaIn in schemaInCollection do 
            let disposeInterface = store.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
            lst.AddRange( disposeInterface.GetSchemas() ) 
        let ret = new RegisteredDistributedFunctionMultiple( lst )
        ret :> RegisteredDistributedFunction
    /// <summary>
    /// Register an action Action&lt;'T>
    /// <param name="name"> name of the action, for debugging purpose </param>
    /// <param name="act"> An action of type Action&lt;'T> to be registered </param>
    /// </summary>
    member x.RegisterAction<'T>( name, act:'T -> Task) = 
        x.RegisterAction<'T>( name, store.ConcurrentCapacity, store.CurrentProviderID, HashStringToGuid( name ), 
            act, false ) 
    /// <summary>
    /// Register as a function Func&lt;'T,Task&lt;'TResult>>
    /// <param name="name"> name of the action </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterFunction<'T,'TResult>( name, capacity, privateID, domainID, func:'T -> Task<'TResult>, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            let runObject = if Utils.IsNull o then Unchecked.defaultof<'T> else o :?> 'T 
            let task = func( runObject) 
            let wrappedFunc ( ta: Task<'TResult>)  = 
                if ta.IsCompleted then 
                    observer.OnNext(ta.Result)
                    observer.OnCompleted()
                else
                    let ex = ta.Exception
                    if Utils.IsNotNull ex then 
                        observer.OnError(ex)
                    else
                        observer.OnCompleted()
            task.ContinueWith( Action<_>(wrappedFunc), token ) |> ignore
        let obj = new DistributedFunctionHolder( name, store.ConcurrentCapacity, executor )
        let _, schemaInCollection = DistributedFunctionStore.InstallDefaultSerializer<'T>()
        let schemaOut, _ = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        let lst = List<_>()
        for schemaIn in schemaInCollection do 
            let disposeInterface = store.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
            lst.AddRange( disposeInterface.GetSchemas() ) 
        let ret = new RegisteredDistributedFunctionMultiple( lst )
        // Return a disposable interface if the caller wants to unregister 
        ret :> RegisteredDistributedFunction
    /// <summary>
    /// Register as a function Func&lt;'T,'TResult>
    /// <param name="name"> name of the action </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be registered </param>
    /// </summary>
    member x.RegisterFunction<'T,'TResult>( name, func:'T -> Task<'TResult>) = 
        x.RegisterFunction<'T, 'TResult>( name, store.ConcurrentCapacity, store.CurrentProviderID, HashStringToGuid( name ), 
            func, false ) 
    /// <summary>
    /// Register a function Func&lt;'TResult>
    /// <param name="name"> name of the action </param>
    /// <param name="capacity"> Concurrency level, if larger than 1, multiple action can be executed at the same time, if less than or equal to 0, no capacity control is exercised </param>
    /// <param name="privateID"> private ID of the provider </param>
    /// <param name="domainID"> ID of the particular function/action </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be registered </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    member x.RegisterFunction<'TResult>( name, capacity, privateID, domainID, func:unit -> Task<'TResult>, bReload) = 
        let executor( jobID: Guid, timeBudget: int, o: Object, token: CancellationToken, observer: IObserver<Object> ) = 
            let task = func() 
            let wrappedFunc ( ta: Task<'TResult>)  = 
                if ta.IsCompleted then 
                    observer.OnNext(ta.Result)
                    observer.OnCompleted()
                else
                    let ex = ta.Exception
                    if Utils.IsNotNull ex then 
                        observer.OnError(ex)
                    else
                        observer.OnCompleted()
            task.ContinueWith( Action<_>(wrappedFunc), token ) |> ignore
        let obj = new DistributedFunctionHolder( name, store.ConcurrentCapacity, executor )
        let schemaIn = Guid.Empty
        let schemaOut, _ = DistributedFunctionStore.InstallDefaultSerializer<'TResult>()
        store.RegisterInternal( privateID, domainID, schemaIn, schemaOut, obj, bReload )
    /// <summary>
    /// Register a function Func&lt;'TResult>
    /// <param name="name"> name of the function </param>
    /// <param name="func"> A function of type Func&lt;'TResult> to be registered </param>
    /// </summary>
    member x.RegisterFunction<'TResult>( name, func:unit -> Task<'TResult> ) = 
        x.RegisterFunction<'TResult>( name, store.ConcurrentCapacity, store.CurrentProviderID, HashStringToGuid( name ), 
            func, false ) 
    /// Try import an action
    /// The return signature is () -> Task
    member x.TryImportUnitActionLocal( name ) = 
        let retFunc = store.TryFindUnitActionLocal( store.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find unit action %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedAction() = 
                let ts = new TaskCompletionSource<unit>()
                let cts = new CancellationTokenSource()
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                let bSet = ts.TrySetResult() 
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnError( ex ) = 
                                let bSet = ts.TrySetException( ex ) 
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnNext( o ) = 
                                // Don't expect return value 
                                ()
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, null, cts.Token, observer )
                ts.Task :> Task
            wrappedAction
    /// Try import an action
    member x.TryImportActionLocal<'T>( name ) =
        let retFunc = store.TryFindActionLocal<'T>( store.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find action %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedAction(param:'T) = 
                let ts = new TaskCompletionSource<unit>()
                let cts = new CancellationTokenSource()
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                let bSet = ts.TrySetResult() 
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnError( ex ) = 
                                let bSet = ts.TrySetException( ex ) 
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnNext( o ) = 
                                // Don't expect return value 
                                ()
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, param, cts.Token, observer )
                ts.Task :> Task
            wrappedAction
    /// Try import a function
    member x.TryImportFunctionLocal<'TResult>( name ) =
        let retFunc = store.TryFindFunctionLocal<'TResult>( store.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find function %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedFunction() = 
                let ts = new TaskCompletionSource<'TResult>()
                let cts = new CancellationTokenSource()
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                let bSet = 
                                    if not ts.Task.IsCompleted then 
                                        ts.TrySetResult( Unchecked.defaultof<_> )
                                    else
                                        false
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnError( ex ) = 
                                let bSet = ts.TrySetException( ex ) 
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnNext( o ) = 
                                    try 
                                        let retObj = if Utils.IsNotNull o then  o:?> 'TResult else Unchecked.defaultof<_>
                                        let bSetSuccessful = ts.TrySetResult( retObj ) 
                                        if not bSetSuccessful then 
                                            let ex = System.Exception( sprintf "Function: %s, task is already in state %A" name ts.Task.Status ) 
                                            this.OnError( ex )
                                    with 
                                    | ex -> 
                                        this.OnError( ex )
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, null, cts.Token, observer )
                ts.Task
            wrappedFunction
    /// Try import an action 
    member x.TryImportFunctionLocal<'T,'TResult>( name ) =
        let retFunc = store.TryFindFunctionLocal<'T, 'TResult>( store.DefaultImportProviderID, HashStringToGuid(name) )
        match retFunc with 
        | NotFound( err ) -> 
            let ex = ArgumentException( sprintf "Fails to find function(IO) %s" name )
            raise(ex)
        | FoundFolder( holder ) -> 
            let wrappedFunction(param:'T) = 
                let ts = new TaskCompletionSource<'TResult>()
                let cts = new CancellationTokenSource()
                let observer = 
                    {
                        new IObserver<Object> with 
                            member this.OnCompleted() = 
                                let bSet = 
                                    if not ts.Task.IsCompleted then 
                                        ts.TrySetResult( Unchecked.defaultof<_> )
                                    else
                                        false
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnError( ex ) = 
                                let bSet = ts.TrySetException( ex ) 
                                if bSet then 
                                    cts.Dispose() |> ignore 
                            member this.OnNext( o ) = 
                                    try 
                                        let retObj = if Utils.IsNotNull o then  o:?> 'TResult else Unchecked.defaultof<_>
                                        let bSetSuccessful = ts.TrySetResult( retObj ) 
                                        if not bSetSuccessful then 
                                            let ex = System.Exception( sprintf "Function: %s, task is already in state %A" name ts.Task.Status ) 
                                            this.OnError( ex )
                                    with 
                                    | ex -> 
                                        this.OnError( ex )
                    }
                // Local action is not identified with a guid in execution
                holder.ExecuteWithTimebudget( Guid.Empty, Timeout.Infinite, param, cts.Token, observer )
                ts.Task
            wrappedFunction
    /// Try import an action 
    /// The return signature is () -> Task
    member x.TryImportUnitAction( serverInfo, name ) = 
        let stub = store.TryFindUnitAction( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedAction() = 
            let ts = new TaskCompletionSource<unit>()
            let cts = new CancellationTokenSource()
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            let bSet = ts.TrySetResult() 
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnError( ex ) = 
                            let bSet = ts.TrySetException( ex ) 
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            ()
                }
            // Local action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, null, cts.Token, observer )
            ts.Task :> Task
        wrappedAction
    /// Try import an action 
    member x.TryImportUnitAction( name ) = 
        x.TryImportUnitAction( null, name ) 
    /// Get performance of an action 
    member x.GetPerformanceUnitAction( serverInfo, name ) = 
        let stub = store.TryFindUnitAction( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance() 
    /// Get performance of an action 
    member x.GetPerformanceUnitAction( name ) = 
        x.GetPerformanceUnitAction( null, name )

    /// Try import an action 
    member x.TryImportAction<'T>( serverInfo, name ) =
        let stub = store.TryFindAction<'T>( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedAction(param:'T) = 
            let ts = new TaskCompletionSource<unit>()
            let cts = new CancellationTokenSource()
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            let bSet = ts.TrySetResult() 
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnError( ex ) = 
                            let bSet = ts.TrySetException( ex ) 
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnNext( o ) = 
                            // Don't expect return value 
                            ()
                }
            stub.ExecutorForClientStub( Timeout.Infinite, param, cts.Token, observer )
            ts.Task :> Task
        wrappedAction
    /// Try import an action 
    member x.TryImportAction<'T>( name ) =
        x.TryImportAction<'T>( name )
    /// Get performance of an action 
    member x.GetPerformanceAction<'T>( serverInfo, name ) =
        let stub = store.TryFindAction<'T>( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance() 
    /// Get performance of an action 
    member x.GetPerformanceAction<'T>( name ) =
        x.GetPerformanceAction<'T>( null, name )
    /// Try import a function
    member x.TryImportFunction<'TResult>( serverInfo, name ) =
        let stub = store.TryFindFunction<'TResult>( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedFunction() = 
            let ts = new TaskCompletionSource<'TResult>()
            let cts = new CancellationTokenSource()
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            let bSet = 
                                if not ts.Task.IsCompleted then 
                                    ts.TrySetResult( Unchecked.defaultof<_> )
                                else
                                    false
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnError( ex ) = 
                            let bSet = ts.TrySetException( ex ) 
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnNext( o ) = 
                                try 
                                    let retObj = if Utils.IsNotNull o then  o:?> 'TResult else Unchecked.defaultof<_>
                                    let bSetSuccessful = ts.TrySetResult( retObj ) 
                                    if not bSetSuccessful then 
                                        let ex = System.Exception( sprintf "Function: %s, task is already in state %A" name ts.Task.Status ) 
                                        this.OnError( ex )
                                with 
                                | ex -> 
                                    this.OnError( ex )
                }
            // Local action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, null, cts.Token, observer )
            ts.Task
        wrappedFunction
    /// Try import a function 
    member x.TryImportFunction<'TResult>( name ) =
        x.TryImportFunction<'TResult>( null, name ) 
    /// Get performance of a function 
    member x.GetPerformanceFunction<'TResult>( serverInfo, name ) =
        let stub = store.TryFindFunction<'TResult>( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance() 
    /// Get performance of a function
    member x.GetPerformanceFunction<'TResult>( name ) =
        x.GetPerformanceFunction<'TResult>( null, name ) 
    /// Try import a function 
    member x.TryImportFunction<'T,'TResult>( serverInfo, name ) =
        let stub = store.TryFindFunction<'T, 'TResult>( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        let wrappedFunction(param:'T) = 
            let ts = new TaskCompletionSource<'TResult>()
            let cts = new CancellationTokenSource()
            let observer = 
                {
                    new IObserver<Object> with 
                        member this.OnCompleted() = 
                            let bSet = 
                                if not ts.Task.IsCompleted then 
                                    ts.TrySetResult( Unchecked.defaultof<_> )
                                else
                                    false
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnError( ex ) = 
                            let bSet = ts.TrySetException( ex ) 
                            if bSet then 
                                cts.Dispose() |> ignore 
                        member this.OnNext( o ) = 
                                try 
                                    let retObj = if Utils.IsNotNull o then  o:?> 'TResult else Unchecked.defaultof<_>
                                    let bSetSuccessful = ts.TrySetResult( retObj ) 
                                    if not bSetSuccessful then 
                                        let ex = System.Exception( sprintf "Function: %s, task is already in state %A" name ts.Task.Status ) 
                                        this.OnError( ex )
                                with 
                                | ex -> 
                                    this.OnError( ex )
                }
            // Local action is not identified with a guid in execution
            stub.ExecutorForClientStub( Timeout.Infinite, param, cts.Token, observer )
            ts.Task
        wrappedFunction
    /// Try import a function
    member x.TryImportFunction<'T,'TResult>( name ) =
        x.TryImportFunction<'T,'TResult>( null, name ) 
    /// Get performance of a function
    member x.GetPerformanceFunction<'T,'TResult>( serverInfo, name ) =
        let stub = store.TryFindFunction<'T, 'TResult>( serverInfo, name, store.DefaultImportProviderID, HashStringToGuid(name) )
        stub.GetServicePerformance() 
    /// Get performance of a function
    member x.GetPerformanceFunction<'T,'TResult>( name ) =
        x.GetPerformanceFunction<'T,'TResult>( null, name )

/// This class services the request of distributed function. 
type internal DistributedFunctionServices() =
    let lastStatisticsQueueCleanRef = ref DateTime.UtcNow.Ticks
    /// Timeout value, in ticks 
    member val internal TimeOutTicks = Int64.MaxValue with get, set
    /// Primary Queue holds all services, it indicates the SecondaryServiceQueue that needs to be looked at. 
    member val internal PrimaryQueue = ConcurrentQueue<_>() with get
    member val internal  EvPrimaryQueue = new ManualResetEvent( false ) with get
    /// Service Queue holds request to be processed.
    member val internal  SecondaryServiceQueue = ConcurrentDictionary<Guid, _>() with get
    /// Statistics Queue holds statistics of the request that is served. 
    member val StatisticsCollection = ConcurrentDictionary<_,_>() with get
    /// Keep statistics of most recent certain seconds worth of requests
    member val StatisticsPeriodInSecond = 600 with get, set
    member val internal bTerminate = false with get, set
    member val internal  InitialMessage = null with get, set

