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
type internal SingleRequestPerformance() = 
    /// Time spent in assignment stage, before the request is queued to network 
    member val InAssignment = 0 with get, set
    /// Time spent in network (including network stack)
    member val InNetwork = 0 with get, set
    /// Time spent in queue of the query engine 
    member val InQueue = 0 with get, set
    /// Time spent in processing 
    member val InProcessing = 0 with get, set
    /// Number of Pending request in queue
    member val NumItemsInQueue = 0 with get, set
    /// Number of Slots Available
    member val NumSlotsAvailable = 0 with get, set
    /// Additional Message
    member val Message : string = null with get, set
    /// Serialize SingleQueryPerformance
    static member Pack( x:SingleRequestPerformance, ms:StreamBase<byte> ) = 
        let inQueue = if x.InQueue < 0 then 0 else if x.InQueue > 65535 then 65535 else x.InQueue
        let inProc = if x.InProcessing < 0 then 0 else if x.InProcessing > 65535 then 65535 else x.InProcessing
        ms.WriteUInt16( uint16 inQueue )
        ms.WriteUInt16( uint16 inProc )
        ms.WriteVInt32( x.NumItemsInQueue )
        ms.WriteVInt32( x.NumSlotsAvailable )
    /// Deserialize SingleQueryPerformance
    static member Unpack( ms:Stream ) = 
        let inQueue = int (ms.ReadUInt16())
        let inProcessing = int (ms.ReadUInt16())
        let numItems = ms.ReadVInt32()
        let numSlots = ms.ReadVInt32()
        SingleRequestPerformance( InQueue = inQueue, InProcessing = inProcessing, 
                                    NumItemsInQueue = numItems, NumSlotsAvailable=numSlots )
    /// Show string that can be used to monitor backend performance 
    abstract BackEndInfo: unit -> string
    override x.BackEndInfo() = 
        sprintf "queue: %dms, proc: %dms, items: %d, slot: %d" x.InQueue x.InProcessing x.NumItemsInQueue x.NumSlotsAvailable
    /// Show string that can be used to monitor frontend performance 
    abstract FrontEndInfo: unit -> string
    override x.FrontEndInfo() = 
        sprintf "assign: %dms, network %dms, queue: %dms, proc: %dms, items: %d, slot: %d" x.InAssignment x.InNetwork x.InQueue x.InProcessing x.NumItemsInQueue x.NumSlotsAvailable

/// Helper class to generate schema ID
type internal SchemaJSonHelper<'T>() = 
    /// Get a GUID that representing the coding of a type. Note that this binds to the 
    /// fullname of the type with assembly (AssemblyQualifiedName) so if the assembly 
    /// of the type changes, the schemaID will change. 
    static member SchemaID() = 
        let schemaStr = "System.Runtime.Serialization.Json.DataContractJsonSerializer:" + typeof<'T>.AssemblyQualifiedName
        let hash = HashStringToGuid(schemaStr)
        hash
    static member Encoder(o:'T, ms:Stream) = 
        let fmt = DataContractJsonSerializer( typeof<'T> )
        fmt.WriteObject( ms, o )
    static member Decoder(ms:Stream) = 
        let fmt = DataContractJsonSerializer( typeof<'T> )
        let o = fmt.ReadObject( ms ) 
        if Utils.IsNotNull o then o :?> 'T else Unchecked.defaultof<_>

/// Helper class to generate schema ID
type internal SchemaBinaryFormatterHelper<'T>() = 
    /// Get a GUID that representing the coding of a type. Note that this binds to the 
    /// fullname of the type with assembly (AssemblyQualifiedName) so if the assembly 
    /// of the type changes, the schemaID will change. 
    static member SchemaID() = 
        let schemaStr = "System.Runtime.Serialization.Formatters.Binary.BinaryFormatter:" + typeof<'T>.AssemblyQualifiedName
        let hash = HashStringToGuid(schemaStr)
        hash
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
        hash
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
type internal DistributedFunctionHolder(name:string, capacity:int, executor: (Guid * int * Object * CancellationToken * IObserver<Object> -> unit) ) = 
    let capacityControl = ( capacity > 0 )
    let currentItems = ref 0 
    let totalItems = ref 0L 
    let cts = new CancellationTokenSource()
    let lock = if not capacityControl then null else new ManualResetEvent(true)
    /// Execute a distributed function with a certain time budget. 
    /// The execution is an observer object. 
    member x.Execute( jobID: Guid, timeBudget: int, input: Object, token: CancellationToken, observer:IObserver<Object>) = 
        let isCancelled() = ( cts.IsCancellationRequested || token.IsCancellationRequested )
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
                        [| cts.Token.WaitHandle; token.WaitHandle |]
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

/// Option for DistributedFunctionHolder with a string to hold information on why fails to find
type internal DistributedFunctionHolderImporter = 
    | FoundFolder of DistributedFunctionHolder
    | NotFound of string
        
/// DistributedFunctionStore provides a central location for handling distributed functions. 
type DistributedFunctionStore internal () as thisStore = 
    do 
        // Clean up registration 
        CleanUp.Current.Register( 700, thisStore, (fun _ -> thisStore.CleanUp(1000)), fun _ -> "DistributedFunctionStore" ) |> ignore 
    /// Capacity of the DistributedFunctionStore
    member val ConcurrentCapacity = 1 with get, set
    /// Current provider, private ID
    member val internal CurrentProviderID = Guid.Empty with get, set
    /// Current provider, public ID 
    member val internal PublicProviderID = Guid.Empty with get, set

    /// Access the common DistributedFunctionStore for the address space. 
    static member val Current = DistributedFunctionStore() with get
    /// Default tag for the codec
    static member val DefaultSerializerTag = DefaultSerializerForDistributedFunction.JSonSerializer with get, set
    /// Install Serializer, only one serializer should be installed per type. 
    /// User should call this to supply its own serializer/deserializer if desired. 
    static member InstallCustomizedSerializer<'T>( fmt: string, serializeFunc, deserializeFunc ) = 
        let ty = typeof<'T>
        let typeID = HashStringToGuid( fmt + ":" + ty.FullName )
        JobDependencies.InstallSerializer<'T>( typeID, serializeFunc )
        JobDependencies.InstallDeserializer<'T>( typeID, deserializeFunc )
    /// Install Default Serializer
    static member InstallDefaultSerializer<'T>() = 
        let typeIDPrajna = SchemaPrajnaFormatterHelper<'T>.SchemaID()
        let typeIDBinary = SchemaBinaryFormatterHelper<'T>.SchemaID()
        let typeIDJSon = SchemaBinaryFormatterHelper<'T>.SchemaID()
        let mutable schemas = [| typeIDPrajna; typeIDBinary; typeIDJSon |]
        let schemaID = 
            match DistributedFunctionStore.DefaultSerializerTag with 
            | DefaultSerializerForDistributedFunction.PrajnaSerializer ->
                JobDependencies.InstallSerializer<'T>( typeIDPrajna, SchemaPrajnaFormatterHelper<'T>.Encoder )
                typeIDPrajna
            | DefaultSerializerForDistributedFunction.BinarySerializer -> 
                JobDependencies.InstallSerializer<'T>( typeIDBinary, SchemaBinaryFormatterHelper<'T>.Encoder )
                typeIDBinary
            | DefaultSerializerForDistributedFunction.JSonSerializer -> 
                JobDependencies.InstallSerializer<'T>( typeIDJSon, SchemaJSonHelper<'T>.Encoder )
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
        JobDependencies.InstallDeserializer<'T>( typeIDBinary, SchemaBinaryFormatterHelper<'T>.Decoder )    
        JobDependencies.InstallDeserializer<'T>( typeIDJSon, SchemaJSonHelper<'T>.Decoder )
        schemaID, schemas
    /// Collection of providers 
    member val internal ProviderCollection = ConcurrentDictionary<Guid, DistributedFunctionProvider>() with get
    /// Register a provider, use this provider as the default. 
    member x.RegisterProvider( provider: DistributedFunctionProvider ) =
        x.ProviderCollection.Item( provider.PrivateID ) <- provider
        x.CurrentProviderID <- provider.PrivateID
        x.PublicProviderID <- provider.PublicID
    /// Collection of exported distributed functions, indexed by providerID, domainID, schemaIn and schemaOut
    /// This set is used 
    member val internal ExportedCollections = ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,ConcurrentDictionary<Guid,DistributedFunctionHolder>>>>() with get
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
            let providerStore = x.ExportedCollections.GetOrAdd( publicID, fun _ -> ConcurrentDictionary<_,_>())
            let domainStore = providerStore.GetOrAdd( domainID, fun _ -> ConcurrentDictionary<_,_>() )
            let schemaInStore = domainStore.GetOrAdd( schemaIn, fun _ -> ConcurrentDictionary<_,_>() )
            if bReload then 
                // bReload is true, always overwrite the content in store. 
                schemaInStore.Item( schemaOut ) <- obj
            else
                // bReload is false, operation will fail if an item of the same name exists in DistributedFunctionStore
                let existingObj = schemaInStore.GetOrAdd( schemaOut, obj )
                if not(Object.ReferenceEquals( obj, existingObj )) then 
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "DistributedFunctionStore, export of function failed, with provider privateID %A, domain %A, schema %A and %A, item of same signature already exists " 
                                                                        privateID domainID schemaIn schemaOut ))
            let dispose = 
                { new IDisposable with 
                        member this.Dispose() = 
                            x.Unregister( publicID, domainID, schemaIn, schemaOut )
                }
            // Return a disposable interface if the caller wants to unregister 
            dispose
                
    /// Unregister an function
    member internal x.Unregister( publicID, domainID, schemaIn, schemaOut ) = 
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
    /// Clean Up 
    member internal x.CleanUp(timeOut:int) = 
        for pair0 in x.ExportedCollections do 
            for pair1 in pair0.Value do 
                for pair2 in pair1.Value do 
                    for pair3 in pair2.Value do 
                        let holder = pair3.Value
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
            lst.Add( disposeInterface ) 
        let dispose = 
            { new IDisposable with 
                    member this.Dispose() = 
                        for item in lst do 
                            item.Dispose() 
            }
            // Return a disposable interface if the caller wants to unregister 
        dispose
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
            lst.Add( disposeInterface ) 
        let dispose = 
            { new IDisposable with 
                    member this.Dispose() = 
                        for item in lst do 
                            item.Dispose() 
            }
            // Return a disposable interface if the caller wants to unregister 
        dispose
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
    /// Try import an action to execution
    member x.TryImportUnitActionLocal( name ) = 
        let retFunc = x.TryFindUnitActionLocal( x.PublicProviderID, HashStringToGuid(name) )
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
                holder.Execute( Guid.Empty, Timeout.Infinite, null, cts.Token, observer )
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

    /// Try import an action to execution
    member x.TryImportActionLocal<'T>( name ) =
        let retFunc = x.TryFindActionLocal<'T>( x.PublicProviderID, HashStringToGuid(name) )
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
                holder.Execute( Guid.Empty, Timeout.Infinite, param, cts.Token, observer )
                doneAction.Wait() |> ignore
                if Utils.IsNotNull !exRet then 
                    raise( !exRet ) 
            wrappedAction

    /// <summary>
    /// Try find an action to execute without input and output parameter 
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

    /// Try import an action to execution
    member x.TryImportFunctionLocal<'TResult>( name ) =
        let retFunc = x.TryFindFunctionLocal<'TResult>( x.PublicProviderID, HashStringToGuid(name) )
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
                holder.Execute( Guid.Empty, Timeout.Infinite, null, cts.Token, observer )
                doneAction.Wait() |> ignore 
                if Utils.IsNotNull !exRet then 
                    raise( !exRet ) 
                else 
                    !res
            wrappedFunction


    /// <summary>
    /// Try find an action to execute without input and output parameter 
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

    /// Try import an action to execution
    member x.TryImportFunctionLocal<'T,'TResult>( name ) =
        let retFunc = x.TryFindFunctionLocal<'T, 'TResult>( x.PublicProviderID, HashStringToGuid(name) )
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
                holder.Execute( Guid.Empty, Timeout.Infinite, param, cts.Token, observer )
                doneAction.Wait() |> ignore 
                if Utils.IsNotNull !exRet then 
                    raise( !exRet ) 
                else 
                    !res
            wrappedFunction

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
