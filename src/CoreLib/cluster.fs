(*---------------------------------------------------------------------------
    Copyright 2013 Microsoft

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
        cluster.fs
  
    Description: 
        Define Cluster, the current active cluster. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Net
open System.Net.Sockets
open System.Runtime.Serialization
open System.Text.RegularExpressions
open System.Threading
open Microsoft.FSharp.Core
open Prajna
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.Network

type internal ClusterMode = 
    | Uninitialized = 0
    | Connected = 2

[<Serializable>]
type internal Partition() = 
    member val Index = 0 with get, set
    override x.GetHashCode() = 
        x.Index
    override x.Equals(obj) = 
        match obj with 
        | :? Partition as y -> x.Index = y.Index
        | _ -> false
    interface ISerializable with 
        member x.GetObjectData( info, context ) = 
            ()

type internal NodeWithInJobType = 
    | NonExist = 0          // The node doesn't exist
    | Default = 1           // Default
    | TCPOnly = 1           // Only use TCP 

/// The CacheFactory class implements cache that avoid instantiation of multiple Cluster class, and save memory.
[<AllowNullLiteral>] 
type internal CacheFactory<'Key,'T when 'T: null>( comparer:IEqualityComparer<'Key> ) = 
    let Factory = ConcurrentDictionary<('Key),('T*(int64 ref))>(comparer) 
    member x.Clear() = 
        Factory.Clear() 
    member x.Count with get() = Factory.Count
    /// Store object info into the Factory class. 
    member x.Store( name, info ) = 
        Factory.Item( name ) <- ( info, ref (PerfADateTime.UtcNowTicks()) )
    /// Retrieve object info from the Factory class. 
    member x.Retrieve( name ) = 
        let success, result = Factory.TryGetValue( name)
        if success then 
            let info, clockRef = result 
            clockRef := (PerfADateTime.UtcNowTicks())
            Some( info )
        else
            None
    /// Retrieve object info from the Factory class. 
    member x.RetrieveWithTimeStamp( name ) = 
        let success, result = Factory.TryGetValue( name)
        if success then 
            Some( result  )
        else
            None

    /// Retrieve object info from the Factory class. 
    member x.GetOrAdd( name, createInfo ) = 
        let tuple = Factory.GetOrAdd( name, fun _ -> createInfo(), ref (0L) )
        let info, clockRef = tuple
        clockRef := (PerfADateTime.UtcNowTicks())
        info

    /// Retrieve object info from the Factory class. 
    member x.Resolve( name ) = 
        let success, result = Factory.TryGetValue( name)
        if success then 
            let info, clockRef = result
            clockRef := (PerfADateTime.UtcNowTicks())
            info
        else
            Unchecked.defaultof<_>

    /// Cache Information, used the existing object in Factory if it is there already
    member x.CacheUseOld( name, info ) = 
        let tuple = info, ref (PerfADateTime.UtcNowTicks())
        let retInfo, clockRef = Factory.GetOrAdd( name, tuple )
        clockRef := (PerfADateTime.UtcNowTicks())
        retInfo

    /// Cache Information, used the existing object in Factory if it is there already
    member x.CacheUseOldIfNotNull( name, info ) = 
        let mutable retInfo = null 
        if Utils.IsNull info then 
            null
        else
            retInfo <- x.CacheUseOld( name, info )
            if Utils.IsNull retInfo then 
                // There is a null object info in the store, use AddOrUpdate to ensure only one wins the update. 
                let tuple = Factory.AddOrUpdate( name, ( fun _ -> info, ref (PerfADateTime.UtcNowTicks()) )
                                                         , ( fun name tuple -> let obj, clockRef = tuple
                                                                               if Utils.IsNull obj then 
                                                                                    info, ref (PerfADateTime.UtcNowTicks())  
                                                                               else
                                                                                    clockRef:= (PerfADateTime.UtcNowTicks())  
                                                                                    tuple ) )
                let retObj, _ = tuple
                retObj
            else
                retInfo

    /// Evict object that hasn't been visited within the specified seconds
    member x.EvictAndReturnEvictedItems( elapseSeconds ) = 
        let t1 = (PerfADateTime.UtcNowTicks())
        // Usually don't evict much
        let items =
            [|
                for obj in Factory do
                    let _, refTime = obj.Value
                    let elapse = t1 - !refTime
                    if elapse >= TimeSpan.TicksPerSecond * (int64 elapseSeconds) then 
                        let b, item = Factory.TryRemove( obj.Key )
                        if b then
                            yield item |> fst
            |]
        items
        
    /// Evict object that hasn't been visited within the specified seconds
    abstract member Evict : int -> unit
    default x.Evict( elapseSeconds ) = 
        x.EvictAndReturnEvictedItems(elapseSeconds) |> ignore

    member x.RemoveAndReturnRemovedItem( name ) = 
        let b, item = Factory.TryRemove( name )
        if b then item |> fst |> Some else None

    /// Remove a certain entry
    abstract member Remove : 'Key -> unit
    default x.Remove( name ) = 
        x.RemoveAndReturnRemovedItem(name) |> ignore

    /// Refresh timer entry of an object
    member x.Refresh( name ) = 
        let success, result = Factory.TryGetValue( name)
        if success then 
            let _, refTimer = result
            refTimer := (PerfADateTime.UtcNowTicks())

    /// func: 'T -> bool,   true: when object is still in use
    /// Evict all object if there is elapseSeconds passed when the object is not in use. 
    member x.Refresh( func, elapseSeconds ) = 
        Factory |> Seq.iter( fun pair -> 
                                let info, refTime = pair.Value
                                if ( func info ) then 
                                    refTime := (PerfADateTime.UtcNowTicks()) )
        x.Evict( elapseSeconds )

    /// Get the list of current members
    member x.toArray() = 
        Factory 
        |> Seq.map( fun pair -> 
                        let info, _ = pair.Value
                        info )
        |> Seq.toArray
    

/// Store information related to the current node for the job
/// This may have specific job related information, e.g., TCP port, RDMA information related to the job. 
[<AllowNullLiteral>]
type internal NodeWithInJobInfo() = 
    member val internal NodeType = NodeWithInJobType.Default with get, set
    member val ListeningPort = 0 with get, set
    member val IPAddresses = null with get, set 
    member x.UseLocalAddresses() = 
        x.IPAddresses <- ClientStatus.LocalIPs
    member x.AddExternalAddress( addr ) = 
        let lenExisting = if Utils.IsNull x.IPAddresses then 0 else x.IPAddresses.Length
        let mutable bFind = false
        for i = 0 to lenExisting - 1 do 
            if System.Linq.Enumerable.SequenceEqual( x.IPAddresses.[i], addr ) then 
                bFind <- true
        if not bFind then 
            let newAddr = Array.zeroCreate<_>  (lenExisting + 1)
            if lenExisting > 0 then 
                Array.Copy( x.IPAddresses, newAddr, lenExisting ) 
            newAddr.[lenExisting] <- addr
            x.IPAddresses <- newAddr
    static member internal DefaultContructFunc (nodeType:NodeWithInJobType, ms:StreamBase<byte> ) : NodeWithInJobInfo  = 
        null
    /// For other Node type, please extend this Construction Function
    static member val internal ConstructFunc = NodeWithInJobInfo.DefaultContructFunc with get, set
    member x.Pack( ms:StreamBase<byte> ) = 
        ms.WriteVInt32( int x.NodeType ) 
        match x.NodeType with 
        | NodeWithInJobType.TCPOnly -> 
            ms.WriteUInt16( uint16 x.ListeningPort ) 
            let lenAddr = if Utils.IsNull x.IPAddresses then 0 else x.IPAddresses.Length
            ms.WriteVInt32( lenAddr ) 
            for i = 0 to lenAddr - 1 do 
                ms.WriteBytesWLen( x.IPAddresses.[i] )
        | _ ->
            Logger.Fail( sprintf "NodeWithInJobInfo.Pack, unsupported NodeType %A" x.NodeType ) 
    static member Unpack( ms:StreamBase<byte> ) = 
        let nodeType = enum<_> ( ms.ReadVInt32() )  
        match nodeType with 
        |  NodeWithInJobType.NonExist -> 
            null 
        |  NodeWithInJobType.TCPOnly -> 
            let x = NodeWithInJobInfo()
            x.ListeningPort <- int (ms.ReadUInt16())
            let lenAddr = ms.ReadVInt32()
            x.IPAddresses <- Array.zeroCreate<_> lenAddr
            for i = 0 to lenAddr - 1 do 
                x.IPAddresses.[i] <- ms.ReadBytesWLen()
            x
        | _ -> 
            NodeWithInJobInfo.ConstructFunc( nodeType, ms ) 
//            Logger.Fail ( sprintf "NodeWithInJobInfo.Unpack, unsupported NodeType %A" x.NodeType )
    override x.ToString( ) = 
        seq {  
            yield ("JPort=" + x.ListeningPort.ToString())
            if Utils.IsNotNull x.IPAddresses then 
                for i = 0 to x.IPAddresses.Length - 1 do 
                    yield ( sprintf "%A" (IPAddress( x.IPAddresses.[i] )) )
        } |> String.concat ","

[<AllowNullLiteral>]
type internal NodeConnectionInfo(machineName:string,port:int) = 
    inherit CacheFactory<string*int64,NodeWithInJobInfo>( StringTComparer<int64>(StringComparer.Ordinal) )
    let mutable bDisposed = false
    member val FirstConnectTicksRef = ref (DateTime.MinValue.Ticks) with get
    member val LastConnectTicksRef = ref (DateTime.MinValue.Ticks) with get
    member val ConnectionToDaemon: NetworkCommandQueue = null with get, set
    // Wait for connection to complete
    member val EvConnectionToDaemon = new ManualResetEvent( false ) with get
    /// Timer 
    member val TimerToReonnect = null with get, set
    /// RecvProc
    member val RecvProc = Unchecked.defaultof<_> with get, set
    member val DisconnectProc = Unchecked.defaultof<_> with get, set
    member x.DaemonDisconnected() = 
        x.Clear() // Clear attached job info
        x.ConnectionToDaemon <- null // Current current daemon connect 
        x.LastConnectTicksRef := (PerfADateTime.UtcNowTicks()) // Record time of disconnection
        if DeploymentSettings.IntervalToReconnectDaemonInMs<0 then 
            // Do not reconnect 
            ()
        elif not bDisposed then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "schedule to reconnect to %s:%d in %d ms" 
                                                                       machineName port
                                                                       DeploymentSettings.IntervalToReconnectDaemonInMs ))
            x.TimerToReonnect <- ThreadPoolTimer.TimerWait ( fun _ -> sprintf "Reconnect to %s:%d" machineName port ) (x.DaemonReconnect) DeploymentSettings.IntervalToReconnectDaemonInMs -1
    member x.DaemonConnect( recvProc, disconnectProc ) = 
        x.RecvProc <- recvProc
        x.DisconnectProc <- disconnectProc
        x.ConnectionToDaemon <- NetworkConnections.Current.AddConnect( machineName, port )
        x.ConnectionToDaemon.GetOrAddRecvProc ( "DaemonConnect", x.RecvProc x.ConnectionToDaemon ) |> ignore
        /// Timestamp refresh. 
        x.ConnectionToDaemon.OnConnect.Add( fun _ -> Logger.LogF(LogLevel.MildVerbose, ( fun _ -> let soc = x.ConnectionToDaemon.Socket
                                                                                                  sprintf "node %s:%d connected, local end point %A ..." 
                                                                                                                machineName port soc.LocalEndPoint))
                                                     x.ConnectionToDaemon.Initialize() )
        /// Mark Store as disconnected
        x.ConnectionToDaemon.OnDisconnect.Add( fun _ -> x.DaemonDisconnected() 
                                                        x.DisconnectProc() )
        // otherwise no processing
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Attempt to connect to %s:%d .... " machineName port ))
    member x.DaemonReconnect() = 
      if not bDisposed then 
        /// Second time to connect 
        let newAttemptQueue = NetworkConnections.Current.AddConnect( machineName, port )
        newAttemptQueue.GetOrAddRecvProc ( "DaemonConnect", x.RecvProc x.ConnectionToDaemon ) |> ignore
        /// Timestamp refresh. 
        newAttemptQueue.OnConnect.Add( fun _ -> Logger.LogF(LogLevel.MildVerbose, ( fun _ -> sprintf "node %s:%d reconnected ..." 
                                                                                                        machineName port ))
                                                newAttemptQueue.Initialize()
                                                x.ConnectionToDaemon <- newAttemptQueue )
        /// Mark Store as disconnected
        newAttemptQueue.OnDisconnect.Add( fun _ -> x.DaemonDisconnected() 
                                                   x.DisconnectProc() )
        // otherwise no processing
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Attempt to reconnect to %s:%d .... " machineName port ))

    interface IDisposable with
        member x.Dispose() = 
            bDisposed <- true
            if Utils.IsNotNull x.TimerToReonnect then 
                x.TimerToReonnect.Cancel()
                x.TimerToReonnect <- null 
            if Utils.IsNotNull x.ConnectionToDaemon then
                (x.ConnectionToDaemon :> IDisposable).Dispose()
            x.EvConnectionToDaemon.Dispose()
            GC.SuppressFinalize(x)

type internal NodeConnectionFactory() as thisInstance = 
    inherit CacheFactory<string*int,NodeConnectionInfo>( StringTComparer<int>(StringComparer.OrdinalIgnoreCase) )
    do
        CleanUp.Current.Register( 800, thisInstance, (fun _ -> ( thisInstance :>IDisposable).Dispose()), fun _ -> "NodeConnectionFactory" ) |> ignore 
    static member val Current = new NodeConnectionFactory() with get
    member x.StoreNodeInfo( machineName, port, jobName, jobVer, info ) = 
        let machineStore = x.GetOrAdd( (machineName, port), fun _ -> new NodeConnectionInfo(machineName, port) )
        if Utils.IsNotNull machineStore && not (Utils.IsNull info) then 
            machineStore.Store( (jobName, jobVer ), info )
    member x.ResolveNodeInfo( machineName, port, jobName, jobVer ) = 
        let machineStore = x.Resolve( machineName, port ) 
        if Utils.IsNull machineStore then 
            null
        else
            machineStore.Resolve( jobName, jobVer )
    member x.DaemonConnect( machineName, port, recvProc, disconnectProc ) = 
        let machineStore = x.GetOrAdd( (machineName, port), fun _ -> new NodeConnectionInfo(machineName, port) ) 
        if Interlocked.CompareExchange( machineStore.FirstConnectTicksRef, (PerfADateTime.UtcNowTicks()), DateTime.MinValue.Ticks ) = DateTime.MinValue.Ticks then 
            /// First time to  connect to a machine port
            machineStore.DaemonConnect( recvProc, disconnectProc ) 
            machineStore.EvConnectionToDaemon.Set() |> ignore 
        else
            /// If not first connect, at least wait for the queue to be created. 
            machineStore.EvConnectionToDaemon.WaitOne() |> ignore 
        machineStore.ConnectionToDaemon

    override x.Evict( elapseSeconds ) =
        let evictedItems = base.EvictAndReturnEvictedItems(elapseSeconds)
        evictedItems
        |> Array.iter (fun nodeInfo -> (nodeInfo :> IDisposable).Dispose())

    override x.Remove( name ) =
        let item = base.RemoveAndReturnRemovedItem( name )
        match item with
        | Some v -> (v :> IDisposable).Dispose()
        | None -> ()

    interface IDisposable with
        member x.Dispose() = 
            x.toArray() 
            |> Array.iter (fun nodeInfo -> (nodeInfo :> IDisposable).Dispose())
            GC.SuppressFinalize(x)

/// Manage Listening port for the job
[<AllowNullLiteral>]
type internal JobListeningPortManagement( jobip : string, minPort : int, maxPort :int ) = 
    static member val Current = null with get, set
    static member Initialize( jobip, minPort, maxPort ) =
        JobListeningPortManagement.Current <- new JobListeningPortManagement( jobip, minPort, maxPort )
        JobListeningPortManagement.Current  
    //  manage the job listening port 
    member val JobListeningIP = jobip with get
    member val JobListeningPortMin = minPort with get
    member val JobListeningPortMax = maxPort with get
    member val JobListeningPortArray = Array.init<_> (maxPort-minPort+1) ( fun _ -> ref 0 ) with get
    // JobListeningPortManagement launches jobs. There are not many jobs in a node, so we will use lock for multithread safety. 
    member val JobListeningPortDict = ConcurrentDictionary<_ , _>(StringComparer.Ordinal) with get, set
    //  Thread safe way to get a unique port, we don't expect heavy contention, so a lock should be fine
    member x.FindAvailablePort() = 
        let nPort = ref 0
        let mutable bTerminateLoop = false
        if true then 
                let mutable i = 0 
                while !nPort <= 0 && i < x.JobListeningPortArray.Length - 1 do 
                    // no read to Volatile.Read, and Ok to read a stale value, as there is a Interlocked.CompareExchange 
                    // which resolves contention
                    let oldValue = !(x.JobListeningPortArray.[i])
                    if oldValue = 0 then 
                        // Potential useful port
                        if Interlocked.CompareExchange( x.JobListeningPortArray.[i], 1, oldValue ) = oldValue then 
                            let usePort = i + x.JobListeningPortMin
                            try 
                                use soc = new Socket( AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp )
                                if (x.JobListeningIP.Equals("", StringComparison.Ordinal)) then
                                    soc.Bind( IPEndPoint( IPAddress.Any, usePort ) )
                                else
                                    soc.Bind(IPEndPoint(IPAddress.Parse(x.JobListeningIP), usePort))
                                soc.Close()
                                nPort := usePort 
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Successful in reserving job port %d" usePort ))
                            with 
                            | e -> 
                                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Try binding to port %d failed exception %A" usePort e))
                                // Marked the port as not usable
                                x.JobListeningPortArray.[i] := 3
                    i <- i + 1
        if (!nPort <= 0 ) then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Can't find any port for the job, port usage is [%s]" 
                                                           ( x.JobListeningPortArray |> Seq.map( fun rv -> (!rv).ToString()) |> String.concat "," ) )    )
        (!nPort) 
    //   use a port
    member x.UsePort( usePort ) = 
        if usePort > 0 then 
            let nPort = usePort - x.JobListeningPortMin
            if nPort < 0 || nPort >= x.JobListeningPortArray.Length then 
                Logger.Fail( sprintf "JobListeningPortManagement.UsePort, use port %d which is out of range of %d - %d" usePort x.JobListeningPortMin x.JobListeningPortMax )
            else
                let oldValue = !(x.JobListeningPortArray.[nPort])
                if oldValue = 1 then 
                    Interlocked.CompareExchange( x.JobListeningPortArray.[nPort], 2, oldValue ) |> ignore
                elif oldValue = 2 then 
                    ()
                else
                    Logger.Fail( sprintf "JobListeningPortManagement.UsePort, use port %d which hasn't been reserved with value %d" usePort oldValue)
        else
            // usePort: 0, will relay by host 
            ()                
    //   Release a port
    member x.ReleasePort( usePort ) = 
        if usePort > 0 then 
            let nPort = usePort - x.JobListeningPortMin
            if nPort < 0 || nPort >= x.JobListeningPortArray.Length then 
                Logger.Fail( sprintf "JobListeningPortManagement.ReleasePort, release port %d which is out of range of %d - %d" usePort x.JobListeningPortMin x.JobListeningPortMax )
            else
                let oldValue = !(x.JobListeningPortArray.[nPort])
                Interlocked.CompareExchange( x.JobListeningPortArray.[nPort], 0, oldValue ) |> ignore
        else
            // Port = 0, will relay 
            ()
    //   Release a port
    member x.PortInfo( usePort ) = 
        if usePort > 0 then 
            let nPort = usePort - x.JobListeningPortMin
            x.JobListeningPortArray.[nPort]    
        else
            ref 0
    // Reserve a port through job signatureName 
    member x.Reserve( signatureName:string, suggestedPort:uint16) = 
        if suggestedPort = 0us then 
            let allocatedPort = ref null
            let addFunc sigName = 
                let usePort = x.FindAvailablePort() 
                if usePort>=0 then 
                    // Find a valid port or usePort=0, the job will not be assigned a job port
                    let nodeInfo = NodeWithInJobInfo(ListeningPort = usePort)
                    nodeInfo.UseLocalAddresses()
                    allocatedPort := nodeInfo
                    x.JobListeningPortDict.Item( (signatureName) ) <- ( nodeInfo, ref (PerfDateTime.UtcNow()) )
                    nodeInfo, ref (PerfDateTime.UtcNow())
                else
                    null, ref (PerfDateTime.UtcNow())
            let nodeInfo, reserveTime = x.JobListeningPortDict.GetOrAdd( signatureName, addFunc )
            if Object.ReferenceEquals( nodeInfo, !allocatedPort ) then 
                // the current port information is not equal to the allocated port
                if not (Utils.IsNull !allocatedPort) && 
                    nodeInfo.ListeningPort <> (!allocatedPort).ListeningPort then 
                        // Port allocated
                        x.ReleasePort( (!allocatedPort).ListeningPort )
            if Utils.IsNull nodeInfo then 
                // failed to allocate port
                x.JobListeningPortDict.TryRemove( signatureName ) |> ignore
            nodeInfo
        else
            let nodeInfoCreate = NodeWithInJobInfo(ListeningPort = int suggestedPort)
            let nodeInfo, reserveTime = x.JobListeningPortDict.GetOrAdd( signatureName, (nodeInfoCreate, ref (PerfDateTime.UtcNow()) ) )
            nodeInfo
    // Use the job port
    member x.Use( signatureName) = 
        let refValue = ref Unchecked.defaultof<_>
        if x.JobListeningPortDict.TryGetValue( signatureName, refValue ) then 
            let nodeInfo, reserveTime = !refValue
            x.UsePort( nodeInfo.ListeningPort ) 
            reserveTime := (PerfDateTime.UtcNow())
            nodeInfo
        else
            Logger.Fail( sprintf "JobListeningPortManagement.Use, can't find reserved port for task %s" signatureName )
    // Release the job port
    member x.Release( signatureName) = 
        let bRemove, tuple = x.JobListeningPortDict.TryRemove( signatureName ) 
        if bRemove then 
                let nodeInfo, reserveTime = tuple
                if not (Utils.IsNull nodeInfo) then 
                    x.ReleasePort( nodeInfo.ListeningPort ) 
    // Clean up those port that is reserved but never used. 
    member x.Cleanup( ) = 
        let t1 = (PerfDateTime.UtcNow())
        let toCleanUp = List<_>(5) 
        for pair in x.JobListeningPortDict do 
            let nodeInfo, reserveTime = pair.Value
            if !(x.PortInfo( nodeInfo.ListeningPort )) = 1 &&  t1.Subtract(!reserveTime).TotalSeconds > DeploymentSettings.TimeOutJobPortReservation then 
                toCleanUp.Add( pair.Key ) 
        for nv in toCleanUp do 
            x.Release( nv )        



/// The CacheFactory class implements cache that avoid instantiation of multiple Cluster class, and save memory. 
type internal CacheFactory<'T>() = 
    static let clock = System.Diagnostics.Stopwatch.StartNew()
        // clock frequency
    static let clockFrequency = System.Diagnostics.Stopwatch.Frequency
    static let Factory = ConcurrentDictionary< string, ('T*int64 ref) >(StringComparer.Ordinal)
    static member Count with get() = Factory.Count
    /// Store object info into the Factory class. 
    static member Store( name, info ) = 
        Factory.Item( name ) <- ( info, ref clock.ElapsedTicks )
    /// Get or add a entry
    static member GetOrAdd( name, infoFunc: unit -> 'T) = 
        Factory.GetOrAdd( name, fun _ -> ( infoFunc(), ref clock.ElapsedTicks) )
    /// Retrieve object info from the Factory class. 
    static member Retrieve( name ) = 
        let success, result = Factory.TryGetValue( name )
        if success then 
            let info, clockRef = result 
            clockRef := clock.ElapsedTicks
            Some( info )
        else
            None
    /// Retrieve object info from the Factory class. 
    static member Resolve( name ) = 
        let success, result = Factory.TryGetValue( name )
        if success then 
            let info, clockRef = result 
            clockRef := clock.ElapsedTicks
            info
        else
            Unchecked.defaultof<_>
    /// Cache Information, used the existing object in Factory if it is there already
    static member CacheUseOld( name, info ) = 
        let tuple = info, ref clock.ElapsedTicks
        let retInfo, clockRef = Factory.GetOrAdd( name, tuple )
        clockRef := clock.ElapsedTicks
        retInfo

    /// Evict object that hasn't been visited within the specified seconds
    static member Evict( elapseSeconds ) = 
        let t1 = clock.ElapsedTicks
        // Usually don't evict much
        for obj in Factory do
            let _, refTime = obj.Value
            let elapse = t1 - !refTime
            if elapse >= clockFrequency * (int64 elapseSeconds) then 
                Factory.TryRemove( obj.Key ) |> ignore

    /// Remove a certain entry
    static member Remove( name ) = 
        Factory.TryRemove( name ) |> ignore

    /// Refresh timer entry of an object
    static member Refresh( name ) = 
        let success, result = Factory.TryGetValue( name )
        if success then 
            let _, refTimer = result
            refTimer := clock.ElapsedTicks

    /// func: 'T -> bool,   true: when object is still in use
    /// Evict all object if there is elapseSeconds passed when the object is not in use. 
    static member Refresh( func, elapseSeconds ) = 
        Factory |> Seq.iter( fun pair -> 
                                let info, refTime = pair.Value
                                if ( func info ) then 
                                    refTime := clock.ElapsedTicks )
        CacheFactory<'T>.Evict( elapseSeconds )

    /// Get the list of current members
    static member toArray() = 
        Factory 
        |> Seq.map( fun pair -> 
                        let info, _ = pair.Value
                        info )
        |> Seq.toArray



/// The CacheFactory class implements cache that avoid instantiation of multiple Cluster class, and save memory. 
type internal HashCacheFactory<'T>() = 
    // clock frequency
    static let Factory = ConcurrentDictionary< _, ('T*int64 ref) >(Tools.BytesCompare())
    static member Count with get() = Factory.Count
    /// Store object info into the Factory class. 
    static member Store( hash, info ) = 
        Factory.Item( hash ) <- ( info, ref (PerfDateTime.UtcNowTicks()) )
    /// Retrieve object info from the Factory class. 
    static member Retrieve( hash ) = 
        let refVal = ref Unchecked.defaultof<_>
        if Factory.TryGetValue( hash, refVal ) then 
            let info, clockRef = !refVal 
            clockRef := (PerfDateTime.UtcNowTicks())
            Some( info )
        else
            None
    /// Resolve object info from the Factory class. 
    static member Resolve( hash ) = 
        let bExist, tuple = Factory.TryGetValue( hash ) 
        if bExist then 
            let info, clockRef = tuple
            clockRef := (PerfDateTime.UtcNowTicks())
            info
        else
            Unchecked.defaultof<_>
    /// Cache Information, used the existing object in Factory if it is there already
    static member CacheUseOld( hash, info ) = 
        let tuple = info, ref (PerfDateTime.UtcNowTicks())
        let returnTuple = Factory.GetOrAdd( hash, tuple )
        let retInfo, clockRef = returnTuple
        clockRef := (PerfDateTime.UtcNowTicks())
        retInfo

    /// Evict object that hasn't been visited within the specified seconds
    static member Evict( elapseSeconds ) = 
        let t1 = (PerfDateTime.UtcNowTicks()) - TimeSpan.TicksPerSecond * (int64 elapseSeconds) 
        // Usually don't evict much
        for obj in Factory do
            let _, refTime = obj.Value
            let elapse = t1 - !refTime
            if elapse <= t1 then 
                Factory.TryRemove( obj.Key ) |> ignore

    /// Remove a certain entry
    static member Remove( hash ) = 
        Factory.TryRemove( hash, ref Unchecked.defaultof<_> ) |> ignore

    /// Refresh timer entry of an object
    static member Refresh( hash ) = 
        let refValue = ref Unchecked.defaultof<_>
        if Factory.TryGetValue( hash, refValue ) then 
            let _, refTimer = !refValue
            refTimer := (PerfDateTime.UtcNowTicks())

    /// func: 'T -> bool,   true: when object is still in use
    /// Evict all object if there is elapseSeconds passed when the object is not in use. 
    static member Refresh( func, elapseSeconds ) = 
        Factory |> Seq.iter( fun pair -> 
                                let info, refTime = pair.Value
                                if ( func info ) then 
                                    refTime := (PerfDateTime.UtcNowTicks()) )
        CacheFactory<'T>.Evict( elapseSeconds )

    /// Get the list of current members
    static member toArray() = 
        Factory 
        |> Seq.map( fun pair -> 
                        let info, _ = pair.Value
                        info )
        |> Seq.toArray


/// Mode for starting a container for a local cluster
type internal LocalClusterContainerMode =
    | AppDomain
    | Process

/// interface for local cluster
type internal ILocalCluster = 
    abstract member ClusterInfo : ClusterInfo with get

/// The configuration for a local cluster
type LocalClusterConfig =
    {
        /// The name of the local cluster
        Name : string
        /// The version of the local cluster
        Version : DateTime
        /// Number of clients 
        NumClients : int
        /// Start the container in AppDomain? If false, start the container as a process
        ContainerInAppDomain : bool
        /// The path to the PrajnaClient executable. If given, the client is started as a process, otherwise, as an AppDomain
        ClientPath : string option
        /// Number of job ports for each client
        NumJobPortsPerClient : int option
        /// The inclusive range of ports that the local cluster can use. It has to at least provide 
        ///     NumClients + NumJobPortsPerClient * NumClients
        /// ports
        PortsRange : (int * int) option
    }

/// Can't add another function to call back without break the function. 
[<AllowNullLiteral>]
type internal NetworkCommandCallback = 
    /// Callback triggered by peer:
    /// calling parameter:
    ///     Controller Command [V, N]
    ///     Peeri
    ///     Payload
    abstract Callback : ControllerCommand * int * StreamBase<byte> * Guid * string * int64 * Cluster -> bool   
//    abstract CallbackEx : ControllerCommand * int * MemStream * string * int64 * Object -> unit
//    default x.CallbackEx( cmd, peeri, ms, name, verNumber, cl ) = 
//        x.Callback( cmd, peeri, ms, name, verNumber )
    
and 
 /// <summary> 
 /// Cluster represents a set of remote nodes in either a private cloud or a public cloud. It is the class that govern 
 /// which remote nodes that services, contracts and data analytical jobs are running upon. 
 /// </summary> 
 [<AllowNullLiteral>]
 Cluster private ( ) as thisInstance =
    // regular expression to match "local[n]" specification for local cluster
    let localNRegex = Regex("^local\[([1-9][0-9]*|)\]$", RegexOptions.Compiled ||| RegexOptions.CultureInvariant)
    let defaultLocalClusterVersion = DateTime.MinValue

    let mutable masterInfo = ClientMasterConfig()
    let mutable clusterStatus = ClusterInfo()
// disable the feature of period update. 
//    let clusterUpdateTimer = Timer( ClusterInfo.PeriodicClusterUpdate, 
//                                        clusterStatus, Timeout.Infinite, Timeout.Infinite)
    let mutable expectedClusterSize = 0 
    let mutable queues : NetworkCommandQueue[] = null
    let mutable bFailedMsgShown = Array.create 0 false 
    // clock for communication 
    let clock = System.Diagnostics.Stopwatch.StartNew()
    // clock frequency
    let clockFrequency = System.Diagnostics.Stopwatch.Frequency
    // last time that we checked for client response
    let mutable lastTime = clock.ElapsedTicks
    // Whether each individual peer has been mapped to end point 
    let mutable bMapped : bool[] = null
    // Mapping: From IP Endpoint to index of peer. 
    let endpointToPeer = ConcurrentDictionary<IPEndPoint,int>()

    // Command to call for DSet
    static let commandCallback = ConcurrentDictionary<ControllerCommand, ConcurrentDictionary<Guid, (int64 ref) * ConcurrentDictionary<string*int64, Cluster*NetworkCommandCallback>>>()
    // Parser to try to call when we fails to find a valid callback 
    static let staticCallback = ConcurrentDictionary<ControllerCommand, ConcurrentQueue<NetworkCommandCallback>>()
    // The factory method to create a local cluster, to be specified by code defined later
    static let mutable createLocalCluster : string * DateTime * int * LocalClusterContainerMode * (string option) * int  * (int * int) -> ILocalCluster = 
        (fun (name, version, numClients, containerMode, clientPath, numJobPortsPerClient, portsRange) -> failwith "not initialized, please call Environment.Init when program starts")
    static member internal SetCreateLocalCluster f = createLocalCluster <- f

    // Factory methods to create a local cluster
    static member internal CreateLocalCluster(name, version, numClients, containerMode, clientPath, numJobPortsPerClient, portsRange) =
        let nJobPortsPerClient = 
            match numJobPortsPerClient with
            | Some v -> v
            | None -> DeploymentSettings.LocalClusterNumJobPortsPerClient
        let pRange = 
            match portsRange with
            | Some v -> v
            | None ->  (DeploymentSettings.LocalClusterStartingPort, 
                        DeploymentSettings.LocalClusterStartingPort +  nJobPortsPerClient * numClients + numClients - 1)
        let cl = createLocalCluster(name, version, numClients, containerMode, clientPath, nJobPortsPerClient, pRange)
        cl

    static member internal CreateLocalCluster (name, version, numClients, containerMode, clientPath) =
        Cluster.CreateLocalCluster (name, version, numClients, containerMode, clientPath, None, None)
    
    static member internal CreateLocalCluster (name, version, numClients, containerMode) =
        Cluster.CreateLocalCluster (name, version, numClients, containerMode, None)

    static member internal CreateLocalCluster (name, version, numClients) =
        Cluster.CreateLocalCluster (name, version, numClients, LocalClusterContainerMode.AppDomain)

    static member internal CreateLocalCluster (name, version) =
        Cluster.CreateLocalCluster (name, version, 1)

    // Create a local cluster from a specified config
    static member internal CreateLocalCluster config = 
        Cluster.CreateLocalCluster (config.Name,
                                          config.Version,
                                          config.NumClients, 
                                          (if config.ContainerInAppDomain then LocalClusterContainerMode.AppDomain else LocalClusterContainerMode.Process),
                                          config.ClientPath,
                                          config.NumJobPortsPerClient,
                                          config.PortsRange)

    member val internal MsgToHost=List<(ControllerCommand*MemStream)>() with get
    member val internal PeerIndexFromEndpoint : ConcurrentDictionary<int64, int> = ConcurrentDictionary<_,_>() with get
    member internal x.Queues with get() = queues
                             and set( q ) = queues <- q
    member val internal QueuesInitialized = 0 with get, set
    member internal x.ExpectedClusterSize with get() = expectedClusterSize
                                          and set(sz) = expectedClusterSize <- sz
    member internal x.MasterInfo with get() = masterInfo
    member internal x.ClusterInfo with get() = clusterStatus
                                  and set( v ) = clusterStatus <- v
    /// <summary>
    /// Return a list of node that form the cluster
    /// </summary>
    member x.Nodes with get() = clusterStatus.ListOfClients
    member val internal ClusterMode = ClusterMode.Uninitialized with get, set
    /// Return name of the cluster 
    member x.Name with get() = clusterStatus.Name
    /// Return version of the cluster 
    member x.Version with get() = clusterStatus.Version
    /// Return version of the cluster as a printable string 
    member x.VersionString with get() = StringTools.VersionToString( clusterStatus.Version )
    /// Return number of nodes. 
    member x.NumNodes with get() = x.Nodes.Length
    member internal x.ReplicationType with get() = clusterStatus.ReplicationType

    member private x.StartCluster (masterFile : string option, clusterInfo : ClusterInfo) =
        x.ReadClusterInfo(clusterInfo)
        if Option.isSome(masterFile) then
            x.SetMaster(masterFile.Value)
        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "attempt to connect to cluster node" ))
        x.BeginCommunication()

    member private x.StartCluster (masterFile : string option, cluster : string) =
        let m = localNRegex.Match(cluster)
        let info, pwd =
            if m.Success then
                let n = Int32.Parse(m.Groups.[1].Value)
                let cl = Cluster.CreateLocalCluster((sprintf "local-%i" n), defaultLocalClusterVersion, n)
                cl.ClusterInfo |> Some, None
            else if cluster = "local" then
                let cl = Cluster.CreateLocalCluster("local-1", defaultLocalClusterVersion)
                cl.ClusterInfo |> Some, None
            else if (not (StringTools.IsNullOrEmpty( cluster ))) && (File.Exists cluster) then 
                ClusterInfo.Read( cluster )           
            else
                None, None
        if Option.isNone(info) then
            let s= sprintf "Fail to parse cluster specification '%s' correctly" cluster
            Logger.Log( LogLevel.Error, (s))
            failwith (s)
        
        info.Value.Persist()
        x.StartCluster(masterFile, info.Value)
        match pwd with
        | Some p -> let connects : NetworkConnections = Cluster.Connects
                    connects.InitializeAuthentication(p)
        | None -> ()
        
    /// <summary>
    /// Construct a Cluster from cluster description.
    /// <param name="cluster">
    /// It can either be 
    /// * a path to cluster file, which should be a .inf file contains the description of the cluster
    /// * or a string in the format "local" or "local[n]", which means a local cluster with 1 client or n clients respectively
    ///   In this case, both the clients and containers are started as an AppDomain. 
    ///   the system uses DeploymentSettings.LocalClusterNumJobPortsPerClient and DeploymentSettings.LocalClusterStartingPort. If these
    ///   predefined ports are not accessible, the local cluster may not function properly. For user who wants to have full control
    ///   of the configuration, please use the constructor that takes LocalClusterConfig as parameter
    /// </param>
    /// </summary>
    new (cluster:string ) as x =
        Cluster( )       
        then 
            x.StartCluster(None, cluster)

    /// <summary>
    /// Construct a Prajna Local cluster from configuration.
    /// <param name="config">User supplied local cluster configuration</param>
    /// </summary>
    new (config : LocalClusterConfig ) as x =
        Cluster()       
        then 
            let cl = Cluster.CreateLocalCluster(config)
            x.StartCluster(None, cl.ClusterInfo)
            
    /// <summary>
    /// Construct a Cluster from a cluster description, with optionally a master node. 
    /// <param name="masterFile"> The master node may be used to query for the cluster used. </param>
    /// <param name="cluster">
    /// It can either be 
    /// * a path to cluster file, which should be a .inf file contains the description of the cluster
    /// * or a string in the format "local" or "local[n]", which means a local cluster with 1 client or n clients respectively
    ///   In this case, both the clients and containers are started as an AppDomain. 
    ///   the system uses DeploymentSettings.LocalClusterNumJobPortsPerClient and DeploymentSettings.LocalClusterStartingPort. If these
    ///   predefined ports are not accessible, the local cluster may not function properly. For user who wants to have full control
    ///   of the configuration, please use the constructor that takes LocalClusterConfig as parameter
    /// </param>
    /// </summary>
    new (masterFile, cluster:string ) as x =
        Cluster( ) 
        then 
            x.StartCluster(masterFile, cluster)

    /// <summary>
    /// Construct a Cluster from a cluster description, with optionally a master node. 
    /// <param name="masterFile"> The master node may be used to query for the cluster used. </param>
    /// <param name="config">User supplied local cluster configuration</param>
    /// </summary>
    new (masterFile, config:LocalClusterConfig ) as x =
        Cluster( ) 
        then 
            let cl = Cluster.CreateLocalCluster(config)
            x.StartCluster(masterFile, cl.ClusterInfo)
    /// Construct a Cluster from ClusterInfo
    static member internal ConstructCluster( clusterInfo: ClusterInfo ) = 
            let createClusterFunc() = 
                let cl = Cluster( )
                cl.ClusterInfo <- clusterInfo
                cl
            ClusterFactory.GetOrAddCluster( clusterInfo.Name, clusterInfo.Version.Ticks, createClusterFunc )
    /// <summary>
    /// Construct a single node using the existing cluster information. 
    /// </summary>
    member x.GetSingleNodeCluster( nodeName: string ) = 
        if StringTools.IsNullOrEmpty( nodeName ) then 
            failwith ( sprintf "GetSingleNodeCluster: input nodename can't be null") 
        else
            let clInfo = ClusterInfo( x.ClusterInfo, nodeName )
            if Utils.IsNull clInfo then 
                failwith ( sprintf "GetSingleNodeCluster: can't find node %s in cluster %A" nodeName x ) 
            else
                clInfo.Persist()
                Cluster.ConstructCluster( clInfo )
    
    /// <summary>
    /// Construct a single node using the existing cluster information. 
    /// </summary>
    member x.GetSingleNodeCluster( nodeIndex: int ) = 
        if nodeIndex < 0 || nodeIndex >= x.NumNodes then 
            failwith (sprintf "Try to construct a single node cluster from %d node of %A, which has only %d nodes "
                                nodeIndex x x.NumNodes)
        else
            let clInfo = ClusterInfo( x.ClusterInfo, nodeIndex )
            if Utils.IsNull clInfo then 
                null 
            else
                clInfo.Persist()
                Cluster.ConstructCluster( clInfo )

    /// <summary>
    /// Construct a single node using the existing cluster information. 
    /// </summary>
    static member GetCombinedCluster( clusters: seq<Cluster> ) = 
        let numClusters = Seq.length clusters
        if numClusters <= 0 then 
            null 
        elif numClusters = 1 then 
            Seq.last clusters
        else
            // More than one clusters
            let nodeLists =List<_>()
            use sha256 = new System.Security.Cryptography.SHA256Managed() 
            let mutable maxTicks = DateTime.MinValue.Ticks
            let mutable clType = Unchecked.defaultof<_>
            for cl in clusters do 
                if not (Utils.IsNull cl) then 
                    for nodei = 0 to cl.NumNodes - 1 do 
                        nodeLists.Add ( cl.Nodes.[nodei] )
                    let namebuf = System.Text.Encoding.UTF8.GetBytes(cl.Name)
                    sha256.TransformBlock( namebuf, 0, namebuf.Length, namebuf, 0 ) |> ignore 
                    let verbuf = System.BitConverter.GetBytes( cl.Version.Ticks )
                    sha256.TransformBlock( verbuf, 0, verbuf.Length, verbuf, 0 ) |> ignore 
                    if ( cl.Version.Ticks > maxTicks ) then 
                        maxTicks <- cl.Version.Ticks 
                        clType <- cl.ClusterInfo.ClusterType
            sha256.TransformFinalBlock( [||], 0, 0 ) |> ignore 
            let hash = sha256.Hash
            let newClusterName = "Combined_" + BytesTools.BytesToHex( hash )                       
            let clusterBase = ClusterInfoBase( clType, DateTime( maxTicks ), nodeLists.ToArray(), Name = newClusterName ) 
            let newClusterInfo = ClusterInfo( clusterBase ) 
            newClusterInfo.Persist()
            Cluster( ClusterInfo = newClusterInfo )

    /// <summary>
    /// Use a single node at the current cluser
    /// </summary>
    member x.UseSingleNodeCluser( nodeName: string ) = 
        let cl = x.GetSingleNodeCluster( nodeName ) 
        if Utils.IsNull cl then 
            Cluster.Current <- None
        else
            Cluster.Current <- Some cl

    /// <summary>
    /// Use a single node at the current cluser
    /// </summary>
    member x.UseSingleNodeCluser( nodeIndex: int ) = 
        let cl = x.GetSingleNodeCluster( nodeIndex ) 
        if Utils.IsNull cl then 
            Cluster.Current <- None
        else
            Cluster.Current <- Some cl


    member private x.SearchForEndPointInternal( ep:IPEndPoint ) = 
            // bMapped will not be null at this point. 
        // Use forward mapping if that is available. 
        if Utils.IsNotNull x.Queues then 
            if Utils.IsNull bMapped then 
                // Make sure bMapped is not null 
                while Utils.IsNull bMapped do 
                    Interlocked.CompareExchange( &bMapped, Array.create x.NumNodes false, null ) |> ignore 
            for i=0 to bMapped.Length-1 do 
                if not bMapped.[i] then 
                    let queuePeer = x.Queues.[i]
                    if (Utils.IsNotNull queuePeer) && (Utils.IsNotNull queuePeer.RemoteEndPoint) then 
                        match queuePeer.RemoteEndPoint with 
                        | :? IPEndPoint as ep1 ->
                            endpointToPeer.Item( ep1 ) <- i
                            bMapped.[i] <- true
                        | _ -> 
                            ()
        let refValue = ref Unchecked.defaultof<_>                       
        if endpointToPeer.TryGetValue( ep, refValue ) then 
            !refValue 
        else
            // JinL: 12/31/2014, ideally, the function should write as a GetOrAdd, however, a remote end point will only map to one peer index. 
            //       Since we don't expect conflict update, we will leave the code as is. 
            // Sanjeevm: 02/25/2014
            //printfn "DNS Address %A" ep.Address
            let mutable nMatches = 0 
            let mutable idxPeer = -1
            try
                let hostName = LocalDNS.GetHostByAddress( ep.Address.GetAddressBytes() )
                let nodes = x.Nodes
                for i = 0 to x.NumNodes-1 do 
                    if hostName.IndexOf( nodes.[i].MachineName, StringComparison.OrdinalIgnoreCase )>=0 then 
                        idxPeer <- i    
                        nMatches <- nMatches + 1
            with e ->
                nMatches <- 2

            if nMatches = 1 then 
                endpointToPeer.Item( ep ) <- idxPeer
                idxPeer
            else
                nMatches <- 0 
                let mutable idxPeer = -1
                let nodes = x.Nodes
                let eaddr = ep.Address.GetAddressBytes()
                for i = 0 to x.NumNodes-1 do
                    if Utils.IsNotNull nodes.[i].InternalIPAddress then
                        for cmpAddr in nodes.[i].InternalIPAddress do
                            if System.Linq.Enumerable.SequenceEqual( eaddr, cmpAddr ) then 
                                idxPeer <- i
                                nMatches <- nMatches + 1
                    if Utils.IsNotNull nodes.[i].ExternalIPAddress then
                        for cmpAddr in nodes.[i].ExternalIPAddress do
                            if System.Linq.Enumerable.SequenceEqual( eaddr, cmpAddr ) then 
                                idxPeer <- i
                                nMatches <- nMatches + 1
                if nMatches <> 1 then 
                    // If we can't determine the peer, we will use -1
                    idxPeer <- -1    
                endpointToPeer.Item( ep ) <- idxPeer
                idxPeer

    member internal x.SearchForEndPoint( queue: NetworkCommandQueue ) = 
        match queue.RemoteEndPoint with 
        | :? IPEndPoint as ep ->
            let refValue = ref Unchecked.defaultof<_>   
            if endpointToPeer.TryGetValue( ep, refValue ) then 
                !refValue
            else
                x.SearchForEndPointInternal( ep )
        | _ -> 
            -1
    member internal x.AssignEndPoint( queue: NetworkCommandQueue, peerIndex ) = 
        match queue.RemoteEndPoint with 
        | :? IPEndPoint as ep ->
            endpointToPeer.Item( ep ) <- peerIndex
        | _ -> 
            ()
    /// Return an array of ClientInfo[] * int[], in which the first is the information of the nodes (i.e., x.Nodes), the second is the resource of
    /// each node 
    member internal x.GetNodeIDsWithResource(usePartitionBy) = 
        x.ClusterInfo.GetNodeIDsWithResource(usePartitionBy)
    // This function should be called to update 
    // expectedClusterSize, and fill in the loadBalancer with right IDs & resource information 
    // for cluster operation
    member internal x.UpdateCluster() = 
        if Utils.IsNotNull x.Nodes && x.Nodes.Length>0  then 
            expectedClusterSize <- x.Nodes.Length
        else
            expectedClusterSize <- 0
//        let mappedNodes = 
//            mapping |> Array.map ( Array.map ( fun i -> 
//                            let node, resource = nodes.[i]
//                            node ) )

    member internal x.ReadClusterInfo( clusterFile:string ) = 
        if not (StringTools.IsNullOrEmpty( clusterFile )) then 
            let o , _ = ClusterInfo.Read( clusterFile )
            match o with 
            | Some ( cl ) -> 
                x.ReadClusterInfo(cl)
            | None -> 
                ()

    member internal x.ReadClusterInfo( clusterInfo:ClusterInfo ) = 
        clusterStatus <- clusterInfo
        x.UpdateCluster()

    member internal x.SetMaster( masterFile:string ) = 
        if not ( StringTools.IsNullOrEmpty( masterFile ) ) then
            masterInfo <- ClientMasterConfig.Parse( masterFile ) 
            clusterStatus.ControllerName <- masterInfo.MasterName
            clusterStatus.ControllerPort <- masterInfo.ControlPort
            if masterInfo.MasterName.Length>0 then 
                clusterStatus.ClusterUpdate()
                x.UpdateCluster()
//                clusterUpdateTimer.Change( 10000, 10000 ) |> ignore


    /// <summary>
    /// Get, Set Current Cluster used by DSet, DStream, service launch, etc..
    /// </summary>
    static member val Current : Cluster option = None with get, set
    /// <summary>
    /// Get the default cluster used by DSet, DStream, service launch, etc..
    /// </summary>
    static member GetCurrent() = 
        match Cluster.Current with 
        | Some cl -> cl
        | None -> null
    /// <summary>
    /// Set the default cluster used by DSet, DStream, service launch, etc..
    /// </summary>
    static member SetCurrent( cl ) = 
        if Utils.IsNull cl then 
            Cluster.Current <- None
        else
            Cluster.Current <- Some cl
    /// Get the current connected queue set 
    static member val Connects = NetworkConnections.Current with get
    static member private UpdateCurrent (cluster : Cluster) = 
        if cluster.ExpectedClusterSize>0 then 
            Cluster.Current <- Some( cluster ) 
            // let clusterName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( cur.Name, cur.Version ) 
            // ClusterFactory.Store( clusterName, cur )
            // ClusterFactory.StoreCluster( cur )
        match Cluster.Current with 
        | Some cl -> 
            Cluster.Current <- Some (ClusterFactory.CacheCluster( cl ) )
        | None -> 
            ()
    /// Load a cluster, with optional a master (which may be queryed for cluster allocation), start the connection to their nodes, and use it as the current cluster
    static member Start( masterFile, cluster : string, ?expClusterSize ) =
        Logger.Log( LogLevel.MildVerbose, ("Done initialization of NetworkConnections() "))
        let bHasMasterFile = Utils.IsNotNull masterFile && File.Exists( masterFile) 
        let bHasCluster = Utils.IsNotNull cluster && File.Exists( cluster )

        if bHasMasterFile || bHasCluster then 
            let cur = 
                match expClusterSize with 
                | None -> Cluster( masterFile |> Some, cluster ) 
                | Some( size ) -> 
                    Cluster( masterFile |> Some, cluster, ExpectedClusterSize=size ) 
            Cluster.UpdateCurrent cur
        Logger.Log( LogLevel.WildVerbose, ("cluster/master information loaded from file, calling Cluster.Connects.Initialize"))
        Cluster.Connects.Initialize()
        ()
    /// Load a cluster, start the connection to their nodes, and use it as the current cluster
    static member StartCluster( cluster : string ) = 
        Cluster.Start( null, cluster )
    /// Stop all connnections and disconnect all clusters. 
    static member Stop() = 
        Logger.Log( LogLevel.ExtremeVerbose, ("Shutdown all clusters, no more jobs should be executed."))
        CleanUp.Current.CleanUpAll()
    interface IComparable<Cluster> with 
        member x.CompareTo y = 
            if System.Object.ReferenceEquals( x, y ) then 
                0
            else
                let xStatus = x.ClusterInfo
                let yStatus = y.ClusterInfo
                let mutable retVal = (xStatus :> IComparable<_>).CompareTo( yStatus )
                retVal
    interface IComparable with 
        member x.CompareTo other =
            match other with 
            | :? Cluster as y ->
                ( x :> IComparable<_>).CompareTo y
            | _ -> 
                InvalidOperationException() |> raise
    interface IEquatable<Cluster> with 
        member x.Equals y = 
            ( x :> IComparable<_>).CompareTo( y ) = 0
    override x.Equals other = 
        ( x :> IComparable ).CompareTo( other ) = 0
    override x.GetHashCode() =
        hash( x.ClusterInfo )
    /// This change from using lazy avoid the member variable to be evaluated during Debug examinationof Cluster, causing 
    /// issue at Debug time. 
    member val private CommunicationInited = ref 0 with get
    member val private CommunicationInitStarted = ref 0 with get
        
//    member val private CommunicationInited = lazy( 
//            if Utils.IsNotNull thisInstance.Nodes then 
//                thisInstance.Queues <- Array.create<NetworkCommandQueue> thisInstance.NumNodes null
//                bFailedMsgShown <- Array.create thisInstance.NumNodes false
//            if Utils.IsNull bFailedMsgShown || bFailedMsgShown.Length <> thisInstance.NumNodes then
//                failwith(sprintf "Failed to begin communication for cluster %s:%s" thisInstance.Name (VersionToString(thisInstance.Version)))
//            thisInstance.ClusterMode <- ClusterMode.Connected
//            Object()
//            ) with get
    /// BeginCommunication: always called to ensure proper initialization of cluster structure
    member internal x.BeginCommunication() = 
        while !x.CommunicationInited = 0 do
            lock ( x.CommunicationInitStarted ) ( fun _ -> 
                if Interlocked.CompareExchange( x.CommunicationInitStarted, 1, 0) = 0 then 
                    if Utils.IsNotNull thisInstance.Nodes then 
                        thisInstance.Queues <- Array.create<NetworkCommandQueue> thisInstance.NumNodes null
                        bFailedMsgShown <- Array.create thisInstance.NumNodes false
                    if Utils.IsNull bFailedMsgShown || bFailedMsgShown.Length <> thisInstance.NumNodes then
                        failwith(sprintf "Failed to begin communication for cluster %s:%s" thisInstance.Name (VersionToString(thisInstance.Version)))
                    thisInstance.ClusterMode <- ClusterMode.Connected
                    Volatile.Write( x.CommunicationInited, 1)
            )


    member internal x.QueueForWrite( peeri ) : NetworkCommandQueue =
        x.BeginCommunication()
        try 
            let queue = x.Queues.[peeri]
            if Utils.IsNull queue then
                x.Queues.[peeri] <- 
                    NodeConnectionFactory.Current.DaemonConnect( x.Nodes.[peeri].MachineName, x.Nodes.[peeri].MachinePort, 
                        (fun queue -> Cluster.ParseHostCommand queue peeri), 
                        (fun _ -> Logger.LogF (LogLevel.MildVerbose, fun _ -> sprintf "Queue %d of cluster %A disconnects ... " peeri x)
                                  x.Queues.[peeri] <- null) )
                x.PeerIndexFromEndpoint.[x.Queues.[peeri].RemoteEndPointSignature] <- peeri
                // Read some receiving command to unblock
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Attempt to connect to %s:%d as peer %d" x.Nodes.[peeri].MachineName x.Nodes.[peeri].MachinePort peeri ))
                x.Queues.[peeri]
            else
                queue
        with 
        | e -> 
            let msg = sprintf "Cluster.QueueForWrite, exception %A" e
            Logger.Log( LogLevel.Error, msg )
            failwith msg
            
    member internal x.Queue( peeri ) = 
        x.Queues.[peeri]  
       
    member internal x.PeerAliveStatus( ) = 
        let bStatus = Array.zeroCreate<_> x.NumNodes
        for peeri = 0 to x.NumNodes - 1 do
            let queue = x.Queue (peeri)
            bStatus.[peeri] <- not (NetworkCommandQueue.QueueFail(queue))
        bStatus
    /// Split a sequence by peer information only to multiple subsequences. 
    /// (Usually to be sent over the network). 
    member internal x.SplitByPeers<'U> peeri (lst:seq<'U>) = 
        let bPeerStatus = x.PeerAliveStatus()
        let mutable nLives, curPeer = 0, peeri
        for peeri = 0 to bPeerStatus.Length - 1 do 
            if not bPeerStatus.[peeri] then 
                nLives <- nLives - 1
                curPeer <- curPeer - 1
        if nLives > 0 && curPeer >= 0 && bPeerStatus.[peeri] then 
            let newLst = List<'U>()
            let mutable cnt = 0
            for obj in lst do 
                if cnt % nLives = curPeer then 
                    newLst.Add( obj ) 
                cnt <- cnt + 1
            newLst.ToArray()
        else
            null
    member internal x.CloseQueue( peeri ) = 
        x.Queues.[peeri].Close()

    member internal x.CloseQueueAndRelease( peeri ) = 
        let q = x.Queues.[peeri]
        if Utils.IsNotNull q then 
            x.CloseQueue( peeri )
            x.Queues.[peeri] <- null
            Cluster.Connects.RemoveConnect( q )
    member internal x.FlushCommunication() = 
        let t1 = (PerfDateTime.UtcNow())
        let mutable bAllEmpty = false
        // Wait for all active command to send out
        while not bAllEmpty && (PerfDateTime.UtcNow()).Subtract(t1).TotalMilliseconds < DeploymentSettings.TimeoutBeforeDisconnect do
            bAllEmpty <- true
            for pi = 0 to x.Queues.Length-1 do
                let queue = x.QueueForWrite( pi )
                if Utils.IsNotNull queue && queue.CanSend && queue.SendQueueLength>0 then 
                    bAllEmpty <- false
            if not bAllEmpty then 
                Thread.Sleep( 1 ) 
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Take %f milliseconds to flushout all connection for Cluster %s" ((PerfDateTime.UtcNow()).Subtract(t1).TotalMilliseconds) x.ClusterInfo.Name ))
    /// EndCommunication: called to shutdown the communication pipes to clients 
    member internal x.EndCommunication() = 
        x.FlushCommunication()
        for i = 0 to x.Queues.Length-1 do
            x.CloseQueueAndRelease( i ) 
    static member internal RegisterStaticParser( commandset, callbackClass ) = 
        for command in commandset do
            let jobCommandQueue = staticCallback.GetOrAdd( command, fun _ -> ConcurrentQueue<_>() )
            jobCommandQueue.Enqueue( callbackClass )
    /// Register Callback for Cluster
    /// commandset: a set of command (usually in the form of array) that the call back function will interpret
    ///     Wildcard is allowed in the form of Verb.Unknown*_, _*Noun.Unknown
    /// Name, ver: 
    ///     For each command, the Parsecommand will first read a string and a int64 parameter, 
    ///     it will matches with callback.
    ///         string*int64, wildcard is allowed in the form ofstring*0L, null*int64, or null*0L
    member internal x.RegisterCallback( jobID, name, ver, commandset, callbackClass ) =
        for command in commandset do
            let jobCommandSet = commandCallback.GetOrAdd( command, fun _ -> new ConcurrentDictionary<_,_>())
            let _, commandSet = jobCommandSet.GetOrAdd( jobID, fun _ -> ref DateTime.UtcNow.Ticks, new ConcurrentDictionary<_,_>( (StringTComparer<int64>(StringComparer.Ordinal)) ) )
            commandSet.Item( (name, ver) ) <- (x, callbackClass)
        Logger.LogF( jobID, LogLevel.WildVerbose, ( fun _ -> sprintf "Register commands %A for job %A" commandset jobID))
    /// Unregister Callback
    member internal x.UnRegisterCallback( jobID, commandset:seq<ControllerCommand> ) = 
        if Utils.IsNotNull commandset && Seq.length(commandset)>0 then 
            for command in commandset do
                let bExist, dic = commandCallback.TryGetValue(command)
                if bExist then 
                    let bRemove, jobCommandSet = dic.TryRemove( jobID )
                    if bRemove && dic.IsEmpty then  
                        commandCallback.TryRemove(command) |> ignore
        else
            let callbackArray = commandCallback.Keys |> Seq.toArray
            x.UnRegisterCallback( jobID, callbackArray )
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Unregister commands %A for job %A" commandset jobID ))
    /// Unregister Callback
    member internal x.UnRegisterOneCallback( jobID, name, ver, commandset:seq<ControllerCommand> ) = 
        if Utils.IsNotNull commandset && Seq.length(commandset)>0 then 
            for command in commandset do
                let bExist, dic = commandCallback.TryGetValue(command)
                if bExist then 
                    let bExist, tuple = dic.TryGetValue( jobID )
                    if bExist then 
                        let _, jobCommandSet = tuple 
                        jobCommandSet.TryRemove( (name, ver) ) |> ignore 
                        if Utils.IsNull name || ver=0L then 
                            let allitems = jobCommandSet |> Seq.toArray 
                            for pair in allitems do  
                                let itemname, itemver = pair.Key 
                                if (Utils.IsNull name || (String.CompareOrdinal( itemname,name)=0 ) )  && (ver=0L || itemver=ver) then  
                                    jobCommandSet.TryRemove( (itemname, itemver) ) |> ignore
                        if jobCommandSet.IsEmpty then 
                            let bRemove, jobCommandSet = dic.TryRemove( jobID )
                            ()
//                            JinL: 9/10/2015, we don't further remove the command entry, so that received network command towards cancelled job will be thrown away ... 
//                            if bRemove && dic.IsEmpty then  
//                                commandCallback.TryRemove(command) |> ignore
        else
            let callbackArray = commandCallback.Keys |> Seq.toArray
            x.UnRegisterOneCallback( jobID, name, ver, callbackArray )
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Unregister commands %A for job %A" commandset jobID ))

    static member internal ParseHostCommand (q : NetworkCommandQueue) (i : int) (command : NetworkCommand) =
        if Utils.IsNull q then 
            Unchecked.defaultof<_>
        else
            //let i = q.Index
            let (pendingCmd, pendingCmdTime) = q.PendingCommand
            let timeNow = (PerfDateTime.UtcNow())

            let cmdOpt, t1 = 
                match pendingCmd with
                | Some (pcmd) ->
                    pendingCmd, pendingCmdTime
                | None -> 
                    Some(command.cmd, command.ms), timeNow
            q.PendingCommand <- (None, timeNow)

            match cmdOpt with 
            | Some( cmd, ms ) ->
                let orgpos = ms.Position
                let mutable bNotBlocked = true
                let mutable bJobIDFound = true 
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "ParseHostCommand, from %A, received Command %A... " q.RemoteEndPoint cmd ))
                try 
                    let jobIDRef = ref Guid.Empty
                    let mutable name = null
                    let mutable ver = 0L
                    let mutable fullname = null
                    let cbItem = ref null
                    let cbRefTuple = ref (null, null)
                    let mutable cb : NetworkCommandCallback = null
                    let mutable x : Cluster = null
                    commandCallback.TryGetValue( cmd, cbItem ) |> ignore
                    // Try wild card Verb matching next, 
                    if Utils.IsNull !cbItem  then 
                        commandCallback.TryGetValue( ControllerCommand( ControllerVerb.Unknown, cmd.Noun), cbItem ) |> ignore
                        if Utils.IsNull !cbItem then 
                            commandCallback.TryGetValue( ControllerCommand( cmd.Verb, ControllerNoun.Unknown), cbItem ) |> ignore
                    let jobCommandSet = !cbItem
                    if not (Utils.IsNull jobCommandSet) then 
                        jobIDRef := ms.ReadGuid()
                        name <- ms.ReadString()
                        ver <- ms.ReadInt64()
                        let bExist, tuple = jobCommandSet.TryGetValue( !jobIDRef ) 
                        if bExist then 
                            let ticksRef, callbackItem = tuple 
                            ticksRef := DateTime.UtcNow.Ticks
                            let ret = callbackItem.TryGetValue( (name, ver), cbRefTuple )
                            if (not ret) then
                                let ret = callbackItem.TryGetValue( (name, 0L), cbRefTuple )
                                if (not ret) then
                                    let ret = callbackItem.TryGetValue( (null, ver), cbRefTuple )
                                    if (not ret) then
                                        let ret = callbackItem.TryGetValue( (null, 0L), cbRefTuple )
                                        if (ret) then
                                            x <- fst !cbRefTuple
                                            cb <- snd !cbRefTuple
                                        else
                                            // We find an entry of the jobCommandSet, but doesn't find particular DSet (name, ver)
                                            // It is possible that the job are being cancelled. 
                                            bJobIDFound <- false 
                                            // Raise Verbose Level, otherwise, too many log message during a cancelled UnitTest job
                                            Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "(OK, job may be cancelled) Receive cmd %A from peer %d with job ID %A payload of %dB, but there is no parsing logic, command will be discarded!" 
                                                                                                    cmd i !jobIDRef (ms.Length) ))
                                    else
                                        x <- fst !cbRefTuple
                                        cb <- snd !cbRefTuple
                                else
                                    x <- fst !cbRefTuple
                                    cb <- snd !cbRefTuple
                            else
                                x <- fst !cbRefTuple
                                cb <- snd !cbRefTuple
                        else
                            // Receive a command that we should parse, but the corresponding jobID has been eliminated .. 
                            bJobIDFound <- false 
                            // Can't find the jobID associated with the return command, we assume that the job has already been cancelled, and the command may be thrown away 
                            // The log level can be reduced later when the code stablize
                            // Raise Verbose Level, otherwise, too many log message during a cancelled UnitTest job
                            Logger.LogF( !jobIDRef, LogLevel.ExtremeVerbose, (fun _ -> sprintf "(OK, job may be cancelled) Receive cmd %A from peer %d with payload of %dB, but there is no parsing logic!" 
                                                                                                    cmd i (ms.Length) ))
                        if Utils.IsNotNull cb then 
                            if Utils.IsNotNull x then 
                                let bExist, peerIndex = x.PeerIndexFromEndpoint.TryGetValue(q.RemoteEndPointSignature)
                                if bExist then 
                                    bNotBlocked <- cb.Callback( cmd, peerIndex, ms, !jobIDRef, name, ver, x )
                                else
                                    bNotBlocked <- cb.Callback( cmd, -1, ms, !jobIDRef, name, ver, x )
                            else
                                bNotBlocked <- cb.Callback( cmd, -1, ms, !jobIDRef, name, ver, null )
                        else 
                            let bExist, staticCommandCallback = staticCallback.TryGetValue( cmd )
                            if bExist then 
                                let mutable bProccessed = false
                                for callback in staticCommandCallback do 
                                    if not bProccessed then 
                                        bProccessed <- callback.Callback( cmd, i, ms, !jobIDRef, name, ver, null )
                                        if bProccessed then 
                                            bJobIDFound <- false     
                    if Utils.IsNull cb && bJobIDFound then 
                        match ( cmd.Verb, cmd.Noun ) with
                        | ( ControllerVerb.Unknown, _ ) ->
                            ()
                        | ( ControllerVerb.Error, _ ) ->
                            let msglen = ms.Length
                            let msg = 
                                try ms.ReadString() with e -> "Failed when try to parse return Error message."
                            Logger.Log( LogLevel.Info, ( sprintf "From peer %d, Error received... Command %A...  %s" i cmd msg ))
                            // q.MarkFail()
                            // We are not going to swallow error, and will simply stop the process. 
                            let ex = System.Runtime.Remoting.RemotingException( msg )
                            raise (ex )
                        | ( ControllerVerb.Warning, _ ) ->
                            let msglen = ms.Length
                            let msg = 
                                try ms.ReadString() with e -> "Failed when try to parse return Warning message."
                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "From peer %d, Warning received... Command %A...  %s" i cmd msg ))
                        | ( ControllerVerb.Verbose, _ ) ->
                            let msglen = ms.Length
                            let msg = 
                                try ms.ReadString() with e -> "Failed when try to parse return Warning message."
                            Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "From peer %d, Command %A...  %s" i cmd msg ))
                        | ( ControllerVerb.Info, _ ) ->
                            let msglen = ms.Length
                            let msg = 
                                try ms.ReadString() with e -> "Failed when try to parse return Info message."
                            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "From peer %d, Info received... %s" i msg ))
                        | ( ControllerVerb.Acknowledge, noun ) ->
                            // Client has received Current Cluster Info
                            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "From peer %d Acknowledge %A received... Parameter %dB" i noun ms.Length ))
                            ()
                        | ( ControllerVerb.Echo2, ControllerNoun.Job ) ->
                            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Echo2, Job from client"))
                        | _ -> 
                            Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "(OK, parsed by other parser) Receive cmd %A from peer %d with payload %A, but there is no parsing logic!" cmd i (ms.GetBuffer()) ))
                    if not bNotBlocked then 
                    // JinL: 06/24/2014
                    // Blocking, the command cannot be further processed (e.g., too many read pending, will need to block further read of the receiving queue 
                        ms.Seek( orgpos, SeekOrigin.Begin ) |> ignore  
                        // Block this command for reprocessing. 
                        q.PendingCommand <- ( cmdOpt, t1 )
                        Logger.Do( LogLevel.ExtremeVerbose, ( fun _ -> 
                               if timeNow.Subtract( q.LastMonitorPendingCommand ).TotalSeconds > DeploymentSettings.MonitorClientBlockingTime then 
                                   // Reset last monitored time
                                   q.LastMonitorPendingCommand <- timeNow
                                   Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> 
                                      sprintf "peer %d is blocked (too many pending received DSet?)" 
                                          i ))
                           ) )
                with
                | ex ->
                    let msg = (sprintf "Receive cmd %A from peer %d, during processing, incur exception %A " cmd i ex )    
                    Logger.Log( LogLevel.Info, msg )
            | None -> 
                ()

            q.GetPendingCommandEventIfNeeded()

    member val private QueuesInitFunc = lazy(
        for peeri=0 to thisInstance.NumNodes-1 do
            let queue = thisInstance.QueueForWrite(peeri)
            if Utils.IsNotNull queue then
                queue.Initialize()
        thisInstance.QueuesInitialized = 1
        ) with get

    /// Initialize write queues
    member internal x.InitializeQueues() =
        x.BeginCommunication()        
        x.QueuesInitFunc.Force() |> ignore

    /// Start connection to all peers
    member internal x.ConnectAll() = 
        for peeri=0 to x.NumNodes-1 do
            x.QueueForWrite(peeri) |> ignore
        x.InitializeQueues()

    /// Disconnect all peers. 
    member internal x.DisconnectAll() = 
        for peeri=0 to x.NumNodes-1 do
            x.CloseQueueAndRelease(peeri) 

    /// Show Cluster Information
    override x.ToString() = 
        if Utils.IsNull x.ClusterInfo then "NULL" else x.ClusterInfo.ToString()

    /// Pack cluster to a byte stream
    member internal x.Pack( ms: MemStream ) = 
        x.ClusterInfo.Pack( ms )
    /// Unpack cluster 
    static member internal Unpack( ms: MemStream ) = 
        let clusterInfoOption = ClusterInfo.Unpack( ms )
        match clusterInfoOption with
        | Some clusterInfo -> 
            Cluster.ConstructCluster( clusterInfo )
        | None _ -> 
            null


and internal ClusterFactory() = 
    inherit CacheFactory<Cluster>()
    /// Try to Find a Cluster with (name, verNumber)
    /// Return cluster if found, 
    /// return null if not found. 
    static member FindCluster( clname, ticks ) = 
        let verCluster = DateTime( ticks )
        let clusterName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( clname, verCluster ) 
        match ClusterFactory.Retrieve( clusterName ) with 
        | Some (cl ) -> cl
        | None -> 
            if File.Exists clusterName then 
                let loadCluster = Cluster( "" |> Some, clusterName )
                ClusterFactory.Store( clusterName, loadCluster )
                loadCluster
            else
                null
    /// Store Cluster Information
    static member StoreCluster( cur:Cluster ) = 
        let clusterName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( cur.Name, cur.Version ) 
        ClusterFactory.Store( clusterName, cur )
    /// Cache Cluster Information 
    static member CacheCluster( cur:Cluster ) = 
        let clusterName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( cur.Name, cur.Version ) 
        let newCur =  ClusterFactory.CacheUseOld( clusterName, cur )
        if not ( File.Exists clusterName ) then 
            newCur.ClusterInfo.Save( clusterName ) |> ignore
        newCur
    /// Get or add a cluster
    static member GetOrAddCluster( clname, ticks:int64, addFunc: unit -> Cluster ) = 
        let clusterName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( clname, DateTime(ticks) ) 
        let cl, _ = ClusterFactory.GetOrAdd( clusterName, addFunc )
        cl
        
