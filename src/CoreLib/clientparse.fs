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
        parse.fs
  
    Description: 
        For Prajna Client: Parse command and execute

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Generic
open System.Net
open System.Threading
open System.IO
open System.Net.Sockets
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Network
open Prajna.Core
open System.Collections.Concurrent

open Prajna.Service

type internal GetRemoteStorageInfoBy = 
    | GetRemoteStorageInfoBySystemManagementCall 
    | GetRemoteStorageInfoByRequest
    | GetRemoteStorageInfoUnnecessary
    | GetRemoteStorageInfoImpossible

type internal RemoteConfig() = 
    /// This somehow doesn't work, though the synchronous version works. 
    static member AsyncGetDriveSpace( machineName ) = 
        async {
            try
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Start to get remote drive information for machine %s" machineName ))
                let bCompleted = ref false
                let driveList = ConcurrentQueue<_>()
                let srvNameSpace = @"\\"+ machineName + @"\root\cimv2"
                let oms = System.Management.ManagementScope( srvNameSpace )
                let oQuery = System.Management.ObjectQuery( "select FreeSpace, Size, Name from Win32_LogicalDisk where DriveType=3" )       
                use oSearch = new System.Management.ManagementObjectSearcher( oms, oQuery )
                let result = System.Management.ManagementOperationObserver()
                result.ObjectReady.Add ( fun obj ->  let oRet = obj.NewObject
                                                     let freeSpace:uint64 = unbox(oRet.GetPropertyValue("FreeSpace"))
                                                     let size:uint64 =  unbox(oRet.GetPropertyValue("FreeSpace"))
                                                     let drName = oRet.GetPropertyValue("Name") :?> string
                                                     driveList.Enqueue( drName, freeSpace, size )
                                        )
                result.Completed.Add( fun obj -> bCompleted := true )
                oSearch.Get( result )
                let t1 = (PerfDateTime.UtcNow())
                while not (!bCompleted) && (PerfDateTime.UtcNow()).Subtract(t1).TotalSeconds < (DeploymentSettings.TimeOutGetRemoteStorageInfo) do 
//                    do! Async.Sleep ( 100 ) 
                    SystemBug.Sleep( 100 )
                if !bCompleted then 
                    return driveList.ToArray()
                else 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "AsyncGetDriveSpace, get remote storage information for machine %s timeout in %0.3f sec with %d entries " machineName ((PerfDateTime.UtcNow()).Subtract(t1).TotalSeconds) driveList.Count ))
                    return driveList.ToArray()
            with 
            | e -> 
                let msg = sprintf "AsyncGetDriveSpace, fail to retrieve remote storage information for machine %s, with exception %A" machineName e
                Logger.Log( LogLevel.Info, msg )
                return Array.empty
        }

    /// Get remote drive information synchronously. 
    static member GetDriveSpace (machineName )  = 
//        let t1 = (PerfDateTime.UtcNow())
        try
            let srvNameSpace = @"\\"+ machineName + @"\root\cimv2"
            let oms = System.Management.ManagementScope( srvNameSpace )
            let oQuery = System.Management.ObjectQuery( "select FreeSpace, Size, Name from Win32_LogicalDisk where DriveType=3" )       
            use oSearch = new System.Management.ManagementObjectSearcher( oms, oQuery )
    //        let t2 = (PerfDateTime.UtcNow())
    //        let elpase2 = t2 - t1
            // Most of the time is spend in the get()
            let oReturnCollection = oSearch.Get()
    //        let t3 = (PerfDateTime.UtcNow())
    //        let elpase3 = t3 - t2
            let drInfo = RemoteConfig.ParseDriveSpace( oReturnCollection )
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "GetDriveSpace, get remote storage information for machine %s with %A " machineName drInfo ))
            drInfo 
        with 
        | e -> 
            /// We will use different features to retrieve remote storage space, stay tuned. 
            let msg = sprintf "GetDriveSpace, fail to retrieve remote storage information for machine %s, with exception %A" machineName e
            Logger.Log( LogLevel.WildVerbose, msg )
            Array.empty

    static member ParseDriveSpace( oReturnCollection ) = 
        let driveList = List<_>()
        for oRet in oReturnCollection do
            let freeSpace:uint64 = unbox(oRet.GetPropertyValue("FreeSpace"))
            let size:uint64 =  unbox(oRet.GetPropertyValue("FreeSpace"))
            let drName = oRet.GetPropertyValue("Name") :?> string
            driveList.Add( drName, freeSpace, size )
        driveList.ToArray()

    static member AsyncGetRemoteStorageInfoViaSystemManagement( cl: Cluster ) =
        let numArray = Array.init cl.NumNodes ( fun i -> i )
        let drInfo = numArray 
                     |> Array.map( fun i -> cl.ClusterInfo.ListOfClients.[i].MachineName ) 
                     |> Array.map( fun machineName -> RemoteConfig.AsyncGetDriveSpace( machineName ) )
        let drJobs = drInfo |> Async.Parallel
        let exceptionCont exn = 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "AsyncGetRemoteStorageInfoViaSystemManagement, exception encountered %A" exn ))
        let cancellationCont ext = 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "AsyncGetRemoteStorageInfoViaSystemManagement, cancellation encountered %A" ext ))
        let RetrievedResult (drList:(string*uint64*uint64)[][]) =
            let clInfo = cl.ClusterInfo
            clInfo.SetAndSaveRemoteStorageInfo( drList )
// Avoid Getting Remote Storage Information. 
        Async.StartWithContinuations( drJobs, RetrievedResult, exceptionCont, cancellationCont )
        ()

    static member GetRemoteStorageInfoViaSystemManagement( cl: Cluster ) =
        let startTask peeri = 
            async {
                let machineName = cl.ClusterInfo.ListOfClients.[peeri].MachineName
                return RemoteConfig.GetDriveSpace( machineName ) 
            }
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Start to retrieve remote storage information"  ))
        let drInfo = cl.ClusterInfo.ListOfClients|> Array.mapi ( fun i node -> startTask i ) |> Async.Parallel |> Async.RunSynchronously
        let clInfo = cl.ClusterInfo
        clInfo.SetAndSaveRemoteStorageInfo( drInfo )
//        let remoteInfo = clInfo.RemoteStorageInfoToString()
//        let clFileName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( cl.Name, cl.Version ) 
//        let storageInfoName = clFileName.Replace( ".inf", ".storage" )
//        StringTools.SaveToFile storageInfoName remoteInfo
//        Logger.LogF( LogLevel.WildVerbose,  fun _ -> sprintf "Save remote storage information for Cluster %s:%s to %s" cl.Name cl.VersionString storageInfoName  )
//        StringTools.MakeFileAccessible( storageInfoName )
// Avoid Getting Remote Storage Information. 
        ()

    static member TaskGetRemoteStorageInfoViaSystemManagement( cl: Cluster ) = 
        Tasks.Task.Run( fun _ -> RemoteConfig.GetRemoteStorageInfoViaSystemManagement(cl ) ) |> ignore


    /// Get the remote storage information of the cluster
    static member GetRemoteStorageInfo( cl: Cluster ) = 
        let clInfo = cl.ClusterInfo
        let methodToGetRemoteStorageInfo = 
            match clInfo.ClusterType with 
            | ClusterType.StandAlone -> 
                GetRemoteStorageInfoBySystemManagementCall             //  Use remote management protocol
            | ClusterType.Azure -> 
                GetRemoteStorageInfoUnnecessary                        //  No Need
            | ClusterType.StandAloneNoAdminForExternal -> 
                if clInfo.GetCurrentPeerIndex()>=0 then 
                    GetRemoteStorageInfoBySystemManagementCall
                else
                    GetRemoteStorageInfoByRequest
            | ClusterType.StandAloneNoAdmin ->
                GetRemoteStorageInfoByRequest
            | ClusterType.StandAloneNoSMB -> 
                GetRemoteStorageInfoImpossible
            | _ -> 
                GetRemoteStorageInfoImpossible
        match methodToGetRemoteStorageInfo with 
        | GetRemoteStorageInfoBySystemManagementCall -> 
            RemoteConfig.TaskGetRemoteStorageInfoViaSystemManagement( cl )
        | _ ->
            // Can't Get Storage Information.  
            ()
        

/// Listener will be run by PrajnaClient to parse input command
[<AllowNullLiteral>]
type internal Listener = 
    val port : int 
    val ip : string
    val connects : ClientConnections
    val listener : Socket
    val callback : Dictionary<Object, ( Object -> bool) >
    val mutable public InListeningState : bool
    val mutable public Activity : bool
    val taskqueue : TaskQueue
    static member SocketForListenWloopback() = 
        let soc = new Socket( AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp )
        try 
            let OptionInValue = BitConverter.GetBytes(1)
            soc.IOControl( DeploymentSettings.SIO_LOOPBACK_FAST_PATH, OptionInValue, null ) |> ignore
        with 
        | e ->
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Fail to set loopback fast path...." ))
        soc
    [<DefaultValue>]
    static val mutable private _Current : Listener 
    // The only constructor is declared private, to ensure that the only way to create a lister is to call StartListener
    private new ( listenIP : string, listenerPort : int ) = 
        {
            ip = listenIP
            port = listenerPort;
            connects = 
                let c = ClientConnections()
                c.Initialize()
                c
            listener =
                let soc = Listener.SocketForListenWloopback()    
                if (listenIP.Equals("", StringComparison.Ordinal)) then
                    soc.Bind( IPEndPoint( IPAddress.Any, listenerPort ) )
                else
                    soc.Bind( IPEndPoint( IPAddress.Parse(listenIP), listenerPort ) )
                soc.Listen( 30 )
                soc
            InListeningState = false
            Activity = false
            callback = Dictionary<Object, ( Object -> bool) >()
            taskqueue = new TaskQueue()
        }
    member x.Port with get() = x.port
    member x.ConnectsClient with get() = x.connects
    member x.Connects with get() = x.connects.Connects 
    member x.Listener with get() = x.listener
    member x.TaskQueue with get() = x.taskqueue
    /// Call back function during listening loop, 
    /// the function takes one parameter, Object,
    /// Return: 
    ///     true: IO activity occurs during call back. 
    ///     false: No IO activity occurs during call back. 
    member x.Callback with get() = x.callback
    /// Error: error in parsing 
    member x.Error ( queue:NetworkCommandQueue, msg ) = 
        Logger.Log( LogLevel.Error, msg )
        let msgError = new MemStream( 1024 )
        msgError.WriteString( msg )
        ( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msgError )

    static member Current with get() = Listener._Current
    static member StartListener( ?listenerIP, ?listenerPort ) = 
        let ip = defaultArg listenerIP DeploymentSettings.ClientIP
        let port = defaultArg listenerPort DeploymentSettings.ClientPort
        Listener._Current <- (new Listener(ip, port) )
        Listener._Current
    static member NullReturn( ) : ControllerCommand * MemStream = 
        ControllerCommand( ControllerVerb.Unknown, ControllerNoun.Unknown ), null    
    static member ErrorAtDaemon( queue:NetworkCommandQueuePeer, msg ) = 
        Logger.Log( LogLevel.Error, msg )
        use msgError = new MemStream( 1024 )
        msgError.WriteString( msg )
        queue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msgError )
    static member ExceptionOfDSetAtDaemon ( queue: NetworkCommandQueuePeer, jobID: Guid, name, ver, ex ) = 
        use msException = new MemStream(1024)
        msException.WriteGuid( jobID )
        msException.WriteString( name )
        msException.WriteInt64( ver )
        msException.WriteException( ex )
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "ExceptionOfDSetAtDaemon at job %A, DSet: %s, message :%A" jobID name ex )
        queue.ToSend( ControllerCommand( ControllerVerb.Exception, ControllerNoun.DSet), msException )
    member x.ParseSendBackAtDaemon( queuePeer : NetworkCommandQueuePeer, returnCmd : ControllerCommand, ms:StreamBase<byte> ) = 
        if queuePeer.CanSend then 
            if returnCmd.Verb<>ControllerVerb.Unknown then 
                queuePeer.ToSend( returnCmd, ms )
    member x.ParseSendBackAtDaemonAndDispose( queuePeer : NetworkCommandQueuePeer, returnCmd : ControllerCommand, ms:StreamBase<byte> ) = 
        if queuePeer.CanSend then 
            if returnCmd.Verb<>ControllerVerb.Unknown then 
                queuePeer.ToSend( returnCmd, ms )
        if Utils.IsNotNull ms then
            ms.Dispose()
    /// Parse Command At Daemon
    member x.ParseCommandAtDaemon (queuePeer : NetworkCommandQueuePeer) 
                                (command : ControllerCommand)
                                (ms : StreamBase<byte>) =
        let queue = queuePeer :> NetworkCommandQueue
        Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Command: %A" command))
        // messageSendBack will be disposed at the end of the function if it is not null
        // Command will be send directly if parsed 
        if true then 
            try 
                match (command.Verb, command.Noun ) with 
                | (ControllerVerb.Availability, ControllerNoun.Blob ) ->
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "ParseCommandAtDaemon Monitor: Availability, Blob rcvd ... "))
                | _ -> 
                    ()
                let bParsed = x.TaskQueue.ParseCommandAtDaemon( queuePeer, command, ms ) 
                if not bParsed then 
                    match (command.Verb, command.Noun ) with
                    | (ControllerVerb.Unknown, _ ) ->
                        ()
                    // Command: Nothing Message - just measure bandwidth
                    | (ControllerVerb.Nothing, ControllerNoun.Message) ->
                        queue.MonitorRcvd()
                    // Command: Echo Message
                    | (ControllerVerb.Echo, ControllerNoun.Message ) ->
                        let len = int ( ms.Length - ms.Position )
                        let sendBuf = Array.zeroCreate<byte> len
                        ms.Read( sendBuf, 0, len ) |> ignore
                        use msSend = new MemStream( len )
                        msSend.Write( sendBuf, 0, len )
                        let retCmd = ControllerCommand( ControllerVerb.EchoReturn, ControllerNoun.Message )
                        queue.MonitorRcvd()
                        queue.ToSend( retCmd, msSend ) 
                    // Command: Set CurrentClusterInfo
                    // We have changed logic, where Set, Cluster always proceed, Set, DSet command in DSet.store 
                    | (ControllerVerb.Set, ControllerNoun.ClusterInfo ) ->
                        if Utils.IsNotNull queuePeer then 
                            // Set CurrentClusterInfo should only be received from NetworkCommandQueuePeer
                            let obj = ClusterInfo.Unpack( ms )
                            match obj with
                            | Some ( cluster ) ->
                                let fname = cluster.Persist()
//                                if Utils.IsNotNull queuePeer.SetDSetMSG then 
//                                    let cmd, msSend = queuePeer.SetDSet( Guid.Empty )
//                                    x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msSend )
//                                else    
                                queue.ToSend( ControllerCommand( ControllerVerb.Acknowledge, ControllerNoun.ClusterInfo ), null )
                            | None ->
                                let msg = "Set CurrentClusterInfo can't be unpacked, object is not based on ClusterInfoBase"
                                Logger.Log( LogLevel.Error, msg )
                                use msSend = new MemStream(1024)
                                msSend.WriteString( msg )
                                queue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.ClusterInfo ), msSend )
                        else
                            let msg = "Set CurrentClusterInfo is received on a socket that is not returned from accept()"
                            Logger.Log( LogLevel.Error, msg )
                            use msSend = new MemStream(1024)
                            msSend.WriteString( msg )
                            queue.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.ClusterInfo ), msSend )
                    // Command : Set DSet
                    | (ControllerVerb.Set, ControllerNoun.DSet ) ->
                        let jobID = ms.ReadGuid()
                        let name, verNumber = DSet.Peek( ms )
                        let jobLifeCycleObj = JobLifeCycleCollectionDaemon.BeginJob( jobID, name, verNumber, queue.RemoteEndPointSignature )
                        if Utils.IsNotNull queuePeer then 
                            let cmd, msReply = queuePeer.SetDSet( jobLifeCycleObj, ms ) 
                            x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply )
                        else
                            let msg = "Set DSet should not be called from outgoing connection"
                            Logger.Log( LogLevel.Error, msg )
                            use msSend = new MemStream(1024)
                            msSend.WriteString( msg )
                            x.ParseSendBackAtDaemon( queuePeer, ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msSend ) 
                    | (ControllerVerb.Get, ControllerNoun.DSet ) ->
                        let jobID = ms.ReadGuid() 
                        let name = ms.ReadString()
                        let verNumber = ms.ReadInt64()
                        let verCluster = ms.ReadInt64()
                        let cmd, msReply = DSetPeer.ParsePeerDSet( jobID, name, verNumber, verCluster )
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.WriteMetadata, ControllerNoun.DSet ) -> 
                        let jobID = ms.ReadGuid()
                        let cmd, msReply = queuePeer.UpdateDSet( jobID, ms ) 
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.Update, ControllerNoun.DSet )
                    | (ControllerVerb.Read, ControllerNoun.DSet ) 
                    | (ControllerVerb.LimitSpeed, ControllerNoun.DSet ) 
                    | (ControllerVerb.WriteAndReplicate, ControllerNoun.DSet )
                    | (ControllerVerb.ReplicateWrite, ControllerNoun.DSet )
                    | (ControllerVerb.Echo, ControllerNoun.DSet ) 
                    | (ControllerVerb.Write, ControllerNoun.DSet ) 
                    | (ControllerVerb.Use, ControllerNoun.DSet ) 
                    | (ControllerVerb.Close, ControllerNoun.DSet ) 
                    | (ControllerVerb.Cancel, ControllerNoun.DSet ) 
                    | (ControllerVerb.ReplicateClose, ControllerNoun.DSet ) 
                    | (ControllerVerb.Fold, ControllerNoun.DSet ) 
                    | (ControllerVerb.ReadToNetwork, ControllerNoun.DSet ) 
                    | (ControllerVerb.Close, ControllerNoun.Partition ) 
                    | (ControllerVerb.Start, ControllerNoun.Service ) 
                    | (ControllerVerb.Stop, ControllerNoun.Service ) ->
                        let bufPos = int ms.Position
                        let jobID = ms.ReadGuid()
                        using ( SingleJobActionDaemon.TryFind(jobID)) ( fun jobAction -> 
                            if Utils.IsNull jobAction then 
                                Logger.LogF( jobID, LogLevel.Warning, (fun _ -> sprintf "receive command %A of payload %dB, however, can't find corresponding jobLifeCycle object, the command will be discarded. " 
                                                                                            command ms.Length))
                            else
                                try 
                                    let name, verNumber = 
                                        match command.Verb with
                                        | (ControllerVerb.Update ) ->
                                            DSet.Peek( ms )
                                        | _ -> 
                                            ms.ReadString(), ms.ReadInt64()                                            
                                    let curDSet = DSetPeerFactory.ResolveDSetPeer( name, verNumber )
                                    if Utils.IsNotNull curDSet then  
                                        // DSet is in the client space. 
                                        match (command.Verb, command.Noun ) with
                                        | (ControllerVerb.Update, ControllerNoun.DSet ) ->
                                            if Utils.IsNotNull queuePeer then 
                                                let cmd, msReply = queuePeer.UpdateDSet( jobID, ms ) 
                                                x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                                            else
                                                let msg = "Set DSet should not be called from outgoing connection"
                                                Listener.ErrorAtDaemon( queuePeer, msg )
                                        | (ControllerVerb.Read, ControllerNoun.DSet ) ->                                                   
                                            let npart = ms.ReadVInt32()
                                            let partitions = Array.zeroCreate<int> npart
                                            for i=0 to partitions.Length - 1 do
                                                partitions.[i] <- ms.ReadVInt32()
                                            let cmd, msInfo, task = Task.ReadDSet( curDSet, queuePeer, partitions )
                                            if Utils.IsNotNull task then 
                                                let foundTask = x.TaskQueue.AddTask( task, null )
                                                x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msInfo )
                                            else
                                                // Nothing to read
                                                use msWire = new MemStream( 1024 )
                                                msWire.WriteString( name ) 
                                                msWire.WriteInt64( verNumber )
                                                queuePeer.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.DSet ), msWire )
                                        | (ControllerVerb.LimitSpeed, ControllerNoun.DSet ) ->
                                            let rcvdSpeed = ms.ReadInt64()
                                            curDSet.PeerRcvdSpeed <- rcvdSpeed
                                            let msg = sprintf "Peer %d: Set recieving speed of every peer to %d" curDSet.CurPeerIndex rcvdSpeed
                                            use msInfo = new MemStream(1024)
                                            msInfo.WriteString( msg ) 
                                            Logger.Log( LogLevel.WildVerbose, msg )
                                            queuePeer.ToSend( ControllerCommand( ControllerVerb.Info, ControllerNoun.Message ), msInfo )
                                        | (ControllerVerb.WriteAndReplicate, ControllerNoun.DSet )
                                        | (ControllerVerb.ReplicateWrite, ControllerNoun.DSet )
                                        | (ControllerVerb.Write, ControllerNoun.DSet ) ->
                                            if ( command.Verb = ControllerVerb.WriteAndReplicate ) then 
                                                // In case of replication, we will need to start at an earlier position. 
                                                let cmd, msInfo = curDSet.ReplicateDSet( ms, bufPos ) 
                                                let hostQueue = curDSet.HostQueue
                                                match (cmd.Verb) with 
                                                | ControllerVerb.Error 
                                                | ControllerVerb.Verbose
                                                | ControllerVerb.Warning ->
                                                    if Utils.IsNotNull hostQueue && hostQueue.CanSend then 
                                                        hostQueue.ToSend( cmd, msInfo )
                                                | _ ->
                                                    ()
                                            let cmd, msReply =curDSet.WriteDSet( jobAction, ms, queue, command.Verb=ControllerVerb.ReplicateWrite ) 
                                            x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                                        | (ControllerVerb.Close, ControllerNoun.Partition ) ->
                                            let parti = ms.ReadVInt32()
                                            let nError = ms.ReadVInt32()
                                            let cmd, msReply =curDSet.EndPartition( ms, queue, parti, x.callback )                                                  
                                            x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                                        | (ControllerVerb.Close, ControllerNoun.DSet ) 
                                        | (ControllerVerb.ReplicateClose, ControllerNoun.DSet ) ->
                                            let cmd, msReply = curDSet.CloseDSet( jobID, ms, queue, command.Verb=ControllerVerb.ReplicateClose, x.callback ) 
                                            let ret = x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                                            ret 
                                        | (ControllerVerb.Use, ControllerNoun.DSet ) ->
                                            let cmd, msReply = DSetPeer.UseDSet( jobID, name, verNumber )
                                            x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                                        | (ControllerVerb.Echo, ControllerNoun.DSet ) ->                                                      
                                            ()
                                        | (ControllerVerb.Cancel, ControllerNoun.DSet ) ->
                                            jobAction.CancelJob()
                                        | _ -> 
                                            Logger.LogF( LogLevel.Warning, (fun _ -> sprintf "receive command %A direct to a DSetPeer %s:%s, don't know how to process, discard the message" 
                                                                                       command curDSet.Name curDSet.VersionString))
                                            ( )
                                    else 
                                        // DSet is in the task space? 
                                        // Is DSet in one of the job that is being executed?
                                        let dsetTask = x.TaskQueue.FindDSet( name, verNumber )
                                        if not (Utils.IsNull dsetTask) then 
                                            use msTask = new MemStream( 1024 )
                                            msTask.WriteGuid( jobID )
                                            msTask.WriteString( dsetTask.Name ) 
                                            msTask.WriteInt64( dsetTask.Version.Ticks ) 
                                            msTask.WriteIPEndPoint( queue.RemoteEndPoint :?> IPEndPoint )
                                            msTask.WriteVInt32( int FunctionParamType.DSet )
                                            ms.Seek( int64 bufPos, SeekOrigin.Begin ) |> ignore
                                            ms.InsertBefore( msTask ) |> ignore
                                            let msForward = msTask
                                            match (command.Verb, command.Noun ) with
                                            | (ControllerVerb.Read, ControllerNoun.DSet ) ->                                                                                                      
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.Read, ControllerNoun.Job ), msForward )
                                                () 
                                            | (ControllerVerb.Fold, ControllerNoun.DSet ) ->
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.Fold, ControllerNoun.Job ), msForward )
                                                ()
                                            | (ControllerVerb.ReadToNetwork, ControllerNoun.DSet ) ->
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.ReadToNetwork, ControllerNoun.Job ), msForward )
                                                ()
                                            | (ControllerVerb.Update, ControllerNoun.DSet ) ->
                                                Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Send UpdateParam to %A for %s" dsetTask.QueueAtClient.EPInfo name))
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.UpdateParam, ControllerNoun.Job ), msForward )
                                                ()
                                            | (ControllerVerb.Echo, ControllerNoun.DSet ) ->
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.Echo2, ControllerNoun.Job ), msForward )
                                                ()
                                            | (ControllerVerb.Use, ControllerNoun.DSet ) ->
                                                ()  
                                            | (ControllerVerb.Cancel, ControllerNoun.DSet ) ->
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.Cancel, ControllerNoun.Job ), msForward )
                                            | (ControllerVerb.Start, ControllerNoun.Service ) ->
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.Start, ControllerNoun.Service ), msForward )
                                                let taskHolder = dsetTask.TaskHolder
                                                if not ( Utils.IsNull taskHolder ) then 
                                                    /// Service doubled as DSet name 
                                                    taskHolder.LaunchedServices.GetOrAdd( dsetTask.Name, true ) |> ignore 
                                                ()
                                            | (ControllerVerb.Stop, ControllerNoun.Service ) ->
                                                dsetTask.QueueAtClient.ToSend( ControllerCommand( ControllerVerb.Stop, ControllerNoun.Service ), msForward )
                                                let taskHolder = dsetTask.TaskHolder
                                                if not ( Utils.IsNull taskHolder ) then 
                                                    /// Service doubled as DSet name 
                                                    taskHolder.LaunchedServices.TryRemove( dsetTask.Name ) |> ignore 
                                                ()
                                            | _ ->
                                                failwith "Logic error, should never reach here"
                                        else
                                            match (command.Verb, command.Noun ) with
                                            | (ControllerVerb.Use, ControllerNoun.DSet ) ->
                                                let cmd, msReply = DSetPeer.UseDSet( jobID, name, verNumber )
                                                x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                                            | (ControllerVerb.Stop, ControllerNoun.Service ) ->
                                                let serviceName = ms.ReadString() 
                                                let taskHolder = x.TaskQueue.FindTaskHolderByService( serviceName ) 
                                                let mutable bSuccess = false
                                                if not (Utils.IsNull taskHolder) then 
                                                    let queueAtClient = taskHolder.JobLoopbackQueue 
                                                    if not (Utils.IsNull queueAtClient) && queueAtClient.CanSend then 
                                                        use msTask = new MemStream( 1024 )
                                                        msTask.WriteGuid( jobID )
                                                        msTask.WriteString( "" ) 
                                                        msTask.WriteInt64( 0L ) 
                                                        msTask.WriteIPEndPoint( queue.RemoteEndPoint :?> IPEndPoint )
                                                        msTask.WriteVInt32( int FunctionParamType.DSet )
                                                        msTask.WriteString( name ) 
                                                        msTask.WriteInt64( verNumber ) 
                                                        msTask.WriteString( serviceName ) 
                                                        queueAtClient.ToSend( ControllerCommand( ControllerVerb.Stop, ControllerNoun.Service ), msTask )
                                                        bSuccess <- true
                                                if bSuccess then 
                                                    () 
                                                else
                                                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "attempt to stop service %s, while can't find running service in task holders %s ..." serviceName (x.TaskQueue.MonitorExecutionTable()) ))
                                                    let bSuccessToStop = true
                                                    use msInfo =  new MemStream( 1024 )
                                                    msInfo.WriteGuid( jobID )
                                                    msInfo.WriteString( name )
                                                    msInfo.WriteInt64( verNumber ) 
                                                    msInfo.WriteBoolean( bSuccessToStop )
                                                    queuePeer.ToSend( ControllerCommand( ControllerVerb.ConfirmStop, ControllerNoun.Service), msInfo )                                                    
                                            | _ -> 
                                                let msg = sprintf "%A,%A, can't find DSet name %s:%s in both DSetPeerFactory and in existing tasks" command.Verb command.Noun name (VersionToString(DateTime(verNumber)))
                                                Listener.ErrorAtDaemon( queuePeer, msg )
                                with
                                | ex -> 
                                    jobAction.EncounterExceptionAtContainer( ex, "___ ParseCommandAtDaemon (Multiple Parser) ___")

                        )
                    | ( ControllerVerb.Link, ControllerNoun.Program ) ->
                        let sigName = ms.ReadString()
                        let sigVersion = ms.ReadInt64()
                        let mutable bCanStart = true
                        let socket = queue.Socket
                        let ipEndPoint = queue.RemoteEndPoint :?> Net.IPEndPoint
                        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Recv (Link, Program) program from %A" ipEndPoint.Address)
//                        if ipEndPoint.Address=Net.IPAddress.Loopback then 
//                            () // JinL: Loopback is set before Listen operation. 
//                        else
//                            bCanStart <- false
//                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Link, Program is not called to a loop back interface: %A" ipEndPoint.Address ))
                        if not bCanStart then 
                            queuePeer.ToSend( ControllerCommand( ControllerVerb.Close, ControllerNoun.Program ), null )    
                        else
                            let cmd, msReply = x.TaskQueue.LinkSeparateProgram( queue, sigName, sigVersion )                                                
                            x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.Register, ControllerNoun.Contract ) -> 
                        // Register a certain service, it should come from loopback queue 
                        let ipEndPoint = queue.RemoteEndPoint :?> Net.IPEndPoint
                        if ipEndPoint.Address<>Net.IPAddress.Loopback then 
                            // We don't expect service registration other than from loop back queue, but it can be supported later. 
                            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf  "Receive Register, Service from endpoint %s, which is not a loopback interface" (LocalDNS.GetShowInfo(ipEndPoint)) ))
                        let name, info, bReload = ContractInfo.UnpackWithName( ms ) 
                        let cmd, msReply = ContractStoreAtDaemon.Current.RegisterContract( name, info, queue.RemoteEndPointSignature, bReload )
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.Get, ControllerNoun.Contract ) -> 
                        // Register a certain service, it should come from loopback queue 
                        let ipEndPoint = queue.RemoteEndPoint :?> Net.IPEndPoint
                        if ipEndPoint.Address<>Net.IPAddress.Loopback then 
                            // We don't expect service registration other than from loop back queue, but it can be supported later. 
                            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf  "Receive Get, Service from endpoint %s, which is not a loopback interface" (LocalDNS.GetShowInfo(ipEndPoint)) ))
                        let name = ms.ReadStringV()
                        let cmd, msReply = ContractStoreAtDaemon.Current.LookforContract( name, queue.RemoteEndPointSignature ) 
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.Request, ControllerNoun.Contract ) -> 
                        let cmd, msReply = ContractStoreAtDaemon.Current.ProcessContractRequest( ms, queue.RemoteEndPointSignature )
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.Reply, ControllerNoun.Contract ) -> 
                        let cmd, msReply = ContractStoreAtDaemon.Current.ProcessContractReply( ms, queue.RemoteEndPointSignature )
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.FailedReply, ControllerNoun.Contract ) -> 
                        let cmd, msReply = ContractStoreAtDaemon.Current.ProcessContractReply( ms, queue.RemoteEndPointSignature )
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | (ControllerVerb.FailedRequest, ControllerNoun.Contract ) -> 
                        let cmd, msReply = ContractStoreAtDaemon.Current.ProcessContractFailedRequest( ms, queue.RemoteEndPointSignature )
                        x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply ) 
                    | ( ControllerVerb.Stop, ControllerNoun.Program ) ->
                        let sigName = ms.ReadString()
                        let sigVersion = ms.ReadInt64()
                        let ipEndPoint = queue.RemoteEndPoint :?> Net.IPEndPoint
                        if ipEndPoint.Address=Net.IPAddress.Loopback then 
                            let cmd, msReply = x.TaskQueue.DelinkSeparateProgram( queue, sigName, sigVersion )   
                            x.ParseSendBackAtDaemonAndDispose( queuePeer, cmd, msReply )                                                    
                        else
                            let msg = sprintf "Stop, Program is not called to a loop back interface: %A" ipEndPoint.Address
                            Listener.ErrorAtDaemon( queuePeer, msg )
                    | (ControllerVerb.Close, ControllerNoun.All ) ->
                        queue.Close()
                    // Acknowledge, warning can be ignored
                    | (ControllerVerb.Acknowledge, _ ) ->
                        // Nothing needs to be returned. 
                        ()
                    // Verbose message.
                    | (ControllerVerb.Verbose, _ ) ->
                        let msg = 
                            try ms.ReadString() with e -> "Failed when try to parse return message at Listener."
                        Logger.Log( LogLevel.MediumVerbose, ( sprintf "From socket %A, received... Command %A...  %s" queue.RemoteEndPoint command msg ))
                    // Acknowledge, warning can be ignored
                    | (ControllerVerb.Info, _ ) ->
                        let msg = 
                            try ms.ReadString() with e -> "Failed when try to parse return message at Listener."
                        Logger.Log( LogLevel.MildVerbose, ( sprintf "From socket %A, received... Command %A...  %s" queue.RemoteEndPoint command msg ))
                    | (ControllerVerb.Warning, _ ) ->
                        let msg = 
                            try ms.ReadString() with e -> "Failed when try to parse return message at Listener."
                        Logger.Log( LogLevel.Info, ( sprintf "From socket %A, received... Command %A...  %s" queue.RemoteEndPoint command msg ))
                        // Nothing needs to be returned. 
                    | (ControllerVerb.Error, _ ) ->
                        let msg = 
                            try ms.ReadString() with e -> "Failed when try to parse error message at Listener."
                        Logger.Log( LogLevel.MildVerbose, ( sprintf "Error from socket %A, received... Command %A...  %s" queue.RemoteEndPoint command msg ))
                        // Nothing needs to be returned. 
                    | ( ControllerVerb.Forward, ControllerNoun.Message ) -> 
                        // Forward message to a different endpoint. 
                        let nForwards = ms.ReadVInt32()
                        let endPoints = 
                            seq {
                                for i = 0 to nForwards - 1 do 
                                    yield ms.ReadIPEndPoint()
                                } |> Seq.toArray
                        let cmdVerb = ms.ReadByte()
                        let cmdNoun = ms.ReadByte()
                        let cmd = ControllerCommand( enum<_>(cmdVerb), enum<_>(cmdNoun) ) 
                        let startPos = ms.Position
                        for i = 0 to endPoints.Length - 1 do 
                            let queueSend = x.Connects.LookforConnect( endPoints.[i] )
                            if Utils.IsNotNull queueSend && queueSend.CanSend then 
                                queueSend.ToSendFromPos( cmd, ms, startPos )
                                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Forward command %A (%dB) to %A ... " 
                                                                                   cmd ms.Length
                                                                                   (LocalDNS.GetShowInfo(queueSend.RemoteEndPoint)) ))
                            else
                                if Utils.IsNull queueSend then 
                                    Logger.Log( LogLevel.MildVerbose, ( sprintf "Forward Message: failed to find forwarding queue %A for command %A" endPoints.[i] cmd ))
                                else
                                    Logger.Log( LogLevel.MildVerbose, ( sprintf "Forward Message: forwarding queue %A has been shutdown for command %A" queueSend.RemoteEndPoint cmd ))
                        ()
                    | _ ->
                        ()
                        // Some message, such as DistributedFunction, will be parsed in another function. 
                        // So we shouldn't throw an error message here. 
                        // let msg = sprintf "Unknown Command %A with %dB payload" command ms.Length
                        // Listener.ErrorAtDaemon( queuePeer, msg )
            with
            | ex ->
                let msg = sprintf "When parsing command %A buf %A get exception %A" command (ms.GetBuffer()) ex
                Listener.ErrorAtDaemon( queuePeer, msg )

 
    static member StartServer( o: Object ) =
        let x = o :?> Listener
        // Maintaining listening state
        if not x.InListeningState then 
            for i = 0 to 4 do 
                let ar = x.Listener.BeginAccept( AsyncCallback( Listener.EndAccept ), x)
                ()
            x.InListeningState <- true
        let mutable lastActive = (PerfDateTime.UtcNow())
        let remoteStorageInfo = Dictionary<_,_>()
        Logger.Log( LogLevel.MildVerbose, (sprintf "Listening on port %d" x.Port ))
        try
            while x.InListeningState do
                x.Activity <- false
                try 
                    // Parse command
                    let channelLists = x.Connects.GetAllChannels()
                    let numChannels = Seq.length channelLists
                    for queue in channelLists do
                        if (not queue.CompRecv.Q.IsEmpty) then
//                            x.Activity <- true
                            lastActive <- (PerfDateTime.UtcNow())

                    // If there is waiting job, executing it. 
                    x.TaskQueue.ExecuteLightJobs() 

                    // DSet validity check 
                    DSetPeerFactory.Refresh( (fun curDSet -> 
                        curDSet.TestForDisconnect() ), DeploymentSettings.DSetPeerTimeout)
                    
                    let curActiveDSetList = DSetPeerFactory.toArray()
                    // Check if the cluster is still in use. 
                    // Send any special message to server. 
                    let curUsedCluster = Dictionary<Cluster, bool>()
                    for curDSet in curActiveDSetList do
                        curUsedCluster.Item( curDSet.Cluster ) <- true 

                    // Cluster Validity check. 
                    // A cluster may be unloaded if we don't receive any incoming communication from peer for a certain period of time & 
                    // there is no DSetPeer associated with it. 
                    try
                        let tNow = (PerfDateTime.UtcNow())
                        ClusterFactory.Refresh( ( fun curCluster -> 
                                                            let clName = ClusterInfo.ConstructClusterInfoFileNameWithVersion( curCluster.Name, curCluster.Version ) 
                                                            if not (remoteStorageInfo.ContainsKey( clName )) then 
                                                                remoteStorageInfo.Item( clName ) <- tNow 
                                                                curCluster.ClusterInfo.LoadRemoteStorageInfo() |> ignore
                                                                RemoteConfig.GetRemoteStorageInfo( curCluster )
                                                            else
                                                                let elapse = tNow.Subtract( remoteStorageInfo.Item( clName ) )
                                                                if elapse.TotalSeconds >= 3600. then 
                                                                    remoteStorageInfo.Item( clName ) <- tNow 
                                                                    RemoteConfig.GetRemoteStorageInfo( curCluster )
                                                            curUsedCluster.ContainsKey( curCluster) ), DeploymentSettings.DSetPeerTimeout)                           
                    with 
                    | e -> 
                        Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Failure during cluster refresh and remote storage information retrieval, with msg %A" e ))
                        () 

                    // Monitor contracts 
                    ContractStoreAtDaemon.Current.MonitorContracts()

                    let callbackArray = x.Callback |> Seq.toArray 
                    for pair in callbackArray do 
                        x.Activity <- x.Activity || pair.Value( pair.Key ) 

                    let curTime = (PerfDateTime.UtcNow())
                    if not x.Activity then
                        let elapse = curTime - lastActive
                        if elapse.TotalSeconds >= DeploymentSettings.ClientInactiveTimeSpan then 
                            lastActive <- curTime
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Prajna main listening loop has been inactive for %f sec" DeploymentSettings.ClientInactiveTimeSpan ))
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Active Objects: DSetPeer:%d, Cluster:%d, Channels:%d, Task:%d" (DSetPeerFactory.Count) (ClusterFactory.Count) (numChannels) (x.TaskQueue.Count) ))
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Threads: %s" (ThreadsInformation.ThreadCollectionInfo()) ))
                     
//                        if numChannels = 0 then 
//                            Threading.Thread.Sleep( 5 )
//                        else
//                            Threading.Thread.Sleep( 1 )
                    else
                        lastActive <- curTime
                    Threading.Thread.Sleep(30)

                with
                | e ->
                    Logger.Log( LogLevel.Error, ( sprintf "Execution Error at PrajnaClient Core Receiving loop, exception %A" e ))

        finally
            Logger.Log( LogLevel.Info, ( "Exit Receiving loop." ))
            Logger.Flush()
            x.Listener.Close()
        ()
    
    /// <summary>
    /// Disconnect of a peer 
    /// </summary> 
    member x.RemoveConnectedQueue (queue:NetworkCommandQueuePeer) ()= 
        x.TaskQueue.CloseConnectedQueueForAllTasks( queue )
        ContractStoreAtDaemon.Current.CloseConnection( queue.RemoteEndPointSignature )

    static member EndAccept( ar ) = 
        let state = ar.AsyncState
        match state with 
        | :? Listener as x ->
            let mutable listenerDisposed = false
            // Post another listening request. 
            try
                x.Listener.BeginAccept( AsyncCallback( Listener.EndAccept ), x) |> ignore
            with
                | :? ObjectDisposedException -> 
                    listenerDisposed <- true
                    Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Listener %A disposed" x.Listener.LocalEndPoint ) )
                | _ as e ->
                    Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Exception when repost BeginAccept: %A" e ) )
            if not (listenerDisposed) then
                try
                    let soc = x.Listener.EndAccept( ar )
                    soc.NoDelay <- true
                    let queue = x.ConnectsClient.AddPeerConnect( soc ) 
                    queue.CallOnClose.Add ( OnPeerClose( CallbackOnClose = x.RemoveConnectedQueue queue ) )
                    // add processing for command 
                    let procItem = (
                        fun (cmd : NetworkCommand) -> 
                            x.ParseCommandAtDaemon queue cmd.cmd (cmd.ms)
                            null
                    )
                    queue.GetOrAddRecvProc("ParseCommandAtDaemon", procItem ) |> ignore
                    // set queue to initialized
                    queue.Initialize()
                    Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Incoming connection established from socket %A" soc.RemoteEndPoint ))
                    x.Activity <- true
                with e ->
                    Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Exception when try to accept conection: %A" e ) )
        | _ ->
            failwith "Incorrect logic, Listener.EndAccept should always be called with Listener as an object"

    interface IDisposable with
        member x.Dispose() = 
           if Utils.IsNotNull x.taskqueue then
              (x.taskqueue :> IDisposable).Dispose()
           GC.SuppressFinalize(x)

