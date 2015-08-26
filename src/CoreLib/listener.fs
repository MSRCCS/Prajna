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
        listener.fs
  
    Description: 
        Listen within a job 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        May. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Threading.Tasks
open System.Reflection
open System.Runtime.Serialization
open System.Net
open System.Net.Sockets
open System.Diagnostics
open Prajna.Tools

open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Network

/// Delegate that is called when some one connects in 
type OnAcceptAction = Action<NetworkCommandQueuePeer>

[<AllowNullLiteral>]
type JobListener() = 
    static member internal SocketForListenWloopback() = 
        let soc = new Socket( AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp )
        try 
            let OptionInValue = BitConverter.GetBytes(1)
            soc.IOControl( DeploymentSettings.SIO_LOOPBACK_FAST_PATH, OptionInValue, null ) |> ignore
        with 
        | e ->
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Fail to set loopback fast path...." ))
        soc
    member val internal ConnectsClient = new ClientConnections() with get
    member val internal Listener:Socket = null with get, set
    member val internal Activity = false with get, set
    member val internal ListeningThread = null with get, set
    member val JobPort = -1 with get, set
    member val internal bTerminateListening = false with get, set
    member val private OnAcceptedExecutor = ExecuteEveryTrigger<NetworkCommandQueuePeer>(LogLevel.MildVerbose) with get
    member internal x.OnAccepted(action : Action<NetworkCommandQueuePeer>, infoFunc : unit -> string) =
        x.OnAcceptedExecutor.Add(action, infoFunc)

    member internal x.InitializeListenningTask( ) = 
        while not x.bTerminateListening do
           try 
                let soc = x.Listener.Accept() 
                if Utils.IsNotNull soc then 
                    let queue = x.ConnectsClient.AddPeerConnect( soc ) 
                    x.OnAcceptedExecutor.Trigger( queue, fun _ -> sprintf "from %s" (LocalDNS.GetShowInfo(queue.RemoteEndPoint)) )
                    // Post another listening request. 
                    Logger.LogF( LogLevel.MildVerbose, (fun _ -> let ep = soc.RemoteEndPoint 
                                                                 let eip = ep :?> IPEndPoint
                                                                 sprintf "incoming connection established from socket %A with name %A" ep (LocalDNS.GetHostByAddress( eip.Address.GetAddressBytes(),false)  ) ))
           with 
           | e -> 
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Exception when Accept: %A" e )         )
    // 05/31/2014, Jin Li, I change the active code to synchronous code, because I observe that on the Prajna cluster, 
    // sometime the EndAccept call doesn't get called for a long time (!!!minutes!!!, even I believe that the packet comes in). 
    // change the code to synchronous listen fixed the issue. The 1st accept still can take 3-5 seconds, but it was not the dreadful minutes. 
    /// Start a listening port on certain port
    static member InitializeListenningPort( jobip : string, jobport : int ) = 
        if jobport >=0 then 
            let soc = JobListener.SocketForListenWloopback()
            if (jobip.Equals("", StringComparison.Ordinal)) then
                soc.Bind( IPEndPoint( IPAddress.Any, jobport ) )
            else
                soc.Bind( IPEndPoint( IPAddress.Parse(jobip), jobport ) )
            soc.Listen( 30 )
            let x = new JobListener( Listener = soc, JobPort=jobport )
            x.ConnectsClient.Initialize()
            ClusterJobInfo.JobListenningPortCollection.GetOrAdd( jobport, true) |> ignore 

//            let ListenerThreadStart = new Threading.ParameterizedThreadStart( JobListener.StartListenning )
//            let ListenerThread = new Threading.Thread( ListenerThreadStart )
//            ListenerThread.IsBackground <- true
//            ListenerThread.Start( x )
//            x.ListeningThread <- ListenerThread // Tasks.Task.Run( fun _ -> x.InitializeListenningTask( ) ) 

            x.ListeningThread <- ThreadTracking.StartThreadForFunction( fun _ -> sprintf "Listening thread on port %s:%d" jobip jobport ) (fun _ ->  JobListener.StartListenning x)
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "starting listening thread on port %d" jobport ))
            
            x
        else
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "jobport %d is not set, no listening thread is started" jobport ))
            null
    member internal x.TerminateListenningTask() = 
        if Utils.IsNotNull x.ListeningThread then 
            x.bTerminateListening <- true
            x.Listener.Close() // Force to close a socket 
            x.ListeningThread.Join() // Wait for the listening thread to terminate
            x.ListeningThread <- null
            () // no way to cancel a synchronous request. 
    static member internal StartListenning o = 
        try
//            match o with 
//            | :? JobListener as x -> 
                o.InitializeListenningTask() 
//            | _ -> 
//                Logger.Log( LogLevel.Error,  sprintf "StartListenning should not be called with a object that is not JobListener"  )
//                ()               
        with
        | e -> 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "JobListener, accept thread aborted with exception %A" e ))
    static member internal InitializeListenningPortAsync( jobip : string, jobport : int ) = 
        if jobport >=0 then 
            let soc = JobListener.SocketForListenWloopback()
            if (jobip.Equals("", StringComparison.Ordinal)) then
                soc.Bind( IPEndPoint( IPAddress.Any, jobport ) )
            else
                soc.Bind( IPEndPoint( IPAddress.Parse(jobip), jobport ) )
            soc.Listen( 30 )
            let x = JobListener( Listener = soc )
            let ar = x.Listener.BeginAccept( AsyncCallback( JobListener.EndAccept ), x)
            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Listening on port %d" jobport ))
            x
        else
            null
    static member internal EndAccept ar =
        let state = ar.AsyncState
        match state with 
        | :? JobListener as x ->
            try
                let soc = x.Listener.EndAccept( ar )
                let queue = x.ConnectsClient.AddPeerConnect( soc ) 
                queue.Initialize()
                // Post another listening request. 
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> let ep = soc.RemoteEndPoint 
                                                             let eip = ep :?> IPEndPoint
                                                             sprintf "incoming connection established from socket %A with name %A" ep (LocalDNS.GetHostByAddress( eip.Address.GetAddressBytes(),false)  ) ))
                x.Activity <- true
//                x.InListeningState <- false
            with
            | e ->
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Exception when try to accept conection: %A" e )    )
            try
                let ar = x.Listener.BeginAccept( AsyncCallback( JobListener.EndAccept ), x)
                ()
            with
            | e ->
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Exception when repost BeginAccept: %A" e )                   )
        | _ ->
            failwith "Incorrect logic, Listener.EndAccept should always be called with Listener as an object"
    member val private m_MonitorBlocking = ConcurrentDictionary<_,DateTime>() with get

    member internal x.ProcessPeerJobCommand (queuePeer : NetworkCommandQueuePeer)
                                   (eip : IPEndPoint)
                                   (t1 : DateTime)
                                   (curTime : DateTime)
                                   (rcvd : Option<ControllerCommand*StreamBase<byte>>)
                                   (command : ControllerCommand)
                                   (ms : StreamBase<byte>) =
        match (command.Verb, command.Noun ) with
            | ControllerVerb.Unknown, _ -> 
                ()
            | ControllerVerb.ContainerInfo, ControllerNoun.Job -> 
                // Get the container information from the peer
                let pos = ms.Position
                let name = ms.ReadString()
                let verNumber = ms.ReadInt64()
                let peerIndex = ms.ReadVInt32()
                let cluster = ClusterFactory.FindCluster( name, verNumber )
                if not (Utils.IsNull cluster) then 
                    // map th peer queue with its index
                    cluster.AssignEndPoint( queuePeer, peerIndex )
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Received InfoContainer, Job from %A, assign the connection to cluster %s of peer %d." (LocalDNS.GetShowInfo(queuePeer.RemoteEndPoint)) name peerIndex ))
                else
                    // This may be caused if the peer launched the job earlier, and the current job hasn't been properly populated. 
                    let t2 = (PerfDateTime.UtcNow())
                    let elapse = t2.Subtract(t1).TotalSeconds
                    if elapse > DeploymentSettings.TimeOutJobStartTimeSkewAmongPeers then 
                        /// Wait a long time and the job still doesn't start
                        let msg1 = sprintf "Rcvd from %A msg of InfoContainer, Job (%dB) claiming from Cluster %s:%s of peer %d, wait for %f seconds, but can't resolve Cluser" (queuePeer.RemoteEndPoint) ( ms.Length - ms.Position ) name (VersionToString(DateTime(verNumber))) peerIndex elapse
                        Logger.Log( LogLevel.Error, (msg1))
                        let msError = new MemStream( 1024 )
                        msError.WriteString( msg1 ) 
                        queuePeer.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
                    elif queuePeer.Shutdown then 
                        let cmdLen = ms.Length
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Queue already shutdown but received Block command InfoContainer, Job (%dB) claiming on Cluster %s:%s of %d from peer %A (%s), command will be discarded. " 
                                                                                   cmdLen name (VersionToString(DateTime(verNumber))) peerIndex 
                                                                                   eip (LocalDNS.GetHostByAddress(eip.Address.GetAddressBytes(), false)) ))
                    else
                        // Put the command back on queue, and wait for it to be ready 
                        // But discard if the queuePeer is gone. 
                        ms.Seek( pos, SeekOrigin.Begin ) |> ignore
                        queuePeer.PendingCommand <- (rcvd, t1)
                        if t1 = curTime then                                         
                            // A new command is blocked. 
                            if not (x.m_MonitorBlocking.ContainsKey( eip )) || 
                                curTime.Subtract( x.m_MonitorBlocking.Item( eip ) ).TotalSeconds >= DeploymentSettings.MonitorPeerBlockingTime then 
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Block command InfoContainer, Job claiming on Cluster %s:%s of %d from peer %A (%s), unable to process immediately." 
                                                                                       name (VersionToString(DateTime(verNumber))) peerIndex 
                                                                                       eip (LocalDNS.GetHostByAddress(eip.Address.GetAddressBytes(), false))  ))
                                Logger.LogStackTrace( LogLevel.MildVerbose )
                                x.m_MonitorBlocking.Item( eip ) <- curTime

            | _, ControllerNoun.DStream -> 
                let pos = ms.Position
                let name = ms.ReadString()
                let verNumber = ms.ReadInt64()
                let dstream = DStreamFactory.ResolveDStreamByName( name, verNumber )
                let mutable bCommandBlocked = (Utils.IsNull dstream || not dstream.bNetworkInitialized || (Utils.IsNotNull dstream.DefaultJobInfo && not( dstream.DefaultJobInfo.JobReady.WaitOne(0))) )
                let reasonBlocked = ref "Unknown"
                if not bCommandBlocked then 
                    bCommandBlocked <- not (dstream.ProcessJobCommand( queuePeer, command, ms ))
                    if bCommandBlocked then 
                        reasonBlocked := sprintf "Unable to process DStream command command %A" command   
                    else
                        // command processed. 
                        x.m_MonitorBlocking.TryRemove( eip ) |> ignore  
                else
                    if Utils.IsNull dstream then 
                        reasonBlocked := sprintf "Unable to resolve DStream %s" name
                    else if not dstream.bNetworkInitialized then
                        reasonBlocked := sprintf "DStream %s has not initialized network" name
                    else
                        reasonBlocked := sprintf "DStream %s does not have ready job" name
                if bCommandBlocked then 
                    // This may be caused if the peer launched the job earlier, and the current job hasn't been properly populated. 
                    let t2 = (PerfDateTime.UtcNow())
                    let elapse = t2.Subtract(t1).TotalSeconds
                    if elapse > DeploymentSettings.TimeOutJobStartTimeSkewAmongPeers then 
                        /// Wait a long time and the job still doesn't start
                        let msg1 = sprintf "Rcvd from %A msg of %A (%dB) for DStream %s:%s, wait for %f seconds, but " (queuePeer.RemoteEndPoint) command ( ms.Length - ms.Position ) name (VersionToString(DateTime(verNumber))) elapse
                        let msg2 = !reasonBlocked
                        Logger.Log( LogLevel.Error, (msg1+msg2))
                        let msError = new MemStream( 1024 )
                        msError.WriteString( msg1 + msg2 ) 
                        queuePeer.ToSend( ControllerCommand( ControllerVerb.Error, ControllerNoun.Message ), msError )
                    elif queuePeer.Shutdown then 
                        let cmdLen = ms.Length
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Queue already shutdown but received Block command %A (%dB) on %s:%s from peer %A (%s), reason: %s, command will be discarded. " command cmdLen name (VersionToString(DateTime(verNumber))) eip (LocalDNS.GetHostByAddress(eip.Address.GetAddressBytes(), false)) !reasonBlocked ))
                    else
                        // Put the command back on queue, and wait for it to be ready 
                        // But discard if the queuePeer is gone. 
                        ms.Seek( pos, SeekOrigin.Begin ) |> ignore
                        queuePeer.PendingCommand <- (rcvd, t1)
                        if t1 = curTime then                                         
                            // A new command is blocked. 
                            if not (x.m_MonitorBlocking.ContainsKey( eip )) || 
                                curTime.Subtract( x.m_MonitorBlocking.Item( eip ) ).TotalSeconds >= DeploymentSettings.MonitorPeerBlockingTime then 
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Block command %A on %s:%s from peer %A (%s), unable to process immediately, reason: %s" command name (VersionToString(DateTime(verNumber))) eip (LocalDNS.GetHostByAddress(eip.Address.GetAddressBytes(), false)) !reasonBlocked ))
                                Logger.LogStackTrace( LogLevel.MildVerbose )
                                x.m_MonitorBlocking.Item( eip ) <- curTime
            | _, ControllerNoun.Message -> 
                let msg = ms.ReadString()
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Rcvd from %A msg of %A .... %s" eip command.Verb msg ))
            | _, _ -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Warning ... Rcvd from %A unparsed msg of %A .... " eip command ))

    // JinL: 12/31/2014, this function is not currently multithread safe. 
    //      to make it multithread safe, we will need to change the function to operate on queue, 
    member internal x.ProcessPeerJobCommandLoop (queuePeer : NetworkCommandQueuePeer) (cmd : NetworkCommand) = 
        //queuePeer.TraceCommand(cmd, fun _ -> "Processing command")
        try
            let ep = queuePeer.RemoteEndPoint
            let eip = ep :?> IPEndPoint
            let curTime = (PerfDateTime.UtcNow())
            let bNetwork, rcvd, t1 = 
                let (pendingCmd, time) = queuePeer.PendingCommand
                match pendingCmd with
                    | Some (pcmd) -> false, pendingCmd, time
                    | None -> true, Some(cmd.cmd, cmd.ms), curTime
            queuePeer.PendingCommand <- (None, curTime)
            match rcvd with 
            | Some ( command, ms ) ->
                x.ProcessPeerJobCommand queuePeer eip t1 curTime rcvd command ms
            | None ->
                ()
        with 
        | e ->           
            Logger.Log( LogLevel.Error, ( sprintf "Exception encountered at ProcessPeerJobCommand with %A" e ))

        queuePeer.GetPendingCommandEventIfNeeded()

    /// Default on Accept
    member internal x.OnIncomingQueue (queuePeer:NetworkCommandQueuePeer) = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "accept a connection from %s." (LocalDNS.GetShowInfo( queuePeer.RemoteEndPoint )) ))
        let procItem = (
            fun (cmd : NetworkCommand) -> 
                x.ProcessPeerJobCommandLoop queuePeer cmd
        )
        let remoteSignature = queuePeer.RemoteEndPointSignature
        queuePeer.AddRecvProc procItem |> ignore
        queuePeer.Initialize()
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "add command parser for backend server %s." (LocalDNS.GetShowInfo( queuePeer.RemoteEndPoint )) ))



