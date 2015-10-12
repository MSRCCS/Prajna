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
        network.fs
  
    Description: 
        Networking module of Prajna

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
        Sanjeev Mehrotra
    Date:
        July. 2013

    Rewritten with significant modifications: December 2014 by Sanjeev Mehrotra	
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Diagnostics
open System.Security.Cryptography
open Prajna.Tools
open Prajna.Tools.Crypt
open Prajna.Tools.Queue
open Prajna.Tools.Network
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.BytesTools
open Prajna.Tools.Process
open LanguagePrimitives
        
type internal ReceivingMode =
    | ReceivingBody
    | ReceivingCommand
    | EnqueuingCommand

type internal SendingMode =
    | SendHeader
    | SendCommand

type ConnectionStatus =
    | NotInitialized = 0
    | ResolveDNS = 1
    | BeginConnect = 2
    | Connected = 3
    | Verified = 4

type internal VerificationCmd =
    | Salt = 0uy
    | SaltSignature = 1uy
    | SaltEncrypt = 2uy
    | RequestEncrypt = 3uy
    | Verified = 4uy
    | VerificationFailed = 5uy
    | AESKeyUsingRSA = 6uy
    | AESKeyUsingPwd = 7uy
    | AESVerificationUsingRSA = 8uy
    | AESVerificationUsingPwd = 9uy

type [<AllowNullLiteral>] NetworkCommand() =
    let bReleased = ref 0

    new (cmd : ControllerCommand, ms : StreamBase<byte>) as x =
        new NetworkCommand()
        then
            x.cmd <- cmd
            if (Utils.IsNotNull ms) then
                x.ms <- ms.Replicate()
                // if position = length, assume stream just written, seek back for read purposes
                if (ms.Position = ms.Length) then
                    x.startPos <- 0L
                else if (ms.Position <> 0L) then
                    failwith "Use constructor with start position"
            else
                x.ms <- new MemStream(0)

    new (cmd : ControllerCommand, ms : StreamBase<byte>, startPos : int64) as x =
        new NetworkCommand()
        then
            x.cmd <- cmd
            x.ms <- ms.Replicate()
            x.startPos <- startPos

    member val startPos = 0L with get, set
    /// The controller command
    member val cmd = Unchecked.defaultof<ControllerCommand> with get, set
    /// The memory stream
    member val ms : StreamBase<byte> = null with get, set
    /// Get the length of the command on the wire
    member val internal buildHeader : bool = true with get, set
    member x.CmdLen() =
        if (Utils.IsNull x.ms) then
            if (x.buildHeader) then
                8L
            else
                0L
        else
            if (x.buildHeader) then
                8L + x.ms.Length - x.startPos
            else
                x.ms.Length - x.startPos

    /// release the underlying memstream
    member x.Release() =
        if (Interlocked.CompareExchange(bReleased, 1, 0)=0) then
            x.ms.Dispose()

    interface IDisposable with
        override x.Dispose() =
            x.Release()
            GC.SuppressFinalize(x)

    override x.Finalize() =
        x.Release()

/// An extension to GenericConn to process NetworkCommand
/// GenericConn is an internal object which contains Send/Recv Components to process SocketAsyncEventArgs objects
/// NetworkCommandQueue contains Send/Recv Components to process NetworkCommand objects
/// The general pipeline looks like following
/// For Recv:
/// Network -> GenericConn (ProcessRecvGenericConn) -> NetworkCommandQueue (function from AddRecvProc) -> Application
/// RecvAsync->queue        CompRecv of GenericConn                       CompRecv of NetworkCommandQueue
///                         SocketAsyncEventArgs->NetworkCommand           NetworkCommand->application
///                         To Queue of NetworkCommandQueue
/// For Send:
/// Network <- (SendAsync)          GenericConn     <- (ProcessSendGenericConn) NetworkCommandQueue    <- Application
///            CompSend of GenericConn                 CompSend of NetworkCommandQueue
///            network<-SocketAsyncEventArgs           SocketAsyncEventArgs<-NetworkCommand
///                                                    Application writes to NetworkCommand queue using ToSend
type [<AllowNullLiteral>] NetworkCommandQueue() as x =
    static let count = ref -1
    static let MagicNumber = System.Guid("45F9F0E2-AAF1-4F38-82AB-75B876E282C9")
    static let MagicNumberBuf = MagicNumber.ToByteArray()
    static let TCPSendBufSize = DeploymentSettings.TCPSendBufSize
    static let TCPRcvBufSize = DeploymentSettings.TCPRcvBufSize

    // connect to GenericConn for processing of SocketAsyncEventArgs
    let xgc = new GenericConn()
    // component for command queue
    let xCRecv = new Component<NetworkCommand>()
    let xCSend = new Component<NetworkCommand>()
    // remote endpoint / remote endpoint signature / endpoint info
    let mutable remoteEndPoint : EndPoint = null
    let mutable localEndPoint: EndPoint = null
    let mutable remoteEndPointSignature = 0L
    let mutable epInfo = ""
    let mutable connectionStatus = ConnectionStatus.NotInitialized

    let xgBuf = new GenericBuf(xgc, 2048) // generic buffer reader/writer allows max buffersize of 2048
    let headerRecv = Array.zeroCreate<byte>(8)
    let headerSend = Array.zeroCreate<byte>(8)
    let mutable body : StreamBase<byte> = null
    let mutable rcvdSpeed = DeploymentSettings.DefaultRcvdSpeed
    let mutable sendSpeed = Config.CurrentNetworkSpeed 
    let unProcessedBytes = ref 0L // unncessary as queue maintains size
    // Maximum Segment Size (MSS) in use
    // see: http://rickardnobel.se/actual-throughput-on-gigabit-ethernet/
    let mutable usedMSS = Config.CurrentNetworkMTU - 40
    let mutable totalBytesRcvd = 0L
    let mutable totalBytesSent = 0L
    let mutable rcvdCommandSerial = 0L

    let mutable curStateSend = SendingMode.SendCommand
    let mutable curStateRecv = ReceivingMode.ReceivingCommand
    let mutable iLastRecvdSeqNo = 0
    let mutable bNoMoreRecv = false

    let eInitialized = new ManualResetEvent(false)
    let mutable bInitialized = false

    let eVerified = new ManualResetEventSlim(false)

    let mutable bLocalVerified = false
    let mutable bRemoteVerified = false

    let rcvdCommandSerialPosition = 6L

    // command thread pool for send commands - used for assembling into SocketAsyncEventArgs
    let mutable sendLenRem = 0L
    let mutable streamReader : StreamReader<byte> = null
    let dequeueSend (dequeueAction : NetworkCommand ref->bool*ManualResetEvent)
                    (cmd : NetworkCommand ref) : bool*ManualResetEvent =
        let (success, event) = dequeueAction(cmd)
        if (success) then
            streamReader <- new StreamReader<byte>((!cmd).ms, (!cmd).startPos)
            sendLenRem <- (!cmd).CmdLen()
            //if ((!cmd).cmd.Verb = ControllerVerb.Link) then
            //    Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "Dequeue cmd %A" (!cmd).cmd )
            if (!cmd).buildHeader then
                x.BuildHeader(!cmd, headerSend)
                curStateSend <- SendingMode.SendHeader
                xgc.CurBufSend <- headerSend
                xgc.CurBufSendOffset <- 0
                xgc.CurBufSendRem <- headerSend.Length
            else
                x.UpdateHeader(!cmd)
                //Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Send cmd to %s len: %d ack:L %d" xgc.ConnKey (BitConverter.ToInt32((!cmd).buffer, 0)) (BitConverter.ToUInt16((!cmd).buffer, 6))))
                curStateSend <- SendingMode.SendCommand
                let (buf, pos, cnt) = streamReader.GetMoreBuffer()
                xgc.CurBufSend <- buf
                xgc.CurBufSendOffset <- pos
                xgc.CurBufSendRem <- cnt
            sendLenRem <-  sendLenRem - int64 xgc.CurBufSendRem
            totalBytesSent <- totalBytesSent + (!cmd).CmdLen()
        (success, event)
    let processSend (cmd : NetworkCommand) =
        xgc.ProcessSendGenericConn x.EQSendSA x.ProcessSendCommand
        //xgc.ProcessSendGenericConn xgc.CompSend.Q.EnqueueWaitTime x.ProcessSendCommand
        //xgc.ProcessSendGenericConn xgc.CompSend.Q.EnqueueWait x.ProcessSendCommand

    // for generic conn recv
    let mutable curRecvCmd : NetworkCommand = null

    // the queues
    // Note: the dispose of the NetworkCommand in the following queues are handled by corresponding Component object
    let mutable sendCmdQ : FixedSizeQ<NetworkCommand> = null
    let mutable sendSAQ : FixedLenQ<RBufPart<byte>> = null
    let mutable recvSAQ : FixedSizeQ<RBufPart<byte>> = null
    let mutable recvCmdQ : FixedLenQ<NetworkCommand> = null

    // Constructors (4 of them)
    // default stuff
    private new (onet : NetworkConnections) as x =
        new NetworkCommandQueue()
        then
            x.ONet <- onet
            x.RequireAuth <- onet.RequireAuth
            x.MyGuid <- onet.MyGuid
            x.MyAuthRSA <- onet.MyAuthRSA
            x.MyExchangeRSA <- onet.MyExchangeRSA
            // receiver q is fixed size to limit # of unprocessed commands (not taken out by ToReceive)
            let xcRecv : Component<NetworkCommand> = x.CompRecv
            xcRecv.Q <- new FixedLenQ<_>(DeploymentSettings.NetworkCmdRecvQSize, DeploymentSettings.NetworkCmdRecvQSize)
            x.RecvCmdQ <- xcRecv.Q :?> FixedLenQ<NetworkCommand>
            let xcSend : Component<NetworkCommand> = x.CompSend
            xcSend.Q <- new FixedSizeQ<_>(int64 DeploymentSettings.MaxSendingQueueLimit, int64 DeploymentSettings.MaxSendingQueueLimit)
            x.SendCmdQ <- xcSend.Q :?> FixedSizeQ<NetworkCommand>
            let cmdQ : FixedSizeQ<NetworkCommand> = x.SendCmdQ
            x.SendCmdQ.MaxLen <- DeploymentSettings.NetworkCmdSendQSize
            x.SendCmdQ.DesiredLen <- DeploymentSettings.NetworkCmdSendQSize

    /// 1. Constructor used when accepting a new connection
    new ( soc : Socket, onet : NetworkConnections ) as x = 
        new NetworkCommandQueue(onet)
        then
            x.ConnectionStatusSet <- ConnectionStatus.Connected
            let eip = soc.RemoteEndPoint :?> IPEndPoint
            x.Port <- eip.Port
            try
                x.InitConnection(soc)
            with 
            | e -> 
                x.MarkFail()
                Logger.Log( LogLevel.Error, ( sprintf "Error in Initialization from %s" (eip.ToString()) ))
    /// 2. Constructor when connecting to machine with name/port (with DNS resolve)
    new ( machineName : string, port : int, onet : NetworkConnections ) as x = 
        new NetworkCommandQueue(onet)
        then
            x.MachineName <- machineName
            x.Port <- port
            x.ConnectionStatusSet <- ConnectionStatus.ResolveDNS
            let mutable ipAddr : IPAddress = null
            let isIp = IPAddress.TryParse(machineName, &ipAddr) 
            if (isIp) then
                NetworkCommandQueue.EndResolveDNS(ipAddr, true, (x, port))
            else
                let success = LocalDNS.BeginDNSResolve(x.MachineName, NetworkCommandQueue.EndResolveDNS, (x, port))
                if (not success) then
                    x.MarkFail()
    /// 3. Constructor when connecting to machine with address/port (w/o DNS resolve)
    new ( addr: IPAddress, port : int, onet : NetworkConnections ) as x = 
        new NetworkCommandQueue(onet)
        then
            let name = LocalDNS.GetHostByAddress( addr.GetAddressBytes(), true )
            x.MachineName <- if Utils.IsNull name then addr.ToString() else name
            x.Port <- port    
            x.ConnectionStatusSet <- ConnectionStatus.BeginConnect
            x.BeginConnect(addr, port)
    //instead of overloading, use static member for clarity
    // 4. Constructor for loopback connect
    // caller needs to be responsible for disposing the returned NetworkCommandQueue
    static member LoopbackConnect(port : int, onet : NetworkConnections, requireAuth : bool, myguid : Guid, rsaParam : byte[]*byte[], pwd : string) =
        let mutable x = null
        let mutable socket = null
        let mutable success = false
        try
            x <- new NetworkCommandQueue(onet)
            // overwrite auth info
            x.RequireAuth <- requireAuth
            x.MyGuid <- myguid
            if (requireAuth) then
                x.MyAuthRSA <- Crypt.RSAFromPrivateKey(fst rsaParam, pwd)
                x.MyExchangeRSA <- Crypt.RSAFromPrivateKey(snd rsaParam, pwd)
                let connList : ConcurrentDictionary<Guid, byte[]*byte[]> = x.ONet.AllowedConnections
                connList.[myguid] <- (x.MyAuthRSA |> Crypt.RSAToPublicKey, x.MyExchangeRSA |> Crypt.RSAToPublicKey)
            x.MachineName <- "Loopback"
            x.Port <- port
            socket <- new Socket( AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp )
            if not (x.ONet.IpAddr.Equals("")) then
                socket.Bind(new IPEndPoint(IPAddress.Parse(x.ONet.IpAddr), 0))
            // set loopback option
            try 
                let OptionInValue = BitConverter.GetBytes(1)
                socket.IOControl( DeploymentSettings.SIO_LOOPBACK_FAST_PATH, OptionInValue, null ) |> ignore
            with 
            | e ->
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Fail to set loopback fast path... " ))
            // connect
            try
                socket.Connect( Net.IPAddress.Loopback, port)
                x.InitConnection(socket)
                success <- true
                x
            with
            | e ->
                x.MarkFail()
                reraise()
        finally
            if not success then
                if Utils.IsNotNull x then (x :> IDisposable).Dispose()
                if Utils.IsNotNull socket then socket.Dispose()

    member x.AddLoopbackProps(requireAuth : bool, myguid : Guid, rsaParam : byte[]*byte[], pwd : string) =
        x.RequireAuth <- requireAuth
        x.MyGuid <- myguid
        if (requireAuth) then
            x.MyAuthRSA <- Crypt.RSAFromPrivateKey(fst rsaParam, pwd)
            x.MyExchangeRSA <- Crypt.RSAFromPrivateKey(snd rsaParam, pwd)
            let connList : ConcurrentDictionary<Guid, byte[]*byte[]> = x.ONet.AllowedConnections
            connList.[myguid] <- (x.MyAuthRSA |> Crypt.RSAToPublicKey, x.MyExchangeRSA |> Crypt.RSAToPublicKey)
        x.MachineName <- "Loopback"

    static member private EndResolveDNS (addr : IPAddress, success : bool, o : obj) =
        let (x, port) = o :?> NetworkCommandQueue*int
        if (not success) then
            x.MarkFail()
        else
            x.BeginConnect(addr, port)

    // ======================================
    member x.Conn with get() = xgc
    member val private MachineName = "" with get, set
    member val private Port = 0 with get, set
    member val private ONet : NetworkConnections = null with get, set
    member internal x.Socket with get() = (xgc :> IConn).Socket
    member val private AESSend : AesCryptoServiceProvider = null with get, set
    member val private AESRecv : AesCryptoServiceProvider = null with get, set
    /// Connection Status
    member x.ConnectionStatus with get() = connectionStatus
    member private x.ConnectionStatusSet with set(v) = connectionStatus <- v
    /// Number of time it fails.
    member val private NumFailed = 0 with get, set

    /// The internal sending component which processes NetworkCommand objects and converts them from NetworkCommand to SocketAsyncEventArgs
    member internal x.CompSend with get() = xCSend
    /// The internal receiving component which processes NetworkCommand objects
    member internal x.CompRecv with get() = xCRecv

    member private x.RecvCmdQ with get() = recvCmdQ and set(v) = recvCmdQ <- v
    member private x.SendCmdQ with get() = sendCmdQ and set(v) = sendCmdQ <- v

    /// Tells if NetworkCommandQueue has failed
    member x.HasFailed with get() = (x.NumFailed > 0)
    /// The remote endpoint of the connection
    member x.RemoteEndPoint with get() = remoteEndPoint
    /// The local endpoint of the connection
    member x.LocalEndPoint with get() = localEndPoint
    /// Int64 value of the RemoteEndPoint
    member x.RemoteEndPointSignature with get() = remoteEndPointSignature
    /// <summary> 
    /// Listen to OnConnect event - use OnConnectAdd to add to this
    /// </summary>
    member val internal OnConnect = ExecuteUponOnce() with get
    /// <summary> 
    /// Listen to Disconnect event - use OnDisconnectAdd to add to this
    /// </summary>
    member val internal OnDisconnect = ExecuteUponOnce() with get
    /// Tells if connection is capable of receiving more data
    member x.Shutdown with get() = bNoMoreRecv

    /// Length of receiving queue of SocketAsyncEventArgs in internal GenericConn
    member x.ReceivingQueueLength 
        with get() =
            if Utils.IsNull xgc.CompRecv.Q then 
                0
            else
                xgc.CompRecv.Q.Count
    /// Length of sending queue of SocketAsyncEventArgs in internal GenericConn
    member x.SendQueueLength
        with get() =
            if Utils.IsNull xgc.CompSend.Q then 
                0
            else
                xgc.CompSend.Q.Count
    /// Length of receiving queue of NetworkCommand
    member x.ReceivingCommandQueueLength
        with get() =
            if Utils.IsNull xCRecv.Q then
                0
            else
                xCRecv.Q.Count
    /// Length of sending queue of NetworkCommand
    member x.SendCommandQueueLength
        with get() =
            if Utils.IsNull xCSend.Q then
                0
            else
                xCSend.Q.Count
    /// Length of sending queue of NetworkCommand in units of bytes
    member x.SendQueueSize
        with get() =
            let sendQ0 = xCSend.Q 
            if Utils.IsNull sendQ0 then 
                0L 
            else
                let sendQ = sendQ0 :?> FixedSizeQ<NetworkCommand>
                sendQ.CurrentSize
    /// Length of receiving queue of SocketAsyncEventArgs in units of bytes in internal GenericConn
    member x.RecvQueueSize
        with get() =
            // JinL: 03/18/2015, need to check for null, otherwise program crashes due to uncatached exception in MonitorChannels
            let recvQ0 = xgc.CompRecv.Q 
            if Utils.IsNull recvQ0 then 
                0L
            else
                let recvQ = recvQ0 :?> FixedSizeQ<RBufPart<byte>>
                recvQ.CurrentSize

    member val private CommandRcvd = 0L with get, set
    /// The total bytes recieved by connection
    member x.TotalBytesRcvd with get() = totalBytesRcvd
    /// The total bytes sent by connection
    member x.TotalBytesSent with get() = totalBytesSent
    /// The index of the last received command
    member x.RcvdCommandSerial with get() = rcvdCommandSerial
    /// Tells if event initialized has been set
    member x.Initialized with get() = bInitialized
    /// A stopwatch which starts when connection established
    member val Stopwatch = Stopwatch() with get
    /// The maximum size of sending token bucket
    member x.MaxTokenSize with get() = xgc.MaxTokenSize and set(v) = xgc.MaxTokenSize <- v

    member val LastSendTicks = DateTime.MinValue with get,set

    /// Monitor the connection
    member x.MonitorRcvd() =
        if (x.CommandRcvd % 1000L = 0L) then
            let bw = double(x.TotalBytesRcvd) * 8000.0 / double(x.Stopwatch.ElapsedMilliseconds)
            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Bandwidth = %f" bw))

    // per connection authentication
    /// Whether authentication is required or not - if authentication required other side has AES key for sending encrypted messages here
    member val RequireAuth = false with get, set
    /// GUID of this connection
    member val MyGuid = Guid() with get, set
    /// A RSA Crypto service provider for authentication
    member val MyAuthRSA : RSACryptoServiceProvider = null with get, set
    /// A RSA Cryto service provider for key exchange
    member val MyExchangeRSA : RSACryptoServiceProvider = null with get, set

    // flow control
    member val private CommandSizeQ = ConcurrentQueue<int>() with get
    member val flowcontrol_lastRcvdCommandSerial = ref -1L
    member val flowcontrol_lock = ref 0
    member val flowcontrol_lastack = ref (PerfDateTime.UtcNow())


    /// A count of unprocessed bytes (those which have not been acknowledged)
    member x.UnProcessedCmdInBytes
        with get() =
            !unProcessedBytes
    /// UnProcessedCmdInBytes in units of MB
    member x.UnProcessedCmdInMB
        with get() =
            (int)(!unProcessedBytes >>> 20)

    /// Current speed limit on the receiving interface
    member x.RcvdSpeed with get() = rcvdSpeed
    /// Current sending limit
    member x.SendSpeed with get() = sendSpeed
    /// Set the sending speed for this connection
    member x.SetSendSpeed(sendSpeedNew : int64) =
        sendSpeed <- sendSpeedNew
        xgc.SendSpeed <- Math.Min(sendSpeed, rcvdSpeed)
    /// Set the receiving speed for this connection
    member x.SetRcvdSpeed(rcvdSpeedNew : int64) =
        rcvdSpeed <- rcvdSpeedNew
        x.ONet.AdjustSendSpeeds()

    // blocked command
    /// Last time pending command was monitored
    member val LastMonitorPendingCommand = (PerfDateTime.UtcNow()) with get, set
    /// The pending command which has not yet been processed
    member val PendingCommand : (Option<ControllerCommand*StreamBase<byte>>*DateTime) = (None, (PerfDateTime.UtcNow())) with get, set
    member val private PendingCommandTimerEvent = new ManualResetEvent(false)
    //member val PendingCommandTimer = Timer(x.SetPendingCommandEventO, null, Timeout.Infinite, Timeout.Infinite)
    member val private PendingCommandTimerKey = PoolTimer.GetTimerKey()
    member private x.SetPendingCommandEvent() =
        x.PendingCommandTimerEvent.Set() |> ignore
    member private x.SetPendingCommandEventO(o) =
        x.PendingCommandTimerEvent.Set() |> ignore
    /// Get Event for pending command if there is one, else null
    /// <returns>The event to wait for, or null if a wait is not required.</returns>
    member x.GetPendingCommandEventIfNeeded() =
        let (pendingCommand, pendingCommandTime) = x.PendingCommand
        match pendingCommand with
            | Some(pcmd) ->
                x.PendingCommandTimerEvent.Reset() |> ignore
                PoolTimer.AddTimer(x.SetPendingCommandEvent, int64 DeploymentSettings.SleepTimeJobLoop)
                x.PendingCommandTimerEvent
            | None ->
                null

    member val private Index = Interlocked.Increment(count) with get     

// old code
//        // modifiy packet enqueueing (enqueue of SocketAsyncEventArgs)
//        let recvQ = (xgc.CompRecv.Q :?> FixedSizeQ<RBufPart<byte>>)
//        let recvEnqueue (rb : RBufPart<byte>) =
//            // modifiy packet enqueueing (enqueue of SocketAsyncEventArgs)
//            let e = (rb.Buf :?> RefCntBufSA).SA
//            let isFull(q : FixedSizeQ<_>, size)() = (((q.CurrentSize + size) > q.MaxSize && (q.CurrentSize > 0L)) || q.Count > q.MaxLen) && not xCRecv.Q.IsEmpty
//            let eq (size : int64) (t) =
//                recvQ.Q.Enqueue(t)
//                Interlocked.Add(recvQ.CurrentSizeRef, size) |> ignore
//                Interlocked.Add(x.ONet.TotalSARecvSize, size) |> ignore
//                recvQ.Full.Set() |> ignore
//            let size = int64 e.BytesTransferred
//            recvQ.EnqueueWaitTimeCond rb (int64 e.BytesTransferred) isFull
//        xgc.RecvQEnqueue <- recvEnqueue
//        //xgc.RecvQEnqueue <- 
//        // modify packet enquing non-empty condition
//        let isDesiredFull() =
//            ((!x.ONet.TotalSARecvSize > int64 x.ONet.MaxMemory && (recvQ.CurrentSize > 0L)) || recvQ.Count > recvQ.DesiredLen) && not xCRecv.Q.IsEmpty
//        //let isDesiredFull() =
//        //    ((recvQ.CurrentSize > recvQ.DesiredSize) || recvQ.Count > recvQ.DesiredLen) && not xCRecv.Q.IsEmpty
//        recvQ.IsFullDesiredCond <- isDesiredFull

    member private x.StartConnection() =
        connectionStatus <- ConnectionStatus.Verified
        eVerified.Set() |> ignore
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Magic Num is Verified from socket %s, status:%A" (LocalDNS.GetShowInfo(x.Socket.RemoteEndPoint)) x.ConnectionStatus))

        recvSAQ <- xgc.CompRecv.Q :?> FixedSizeQ<RBufPart<byte>>
        sendSAQ <- xgc.CompSend.Q :?> FixedLenQ<RBufPart<byte>>

        // initialize processing current command
        curStateRecv <- ReceivingMode.ReceivingCommand
        xgc.CurBufRecv <- headerRecv
        xgc.CurBufRecvOffset <- 0
        xgc.CurBufRecvRem <- headerRecv.Length
        xgc.CurBufRecvMs <- null

        // make connections
        xCSend.ConnectTo x.ONet.CmdProcPoolSend false xgc.CompSend None None
        // xgc.CompSend already connected      
        xgc.CompRecv.ConnectTo x.ONet.bufProcPool true xCRecv None None
        //xCRecv.ConnectTo null (Some(fun _ -> bNoMoreRecv <- true)) None
        xCRecv.ConnectTo x.ONet.CmdProcPoolRecv false null None None

        // initialize processing - can overwrite following prior to start:
        // 1. Enqueue logic (not part of component)
        // 2. .Dequeue - part of component
        // 3. .Proc - part of component
        // 4. .ReleaseItem - part of component
        // 5. AfterSendCallback - not part of component, but do not use (put in release instead)
        // Cmd Send ====
        // 1. Enqueue, utilize correct function in ToSend
        xCSend.Dequeue <- x.DQSendCmd
        xCSend.Proc <- processSend
        xCSend.ReleaseItem <- x.SendCmdRelease
        // SA Recv ====
        xgc.RecvQEnqueue <- x.EQRecvSASizeWait
        xgc.CompRecv.Dequeue <- xgc.RecvDequeueGenericConn xgc.CompRecv.Dequeue
        xgc.CompRecv.Proc <- xgc.ProcessRecvGenericConnMs x.ProcessRecvCommand
        xgc.CompRecv.ReleaseItem <- x.RecvSARelease
        // SA Send ====
        // Enqueue, change in in xCSend.Processing (see above) as argument to ProcessSendGenericConn in processSend
        // xgc.CompSend.Dequeue - already set
        // xgc.CompSend.Proc - already set in genericnetwork
        xgc.CompSend.ReleaseItem <- x.SendSARelease
        // Cmd Recv ===
        // Enqueue, change in EnqueueCmd
        // xCRecv.Dequeue - use standard DequeueWaitTime
        // xCRecv.Proc - set below using InitMultipleProcess
        xCRecv.ReleaseItem <- x.RecvCmdRelease

        // start base network (generic network)
        xgc.StartNetwork()
        // start NetworkCommand sender
        xCSend.StartProcess x.ONet.CmdProcPoolSend xgc.CTS ("CmdSend:"+xgc.ConnKey) (fun k -> "CommandSendPool:" + k)
        // perform callback - future enumerations will automatically invoke
        x.OnConnect.Trigger()
        // no networking starts unitl initialized set to true
        ThreadPoolWaitHandles.safeWaitOne( eInitialized ) |> ignore
        // start current processing recv pool - provided its needed (at least one processor must be registered)
        if (xCRecv.Processors.Count > 0) then
            // start NetworkCommand receiver processing
            xCRecv.InitMultipleProcess()
            xCRecv.StartProcess x.ONet.CmdProcPoolRecv xgc.CTS ("CmdRecv:"+xgc.ConnKey) (fun k -> "CommandRecvPool:" + k)

    static member private BufferEqual (buf1 : byte[]) (buf2 : byte[]) =
        if (buf1.Length <> buf2.Length) then
            false
        else
            let mutable i = 0
            let mutable bContinue = true
            while (i < buf1.Length && bContinue) do
                if (buf1.[i] <> buf2.[i]) then
                    bContinue <- false
                else
                    i <- i + 1
            (i = buf1.Length)

    member private x.AsyncSendBufWSizeTryCatch(buf : byte[], bufferOffset : int, bufferSize : int) =
        try
            xgBuf.AsyncSendBufWithSize(buf, bufferOffset, bufferSize)
        with e ->
            x.MarkFail()

    member private x.AsyncRecvBufWSizeTryCatch(callback : obj*byte[]*int*int->unit, state : obj, bufferOffset : int) =
        let wrappedFunc(state, buf, offset, size) =
            try
                callback(state, buf, offset, size)
            with e->
                Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Async Recv Buffer fails with exception %A" e))
                x.MarkFail()
        try
            xgBuf.AsyncRecvBufWithSize(wrappedFunc, state, bufferOffset)
        with e->
            x.MarkFail()

    /// Add receiver processing - multiple processeros may be added
    /// <param name="processItem">The function to process NetworkCommand objects from queue</param>
    member x.AddRecvProc (processItem : NetworkCommand->ManualResetEvent) =
        xCRecv.AddProc(processItem)

    /// Add receiver processing - multiple processeros may be added
    /// <param name="processItem">The function to process NetworkCommand objects from queue</param>
    member x.GetOrAddRecvProc (name, processItem : NetworkCommand->ManualResetEvent) =
        xCRecv.GetOrAddProc(name, processItem)

    member private x.SetRecvAES(buf : byte[]) =
        x.AESRecv <- new AesCryptoServiceProvider()
        use ms = new MemStream(buf)
        x.AESRecv.BlockSize <- ms.ReadInt32()
        x.AESRecv.KeySize <- ms.ReadInt32()
        x.AESRecv.Key <- ms.ReadBytesWLen()

    member private x.Verification(o : obj, buf : byte[], bufOffset : int, bufSize : int) =
        let cmd : VerificationCmd = EnumOfValue(buf.[bufOffset])
        let recvBuf = Array.sub buf (bufOffset+1) (bufSize-1)
        match cmd with
            | VerificationCmd.AESKeyUsingRSA ->
                Logger.LogF( LogLevel.Info, (fun _ -> "Receive RSA Encrypted information"))
                let keyBufLen = BitConverter.ToInt32(recvBuf, 0)
                let decryptBuf = x.MyExchangeRSA.Decrypt(Array.sub recvBuf sizeof<int32> keyBufLen, false)
                x.SetRecvAES(decryptBuf)
                // send signature to verify yourself of entire recvBuf
                let signature = x.MyAuthRSA.SignData(recvBuf, new SHA256CryptoServiceProvider())
                let bufToSend = Array.concat([[|byte VerificationCmd.AESVerificationUsingRSA|]; signature])
                x.AsyncSendBufWSizeTryCatch(bufToSend, 0, bufToSend.Length)
                bLocalVerified <- true // not really, but can continue, other side will terminate if fails
            | VerificationCmd.AESKeyUsingPwd ->
                Logger.LogF( LogLevel.Info, (fun _ -> "Receive Pwd Encrypted information"))
                use ms = new MemStream()
                try 
                    let decryptBuf = Crypt.DecryptWithParams(recvBuf, x.ONet.Password)
                    x.SetRecvAES(decryptBuf)
                    ms.WriteBytes(x.MyGuid.ToByteArray())
                    ms.WriteBytesWLen(Crypt.RSAToPublicKey x.MyAuthRSA)
                    ms.WriteBytesWLen(Crypt.RSAToPublicKey x.MyExchangeRSA)
                    let cryptBuf = Crypt.EncryptWithParams(ms.GetValidBuffer(), x.ONet.Password)
                    let bufToSend = Array.concat([[|byte VerificationCmd.AESVerificationUsingPwd|]; HashByteArray(decryptBuf); cryptBuf])
                    x.AsyncSendBufWSizeTryCatch(bufToSend, 0, bufToSend.Length)
                    bLocalVerified <- true
                with e ->
                    let str = "Invalid password during authentication, connection terminates"
                    Logger.LogF(LogLevel.Info, fun _ -> str)
                    x.MarkFail()
                    raise(Exception(str))
            | VerificationCmd.AESVerificationUsingRSA ->
                let (key, signBuf) = o :?> byte[]*byte[]
                let rsa = Crypt.RSAFromKey(key)
                if not (rsa.VerifyData(signBuf, new SHA256CryptoServiceProvider(), recvBuf)) then
                    Logger.LogF( LogLevel.Info, (fun _ -> "RSA signature does not verify - terminate"))
                    x.MarkFail()
                else
                    bRemoteVerified <- true
                    Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Connection verification from %s succeeds using RSA" x.EPInfo))
            | VerificationCmd.AESVerificationUsingPwd ->
                let (sentBufHash) = o :?> byte[]
                if not (NetworkCommandQueue.BufferEqual (Array.sub recvBuf 0 sentBufHash.Length) sentBufHash) then
                    Logger.LogF( LogLevel.Info, (fun _ -> "Hash using Pwd does not match - terminate"))
                    x.MarkFail()
                else
                    bRemoteVerified <- true
                    let decryptBuf = Crypt.DecryptWithParams(Array.sub recvBuf sentBufHash.Length (recvBuf.Length-sentBufHash.Length), x.ONet.Password)
                    use ms = new MemStream(decryptBuf)
                    let guid = Guid(ms.ReadBytes(sizeof<Guid>))
                    let publicKey = ms.ReadBytesWLen()
                    let exchangePublicKey = ms.ReadBytesWLen()
                    Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Connection verification from %s succeeds using password - adding %s to allowed connection list" x.EPInfo (guid.ToString("N"))))
                    if not (fst (x.ONet.CheckForAllowedConnection(guid))) then
                        x.ONet.AddToAllowedConnection(guid, publicKey, exchangePublicKey)
            | _ ->
                // unknown command or failure, terminate
                x.MarkFail()
        // post another receive until verified
        if (bLocalVerified && bRemoteVerified) then
            x.StartConnection()
        else if (not x.HasFailed) then
            x.AsyncRecvBufWSizeTryCatch(x.Verification, o, 0)        

    member private x.StartVerification(o : obj, buf : byte[], bufOffset : int, bufSize : int) =
        if (bufSize < MagicNumberBuf.Length + sizeof<Guid> + sizeof<bool>) then
            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Fail - initial buffer received is too small"))
            x.MarkFail()
        else
            let guid = Guid(Array.sub buf bufOffset MagicNumberBuf.Length)
            if (guid.CompareTo(MagicNumber) <> 0) then
                Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Fail - magic number not correct"))
                x.MarkFail()
            else
                let remoteGuid = Guid(Array.sub buf MagicNumberBuf.Length sizeof<Guid>)
                bLocalVerified <- not (BitConverter.ToBoolean(buf, MagicNumberBuf.Length+sizeof<Guid>))
                // generate AES key for encryption / decryption
                if (x.RequireAuth) then
                    // generate AES key
                    x.AESSend <- new AesCryptoServiceProvider()
                    x.AESSend.BlockSize <- Crypt.DefaultBlkSize
                    x.AESSend.KeySize <- Crypt.DefaultKeySize
                    x.AESSend.GenerateKey()
                    // search for GUID
                    let (allowed, key) = x.ONet.CheckForAllowedConnection(remoteGuid)
                    let buf2 = Array.zeroCreate<byte>(1024)
                    NetUtils.RandGen.NextBytes(buf2)
                    if (allowed) then
                        // use public encryption key of remote end to encrypt buffer
                        let rsa = Crypt.RSAFromKey(snd key)
                        use ms= new MemStream()
                        ms.WriteInt32(x.AESSend.BlockSize)
                        ms.WriteInt32(x.AESSend.KeySize)
                        ms.WriteBytesWLen(x.AESSend.Key)
                        let encryptBuf = rsa.Encrypt(ms.GetValidBuffer(), false)
                        let signBuf = Array.concat([BitConverter.GetBytes(encryptBuf.Length); encryptBuf; buf2])
                        let bufToSend = Array.concat([[|byte VerificationCmd.AESKeyUsingRSA|]; signBuf])
                        x.AsyncSendBufWSizeTryCatch(bufToSend, 0, bufToSend.Length)
                        x.AsyncRecvBufWSizeTryCatch(x.Verification, (fst key, signBuf), 0)
                    else
                        // create buffer with information
                        if (3*(sizeof<int>)+x.AESSend.Key.Length > 1024) then
                            failwith "Illegal blk /key size"
                        use ms = new MemStream(buf2, true)
                        ms.WriteInt32(x.AESSend.BlockSize)
                        ms.WriteInt32(x.AESSend.KeySize)
                        ms.WriteBytesWLen(x.AESSend.Key)
                        // use AES encryption using symmetric encryption (AES) using shared secret (password)
                        let bufToSend = Array.concat([[|byte VerificationCmd.AESKeyUsingPwd|]; Crypt.EncryptWithParams(buf2, x.ONet.Password)])
                        x.AsyncSendBufWSizeTryCatch(bufToSend, 0, bufToSend.Length)
                        x.AsyncRecvBufWSizeTryCatch(x.Verification, HashByteArray(buf2), 0)
                else
                    bRemoteVerified <- true
                    if (not bLocalVerified) then
                        x.AsyncRecvBufWSizeTryCatch(x.Verification, null, 0)
                    else
                        x.StartConnection()

    member private x.Start() =
        xgc.OnSocketClose <- Some(x.OnSocketClose)
        xgc.OnSocketCloseState <- null
        // send (magic number, guid, bool)
        let buf = Array.concat([MagicNumberBuf; x.MyGuid.ToByteArray(); BitConverter.GetBytes(x.RequireAuth)])
        x.AsyncSendBufWSizeTryCatch(buf, 0, buf.Length)
        x.AsyncRecvBufWSizeTryCatch(x.StartVerification, null, 0)

    member private x.InitConnection(soc : Socket) =
        soc.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true)
        soc.SendBufferSize <- TCPSendBufSize
        soc.ReceiveBufferSize <- TCPRcvBufSize
        soc.SendTimeout <- Int32.MaxValue
        soc.ReceiveTimeout <- Int32.MaxValue
        soc.NoDelay <- true
        remoteEndPoint <- soc.RemoteEndPoint
        localEndPoint <- soc.LocalEndPoint
        remoteEndPointSignature <- LocalDNS.IPEndPointToInt64(soc.RemoteEndPoint :?> IPEndPoint)
        epInfo <- LocalDNS.GetShowInfo(soc.RemoteEndPoint)
        //xgc.SetTokenUse(16384L*8L, (int64 DeploymentSettings.SendTokenBucketSize)*8L, usedMSS, Math.Min(x.SendSpeed, x.RcvdSpeed))
        xgc.InitConnectionAndStart(soc, x.ONet) x.Start

    /// Begin connection to IP address
    /// <param name="addr">The IPAddress to connect to</param>
    /// <param name="port">The port to connect to</param>
    member x.BeginConnect( addr:IPAddress, port ) = 
        let mutable soc = null
        try 
            x.Port <- port  
            remoteEndPoint <- IPEndPoint( addr, port )
            remoteEndPointSignature <- LocalDNS.IPEndPointToInt64( x.RemoteEndPoint :?> IPEndPoint ) 
            soc <- new Socket( AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp )
            if not (x.ONet.IpAddr.Equals("")) then
                Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Local bind to %A" x.ONet.IpAddr)
                soc.Bind(new IPEndPoint(IPAddress.Parse(x.ONet.IpAddr), 0))
            connectionStatus <- ConnectionStatus.BeginConnect
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "BeginConnect to host %s with address %A" (x.MachineName) (addr.ToString()) ))
            let ar = soc.BeginConnect( addr, port, AsyncCallback(NetworkCommandQueue.EndConnect), (x, soc) )
            ()
        with 
        | e ->
            x.MarkFail() 
            if Utils.IsNotNull soc then soc.Dispose()
            Logger.Log( LogLevel.Error, (sprintf "NetworkCommandQueue.BeginConnect failed with exception %A" e ))
            
    static member private EndConnect( ar ) =
        let (x, soc) = ar.AsyncState :?> (NetworkCommandQueue*Socket)
        if not x.Shutdown then 
            try
                soc.EndConnect( ar )
                x.ConnectionStatusSet <- ConnectionStatus.Connected
                x.InitConnection(soc)
            with
            | e ->
                x.MarkFail()
                (x :> IDisposable).Dispose()
                soc.Dispose()                
                Logger.Log( LogLevel.Error, (sprintf "NetworkCommandQueue.BEndConnect failed with exception %A" e ))

    /// Initialize the NetworkCommandQueue - Only call after AddRecvProc are all done
    member x.Initialize() =
        x.Stopwatch.Start()
        eInitialized.Set() |> ignore
        bInitialized <- true

    member val private CloseDone = ref 0 with get
    /// <summary>
    /// Normal close of socket/queue, 
    /// </summary>
    abstract Close : unit -> unit
    default x.Close() =
        if (Interlocked.CompareExchange(x.CloseDone, 1, 0) = 0) then
            // Logger.LogStackTrace(LogLevel.MildVerbose)
            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Close of NetworkCommandQueue %s" x.EPInfo))
            Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "SA Recv Stack size %d %d" x.ONet.BufStackRecv.StackSize x.ONet.BufStackRecv.GetStack.Size)
            Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "SA Send Stack size %d %d" x.ONet.BufStackSend.StackSize x.ONet.BufStackSend.GetStack.Size)
            Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Memory Stream Stack size %d %d" MemoryStreamB.MemStack.StackSize MemoryStreamB.MemStack.GetStack.Size)
            MemoryStreamB.DumpStreamsInUse()
            MemoryStreamB.MemStack.DumpInUse(LogLevel.MildVerbose)
            x.ONet.BufStackRecv.DumpInUse(LogLevel.MildVerbose)
            x.ONet.BufStackSend.DumpInUse(LogLevel.MildVerbose)
            xCSend.SelfClose()
            // manually terminate everything - this will do following
            // 1) clear the queues, 2) set the IsTerminated flag, 3) set event being waited upon
            xCSend.SelfTerminate()
            xCRecv.SelfTerminate()
            xgc.CompSend.SelfTerminate()
            xgc.CompRecv.SelfTerminate()
            // in case in middle of sending, callback does not execute
            xgc.SendFinished.Set() |> ignore

    member private x.OnSocketClose (conn : IConn) (o : obj) =
        if (Utils.IsNotNull x.ONet) then
            x.ONet.RemoveConnect(x)
        x.Close()
        // also clear send queue
        xCSend.Q.Clear()

    /// <summary> 
    /// Terminate network queue, all pending actions are discarded. 
    /// </summary>
    member x.Terminate() = 
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Terminate of NetworkCommandQueue %s" x.EPInfo))
        // terminate propagates, through pipeline and back
        xCSend.SelfTerminate()
        if (Utils.IsNotNull x.ONet) then
            x.ONet.RemoveConnect(x)
        x.Close()
        // explicitly set shutdown to true since threadpools may not have initialized
        bNoMoreRecv <- true

    /// Mark the connection as having failed and terminate
    member x.MarkFail() = 
        x.NumFailed <- x.NumFailed + 1
        x.Terminate()

    /// Tells if queue is null or has failed
    static member inline QueueFail(x : NetworkCommandQueue) =
        Utils.IsNull x || x.HasFailed

    // processing of received commands =====================
    member private x.UpdateFlowControl(diff : int, iLastSeq : int) =
        let mutable commandSize = ref 0
        let mutable i = 0
        let mutable ackSize = 0
        while (i < diff && x.CommandSizeQ.Count > 0) do
            if (x.CommandSizeQ.TryDequeue(commandSize)) then
                ackSize <- ackSize + !commandSize
                i <- i + 1
        Interlocked.Add(unProcessedBytes, -(int64 ackSize)) |> ignore
        if (i <> diff || diff < 0 || !unProcessedBytes < 0L) then
            let iProc = i
            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "ACK information incorrect NewACK: %d UnprocessedACK: %d UnprocessedBytes: %d iLastRecvdSeqNo: %d iLastSeq(from header): %d " diff iProc x.UnProcessedCmdInBytes iLastRecvdSeqNo iLastSeq))
        iLastRecvdSeqNo <- iLastRecvdSeqNo + i

    member x.EPInfo with get() = epInfo

    member private x.TraceCommand(cmd : NetworkCommand, s : unit->string) =
        if (cmd.cmd.Verb = ControllerVerb.SyncClose ||
            cmd.cmd.Verb = ControllerVerb.ConfirmClose (*||
            cmd.cmd.Verb = ControllerVerb.SyncClosePartition ||
            cmd.cmd.Verb = ControllerVerb.ConfirmClosePartition ||
            (cmd.cmd.Verb = ControllerVerb.Close && cmd.cmd.Noun = ControllerNoun.Job)*))  then
            Logger.LogF( LogLevel.Info, (fun _ -> sprintf "%s %A %s" x.EPInfo cmd.cmd (s())))

    member private x.TraceCurRecvCommand(s : unit->string) =
        if (curRecvCmd.cmd.Verb = ControllerVerb.SyncClose ||
            curRecvCmd.cmd.Verb = ControllerVerb.ConfirmClose (*||
            curRecvCmd.cmd.Verb = ControllerVerb.SyncClosePartition ||
            curRecvCmd.cmd.Verb = ControllerVerb.ConfirmClosePartition ||
            (curRecvCmd.cmd.Verb = ControllerVerb.Close && curRecvCmd.cmd.Noun = ControllerNoun.Job)*))  then
            Logger.LogF( LogLevel.Info, (fun _ -> sprintf "%s %A %s" x.EPInfo curRecvCmd.cmd (s())))

    member private x.BuildCommand() =
//        let iLastSeq = int (BitConverter.ToUInt16(headerRecv, 6))
//        Logger.LogF( LogLevel.ExtremeVerbose, fun _ -> sprintf "Receive command from %s of len: %d ack: %d prev: %d" x.EPInfo (body.Length+4) iLastSeq iLastRecvdSeqNo )
//        let diff = int (Operators.uint16(iLastSeq - iLastRecvdSeqNo)) // unchecked for overflow
//        x.UpdateFlowControl(diff, iLastSeq)
        let command = ControllerCommand(enum<ControllerVerb>(int headerRecv.[4]), enum<ControllerNoun>(int headerRecv.[5]))
        if (command.Verb = ControllerVerb.Decrypt) then
            // decrypt the body
            use decryptMs = Crypt.DecryptStream(x.AESRecv, body)
            body.Dispose()
            decryptMs.ReadInt32() |> ignore
            let verb = enum<ControllerVerb>(int (decryptMs.ReadByte()))
            let noun = enum<ControllerNoun>(int (decryptMs.ReadByte()))
            let command = ControllerCommand(verb, noun)
            let ms = decryptMs.Replicate(decryptMs.Position, decryptMs.Length-decryptMs.Position)
            ms.Info <- sprintf "Cmd:%A:" command
            curRecvCmd <- new NetworkCommand(command, ms)
        else
            if (Utils.IsNotNull body) then
                body.Seek(0L, SeekOrigin.Begin) |> ignore
                body.Info <- sprintf "Info:%A:" command
            curRecvCmd <- new NetworkCommand(command, body)
            if (Utils.IsNotNull body) then
                body.Dispose()
            //x.TraceCurRecvCommand(fun _ -> sprintf "Built bodyLen:%d eRem: %d" body.Length xgc.ERecvRem)
        curStateRecv <- ReceivingMode.EnqueuingCommand
        
    member private x.EnqueueCmd() =
        //let (success, event) = xCRecv.Q.EnqueueWaitTime(curRecvCmd)
        let (success, event) = x.EQRecvCmdWait(curRecvCmd)
        if (success) then
            let iLastSeq = int (BitConverter.ToUInt16(headerRecv, 6))
            //Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Receive command from %s of len: %d ack: %d prev: %d" x.EPInfo (body.Length+4) iLastSeq iLastRecvdSeqNo))
            let diff = int (Operators.uint16(iLastSeq - iLastRecvdSeqNo)) // unchecked for overflow
            x.UpdateFlowControl(diff, iLastSeq)
            rcvdCommandSerial <- rcvdCommandSerial + 1L
            totalBytesRcvd <- totalBytesRcvd + curRecvCmd.CmdLen()
            x.CommandRcvd <- x.CommandRcvd + 1L

            //x.TraceCurRecvCommand(fun _ -> sprintf "Enqueue")
            curStateRecv <- ReceivingMode.ReceivingCommand
            xgc.CurBufRecv <- headerRecv
            xgc.CurBufRecvOffset <- 0
            xgc.CurBufRecvRem <- headerRecv.Length
            xgc.CurBufRecvMs <- null
            curRecvCmd <- null
        //else
        //    x.TraceCurRecvCommand(fun _ -> sprintf "Wait To Enqueue")
        event

    member private x.ProcessRecvCommand() : ManualResetEvent =
        match curStateRecv with
            | ReceivingMode.ReceivingCommand ->
                let totalLen = BitConverter.ToInt32(headerRecv, 0)
                // allow zero length array creation so memstream is not null
                let bodyLen = totalLen - 4
                if (bodyLen <> 0) then
                    body <- new MemoryStreamB()
                    curStateRecv <- ReceivingMode.ReceivingBody
                    xgc.CurBufRecv <- null
                    xgc.CurBufRecvMs <- body
                    xgc.CurBufRecvRem <- bodyLen
                    null
                else
                    body <- null
                    x.BuildCommand()
                    x.EnqueueCmd()
            | ReceivingMode.ReceivingBody ->
                x.BuildCommand()
                x.EnqueueCmd()
            | ReceivingMode.EnqueuingCommand ->
                x.EnqueueCmd()

    // processing of commands to send =====================
    member private x.BuildHeader(command : ControllerCommand, cmdLen : int, buf : byte[]) =
        let intBuf = BitConverter.GetBytes(cmdLen)
        Buffer.BlockCopy(intBuf, 0, buf, 0, 4)
        buf.[4] <- byte(command.Verb)
        buf.[5] <- byte(command.Noun)
        Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Send command to %s len: %d ack: %d" x.EPInfo cmdLen x.RcvdCommandSerial))
        let rcvdSerialBuf = BitConverter.GetBytes(Operators.uint16(x.RcvdCommandSerial))
        Buffer.BlockCopy(rcvdSerialBuf, 0, buf, 6, 2)

    member private x.BuildHeader(cmd : NetworkCommand, buf : byte[]) =
        x.BuildHeader(cmd.cmd, int(cmd.CmdLen())-4, buf)

    member private x.BuildHeader(command : ControllerCommand, cmdLen : int, ms : MemStream) =
        ms.WriteInt32(cmdLen)
        ms.WriteByte(byte(command.Verb))
        ms.WriteByte(byte(command.Noun))
        Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Send command to %s len: %d ack: %d" x.EPInfo cmdLen x.RcvdCommandSerial))
        ms.WriteUInt16(Operators.uint16(x.RcvdCommandSerial))

    member private x.UpdateHeader(cmd : NetworkCommand) =
        let pos = cmd.ms.Position
        cmd.ms.Seek(rcvdCommandSerialPosition, SeekOrigin.Begin) |> ignore
        cmd.ms.WriteUInt16(Operators.uint16(x.RcvdCommandSerial))
        cmd.ms.Seek(pos, SeekOrigin.Begin) |> ignore

    member private x.ProcessSendCmd(cmd : NetworkCommand)() =
        match curStateSend with
            | SendingMode.SendHeader ->
                curStateSend <- SendingMode.SendCommand
            | SendingMode.SendCommand ->
                ()
        let (buf, pos, cnt) = streamReader.GetMoreBuffer()
        xgc.CurBufSend <- buf
        xgc.CurBufSendOffset <- pos
        xgc.CurBufSendRem <- cnt
        sendLenRem <- sendLenRem - int64 xgc.CurBufSendRem
        if (Utils.IsNull buf) then
            if (sendLenRem <> 0L) then
                failwith "Send Len not correct"
            streamReader.Release()
            (true, 0=xCSend.Q.Count)
        else
            (false, false)

    member private x.ProcessSendCommand = x.ProcessSendCmd (!xCSend.Item)

    // Queue functions -------
    // Cmd Send ==============
    member inline private x.SendCmdFullMax (size : int64) () =
        ((!x.ONet.TotalSASendSize + !x.ONet.TotalCmdSendSize + size) > int64 x.ONet.MaxMemory ||
           sendCmdQ.CurrentSize > sendCmdQ.MaxSize ||
           sendCmdQ.Count > sendCmdQ.MaxLen) 
        && (sendCmdQ.CurrentSize > 0L) 

    member inline private x.SendCmdFullDesired() =
        ((!x.ONet.TotalSASendSize + !x.ONet.TotalCmdSendSize) > int64 x.ONet.MaxMemory ||
          sendCmdQ.CurrentSize > sendCmdQ.DesiredSize ||
          sendCmdQ.Count > sendCmdQ.DesiredLen)
        && (sendCmdQ.CurrentSize > 0L) 

    member inline private x.EQSendCmdAction (size : int64) (cmd : NetworkCommand) =
        sendCmdQ.Q.Enqueue(cmd)
        Interlocked.Add(sendCmdQ.CurrentSizeRef, size) |> ignore
        Interlocked.Add(x.ONet.TotalCmdSendSize, size) |> ignore
        sendCmdQ.Full.Set() |> ignore

    member inline private x.EQSendCmdSyncSize (cmd : NetworkCommand) =
        let size = cmd.CmdLen()               
        BaseQ<_>.EnqueueSyncNoInline(x.EQSendCmdAction size, cmd, x.SendCmdFullMax size, sendCmdQ.Empty)

    member inline private x.SendCmdFullMax1 (size : int64) () =
        false

    member inline private x.EQSendCmdSyncSizeNonBlock (cmd : NetworkCommand) =
        let size = cmd.CmdLen()               
        BaseQ<_>.EnqueueSyncNoInline(x.EQSendCmdAction size, cmd, x.SendCmdFullMax1 size, sendCmdQ.Empty)

    // dequeue does not set any events, just simple dequeue
    member private x.DQSendCmd (cmd : NetworkCommand ref) =
        let (success, event) = BaseQ<_>.DequeueNoInline((fun r -> sendCmdQ.Q.TryDequeue(r)), cmd, (fun() -> sendCmdQ.IsEmpty), sendCmdQ.Full, sendCmdQ.WaitTimeDequeueMs)
        if (success) then
            streamReader <- new StreamReader<byte>((!cmd).ms, (!cmd).startPos)
            sendLenRem <- (!cmd).CmdLen()
            if (!cmd).buildHeader then
                x.BuildHeader(!cmd, headerSend)
                curStateSend <- SendingMode.SendHeader
                xgc.CurBufSend <- headerSend
                xgc.CurBufSendOffset <- 0
                xgc.CurBufSendRem <- headerSend.Length
            else
                x.UpdateHeader(!cmd)
                //Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Send cmd to %s len: %d ack:L %d" xgc.ConnKey (BitConverter.ToInt32((!cmd).buffer, 0)) (BitConverter.ToUInt16((!cmd).buffer, 6))))
                curStateSend <- SendingMode.SendCommand
                let (buf, pos, cnt) = streamReader.GetMoreBuffer()
                xgc.CurBufSend <- buf
                xgc.CurBufSendOffset <- pos
                xgc.CurBufSendRem <- cnt
            sendLenRem <-  sendLenRem - int64 xgc.CurBufSendRem
            totalBytesSent <- totalBytesSent + (!cmd).CmdLen()

        (success, event)

    member private x.SendCmdRelease(cmd : NetworkCommand ref) =
        let size = (!cmd).CmdLen()
        (!cmd).Release()
        Interlocked.Add(sendCmdQ.CurrentSizeRef, -size) |> ignore
        Interlocked.Add(x.ONet.TotalCmdSendSize, -size) |> ignore
        Ev.SetOnNotCondNoInline(x.SendCmdFullDesired, sendCmdQ.Empty)

    // SA Send =====================
    member inline private x.SendSAFullMax() =
        sendSAQ.Q.Count + 1 > sendSAQ.MaxLen

    member inline private x.EQSendSAAction (e : SocketAsyncEventArgs) (rb : RBufPart<byte>) =
        sendSAQ.Q.Enqueue(rb)
        Interlocked.Add(x.ONet.TotalSASendSize, int64 e.Count) |> ignore
        sendSAQ.Full.Set() |> ignore

    member private x.EQSendSA (rb : RBufPart<byte>) =
        let e = (rb.Elem :?> RefCntBufSA).SA
        BaseQ<_>.EnqueueNoInline(x.EQSendSAAction e, rb, x.SendSAFullMax, sendSAQ.Empty, sendSAQ.WaitTimeEnqueueMs)

    // use standard DQ functions for fixed len q - standard dequeue will set sendSAQ.Empty upon empty

    member private x.SendSARelease(rb : RBufPart<byte> ref) =
        Interlocked.Add(x.ONet.TotalSASendSize, int64 (-(!rb).Count)) |> ignore
        Ev.SetOnNotCondNoInline(x.SendCmdFullDesired, sendCmdQ.Empty)

    // SA Recv ==========================
    member inline private x.RecvSAFullMax (size : int64) () =
        ((!x.ONet.TotalSARecvSize + !x.ONet.TotalCmdRecvSize + size) > int64 x.ONet.MaxMemory ||
         recvSAQ.CurrentSize > recvSAQ.MaxSize ||
         recvSAQ.Count > recvSAQ.MaxLen)
        && recvSAQ.CurrentSize > 0L
        && not recvCmdQ.Q.IsEmpty

    member inline private x.RecvSAFullDesired () =
        ((!x.ONet.TotalSARecvSize + !x.ONet.TotalCmdRecvSize) > int64 x.ONet.MaxMemory ||
         recvSAQ.CurrentSize > recvSAQ.DesiredSize ||
         recvSAQ.Count > recvSAQ.DesiredLen)
        && recvSAQ.CurrentSize > 0L
        && not recvCmdQ.Q.IsEmpty

    member inline private x.EQRecvSAAction (e : SocketAsyncEventArgs) (size : int64) (rb : RBufPart<byte>) =
        recvSAQ.Q.Enqueue(rb)
        Interlocked.Add(recvSAQ.CurrentSizeRef, size) |> ignore
        Interlocked.Add(x.ONet.TotalSARecvSize, size) |> ignore
        //Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "ID:%d Add:%d" rb.Buf.Id size)
        recvSAQ.Full.Set() |> ignore

    member private x.EQRecvSASizeWait (rb : RBufPart<byte>) =
        let e = (rb.Elem :?> RefCntBufSA).SA
        let size = int64 e.BytesTransferred
        BaseQ.EnqueueNoInline(x.EQRecvSAAction e size, rb, x.RecvSAFullMax size, recvSAQ.Empty, recvSAQ.WaitTimeEnqueueMs)

    // use standard dequeue for fixed size q (using RecvDequeueGenericConn) - won't set any events, use release to set

    member private x.RecvSARelease(rb : RBufPart<byte> ref) =
        // call the bease class SA Release
        xgc.RecvRelease(rb)
        let size = int64 (!rb).Count
        Interlocked.Add(recvSAQ.CurrentSizeRef, -size) |> ignore
        Interlocked.Add(x.ONet.TotalSARecvSize, -size) |> ignore
        Ev.SetOnNotCondNoInline(x.RecvSAFullDesired, recvSAQ.Empty)
        //Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "ID:%d Release:%d" (!rb).Buf.Id size)

    // Cmd Recv ===============================
    member inline private x.RecvCmdFullMax() =
        recvCmdQ.Q.Count + 1 > recvCmdQ.MaxLen

    member inline private x.EQRecvCmdAction (cmd : NetworkCommand) =
        recvCmdQ.Q.Enqueue(cmd)
        Interlocked.Add(x.ONet.TotalCmdRecvSize, cmd.CmdLen()) |> ignore
        recvCmdQ.Full.Set() |> ignore

    member private x.EQRecvCmdWait (cmd : NetworkCommand) =
        BaseQ.EnqueueNoInline(x.EQRecvCmdAction, cmd, x.RecvCmdFullMax, recvCmdQ.Empty, recvCmdQ.WaitTimeEnqueueMs)

    // use standard dequeue for fixed len q - sets recvCmdQ.Empty event when below desired length

    member private x.RecvCmdRelease (cmd : NetworkCommand ref) =
        Interlocked.Add(x.ONet.TotalCmdRecvSize, -(!cmd).CmdLen()) |> ignore
        Ev.SetOnNotCondNoInline(x.RecvSAFullDesired, recvSAQ.Empty)
        (!cmd).Release()
        // also release underlying stream, if someone needs it, should copy and replicate it
        // can't do for sending side, since multiple queues often contain same stream (since ToSend does not replicate)
        ((!cmd).ms :> IDisposable).Dispose()

    // ===========================

    /// Add command to sender queue using encryption - decryption takes place automatically
    /// enqueue takes place synchronously, blocks caller
    /// <param name="command">The ControllerCommand to send</param>
    /// <param name="arr">The associated buffer to send</param>
    /// <param name="offset">The offset into the buffer to send</param>
    /// <param name="arrCount">The length of content in buffer to send (starting at offset)</param>
    member x.ToSendEncrypt (command, arr:byte[], offset:int, arrCount:int) =
        if (connectionStatus < ConnectionStatus.Verified) then
            eVerified.Wait() |> ignore
        use ms = new MemStream(arrCount + 8)
        x.BuildHeader(command, arrCount + 4, ms)
        ms.Write(arr, offset, arrCount)
        let encryptMs = Crypt.EncryptStream(x.AESSend, ms)
        let cmd = new NetworkCommand(ControllerCommand(ControllerVerb.Decrypt, ControllerNoun.Message), encryptMs)
        x.CommandSizeQ.Enqueue(int(cmd.CmdLen()))
        Interlocked.Add(unProcessedBytes, int64 (cmd.CmdLen())) |> ignore
        let sendQ = xCSend.Q :?> FixedSizeQ<NetworkCommand>
        x.EQSendCmdSyncSize cmd |> ignore

    // ===========================================

    /// Add command to sender queue - enqueue takes place synchronously, blocks caller
    /// <param name="command">The ControllerCommand to send</param>
    /// <param name="sendStream">The associated MemStream to send</param>
    /// <param name="bExpediateSend">Optional - Unused parameter</param>
    member x.ToSend (command, sendStream:StreamBase<byte>, ?bExpediateSend) =
        let cmd = new NetworkCommand(command, sendStream)
        x.CommandSizeQ.Enqueue(int(cmd.CmdLen()))
        Interlocked.Add(unProcessedBytes, int64 (cmd.CmdLen())) |> ignore
        let sendQ = xCSend.Q :?> FixedSizeQ<NetworkCommand>
        //sendQ.EnqueueSyncSize cmd (int64 (cmd.CmdLen())) |> ignore
        x.EQSendCmdSyncSize cmd |> ignore
        x.LastSendTicks <- (PerfDateTime.UtcNow())

    member x.ToSendNonBlock (command, sendStream:StreamBase<byte>, ?bExpediateSend) =
        let cmd = new NetworkCommand(command, sendStream)
        x.CommandSizeQ.Enqueue(int(cmd.CmdLen()))
        Interlocked.Add(unProcessedBytes, int64 (cmd.CmdLen())) |> ignore
        let sendQ = xCSend.Q :?> FixedSizeQ<NetworkCommand>
        //sendQ.EnqueueSyncSize cmd (int64 (cmd.CmdLen())) |> ignore
        x.EQSendCmdSyncSizeNonBlock cmd |> ignore
        x.LastSendTicks <- (PerfDateTime.UtcNow())

    /// Add command to sender queue - enqueue takes place synchronously, blocks caller
    /// <param name="command">The ControllerCommand to send</param>
    /// <param name="sendStream">The associated MemStream to send</param>
    /// <param name="startPos">Start sending from this position</param>
    /// <param name="bExpediateSend">Optional - Unused parameter</param>
    member x.ToSendFromPos (command, sendStream:StreamBase<byte>, startPos:int64, ?bExpediateSend) =
        let cmd = new NetworkCommand(command, sendStream, startPos)
        x.CommandSizeQ.Enqueue(int(cmd.CmdLen()))
        Interlocked.Add(unProcessedBytes, int64 (cmd.CmdLen())) |> ignore
        let sendQ = xCSend.Q :?> FixedSizeQ<NetworkCommand>
        x.EQSendCmdSyncSize cmd |> ignore

    /// Tells if we are we allowed to send
    member x.CanSend
        with get() =
            (not xCSend.Closed) && (not x.Shutdown)

    /// Wrap the Memstream to forward to 1 endPoint during the communication. 
    /// <param name="endPoint">The endpoint to forward to</param>
    /// <param name="command">The controller command</param>
    /// <param name="sendStream">The associated MemStream to send</param>
    /// <param name="bExpediateSend">Optional argument - unused for now</param>
    member x.ToForward( endPoint:IPEndPoint, command:ControllerCommand, sendStream:StreamBase<byte>, ?bExpediateSend ) = 
        let bExpediate = defaultArg bExpediateSend false
        use forwardHeader = sendStream.GetNew()
        forwardHeader.WriteVInt32( 1 )
        forwardHeader.WriteIPEndPoint( endPoint )
        forwardHeader.WriteByte( byte command.Verb )
        forwardHeader.WriteByte( byte command.Noun )
        sendStream.InsertBefore( forwardHeader ) |> ignore
        x.ToSend( ControllerCommand( ControllerVerb.Forward, ControllerNoun.Message ), forwardHeader, bExpediate )

    /// Wrap the Memstream to forward to multiple endPoints during the communication. 
    /// <param name="endPoints">The endpoints to forward to</param>
    /// <param name="command">The controller command</param>
    /// <param name="sendStream">The associated MemStream to send</param>
    /// <param name="bExpediateSend">Optional argument - unused for now</param>
    member x.ToForward( endPoints:IPEndPoint[], command:ControllerCommand, sendStream:StreamBase<byte>, ?bExpediateSend ) = 
        let bExpediate = defaultArg bExpediateSend false
        use forwardHeader = sendStream.GetNew()
        forwardHeader.WriteVInt32( endPoints.Length )
        for i = 0 to endPoints.Length - 1 do
            forwardHeader.WriteIPEndPoint( endPoints.[i] )
        forwardHeader.WriteByte( byte command.Verb )
        forwardHeader.WriteByte( byte command.Noun )
        sendStream.InsertBefore( forwardHeader ) |> ignore
        x.ToSend( ControllerCommand( ControllerVerb.Forward, ControllerNoun.Message ), forwardHeader, bExpediate )

    member x.DisposeResource() = 
        x.OnDisconnect.Trigger()
        (xgc :> IDisposable).Dispose()
        (xCRecv :> IDisposable).Dispose()
        (xCSend :> IDisposable).Dispose()
        (xgBuf :> IDisposable).Dispose()
        eInitialized.Dispose()
        eVerified.Dispose()
        x.PendingCommandTimerEvent.Dispose()        

    interface IDisposable with
        member x.Dispose() = 
            x.DisposeResource()
            GC.SuppressFinalize(x);

// can add following to timer routine:
// - garbage collect, monitor, keep alive (all timer based, but all timers currently removed)
/// A set of NetworkCommandQueue that are established via the Connect call
/// Each connect is reference counted, so that the connect will shutdown when the reference reaches 0
and [<AllowNullLiteral>] NetworkConnections() as x = 
    inherit GenericNetwork(true, DeploymentSettings.NumNetworkThreads)

    //let monitorTimer = Timer((fun o -> x.MonitorChannels(x.GetAllChannels())), null, Timeout.Infinite, Timeout.Infinite)
    let channelsCollection = ConcurrentDictionary<int64, NetworkCommandQueue>() 
    do
        CleanUp.Current.Register( 1000, x, x.Close, fun _ -> "NetworkConnections" ) |> ignore 
    static let staticConnects = new NetworkConnections()
    static do staticConnects.Initialize()

    /// The current NetworkConnections - only one instantiation exists
    static member Current with get() = staticConnects

    // Authentication stuff ========================
    member val private MachineID = DetailedConfig.GetMachineID
    member val internal RequireAuth = false with get, set
    // password is shared secret between two nodes and can always be used for authentication
    member val internal Password = "" with get, set
    // key file contains following (guid, key) pairs
    // 1. It contains (my guid, my private key)
    // 2. It contains (guid, public key) for allowed connections
    member val private KeyFile = "" with get, set
    member val private KeyFilePassword = "" with get, set
    // contains keys as (authentication key, encryption key)
    member val internal AllowedConnections = ConcurrentDictionary<Guid, byte[]*byte[]>() with get
    member val internal MyGuid : Guid = Guid() with get, set
    member val internal MyAuthRSA : RSACryptoServiceProvider = null with get, set
    member val internal MyExchangeRSA : RSACryptoServiceProvider = null with get, set

    member val internal IpAddr = "" with get, set

    /// Get the authentication parameters
    /// <returns>
    /// The authentication parameters as (RequireAuth, MyGUID, PrivateKey, KeyFilePassword)
    /// RequireAuth - whether authentication is currently required
    /// MyGUID - my own GUID
    /// PrivateKey - my own private key
    /// KeyFilePassword - password for the key file
    /// </returns>
    member x.GetAuthParam() =
        if (x.RequireAuth) then
            let keys = (Crypt.RSAToPrivateKey(x.MyAuthRSA, x.KeyFilePassword), Crypt.RSAToPrivateKey(x.MyExchangeRSA, x.KeyFilePassword))
            (x.RequireAuth, x.MyGuid, keys, x.KeyFilePassword)
        else
            (x.RequireAuth, x.MyGuid, (null, null), "")

    member private x.CreateDirectory(file : string) =
        let dirpath = Path.GetDirectoryName(file)
        if Utils.IsNotNull dirpath then
            FileTools.DirectoryInfoCreateIfNotExists (dirpath) |> ignore

    /// Initialize authentication using a shared secret password 
    member x.InitializeAuthentication (pwd : string) : unit =
        let keyFile =  Path.Combine( [| DeploymentSettings.LocalFolder; "Keys";  Guid.NewGuid().ToString("D") |] )
        let keyFilePwd = Guid.NewGuid().ToString("D")
        x.InitializeAuthentication(pwd, keyFile, keyFilePwd)

    /// Initialize authentication / security information from keyfile (encrypted with keyfilePwd)
    /// while also allowing for shared secret (password) to be used for authentication.
    /// <param name="pwd">A shared secret password which can be used for authentication</param>
    /// <param name="keyfile">
    /// The base key file, other guids are obtained from other files in same directory
    /// Own key information is stored in a file with the name "keyfile_mykey.txt" which has following format
    /// - 16 byte GUID
    /// - Length of authentication key blob (4 bytes)
    /// - A private key blob which has been encrypted and contains information to initialize RSA
    /// - Length of encryption key blob (4 bytes)
    /// - A private key blob which has been encrypted and contains information to initialize RSA
    /// Other key information is stored in files "keyfile_<GUID>.txt" where <GUID> is guid of connection and has following format
    /// - 16 byte GUID
    /// - Key blob (unencrypted) with length containing authentication public key
    /// - Key blob (unencrypted) with length containing encryption public key
    /// </param>
    /// <param name="keyfilepwd">The password used to decrypt the encrypted private key information</param> 
    member x.InitializeAuthentication(pwd : string, keyfile : string, keyfilePwd : string) =
        x.RequireAuth <- true
        x.Password <- pwd
        x.KeyFile <- keyfile
        x.KeyFilePassword <- keyfilePwd
        let mykeyfile = keyfile + "_mykey.txt"
        // if current key file does not exist, create one
        if not (File.Exists(mykeyfile)) then
            let (privateKey, publicKey) = Crypt.RSAGetNewKeys(x.KeyFilePassword, KeyNumber.Signature)
            x.MyGuid <- Guid.NewGuid()
            x.MyAuthRSA <- Crypt.RSAFromPrivateKey(privateKey, x.KeyFilePassword)
            x.CreateDirectory(mykeyfile)
            use ms = new MemStream()
            ms.WriteBytes(x.MyGuid.ToByteArray())
            ms.WriteBytesWLen(privateKey)
            let (exchangePrivateKey, exchangePublicKey) = Crypt.RSAGetNewKeys(x.KeyFilePassword, KeyNumber.Exchange)
            x.MyExchangeRSA <- Crypt.RSAFromPrivateKey(exchangePrivateKey, x.KeyFilePassword)
            ms.WriteBytesWLen(exchangePrivateKey)
            File.WriteAllBytes(mykeyfile, ms.GetValidBuffer())
            Logger.LogF (LogLevel.MildVerbose, fun _ -> sprintf "InitializeAuthentication: write to key file: %s" mykeyfile)
        else
            use ms = new MemStream(File.ReadAllBytes(mykeyfile))
            x.MyGuid <- Guid(ms.ReadBytes(sizeof<Guid>))
            x.MyAuthRSA <- Crypt.RSAFromPrivateKey(ms.ReadBytesWLen(), x.KeyFilePassword)
            x.MyExchangeRSA <- Crypt.RSAFromPrivateKey(ms.ReadBytesWLen(), x.KeyFilePassword)
            Logger.LogF (LogLevel.MildVerbose, fun _ -> sprintf "InitializeAuthentication: read from key file: %s" mykeyfile)
        // add self to allowed list
        x.AllowedConnections.[x.MyGuid] <- (Crypt.RSAToPublicKey(x.MyAuthRSA), Crypt.RSAToPublicKey(x.MyExchangeRSA))

    /// Obtain key information for security (authentication / encryption) from files and load in memory
    /// <param name="keyfile">
    /// The base key file, other guids are obtained from other files in same directory
    /// Own key information is stored in a file with the name "keyfile_mykey.txt" which has following format
    /// - 16 byte GUID
    /// - Length of authentication key blob (4 bytes)
    /// - A private key blob which has been encrypted and contains information to initialize RSA
    /// - Length of encryption key blob (4 bytes)
    /// - A private key blob which has been encrypted and contains information to initialize RSA
    /// Other key information is stored in files "keyfile_<GUID>.txt" where <GUID> is guid of connection and has following format
    /// - 16 byte GUID
    /// - Key blob (unencrypted) with length containing authentication public key
    /// - Key blob (unencrypted) with length containing encryption public key
    /// </param>
    static member ObtainKeyInfoFromFiles(keyfile : string) =
        use ms = new MemStream()
        let mykeyfile = keyfile + "_mykey.txt"
        if (File.Exists(mykeyfile)) then
            ms.WriteBytes(File.ReadAllBytes(mykeyfile))
        let dirpath = Path.GetDirectoryName(mykeyfile)
        let filename = Path.GetFileName(keyfile)
        let files = Directory.GetFiles(dirpath, filename + "_" + "*" + ".txt", SearchOption.TopDirectoryOnly)
        for f in files do
            let guidStr = f.Substring(keyfile.Length+1, f.Length-keyfile.Length-5)
            if not (guidStr.Equals("mykey", StringComparison.OrdinalIgnoreCase)) then
                try
                    let guid = Guid(guidStr)
                    ms.WriteBytes(guid.ToByteArray())
                    ms.WriteBytes(File.ReadAllBytes(f))
                with e -> ()
        let outBuf = ms.GetValidBuffer()
        outBuf

    /// Initialize authentication / security information from key information obtained in buffer
    /// <param name="keyInfo>
    /// The key information blob as a byte array
    /// The blob consists of:
    /// Own key information with following format
    /// - 16 byte GUID
    /// - Length of authentication key blob (4 bytes)
    /// - A private key blob which has been encrypted and contains information to initialize RSA
    /// - Length of encryption key blob (4 bytes)
    /// - A private key blob which has been encrypted and contains information to initialize RSA
    /// Key information for allowed connections in following format - can be multiple of these
    /// - 16 byte GUID
    /// - Key blob (unencrypted) with length containing authentication public key
    /// - Key blob (unencrypted) with length containing encryption public key
    /// </param>
    /// <param name="keyfilepwd">The password used to decrypt the encrypted private key information</param>
    member x.InitializeAuthentication(keyInfo : byte[], keyfilepwd : string) =
        x.RequireAuth <- true
        x.KeyFilePassword <- keyfilepwd
        // get guid / key info for self
        use ms = new MemStream(keyInfo)
        x.MyGuid <- new Guid(ms.ReadBytes(sizeof<Guid>))
        x.MyAuthRSA <- Crypt.RSAFromPrivateKey(ms.ReadBytesWLen(), x.KeyFilePassword)
        x.MyExchangeRSA <- Crypt.RSAFromPrivateKey(ms.ReadBytesWLen(), x.KeyFilePassword)
        // add self to allowed list
        x.AllowedConnections.[x.MyGuid] <- (Crypt.RSAToPublicKey(x.MyAuthRSA), Crypt.RSAToPublicKey(x.MyExchangeRSA))
        let mutable bContinue = true
        while (bContinue) do
            try
                let guid = Guid(ms.ReadBytes(sizeof<Guid>))
                let keySign = ms.ReadBytesWLen()
                let keyExchange = ms.ReadBytesWLen()
                x.AllowedConnections.[guid] <- (keySign, keyExchange)
            with e ->
                bContinue <- false
            if (ms.Position >= ms.Length) then
                bContinue <- false

    /// Add to allowed connection list by adding to file and concurrent dictionary
    member internal x.AddToAllowedConnection(guid : Guid, publicKey : byte[], exchangePublicKey : byte[]) =
        if not (x.KeyFile.Equals("", StringComparison.Ordinal)) then
            let thiskeyfile = x.KeyFile + "_" + guid.ToString("N") + ".txt"
            x.CreateDirectory(thiskeyfile)
            use ms = new MemStream()
            ms.WriteBytesWLen(publicKey)
            ms.WriteBytesWLen(exchangePublicKey)
            File.WriteAllBytes(thiskeyfile, ms.GetValidBuffer())
        // add to allowed connections
        x.AllowedConnections.[guid] <- (publicKey, exchangePublicKey)

    /// Check for allowed connections
    member internal x.CheckForAllowedConnection(guid : Guid) =
        if (x.AllowedConnections.ContainsKey(guid)) then
            (true, x.AllowedConnections.[guid])
        else
            let thiskeyfile = x.KeyFile + "_" + guid.ToString("N") + ".txt"
            if (File.Exists(thiskeyfile)) then
                use ms = new MemStream(File.ReadAllBytes(thiskeyfile))
                x.AllowedConnections.[guid] <- (ms.ReadBytesWLen(), ms.ReadBytesWLen())
                (true, x.AllowedConnections.[guid])
            else
                (false, (null, null))

    //member val internal CmdProcPoolRecv = ThreadPoolWithWaitHandles<string>("Cmd Process Recv") with get
    member internal x.CmdProcPoolRecv with get() = x.netPool
    //member val internal CmdProcPoolSend = ThreadPoolWithWaitHandles<string>("Cmd Process Send") with get
    member internal x.CmdProcPoolSend with get() = x.netPool

    // Current speed limit on outgoing interface - for all channels
    member val private SendSpeed = Config.CurrentNetworkSpeed with get, set
    // set send speed per channel
    member val private AdjustSpeed = SingleThreadExec()
    member internal x.AdjustSendSpeeds() =
        let adjustSendSpeeds() =
            let channelLists = x.GetAllChannels()
            // Sending speed calculation for alll channels
            let nDesiredRate = channelLists 
                                |> Seq.map ( fun (queue : NetworkCommandQueue) -> 
                                    if Utils.IsNotNull queue && ( queue.CanSend ) && queue.SendQueueLength>0 then queue.RcvdSpeed else 1000000L ) 
                                |> Seq.toArray
            let nSendingRate = Algorithm.WaterFilling nDesiredRate x.SendSpeed
            Seq.iter2 (fun (q: NetworkCommandQueue) s -> q.SetSendSpeed(s) ) channelLists nSendingRate
        x.AdjustSpeed.ExecOnce(adjustSendSpeeds)
    /// Maximum Number of Channels to monitor 
    static member val private MaxChannelsToMonitor = 20 with get, set
    /// Channel monitor levels 
    static member val private ChannelMonitorLevel = LogLevel.MildVerbose with get, set
    /// Interval in seconds to monitor channel connectivity (in Ms)
    static member val private ChannelMonitorInterval = 30000 with get, set
    member private x.StartMonitor() =
        PoolTimer.AddTimer((fun o -> x.MonitorChannels(x.GetAllChannels())), 
            DeploymentSettings.NetworkActivityMonitorIntervalInMs, DeploymentSettings.NetworkActivityMonitorIntervalInMs)
//        monitorTimer.Change(10000, 10000) |> ignore
//        Logger.LogF(NetworkConnections.ChannelMonitorLevel, ( fun _ -> 
//            let timer = ThreadPoolTimer.TimerWait( fun _ -> "Network Channels Monitoring Timer" ) 
//                                                    ( fun _ -> x.MonitorChannels(x.GetAllChannels()) )
//                                                    ( NetworkConnections.ChannelMonitorInterval) ( NetworkConnections.ChannelMonitorInterval) 
//            ()
//                                    )) 

    member val private Initialized = ref 0 with get
    member val internal MaxMemory = 0UL with get, set
    /// Initialize the object
    member x.Initialize( ) = 
        if (Interlocked.CompareExchange(x.Initialized, 1, 0) = 0) then
            // determine max stack memory in bytes
            let mutable maxMemory = DetailedConfig.GetMemorySpace
            if (DeploymentSettings.MaxNetworkStackMemory > 0) then 
                maxMemory <- Math.Min(maxMemory, uint64 DeploymentSettings.MaxNetworkStackMemory)
            if (DeploymentSettings.MaxNetworkStackMemoryPercentage > 0.0) then
                maxMemory <- Math.Min(maxMemory, uint64(DeploymentSettings.MaxNetworkStackMemoryPercentage*float DetailedConfig.GetMemorySpace))
            maxMemory <- maxMemory >>> 1 // half for send, recv
            x.MaxMemory <- maxMemory
            let mutable bufSize = DeploymentSettings.NetworkSocketAsyncEventArgBufferSize
            let mutable maxNumStackBufs = maxMemory / uint64 bufSize
            if (maxNumStackBufs < 50UL) then
                // lower the buffer size if not sufficient maximum buffers
                bufSize <- int(maxMemory / 50UL)
                maxNumStackBufs <- 50UL
            if (bufSize < 64000) then
                failwith "Not sufficient memory"
            let numInitBufs = Math.Min(DeploymentSettings.InitNetworkSocketAsyncEventArgBuffers, int maxNumStackBufs)
            let bufSize = bufSize // make it unmutable so it can be captured by closure
            let maxNumStackBufs = maxNumStackBufs
            Logger.LogF(LogLevel.Info, fun _ -> sprintf "Initialize network stack with initial buffers: %d max buffers: %d buffer size: %d" numInitBufs maxNumStackBufs bufSize)
            x.InitStack(numInitBufs, bufSize, int maxNumStackBufs)
            // for internal queues of SocketAsyncEventArgs in genericnetwork.fs
            // since flow control takes into account NetworkCommand->SocketAsyncEventArgs and reverse conversion
            // no need to control queue size here since unProcessedBytes is representative of bytes:
            // 1. waiting to be converted from NetworkCommand->SocketAsyncEventArgs
            // 2. on network
            // 3. waiting to be converted from SocketAsyncEventArgs->NetworkCommand
            x.fnQRecv <- (fun() -> 
                let q = new FixedSizeQ<RBufPart<byte>>(int64 DeploymentSettings.MaxSendingQueueLimit, int64 DeploymentSettings.MaxSendingQueueLimit)
                q.MaxLen <- DeploymentSettings.NetworkSARecvQSize
                q.DesiredLen <- DeploymentSettings.NetworkSARecvQSize
                q :> BaseQ<RBufPart<byte>>
            )
            x.fnQSend <- Some(fun() -> new FixedLenQ<RBufPart<byte>>(DeploymentSettings.NetworkSASendQSize, DeploymentSettings.NetworkSASendQSize) :> BaseQ<RBufPart<byte>>)
            // initialize shared memory pool for fast memory stream
            MemoryStreamB.InitMemStack(DeploymentSettings.InitBufferListNumBuffers, DeploymentSettings.BufferListBufferSize)
            // start the monitoring
            x.StartMonitor() 

    member val TotalSARecvSize = ref 0L with get
    member val TotalSASendSize = ref 0L with get
    member val TotalCmdRecvSize = ref 0L with get
    member val TotalCmdSendSize = ref 0L with get

    /// Get the current channel collection
    member x.ChannelsCollection with get() = channelsCollection

    /// Add a channel to collection - this is only needed in case a class inherits NetworkCommandQueue 
    /// <param name="newChannel">The channel to add to collection</param>
    /// <returns>The channel in collection</returns>
    member x.AddToCollection(newChannel : NetworkCommandQueue) =
        let socket = newChannel.Socket
        let newSignature = LocalDNS.IPEndPointToInt64( socket.RemoteEndPoint :?> IPEndPoint )
        // We expect most socket added to be unique, so it is ok to use value rather than valueFunc here. 
        let addedChannel = channelsCollection.GetOrAdd( newSignature, newChannel )
        if not (Object.ReferenceEquals( addedChannel, newChannel )) then 
            channelsCollection.Item(newSignature) <- newChannel
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "add to channel accepted socket from %A with name %A, but channel with same remote endpoint already exist!" socket.RemoteEndPoint (LocalDNS.GetShowInfo( socket.RemoteEndPoint ) ) ))
        Logger.LogF( LogLevel.WildVerbose, (fun _ -> let ep = socket.RemoteEndPoint 
                                                     let eip = ep :?> IPEndPoint
                                                     sprintf "add to channel accepted socket from %A with name %A" ep (LocalDNS.GetHostByAddress( eip.Address.GetAddressBytes(),false)  ) ))

    /// Given a socket, create a NetworkCommandQueue and add it to collection
    /// <param name="socket">The socket from which to create NetworkCommandQueue</param>
    /// <returns>The NetworkCommandQueue created from the socket</returns>
    member x.AddConnect( socket : Socket ) = 
        let newChannel = new NetworkCommandQueue( socket, x )
        x.AddToCollection(newChannel)
        newChannel

    /// Search for connection in collection by endpoint
    /// <param name="endPoint">The endpoint to search for</param>
    /// <returns>The channel if found, else null</returns>
    member x.LookforConnect( endPoint: IPEndPoint ) = 
        let signature = LocalDNS.IPEndPointToInt64( endPoint )
        x.LookforConnectBySignature( signature )

    /// Search for connection in collection by signature
    /// <param name="signature">The signature of the endpoint to search for</param>
    /// <returns>The channel if found, else null</returns>
    member x.LookforConnectBySignature( signature ) = 
        let findQueue = ref null
        if channelsCollection.TryGetValue( signature, findQueue ) then 
            !findQueue 
        else
            null 
              
    /// Given a machine name and port, create socket and connect, then create a NetworkCommandQueue and add it to collection
    /// <param name="machineName">The machine to connect to</param>
    /// <param name="port">The port to connect to</param>
    /// <returns>The NetworkCommandQueue created from the socket</returns>
    member x.AddConnect( machineName, port ) = 
        let addr = 
                let bParsed, addrParsed = IPAddress.TryParse(machineName)
                if bParsed then
                    addrParsed
                else
                    LocalDNS.GetAnyIPAddress( machineName, true )        
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Connecting to %s using addr %A" machineName addr)
        if Utils.IsNull addr then 
            null 
        else
            x.AddConnect( addr, port )

    /// Given a IP address and port, create socket and connect, then create a NetworkCommandQueue and add it to collection
    /// <param name="addr">The IP address to connect to</param>
    /// <param name="port">The port to connect to</param>
    /// <returns>The NetworkCommandQueue created from the socket</returns>
    member x.AddConnect( addr:IPAddress, port ) = 
        let newSignature = LocalDNS.IPv4AddrToInt64( addr, port )
        let addedChannel = channelsCollection.GetOrAdd( newSignature, fun _ -> new NetworkCommandQueue(addr, port, x) )
        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "add to channel outgoing socket to %s" (LocalDNS.GetShowInfo( IPEndPoint(addr, port)) ) ))
        addedChannel

    /// Add a loopback connection to specified port, with optional security parameters specified
    /// <param name="port">The port for loopback connection to connect to</param>
    /// <param name="requireAuth">Optional - specifies if authentication required</param>
    /// <param name="guid">Optional - own GUID for loopback</param>
    /// <param name="rsaParam">Optional - byte array for RSA information blob</param>
    /// <param name="pwd">Optional - password for decrypting RSA information blob</param>
    /// <returns>The NetworkCommandQueue for the channel being added</returns>
    member x.AddLoopbackConnect(port : int, ?requireAuth : bool, ?guid : Guid, ?rsaParam : byte[]*byte[], ?pwd : string) =
        let requireAuth = defaultArg requireAuth false
        let guid = defaultArg guid x.MyGuid
        let rsaParam = defaultArg rsaParam (null, null)
        let pwd = defaultArg pwd ""
        let queue = NetworkCommandQueue.LoopbackConnect(port, x, requireAuth, guid, rsaParam, pwd)
        x.AddToCollection(queue)
        queue

    /// Remove a channel from collection
    /// <param name="channel">The channel to remove</param>
    member x.RemoveConnect( channel:NetworkCommandQueue ) =
        let newSignature = channel.RemoteEndPointSignature
        let bRemove, _ = channelsCollection.TryRemove( newSignature )
        if not bRemove then 
            for pair in channelsCollection do 
                if Object.ReferenceEquals( channel, pair.Value ) then 
                    channelsCollection.TryRemove( pair.Key ) |> ignore
                    Logger.LogF( LogLevel.Warning, (fun _ -> sprintf "remove channel to machine %A, but remote signature of the channel cannot be found" channel.EPInfo ))
                    pair.Value.OnDisconnect.Trigger()
        else
            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "remove channel of RemoteEndPoint %s." (LocalDNS.GetShowInfo( channel.RemoteEndPoint ) ) ))
            channel.OnDisconnect.Trigger() 

    // Some optimization to avoid repeated memory allocation
    member val private CurChannelList = Array.zeroCreate<_> 0 with get, set
    /// Get all channels in the collection as a sequence
    member x.GetAllChannels() = 
        // channelsCollection.Values :> seq<_>
        channelsCollection |> Seq.map( fun pair -> pair.Value )
    /// Monitor information
    member private x.MonitorChannels( channelLists ) = 
        Logger.LogF(NetworkConnections.ChannelMonitorLevel, fun _ -> 
            let numChannels = Seq.length channelLists
            let seqHeader = seq {
                let currentProcess = System.Diagnostics.Process.GetCurrentProcess()
                let gcinfo = GCPolicy.GetGCStatistics()
                yield sprintf "Number of active channels ........... %d .... Memory %d MB ... %s" 
                                numChannels (currentProcess.WorkingSet64>>>20) gcinfo
            }
            let printChannelInfo = (fun (ch:NetworkCommandQueue) ->
                sprintf "\
Channel :%A \
Last ToSent Ticks: %A Last Socket Sent Ticks: %A sendcount: %d finishsendcount: %d \
Last Socket Recv Start: %A recvcount: %d finishrecvcount: %d \
sending cmd queue: %d sending queue:%d receiving queue:%d receiving command queue: %d \
sending queue size:%d recv queue size: %d \
UnprocessedCmD:%d bytes Status:%A" 
                    (LocalDNS.GetShowInfo(ch.RemoteEndPoint)) 
                    ch.LastSendTicks ch.Conn.LastSendTicks ch.Conn.SendCounter ch.Conn.FinishSendCounter 
                    ch.Conn.LastRecvTicks ch.Conn.RecvCounter ch.Conn.FinishRecvCounter
                    (ch.SendCommandQueueLength) (ch.SendQueueLength) (ch.ReceivingQueueLength) (ch.ReceivingCommandQueueLength) 
                    (ch.SendQueueSize) (ch.RecvQueueSize) 
                    (ch.UnProcessedCmdInBytes) ch.ConnectionStatus
            )
            let showLists = channelLists |> Seq.truncate NetworkConnections.MaxChannelsToMonitor
            Seq.append seqHeader (Seq.map printChannelInfo showLists) |> String.concat( Environment.NewLine )
        )
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "SA Recv Stack size %d %d" x.BufStackRecv.StackSize x.BufStackRecv.GetStack.Size)
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "SA Send Stack size %d %d" x.BufStackSend.StackSize x.BufStackSend.GetStack.Size)
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Memory Stream Stack size %d %d" MemoryStreamB.MemStack.StackSize MemoryStreamB.MemStack.GetStack.Size)
        MemoryStreamB.DumpStreamsInUse()
        MemoryStreamB.MemStack.DumpInUse(LogLevel.MildVerbose)
        x.BufStackRecv.DumpInUse(LogLevel.MildVerbose)
        x.BufStackSend.DumpInUse(LogLevel.MildVerbose)

    member val private nCloseCalled = ref 0 with get, set
    member val private EvCloseExecuted = new ManualResetEvent(false) with get

//    /// Additional action at shutdown
//    member val ActionAtClose = ExecuteUponOnce() with get

    /// Close All Active Connection, to be called when the program gets shutdown.
    override x.Close() = 
        if Interlocked.CompareExchange( x.nCloseCalled, 1, 0)=0  then 
            let allchannels = x.GetAllChannels()
            for ch in allchannels do 
                ch.Close()
            // shutdown threadpools
            x.CmdProcPoolSend.CloseAllThreadPool()
            x.CmdProcPoolRecv.CloseAllThreadPool()
            base.Close()
//            ThreadPoolWait.TerminateAll()
//            x.ActionAtClose.Trigger()
//            ThreadTracking.CloseAllActiveThreads()
            Logger.Log( LogLevel.MildVerbose, ("All sockets closed and IO Thread joined "))
            x.EvCloseExecuted.Set() |> ignore 
        x.EvCloseExecuted.WaitOne() |> ignore 
    override x.Finalize() =
        /// Close All Active Connection, to be called when the program gets shutdown.
        CleanUp.Current.CleanUpAll()
    interface IDisposable with
        /// Close All Active Connection, to be called when the program gets shutdown.
        member x.Dispose() = 
            x.EvCloseExecuted.Dispose()
            base.DisposeResource()
            CleanUp.Current.CleanUpAll()
            GC.SuppressFinalize(x)
