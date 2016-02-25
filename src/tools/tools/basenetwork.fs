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

	Author: Sanjeev Mehrotra
 ---------------------------------------------------------------------------*)
 
namespace Prajna.Tools.Network

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.Net
open System.Net.Sockets
open System.Threading
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp
open Prajna.Tools.Serialize
open Prajna.Tools.Queue
open Prajna.Tools.Process

// ==============================================================================================
// interfaces

/// A generic interface for a connection
[<AllowNullLiteral>]
type IConn =
    interface
        /// A socket for the connection
        abstract Socket : Socket with get, set
        /// Initialize the connection using (socket, state)
        abstract Init : Socket*obj->unit
        /// Close the connection
        abstract Close : unit->unit
    end

// ======================================================================

// module here is essentially class with all static members
// use class with static members, otherwise cannot have multiple methods with same name
// (i.e. let does not support polymorphism)
//module NetUtils =
/// Abstract Class with static members for network utility functions
type [<AbstractClass>] NetUtils() =
    static let randGen = System.Random()
    static member internal RandGen with get() = randGen

    /// Get a TCP Listener which listens on a port
    /// <param name="startPort">
    /// Optional argument - default random port between 30000 and 65535
    /// the first port which the listener tries
    /// </param>
    /// <returns>
    /// A tuple of (listener, randomPort)
    /// listener - the TCPListener
    /// randomPort - the port on which it is listening
    /// </returns>
    static member GetTCPListener(startPort : Option<int>) =
        let mutable randomPort =
            match startPort with
                | None -> NetUtils.RandGen.Next(30000, 65535)
                | Some(x) -> x
        let mutable success = false
        let mutable listen = null
        let ipAddr = IPAddress.Any
        while (not success) do
            try
                listen <- Net.Sockets.TcpListener(ipAddr, randomPort)
                listen.Start()
                success <- true
            with
                ex -> randomPort <- NetUtils.RandGen.Next(30000, 65535)
        (listen, randomPort)

    // make following internal as LocalDNS.GetShowInfo provides similar functionality
    static member internal GetRemoteIPAddrPort(sock : Socket) =
        let ep = sock.RemoteEndPoint :?> System.Net.IPEndPoint
        (ep.Address.ToString().ToLower(), ep.Port)

    static member internal GetIPKey(ipAddr : string, ipPort : int) =
        (ipAddr + ":" + ipPort.ToString()).ToLower()

    // define key of socket to be address:port
    static member internal GetIPKey(sock : Socket) =
        let (addr, port) = NetUtils.GetRemoteIPAddrPort(sock)
        NetUtils.GetIPKey(addr, port)

    static member internal ParseAddrPort (addrport : string, port : int) =
        let position = addrport.LastIndexOf(":")
        if (position = -1) then
            (addrport, port)
        else
            let (portString : string) = addrport.Substring (position+1)
            (addrport.Substring(0, position), Convert.ToInt32(portString))

    /// Parse a string into (address, port)
    /// <param name="addrport">The string represting address and port</param>
    /// <returns>A tuple of (string, int) representing the address and port</returns>
    static member ParseAddrPort (addrport : string) =
        let position = addrport.LastIndexOf(":")
        if (position = -1) then
            ("", Convert.ToInt32(addrport))
        else
            (addrport.Substring(0, position), Convert.ToInt32(addrport.Substring(position+1)))

    static member internal RecvAsync(sock : Socket, e : SocketAsyncEventArgs) =
        try
            let asyncOper = sock.ReceiveAsync(e)
            if (not asyncOper && (0 = e.BytesTransferred)) then
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Recv %d bytes from %s， asyncOper %A, socketError: %A, close" e.BytesTransferred (sock.RemoteEndPoint.ToString()) asyncOper e.SocketError))
                (true, asyncOper)
                //(false, asyncOper)
            else
                Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Recv from %s， asyncOper %A, socketError: %A" (sock.RemoteEndPoint.ToString()) asyncOper e.SocketError))
                (false, asyncOper)
        with ex ->
            //Logger.LogF(LogLevel.Error, fun _ -> sprintf "Socket %A encounters exception %A error %A" sock.RemoteEndPoint ex e.SocketError)
            match ex with
                | :? ObjectDisposedException as ex -> (true, true)
                | :? SocketException as ex when (e.SocketError = SocketError.ConnectionReset || true) -> (true, true)
                | _ -> reraise()
                       (true, true)

    static member internal SendAsync(sock : Socket, e : SocketAsyncEventArgs) =
        try
            let asyncOper = sock.SendAsync(e)
            if (not asyncOper && (0 = e.BytesTransferred)) then
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Send %d bytes to %s， asyncOper %A, socketError: %A, close" e.BytesTransferred (sock.RemoteEndPoint.ToString()) asyncOper e.SocketError))
                //(true, asyncOper)
                (false, asyncOper)
            else
                Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Send to %s， asyncOper %A, socketError: %A" (sock.RemoteEndPoint.ToString()) asyncOper e.SocketError))
                (false, asyncOper)
        with ex ->
            //Logger.LogF(LogLevel.Error, fun _ -> sprintf "Socket %A encounters exception %A error %A" sock.RemoteEndPoint ex e.SocketError)
            match ex with
                | :? ObjectDisposedException as ex -> (true, true)
                | :? SocketException as ex when (e.SocketError = SocketError.ConnectionReset || true) -> (true, true)
                | _ -> reraise()
                       (true, true)

    // From https://msdn.microsoft.com/en-us/library/ms145160(v=vs.110).aspx
    // If you are using a connection-oriented protocol, Send will block until all of the bytes in the buffer are sent, unless a time-out was set by using Socket.SendTimeout
    static member internal SendSync(sock : Socket, buf : byte[], offset : int, count : int) =
        try
            let ret = sock.Send(buf, offset, count, SocketFlags.None)
            if (ret <> count) then
                // for blocking socket with inifinte send timeout, all data should be sent (differs from win32 behavior)
                (true, ret)
            else
                (false, count)
        with
            | :? ObjectDisposedException as ex -> (true, 0)
            | :? SocketException as ex -> (true, 0)

    static member internal RecvSync(sock : Socket, buf : byte[], offset : int, count : int) =
        try
            let ret = sock.Receive(buf, offset, count, SocketFlags.None)
            if (ret = 0) then
                (true, ret)
            else
                (false, ret)
        with
            | :? ObjectDisposedException as ex -> (true, 0)
            | :? SocketException as ex -> (true, 0)

    static member internal RecvOrClose(conn : IConn, fn, e) =
        let (closed, asyncOper) = NetUtils.RecvAsync(conn.Socket, e)
        if (closed) then
            //Logger.LogF(LogLevel.MildVerbose, (fun _ -> sprintf "baseNetwork; close is called %A, BytesTransferred %d, SocketError %A, receive buffer size:%d" (LocalDNS.GetShowInfo(conn.Socket.RemoteEndPoint)) e.BytesTransferred e.SocketError conn.Socket.ReceiveBufferSize ))
            conn.Close()
        else if (not asyncOper) then
            fn e

    static member internal SendOrClose(conn : IConn, fn, e : SocketAsyncEventArgs) =
        let (closed, asyncOper) = NetUtils.SendAsync(conn.Socket, e)
        if (closed) then
            conn.Close()
            true
        else if (not asyncOper) then
            if (e.BytesTransferred > 0) then
                fn e
                true
            else
                false // no data transferred
        else
            true

    static member internal SyncSendOrClose(conn : IConn, buf : byte[], offset : int, count : int) =
        let (closed, sent) = NetUtils.SendSync(conn.Socket, buf, offset, count)
        if (closed) then
            conn.Close()

    static member internal SyncSendOrCloseWithSize(conn : IConn, buf : byte[], offset : int, count : int) =
        let (closed, sent) = NetUtils.SendSync(conn.Socket, BitConverter.GetBytes(count), 0, sizeof<int>)
        if (not closed) then
            NetUtils.SyncSendOrClose(conn, buf, offset, count)
        else
            conn.Close()

    static member internal SyncRecvOrClose(conn : IConn, buf : byte[], offset : int, count : int) =
        let (closed, sent) = NetUtils.RecvSync(conn.Socket, buf, offset, count)
        if (closed) then
            conn.Close()

// ====================================================================================

/// A class capable of reading/writing value types asynchronously using an underlying connection
type GenericVal<'V>(conn : IConn) as x =
    let xConn = conn
    let eSendVal = new SocketAsyncEventArgs()
    do eSendVal.Completed.Add(x.AsyncSendValueCb)
    do eSendVal.SetBuffer(Array.zeroCreate<byte>(sizeof<'V>), 0, sizeof<'V>)
    let eRecvVal = new SocketAsyncEventArgs()
    do eRecvVal.Completed.Add(x.AsyncRecvValueCb)
    do eRecvVal.SetBuffer(Array.zeroCreate<byte>(sizeof<'V>), 0, sizeof<'V>)

    member internal x.AsyncRecvValueCb (e : SocketAsyncEventArgs) =
        if (e.Offset + e.BytesTransferred <> sizeof<'V>) then
            let newOffset = e.Offset + e.BytesTransferred
            e.SetBuffer(newOffset, (sizeof<'V>)-newOffset)
            NetUtils.RecvOrClose(xConn, x.AsyncRecvValueCb, e)
        else
            let (callback, state) = e.UserToken :?> (obj*'V->unit)*obj
            let value = Prajna.Tools.Serialize.ConvertTo<'V>(e.Buffer)
            callback(state, value)

    /// Asynchronously start receiving a value
    /// <param name="callback">
    /// A callback which executes after receive completes.
    /// Passes in a state and the value retrieved (i.e. callback(state, value))
    /// </param>
    /// <param name="state">
    /// The state to be passed into the callback
    /// </param>
    member x.AsyncRecvValue(callback : obj*'V->unit, state : obj) =
        eRecvVal.UserToken <- (callback, state)
        eRecvVal.SetBuffer(0, sizeof<'V>) // reset
        NetUtils.RecvOrClose(xConn, x.AsyncRecvValueCb, eRecvVal)

    member internal x.AsyncSendValueCb (e : SocketAsyncEventArgs) =
        if (e.Offset + e.BytesTransferred <> sizeof<'V>) then
            let newOffset = e.Offset + e.BytesTransferred
            e.SetBuffer(newOffset, (sizeof<'V>)-newOffset)
            NetUtils.SendOrClose(xConn, x.AsyncSendValueCb, e) |> ignore
        else
            let (callback, state) = e.UserToken :?> (obj->unit)*obj
            callback(state)

    /// Asynchronously start sending a value
    /// <param name="callback">
    /// A callback which executes after sending completes.
    /// Passes in state (i.e. callback(state))
    /// </param>
    /// <param name="state">The state to be passed in the callback</param>
    /// <param name="value">The value to send</param> 
    member x.AsyncSendValue(callback : obj->unit, state : obj, value : 'V) =
        eSendVal.UserToken <- (callback, state)
        let valBuf = Prajna.Tools.Serialize.ConvertFrom<'V>(value)
        Buffer.BlockCopy(valBuf, 0, eSendVal.Buffer, 0, sizeof<'V>)
        eSendVal.SetBuffer(0, sizeof<'V>) // reset
        NetUtils.SendOrClose(xConn, x.AsyncSendValueCb, eSendVal) |> ignore

    interface IDisposable with
        /// Releases all resources used by the current instance.
        member x.Dispose() = 
            eSendVal.Dispose()
            eRecvVal.Dispose()
            GC.SuppressFinalize(x)
// ====================================================================================

/// A class capable of reading/writing a buffer asynchronously using an underlying connection
type GenericBuf(conn : IConn, maxBufferSize : int) as x =
    let xgInt = new GenericVal<int>(conn)
    let xConn = conn
    let eSendBuf = new SocketAsyncEventArgs()
    do eSendBuf.Completed.Add(x.AsyncSendBufCb)
    do eSendBuf.SetBuffer(Array.zeroCreate<byte>(maxBufferSize+sizeof<int>), 0, maxBufferSize)
    let eRecvBuf = new SocketAsyncEventArgs()
    do eRecvBuf.Completed.Add(x.AsyncRecvBufCb)
    do eRecvBuf.SetBuffer(Array.zeroCreate<byte>(maxBufferSize), 0, maxBufferSize)
    // use AutoResetEvent so only one thread unblocks at one time when wait completes
    let eSendComplete = new AutoResetEvent(true)
    let mutable recvBufferSize = maxBufferSize
    let mutable sendBufferSize = 0

    member private x.AsyncRecvBufCb(e : SocketAsyncEventArgs) =
        if (e.Offset + e.BytesTransferred <> recvBufferSize) then
            let newOffset = e.Offset + e.BytesTransferred
            e.SetBuffer(newOffset, recvBufferSize-newOffset)
            NetUtils.RecvOrClose(xConn, x.AsyncRecvBufCb, e)
        else
            let (callback, state, bufferOffset) = e.UserToken :?> (obj*byte[]*int*int->unit)*obj*int
            callback(state, e.Buffer, bufferOffset, recvBufferSize)

    /// Asynchronously receive a buffer and execute a callback when receive finishes
    /// <param name="callback">
    /// A callback which executes once receive completes
    /// Callback takes in (state, buffer, offset size)=(obj, byte[], int, int) tuple
    ///   state - arbitrary state specified by "state" parameter
    ///   buffer - the buffer which contains the data retrieved
    ///            the callback must copy the data or appropriately control bufferOffset 
    ///            before starting another AsyncRecvBuf if needed as buffer is reused
    ///   offset - the offfset in buffer where data has been written
    ///   size - the amount of data which has been written to buffer
    /// </param>
    /// <param name="state">
    /// The state to pass to the callback
    /// </param>
    /// <param name="bufferOffset">
    /// The bufferOffset where data is to be written within internal buffer
    /// </param>
    /// <param name="bufferSize">
    /// The length of data to be read
    /// </param>
    member x.AsyncRecvBuf(callback : obj*byte[]*int*int->unit, state : obj, bufferOffset : int, bufferSize : int) =
        if (bufferOffset + bufferSize <= maxBufferSize) then
            eRecvBuf.UserToken <- (callback, state, bufferOffset)
            recvBufferSize <- bufferSize
            eRecvBuf.SetBuffer(bufferOffset, bufferSize)
            NetUtils.RecvOrClose(xConn, x.AsyncRecvBufCb, eRecvBuf)
        else
            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Receive size %d larger than maximum allowed %d - connection closes" (bufferOffset+bufferSize) maxBufferSize))
            xConn.Close()

    member private x.AsyncRecvBufWithSizeCB(o : obj, bufferSize : int) =
        let (callback, state, bufferOffset) = o :?> (obj*byte[]*int*int->unit)*obj*int
        x.AsyncRecvBuf(callback, state, bufferOffset, bufferSize)

    /// Asynchronously receive a buffer along with its size and execute callback when receive finishes
    /// The length of the buffer is first read, then the buffer is read.
    /// Receive fails and underlying connection is closed if buffer size is larger than maximum allowed.
    /// <param name="callback">
    /// A callback which executes once receive completes
    /// Callback takes in (state, buffer, offset size)=(obj, byte[], int, int) tuple
    ///   state - arbitrary state specified by "state" parameter
    ///   buffer - the buffer which contains the data retrieved
    ///            the callback must copy the data or appropriately control bufferOffset 
    ///            before starting another AsyncRecvBuf if needed as buffer is reused
    ///   offset - the offfset in buffer where data has been written
    ///   size - the amount of data which has been written to buffer
    /// </param>
    /// <param name="state">
    /// The state to pass to the callback
    /// </param>
    /// <param name="bufferOffset">
    /// The bufferOffset where data is to be written within internal buffer
    /// </param>
    member x.AsyncRecvBufWithSize(callback : obj*byte[]*int*int->unit, state : obj, bufferOffset : int) =
        xgInt.AsyncRecvValue(x.AsyncRecvBufWithSizeCB, (callback, state, bufferOffset))

    member private x.AsyncSendBufCb(e : SocketAsyncEventArgs) =
        if (e.Offset + e.BytesTransferred <> sendBufferSize) then
            let newOffset = e.Offset + e.BytesTransferred
            e.SetBuffer(newOffset, sendBufferSize-newOffset)
            NetUtils.SendOrClose(xConn, x.AsyncSendBufCb, e) |> ignore
        else
            let (callbackO, state) = e.UserToken :?> (Option<obj->unit>)*obj
            match callbackO with
                | Some(callback) -> callback(state)
                | None -> ()
            eSendComplete.Set() |> ignore

    /// Asynchronously send buffer and execute callback upon completion
    /// <param name="callbackO">
    /// Optional parameter to specify callback to execute
    /// If specified callback takes in state, i.e. callback(state)
    /// </param>
    /// <param name="state">The state to pass into the callback</param>
    /// <param name="buf">The buffer to send</param>
    /// <param name="bufferOffset">The buffer offset in the buffer to send</param>
    /// <param name="bufferSize">The amount of data to send starting at bufferOffset</param>
    member x.AsyncSendBuf(callbackO : Option<obj->unit>, state : obj, buf : byte[], bufferOffset : int, bufferSize : int) =
        // wait for send complete prior to reusing eSendBuf
        eSendComplete.WaitOne() |> ignore
        if (bufferSize <= eSendBuf.Buffer.Length) then
            eSendBuf.UserToken <- (callbackO, state)
            sendBufferSize <- bufferSize
            eSendBuf.SetBuffer(0, bufferSize)
            Buffer.BlockCopy(buf, bufferOffset, eSendBuf.Buffer, 0, bufferSize)
            NetUtils.SendOrClose(xConn, x.AsyncSendBufCb, eSendBuf) |> ignore
        else
            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Send size %d larger than maximum allowed %d - connection closes" bufferSize eSendBuf.Buffer.Length))
            xConn.Close()            

    /// The same as AsyncSendBuf, except callback is mandatory
    /// <param name="callback">
    /// Callback takes in state, i.e. callback(state)
    /// </param>
    /// <param name="state">The state to pass into the callback</param>
    /// <param name="buf">The buffer to send</param>
    /// <param name="bufferOffset">The buffer offset in the buffer to send</param>
    /// <param name="bufferSize">The amount of data to send starting at bufferOffset</param>
    member internal x.AsyncSendBuf(callback : obj->unit, state : obj, buf : byte[], bufferOffset : int, bufferSize : int) =
        x.AsyncSendBuf(Some(callback), state, buf, bufferOffset, bufferSize)

    /// The same as AsyncSendBuf, except no callback
    /// <param name="state">The state to pass into the callback</param>
    /// <param name="buf">The buffer to send</param>
    /// <param name="bufferOffset">The buffer offset in the buffer to send</param>
    /// <param name="bufferSize">The amount of data to send starting at bufferOffset</param>
    member internal x.AsyncSendBuf(buf : byte[], bufferOffset : int, bufferSize : int) =
        x.AsyncSendBuf(None, null, buf, bufferOffset, bufferSize)

    /// Asynchronously send buffer along with size of buffer and execute callback upon completion
    /// The buffer size is sent first, followed by the buffer (so receiver does not need to know size a priori)
    /// <param name="callbackO">
    /// Optional parameter to specify callback to execute
    /// If specified callback takes in state, i.e. callback(state)
    /// </param>
    /// <param name="state">The state to pass into the callback</param>
    /// <param name="buf">The buffer to send</param>
    /// <param name="bufferOffset">The buffer offset in the buffer to send</param>
    /// <param name="bufferSize">The amount of data to send starting at bufferOffset</param>
    member x.AsyncSendBufWithSize(callbackO : Option<obj->unit>, state : obj, buf : byte[], bufferOffset : int, bufferSize : int) =
        eSendComplete.WaitOne() |> ignore
        if (bufferSize + sizeof<int> <= eSendBuf.Buffer.Length) then
            eSendBuf.UserToken <- (callbackO, state)
            sendBufferSize <- bufferSize + sizeof<int>
            eSendBuf.SetBuffer(0, bufferSize + sizeof<int>)
            let sizeBuf = BitConverter.GetBytes(bufferSize)
            Buffer.BlockCopy(sizeBuf, 0, eSendBuf.Buffer, 0, sizeBuf.Length)
            Buffer.BlockCopy(buf, bufferOffset, eSendBuf.Buffer, sizeBuf.Length, bufferSize)
            NetUtils.SendOrClose(xConn, x.AsyncSendBufCb, eSendBuf) |> ignore
        else
            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Send size %d larger than maximum allowed %d - connection closes" (bufferSize+sizeof<int>) eSendBuf.Buffer.Length))
            xConn.Close()

    /// The same as AsyncSendBufWithSize, but no callback
    member internal x.AsyncSendBufWithSize(buf : byte[], bufferOffset : int, bufferSize : int) =
        x.AsyncSendBufWithSize(None, null, buf, bufferOffset, bufferSize)

    interface IDisposable with
        /// Releases all resources used by the current instance.
        member x.Dispose() = 
            (xgInt :> IDisposable).Dispose()
            eSendBuf.Dispose()
            eRecvBuf.Dispose()
            eSendComplete.Dispose()
            GC.SuppressFinalize(x);
// =================================================================================

/// maintains multiple network connections
[<AllowNullLiteral>]
[<AbstractClass>]
type Network() =
    let mutable listen : TcpListener = null
    let networkQ = ConcurrentDictionary<string, IConn>()
    let numConn = ref 0
    let localAddr = Dns.GetHostAddresses("")
    let localAddrV4 = localAddr |> Array.filter (fun a -> a.AddressFamily = AddressFamily.InterNetwork)

    member private x.GetConnNum() =
        Interlocked.Increment(numConn)

    member private x.GetSocketID(sock : Socket) =
        let ipRemote = sock.RemoteEndPoint
        let addr = (ipRemote :?> IPEndPoint)
        (addr, addr.Address.ToString() + ":" + addr.Port.ToString() + ":" + "Listen_" + x.GetConnNum().ToString())

    member private x.InitConn(conn : IConn, sock : Socket, state : obj) =
        let (addr, id) = x.GetSocketID(sock)
        networkQ.[id] <- conn
        conn.Init(sock, state)

    /// Close all network connections held by Network
    member x.CloseConns() =
        for conn in (networkQ) do
            let iconn = conn.Value
            iconn.Close()
            iconn.Socket.Dispose()

    member private x.AfterAccept(ar : IAsyncResult) =
        let (listen, newFn, state) = ar.AsyncState :?> (TcpListener*(unit->IConn)*obj)
        // default is blocking with sendtimeout of 0 (infinite)
        let socket =
            try
                listen.EndAcceptSocket(ar)
            with e ->
                null
        try
            // start another
            listen.BeginAcceptSocket(AsyncCallback(x.AfterAccept), ar.AsyncState) |> ignore
        with e->
            ()
        if (Utils.IsNotNull socket) then
            // add connection to dictionary and start receiving on socket
            let conn = newFn()
            x.InitConn(conn, socket, state)

    /// Generic function to listen on given port for incoming connections
    /// Create a new connection of type 'T and add it to list when connection is accepted
    /// <param name="port">The port to listen on</param>
    /// <param name="state">
    /// The state passed to connection initialization.  When a connection of IConn is created
    /// conn.Init(socket, state) is called
    /// </param>
    member x.Listen<'T when 'T :> IConn and  'T : (new: unit->'T)>(port : int, state : obj) =
        listen <- new TcpListener(IPAddress.Any, port)
        listen.Start()
        try
            let newFn = (fun () -> new 'T() :> IConn)
            listen.BeginAcceptSocket(AsyncCallback(x.AfterAccept), (listen, newFn, state)) |> ignore
        with e->
            ()

    /// Generic function to listen on given port for incoming connections
    /// Create a new connection of type 'T and add it to list when connection is accepted
    /// <param name="port">The port to listen on</param>
    /// <param name="bind">The local address to bind to</param>
    /// <param name="state">
    /// The state passed to connection initialization.  When a connection of IConn is created
    /// conn.Init(socket, state) is called
    /// </param>
    member x.Listen<'T when 'T :> IConn and  'T : (new: unit->'T)>(port : int, bind : string, state : obj) =
        if (bind.Equals("")) then
            x.Listen<'T>(port, state)
        else
            let addr = Dns.GetHostAddresses(bind)
            listen <- new TcpListener(addr.[0], port)
            listen.Start()
            try
                let newFn = (fun () -> new 'T() :> IConn)
                listen.BeginAcceptSocket(AsyncCallback(x.AfterAccept), (listen, newFn, state)) |> ignore
            with e->
                ()
    /// Stop listening for incoming connections
    member x.StopListen() =
        try
            listen.Stop()
        with e->
            ()

    /// Generic function to connect to given IPAddress and port
    /// Each conn can be different class ('T) so long as interface IConn is implemented
    /// <param name="addr">The IPAddress to connect to.</param>
    /// <param name="port">The port to connect to.</param>
    /// <param name="state">
    /// The state passed into connection initialization.  When connection is created, conn.Init(socket, state) is called.
    /// </param>
    member x.Connect<'T when 'T :> IConn and  'T : (new: unit->'T)>(addr : IPAddress, port : int, state : obj) =
        let sock = new System.Net.Sockets.Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        let conn = new 'T() :> IConn
        let ep = IPEndPoint(addr, port)
        try
            sock.Connect(ep)
            x.InitConn(conn, sock, state)
            conn
        with e ->
            null

    /// Generic function to connect to given address string
    /// Each conn can be different class so long as interface IConn is implemented
    /// A random host IPAddress is chosen for both local and remote endpoints from the list of possible interfaces.
    /// <param name="addrStr">An address string of the remote end</param>
    /// <param name="port">The port of the remote end</param>
    /// <param name="state">
    /// The state passed into connection initialization.  When connection is created, conn.Init(socket, state) is called.
    /// </param>
    member x.Connect<'T when 'T :> IConn and  'T : (new: unit->'T)>(addrStr : string, port : int, state : obj) =
        let mutable success = false
        let mutable sock =null 
        try
            sock <- new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
            let conn = new 'T() :> IConn
            try
                // use IP v4 address, randomly pick one for local, use 0 for port since don't care
                let index = NetUtils.RandGen.Next(0, localAddrV4.Length)
                sock.Bind(IPEndPoint(localAddrV4.[index], 0))
                Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Bind to local %d out of %d - %A" index localAddrV4.Length localAddrV4.[index]))
                // use IP v4 address, randomly pick one for remote
                let addrs = Dns.GetHostAddresses(addrStr)
                let addrsv4 = addrs |> Array.filter (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
                let index = NetUtils.RandGen.Next(0, addrsv4.Length)
                Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Connect to remote %d - %A" index addrsv4.[index]))
                sock.Connect(addrsv4.[index], port)
                x.InitConn(conn, sock, state)
                success <- true
                conn
            with e ->
                try
                    sock <- new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                    sock.Connect(addrStr, port)
                    x.InitConn(conn, sock, state)
                    success <- true
                    conn
                with e->
                    null 
        finally
            // if success is true, "sock" will be recorded in networkQ (as a member of IConn), and will be diposed there
            if not success && Utils.IsNotNull sock then
                sock.Dispose()

    /// Generic function to connect to given address string
    /// Each conn can be different class so long as interface IConn is implemented
    /// A random host IPAddress is chosen for both local and remote endpoints from the list of possible interfaces.
    /// <param name="addrStr">An address string of the remote end</param>
    /// <param name="port">The port of the remote end</param>
    /// <param name="state">
    /// The state passed into connection initialization.  When connection is created, conn.Init(socket, state) is called.
    /// </param>
    member x.Connect<'T when 'T :> IConn and  'T : (new: unit->'T)>(addrStr : string, port : int, bind : string, state : obj) =
        if (bind.Equals("")) then
            x.Connect<'T>(addrStr, port, state)
        else
            let mutable success = false
            let mutable sock = null
            try
                sock <- new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                let conn = new 'T() :> IConn
                let addr = (Dns.GetHostAddresses(bind)).[0]
                try
                    sock.Bind(IPEndPoint(addr, 0))
                    Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Bind to local %A" addr))
                    // use IP v4 address, randomly pick one for remote
                    let addrs = Dns.GetHostAddresses(addrStr)
                    let addrsv4 = addrs |> Array.filter (fun a -> a.AddressFamily = AddressFamily.InterNetwork)
                    let index = NetUtils.RandGen.Next(0, addrsv4.Length)
                    Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Connect to remote %d - %A" index addrsv4.[index]))
                    sock.Connect(addrsv4.[index], port)
                    x.InitConn(conn, sock, state)
                    success <- true
                    conn
                with e->
                    try
                        sock <- new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
                        sock.Bind(IPEndPoint(addr, 0))
                        sock.Connect(addrStr, port)
                        x.InitConn(conn, sock, state)
                        success <- true
                        conn
                    with e->
                        null 
            finally
                // if success is true, "sock" will be recorded in networkQ (as a member of IConn), and will be diposed there
                if not success && Utils.IsNotNull sock then
                    sock.Dispose()

    interface IDisposable with
        /// Releases all resources used by the current instance.
        member x.Dispose() = 
            x.CloseConns()
            GC.SuppressFinalize(x);
                                    
    static member internal SrcDstBlkCopy(src : byte[], srcOffset : int byref, srcLen : int byref,
                                         dst : byte[], dstOffset : int byref, dstLen : int byref) =
        let toCopy = Math.Min(srcLen, dstLen)
        if (toCopy > 0) then
            if (srcOffset < 0) then
                failwith "offset cannot be less than zero"
            Buffer.BlockCopy(src, srcOffset, dst, dstOffset, toCopy)
            srcOffset <- srcOffset + toCopy
            srcLen <- srcLen - toCopy
            dstOffset <- dstOffset + toCopy
            dstLen <- dstLen - toCopy

    // dst is memstream
    static member internal SrcDstBlkCopy(src : byte[], srcOffset : int byref, srcLen : int byref,
                                         dst : StreamBase<byte>, dstLen : int byref) =
        let toCopy = Math.Min(srcLen, dstLen)
        if (toCopy > 0) then
            dst.Write(src, srcOffset, toCopy)
            srcOffset <- srcOffset + toCopy
            srcLen <- srcLen - toCopy
            dstLen <- dstLen - toCopy
        toCopy

    // dst is memstream
    static member internal SrcDstBlkNoCopy(src : RBufPart<byte>, srcOffset : int byref, srcLen : int byref,
                                           dst : StreamBase<byte>, dstLen : int byref) =
        let toCopy = Math.Min(srcLen, dstLen)
        if (toCopy > 0) then
            //dst.Write(src, srcOffset, toCopy)
            dst.AppendNoCopy(src, int64 src.Offset + int64 srcOffset, int64 toCopy)
            srcOffset <- srcOffset + toCopy
            srcLen <- srcLen - toCopy
            dstLen <- dstLen - toCopy
        toCopy

    // src is memstream
    static member internal SrcDstBlkCopy(src : StreamBase<byte>, srcLen : int byref,
                                         dst : byte[], dstOffset : int byref, dstLen : int byref) =
        let toCopy = Math.Min(src.Length-src.Position, int64 srcLen)
        let toCopy = int32(Math.Min(Math.Min(toCopy, int64 dstLen), int64 Int32.MaxValue))
        if (toCopy > 0) then
            let amtRead = src.Read(dst, dstOffset, toCopy)
            if (amtRead >= 0) then
                srcLen <- srcLen - amtRead
                dstOffset <- dstOffset + amtRead
                dstLen <- dstLen - amtRead
                amtRead
            else
                0
        else
            toCopy

// =============================================================================
// Not really network stuff, can be used by any processing pipeline that
// wants to implement processing on thread pool and have flow control (using the resizequeue structures)

type [<AllowNullLiteral>] internal ComponentBase() =

    static let componentCount = ref -1
    let componentId = Interlocked.Increment(componentCount)
    let mutable sharedStateObj : obj = null

    static member GetNextId() : int = Interlocked.Increment(componentCount)
    member x.ComponentId with get() = componentId
    member val Notify : SharedComponentState->unit = (fun s -> ()) with get, set
    member x.SharedStateObj with get() = sharedStateObj and set(v) = sharedStateObj <- v

and [<AllowNullLiteral>] internal SharedComponentState() =
    let items = ConcurrentDictionary<int, ComponentBase>()
    let notify = SingleThreadExec()

    member x.Notify() =
        for item in items do
            item.Value.Notify(x)

    member x.Add(id : int, c : ComponentBase) =
        items.[id] <- c
        notify.ExecOnce(x.Notify)

    member x.Remove(id : int) =
        let (ret, c) = items.TryRemove(id)
        if (ret) then
            notify.ExecOnce(x.Notify)

    member x.Items with get() = items

    // allow threadpool to close
    member val AllowClose = false with get, set

    // static add
    static member val SharedState = Dictionary<obj, SharedComponentState>() with get
    static member Add(id : int, c : ComponentBase, o : obj) =
        let sc : SharedComponentState ref = ref null
        lock (SharedComponentState.SharedState) (fun _ ->
            for s in SharedComponentState.SharedState do
                let sc1 = s.Value
                if (Object.ReferenceEquals(s.Key, o)) then
                    sc := sc1
            if Utils.IsNull !sc then
                sc := new SharedComponentState()
                SharedComponentState.SharedState.[o] <- !sc
            (!sc).Add(id, c)
        )
        o
        
    static member Remove(id : int, key : obj) : bool =
        let bDone = ref false
        if (Utils.IsNotNull key) then
            lock (SharedComponentState.SharedState) (fun _ ->
                let sc = SharedComponentState.SharedState.[key]
                sc.Remove(id)
                if (sc.Items.Count = 0) then
                    bDone := sc.AllowClose
                    SharedComponentState.SharedState.Remove(key) |> ignore
            )
        !bDone

/// A component class provides a generic tool to build a processing pipeline using a threadpool
/// It provides the following functionality:
/// 1. An arbitrary object (an item of type 'T) is dequeue from a queue - a queue must have a BaseQ as a base class
/// 2. The object is repeatedly "processed" until the processing says it is finished by arbirary piece of processing code
///    The processing code returns a tuple of (bool, ManualResetEvent): (complete, event)
///    When complete == true, then item has completed and new item is to be dequeued
///    When event <> null, then processing cannot continue and must wait for event to fire before trying again
///    There may be cases when complete is true, but event is not null, in these cases, processing waits for event to fire
///    However, if complete is false, event must be non-null
/// The processing may occur on its own thread or on a threadpool
/// If  event <> null,
/// - If threadpool is being used, the component processing is removed from the threadpool work item queue
///   and gets requeued once event fires
/// - If processing is occuring on own thread, then that thread blocks for event to fire
/// There is also support for multiple processing steps to take place in a single item
type [<AllowNullLiteral>] Component<'T when 'T:null and 'T:equality>() =
    static let systemwideProcess = ConcurrentDictionary<string, ('T->ManualResetEvent)*bool>(StringComparer.Ordinal)
    let compBase = ComponentBase()
    let bRelease = ref 0
    let item : 'T ref = ref null
    let mutable q : BaseQ<'T> = null
    let mutable bIsClosed = false
    let isClosed() =
        bIsClosed && q.IsEmpty
    let mutable proc = Unchecked.defaultof<unit->ManualResetEvent*bool>
    let bCloseDone = ref -1
    let bTerminateDone = ref -1
    let processors = ConcurrentDictionary<string, ('T->ManualResetEvent)*bool>(StringComparer.Ordinal)
    let processorCount = ref -1
    let waitTimeMs = 0
    let lockObj = Object()
    let mutable bStartedProcessing = false
    let [<VolatileField>] mutable bMultipleInit = false
    let [<VolatileField>] mutable isTerminated = false
    let [<VolatileField>] mutable bInProcessing = false
    let [<VolatileField>] mutable procCount = 0

    override x.Finalize() =
        /// Close All Active Connection, to be called when the program gets shutdown.
        x.ReleaseAllItems()

    /// Standard form for all class that use CleanUp service
    interface IDisposable with
        /// Close All Active Connection, to be called when the program gets shutdown.
        member x.Dispose() = 
            x.ReleaseAllItems()
            if Utils.IsNotNull q then
                (q :> IDisposable).Dispose()
            GC.SuppressFinalize(x)

    // accessors
    member internal x.Item with get() = item
    member internal x.Closed with get() = bIsClosed and set(v) = bIsClosed <- v
    // Systemwide processor for component
    static member internal SystemwideProcessors with get() = systemwideProcess

    member internal x.Processors with get() = processors
    member internal x.WaitTimeMs with get() = waitTimeMs

    /// The internal component Q into which elements are queued for processing
    member x.Q with get() : BaseQ<'T> = q and set(v) = q <- v

    // changing enqueue/dequeue times based on number of items being processed by thread poool
    static member internal ChangeQTime<'TP,'TN when 'TN:null and 'TN:equality>
        (tpool : ThreadPoolWithWaitHandles<'TP>)
        (comp : Component<'T>)
        (compN : Component<'TN>)
        (adjustOwnEnqueue : bool)
        (s : SharedComponentState) =

        let setTime(timeMs) =
            let q : BaseQ<'T> = comp.Q
            q.WaitTimeDequeueMs <- timeMs
            if adjustOwnEnqueue then
                q.WaitTimeEnqueueMs <- timeMs
            if Utils.IsNotNull compN then
                let qN : BaseQ<'TN> = compN.Q
                qN.WaitTimeEnqueueMs <- timeMs

        if Utils.IsNotNull tpool then
            let count = s.Items.Count
            if (count <= ThreadPoolWithWaitHandles<'TP>.DefaultNumParallelExecution) then
                setTime(0)
            else
                setTime(0)

    // Actions
    /// A (unit->bool) function which returns true/false to tell if any more data needs to be processed
    member val IsClosed = isClosed with get, set
    /// A function which dequeues data from the internal BaseQ
    member val Dequeue = Unchecked.defaultof<'T ref -> bool*ManualResetEvent> with get, set
    /// A function which processes each item, if multiple processing needs to take place, use
    /// AddProc or RegisterItem to add processing and UnregisterItem to remove processing
    member val Proc = Unchecked.defaultof<'T -> bool*ManualResetEvent> with get, set
    /// A function to execute when no more items are in queue and IsClosed returns true
    member val Close = Unchecked.defaultof<unit->unit> with get, set
    // terminate may get called outside of processing pipeline,
    //   e.g. through SelfTerminate, so set to empty function
    /// A function to execute for non-graceful termination, i.e. without waiting for queue to empty
    member val Terminate = (fun _ -> ()) with get, set
    /// A function which gets executed once an item is finished processing
    member val ReleaseItem : 'T ref -> unit = (fun _ -> ()) with get, set

    member private x.CloseDone with get() = bCloseDone
    member private x.TerminateDone with get() = bTerminateDone
    member private x.IsTerminated with get() = isTerminated
    /// the event being waited upon for processing to resume
    member val private ProcWaitHandle : ManualResetEvent = null with get, set
    member private x.InProcessing with get() = bInProcessing and set(v) = bInProcessing <- v
    member private x.ProcCount with get() = procCount and set(v) = procCount <- v
    member private x.CompBase with get() = compBase

    member x.ReleaseAllItems() =
        if (Interlocked.CompareExchange(bRelease, 1, 0)=0) then
            isTerminated <- true
            let spinWait = new SpinWait()
            let oldCnt = x.ProcCount
            // wait to prevent double release call for ReleaseItem on current item
            while (x.InProcessing && oldCnt=x.ProcCount) do
                // if x.ProcCount has increased, then even if InProcessing, then Process has seen "isTerminated=true" condition
                spinWait.SpinOnce()
            let itemDQ : 'T ref = ref Unchecked.defaultof<'T>
            if (Utils.IsNotNull !item) then
                x.ReleaseItem(item)
                item := null
            if (Utils.IsNotNull x.Q) then
                while (not x.Q.IsEmpty) do
                    let (success, event) = x.Q.DequeueWait(itemDQ)
                    if (success) then
                        x.ReleaseItem(itemDQ)
                        itemDQ := null

    // default CloseAction by setting next component to close in pipeline 
    // this gets called once bIsClosed is true && queue is empty, triggers next component to close
    /// A default "Close" function which can be used
    /// <param name="self">A reference to the component</param>
    /// <param name="nextComponent>A reference to next component in pipeline - can be set to null</param>
    /// <param name="triggerNext>An option for code to execute after close done</param>
    static member DefaultClose<'TN when 'TN:null and 'TN:equality> 
        (self : Component<'T>) (nextComponent : Component<'TN>) (triggerNext : Option<unit->unit>) () =
        if (Interlocked.Increment(self.CloseDone) = 0) then  
            if (Utils.IsNotNull nextComponent) then
                nextComponent.Closed <- true
                // force a dequeue
                if (Utils.IsNotNull nextComponent.Q) then
                    try
                        nextComponent.Q.Full.Set() |> ignore
                    with
                        | :? System.ObjectDisposedException -> ()
            match triggerNext with
                | None -> ()
                | Some(nextClose) -> nextClose()

    // default terminate action, called immediately
    /// A default "Terminate" function which can be used
    /// <param name="self">A reference to the component</param>
    /// <param name="nextComponent">A reference to next component in pipeline - can be set to null</param>
    /// <param name="triggerNext">An option for code to execute after terminate done</param>
    static member DefaultTerminate<'TN when 'TN:null and 'TN:equality>
        (self : Component<'T>) (nextComponent : Component<'TN>) (triggerNext : Option<unit->unit>) () =
        if (Interlocked.Increment(self.TerminateDone) = 0) then
            if (Utils.IsNotNull nextComponent) then
                nextComponent.ReleaseAllItems()
                nextComponent.Terminate()
            match triggerNext with
                | None -> ()
                | Some(nextTerminate) -> nextTerminate()

    // default connect to next component
    // should not be internal, but can only be non-internalized if ThreadPoolWithWaitHandles is non-internalized
    /// Connect current component to next component using:
    /// - "DequeueWait" for dequeue action
    /// - "DefaultClose" for close action
    /// - "DefaultTerminate" for terminate action
    /// <param name="threadPool">The threadpool being used for processing</param>
    /// <param name="adjustOwnEnqueue">True for first component in pipeline</param>
    /// <param name="nextComponent">The next component in pipeline - can be null</param>
    /// <param name="nextClose">An option for code to execute after close done</param>
    /// <param name="nextTerminate">An option for code to execute after terminate done</param>
    member internal x.ConnectTo<'TP,'TN when 'TN:null and 'TN:equality> 
        (threadPool : ThreadPoolWithWaitHandles<'TP>) (adjustOwnEnqueue : bool) 
        (nextComponent : Component<'TN>) (nextClose : Option<unit->unit>) (nextTerminate : Option<unit->unit>) =
        x.Dequeue <- q.DequeueWait
        x.Close <- Component<'T>.DefaultClose x nextComponent nextClose
        x.Terminate <- Component<'T>.DefaultTerminate x nextComponent nextTerminate
        compBase.Notify <- Component<_>.ChangeQTime threadPool x nextComponent adjustOwnEnqueue

    // self-close component (only use at ends of pipeline, intermediate components close automatically through CloseAction)
    /// Self close the component and start closing the pipeline
    abstract SelfClose : unit->unit
    default x.SelfClose() =
        x.Closed <- true
        // force a dequeue
        if (Utils.IsNotNull x.Q) then
            try
                x.Q.Full.Set() |> ignore
            with
                | :? System.ObjectDisposedException -> ()

    /// Self terminate the component and start terminating the pipeline
    abstract SelfTerminate : unit->unit
    default x.SelfTerminate() =
        // clear the queue
        x.ReleaseAllItems()
        if (Utils.IsNotNull x.Q) then
            x.Q.Clear()
        isTerminated <- true
        // clear other queues down the line
        x.Terminate()
        x.SelfClose()
        // if waiting for event, set it
        // make sure we exit Process at least once after setting "IsTerminated" to true, otherwise ProcWaitHandle may change
        let oldCount = procCount
        while (x.InProcessing && oldCount = procCount) do
            Thread.Sleep(10)
        // other thread may execute Process again and handle may be incorrect, however
        //    it does not matter as "IsTerminated" has already been set to true
        //    therefore, if Process has been called again, it already terminates
        //    if this was issue, can use counter to make sure it has not entered Process again.
        // take handle snapshot as it may change in other thread and become null
        let handle = x.ProcWaitHandle
        if (Utils.IsNotNull handle) then
            try
                handle.Set() |> ignore
            with
                | :? ObjectDisposedException as ex -> ()

    static member internal WaitAndExecOnSystemTP(event : WaitHandle, timeOut : int) (fn : obj->bool->unit, state : obj) =
        if (Utils.IsNotNull event) then
            let waitAndExecOnSystemTPWrap(o : obj) (bTimeOut : bool) =
                let (rwh, registrationCompleted, state) = o :?> RegisteredWaitHandle ref*ManualResetEventSlim*obj
                fn (state) (bTimeOut)
                registrationCompleted.Wait()
                (!rwh).Unregister(null) |> ignore
                registrationCompleted.Dispose()
            let rwh = ref Unchecked.defaultof<RegisteredWaitHandle>
            let registrationCompleted = new ManualResetEventSlim(false)
            rwh := ThreadPool.RegisterWaitForSingleObject(event, new WaitOrTimerCallback(waitAndExecOnSystemTPWrap), (rwh, registrationCompleted, state), timeOut, true)
            registrationCompleted.Set()
        else
            Component<_>.WaitAndQueueOnSystemTP(null, timeOut) (fn, state)

    static member internal WaitAndQueueOnSystemTP(event : WaitHandle, timeOut : int) (fn : obj->bool->unit, state : obj) =
        if (Utils.IsNotNull event) then
            let wrappedFunc (o : obj) =
                let (state, bTimeOut) = o :?> (obj*bool)
                fn (state) (bTimeOut)
            let addToQ (o : obj) (bTimeOut : bool) =
                ThreadPool.QueueUserWorkItem(new WaitCallback(wrappedFunc), (o, bTimeOut)) |> ignore
            Component<_>.WaitAndExecOnSystemTP(event, timeOut) (addToQ, state)
        else
            ThreadPool.QueueUserWorkItem(new WaitCallback(fun o -> fn (o) (false)), state) |> ignore

    // process on own thread
    static member private ProcessOnOwnThread (o : obj) (fn : Option<unit->unit>) =
        let action = o :?> (unit->ManualResetEvent*bool)
        let mutable bContinue = true
        while (bContinue) do
            let (waitEvent, bStop) = action()
            if (bStop) then
                bContinue <- false
                match fn with
                    | None ->()
                    | Some(cb) -> cb()
            else if Utils.IsNotNull waitEvent then
                waitEvent.WaitOne() |> ignore

    /// Start component processing on own thread
    /// <param name="tpKey">A key to identify the thread</param>
    /// <param name="infoFunc">A function which returns information about the thread</param>
    static member StartProcessOnOwnThread(proc : unit->ManualResetEvent*bool)
                                         (tpKey : 'TP)
                                         (fnCb : Option<unit->unit>)
                                         (infoFunc : 'TP -> string) : unit =
        let thread = ThreadTracking.StartThreadForAction ( fun _ -> infoFunc tpKey ) (Action<_>( fun _ -> Component<'T>.ProcessOnOwnThread(proc) (fnCb)))
        thread.IsBackground <- false
        ()
//        let threadStart = ParameterizedThreadStart(Component<'T>.ProcessOnOwnThread)
//        let thread = Thread(threadStart)
//        // main thread must wait for this thread to stop
//        thread.IsBackground <- false
//        thread.Start(proc)

    // thread pool to process on
    static member internal StartOnThreadPool(threadPool : ThreadPoolWithWaitHandles<'TP>)
                                            (processFunc : unit -> ManualResetEvent*bool)
                                            (cts : CancellationToken)
                                            (tpKey : 'TP)
                                            (infoFunc : 'TP -> string) =
        threadPool.EnqueueRepeatableFunction processFunc cts tpKey infoFunc
        threadPool.TryExecute()

    // thread pool using system thread pool
    static member internal StartOnSystemThreadPool (tpKey: 'TP) (processFunc : unit->ManualResetEvent*bool) (finishCb : Option<unit->unit>) =
        let waitAndContinue (o : obj) (bTimeOut : bool) =
            try
                Process.ReportThreadPoolWorkItem((fun _ -> sprintf "%A start execute as callback for RegisterWaitForSingleObject 1" tpKey), true)
                let (rwh, handle, registrationCompleted, func) = o :?> RegisteredWaitHandle ref*ManualResetEvent*ManualResetEventSlim*WaitCallback
                if (not bTimeOut) then
                    System.Threading.ThreadPool.QueueUserWorkItem(func, func) |> ignore
                    registrationCompleted.Wait()
                    (!rwh).Unregister(null) |> ignore
                    registrationCompleted.Dispose()
            finally
                Process.ReportThreadPoolWorkItem(( fun _ -> sprintf "%A end execute as callback for RegisterWaitForSingleObject 1" tpKey), false)

        let wrappedFunc (o : obj) : unit =
            try
                Process.ReportThreadPoolWorkItem(( fun _ -> sprintf "%A start execute as a user work item" tpKey), true)
                let func = o :?> WaitCallback
                let (event, finish) = processFunc()
                if (not finish) then
                    if (Utils.IsNotNull event) then
                        let rwh = ref Unchecked.defaultof<RegisteredWaitHandle>
                        let registrationCompleted = new ManualResetEventSlim(false)
                        rwh := ThreadPool.RegisterWaitForSingleObject(event, waitAndContinue, (rwh, event, registrationCompleted, func), -1, true)
                        registrationCompleted.Set()
                    else
                        // queue again if not finished
                        System.Threading.ThreadPool.QueueUserWorkItem(func, func) |> ignore
                else
                    match finishCb with
                        | None -> ()
                        | Some(cb) -> cb()
            finally
                Process.ReportThreadPoolWorkItem((fun _ -> sprintf "%A end execute as a user work item" tpKey), false)

        // start the work
        let wc = new WaitCallback(wrappedFunc)
        System.Threading.ThreadPool.QueueUserWorkItem(wc, wc) |> ignore

    static member internal ExecTP(threadPool : ThreadPoolWithWaitHandles<'TP>) =
        // now allow closing of threadpool
        let (ret, value) = SharedComponentState.SharedState.TryGetValue(threadPool)
        if (ret) then
            value.AllowClose <- true
            if (value.Items.Count = 0) then
               threadPool.Cancel() 
        else
            threadPool.Cancel()

    static member internal ThreadPoolWait (tp : ThreadPoolWithWaitHandles<'TP>) =
        tp.WaitForAll( -1 ) // use with EnqueueRepeatableFunction in AddWorkItem
        //tp.HandleDoneExecution.Wait() // for other cases

    /// Add work item on pool, but don't necessarily start it until ExecTP is called
    /// <param name="threadPool">The thread pool to execute upon</param>
    /// <param name="tpKey">A key to identify the processing component</param>
    /// <param name="infoFunc">A function which returns information about the processing component</param>
    static member internal AddWorkItem(func : unit->ManualResetEvent*bool)
                                        (threadPool : ThreadPoolWithWaitHandles<'TP>)
                                        (cts : CancellationToken)
                                        (tpKey : 'TP)
                                      (infoFunc : 'TP -> string) =
        let compBase = ComponentBase()
        compBase.SharedStateObj <- SharedComponentState.Add(compBase.ComponentId, compBase, threadPool)
//        let finishCb : Option<unit->unit> =
//            if (Utils.IsNull threadPool) then
//                None
//            else
//                threadPool.HandleDoneExecution.Reset() |> ignore
//                let fnFinish() =
//                    let bDone = SharedComponentState.Remove(compBase.ComponentId, compBase.SharedStateObj)
//                    if (bDone) then
//                        threadPool.HandleDoneExecution.Set() |> ignore
//                Some(fnFinish)
        //Component<'T>.StartOnSystemThreadPool func finishCb
        threadPool.EnqueueRepeatableFunction func cts tpKey infoFunc
        //Component<'T>.StartProcessOnOwnThread func tpKey finishCb infoFunc
        //Prajna.Tools.ThreadPool.Current.AddWorkItem(func, finishCb, infoFunc(tpKey))

    /// Start component processing on threadpool - internal as ThreadPoolWithWaitHandles is not internal
    /// <param name="threadPool>The thread pool to execute upon</param>
    /// <param name="cts">A cancellation token to cancel the processing</param>
    /// <param name="tpKey">A key to identify the processing component</param>
    /// <param name="infoFunc">A function which returns information about the processing component</param>
    member internal x.StartProcess(threadPool : ThreadPoolWithWaitHandles<'TP>)
                                  (cts : CancellationToken)
                                  (tpKey : 'TP)
                                  (infoFunc : 'TP -> string) : unit =
        lock (lockObj) (fun _ ->
            proc <- Component.Process item x.Dequeue x.Proc x.IsClosed x.Close tpKey infoFunc x
            compBase.SharedStateObj <- SharedComponentState.Add(compBase.ComponentId, compBase, threadPool)
            Component<'T>.StartOnSystemThreadPool tpKey proc None
            //Component<'T>.StartOnThreadPool threadPool proc cts tpKey infoFunc
            //Component<'T>.StartProcessOnOwnThread proc tpKey finishCb infoFunc
            //Prajna.Tools.ThreadPool.Current.AddWorkItem(proc, None, infoFunc(tpKey))
            bStartedProcessing <- true
        )

    /// Start work item on pool
    /// <param name="threadPool">The thread pool to execute upon</param>
    /// <param name="tpKey">A key to identify the processing component</param>
    /// <param name="infoFunc">A function which returns information about the processing component</param>
    static member internal StartWorkItem(func : unit->ManualResetEvent*bool)
                                        (threadPool : ThreadPoolWithWaitHandles<'TP>)
                                        (cts : CancellationToken)
                                        (tpKey : 'TP)
                                        (infoFunc : 'TP -> string) =
        let compBase = new ComponentBase()
        compBase.SharedStateObj <- SharedComponentState.Add(compBase.ComponentId, compBase, threadPool)
        Component<'T>.StartOnSystemThreadPool tpKey func None
        //Component<'T>.StartOnThreadPool threadPool func cts tpKey infoFunc
        //Component<'T>.StartProcessOnOwnThread func tpKey finishCb infoFunc
        //Prajna.Tools.ThreadPool.Current.AddWorkItem(func, None, infoFunc(tpKey))
        compBase

    //  internally overwrites Dequeue and Proc (Dequeue reuses old Dequeue for internal dequeueing)
    /// Initialize the use of multiple processors - this is useful if multiple actions need to be performed
    /// on each item in the queue - use AddProc/RegisterProc to add processing, UnregisterProc to remove processing
    member x.InitMultipleProcess() =
        let internalDQ = x.Dequeue
        let dequeueItem(item : 'T ref) =
            let (success, event) = internalDQ item
            if (success) then
                for pair in processors do
                    processors.[pair.Key] <- (fst pair.Value, false)
            (success, event)
        let processItem(item : 'T) =
            let mutable bContinue = true
            let mutable eventProc = null
            for pair in processors do
                if (bContinue && snd pair.Value = false) then
                    let event = (fst pair.Value)(item)
                    if (Utils.IsNotNull event) then
                        bContinue <- false
                        eventProc <- event
                    else
                        processors.[pair.Key] <- (fst pair.Value, true)
            (Utils.IsNull eventProc, eventProc)
        // overwrite dequeue / process
        x.Dequeue <- dequeueItem
        x.Proc <- processItem
        bMultipleInit <- true   

    // check and make sure initialization has been called
    member private x.CheckAndInitMultipleProcess() =
        // avoid frequent lock with first check
        if (bMultipleInit = false) then
            lock (lockObj) (fun() ->
                if (bStartedProcessing && bMultipleInit = false) then
                    x.InitMultipleProcess()
                    bMultipleInit <- true
            )

    /// Register a processor for the component with name
    /// If this is used, the Proc property does not need to be set
    /// <param name="name">The name of the processing component - used for unregistering</param>
    /// <param name="processItem">A function which does processing, returns event if cannot complete</param>
    member x.RegisterProc(name : string, processItem) =
        x.CheckAndInitMultipleProcess()
        let cnt = Interlocked.Increment(processorCount)
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Registering processor %d : %s to %A" cnt name x)
        processors.[name] <- (processItem, false)

    /// Get or add a new processor for the component with name
    /// If this is used, the Proc property does not need to be set
    /// <param name="name">The name of the processing component - used for unregistering</param>
    /// <param name="processItem">A function which does processing, returns event if cannot complete</param>
    member x.GetOrAddProc(name: string, processItem : 'T->ManualResetEvent) : unit =
        let addFn (name : string) =
            x.CheckAndInitMultipleProcess()
            let cnt = Interlocked.Increment(processorCount)
            Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Registering processor %d : %s to %A" cnt name x)
            (processItem, false)
        processors.GetOrAdd(name, addFn) |> ignore

    /// Register a processor for the component - same as RegisterProc, but default name is created and returned
    /// <param name="processItem">A function which does processing, returns event if cannot complete</param>
    /// <returns>The internal name of the processor - can use for unregistering</returns>
    member x.AddProc(processItem : 'T->ManualResetEvent) =
        x.CheckAndInitMultipleProcess()
        let cnt = Interlocked.Increment(processorCount)
        let name = sprintf "Listener:%d:%d" compBase.ComponentId cnt
        Logger.LogF(LogLevel.MildVerbose, fun _ -> sprintf "Registering processor %d : %s to %A" cnt name x)
        processors.[name] <- (processItem, false)
        name

    /// Unregister a processor for the component
    /// <param name="name">The name of the processing to be removed</param>
    member x.UnregisterProc(name) =
        let processItem = ref Unchecked.defaultof<('T->ManualResetEvent)*bool>
        processors.TryRemove(name, processItem) |> ignore

    // Notes on processAction:
    // In general processAction should only return complete=true if eventProc is null
    // However, there may be some cases (e.g. SendAsync on network) where this may not be true
    // The only case where processAction should return complete=true and "Utils.IsNotNull eventProc" is the folllowing:
    //     if complete=true happens by the time the event fires.
    static member internal Process (item : 'T ref) 
                                   (dequeueAction : 'T ref -> bool*ManualResetEvent) 
                                   (processAction : 'T -> bool*ManualResetEvent) 
                                   (isClosed : unit -> bool)
                                   (closeAction : unit -> unit)
                                   (tpKey : 'TP)
                                   (infoFunc : 'TP -> string)
                                   (x : Component<'T>)  () : 
                                   ManualResetEvent * bool =
        x.InProcessing <- true
        x.ProcCount <- x.ProcCount + 1
        try
            if (x.IsTerminated) then
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "ThreadPool Function Work item %A terminates" (infoFunc(tpKey))))
                x.ProcWaitHandle <- null
                x.InProcessing <- false
                closeAction()
                // no more running on thread pool
                SharedComponentState.Remove(x.CompBase.ComponentId, x.CompBase.SharedStateObj) |> ignore
                (null, true)
            else if (Utils.IsNotNull !item) then
                // continue to process remaining work item
                let (complete, eventProc) = processAction(!item)
                if (complete) then
                    x.ReleaseItem(item)
                    item := null
                x.ProcWaitHandle <- eventProc
                x.InProcessing <- false
                (eventProc, false)
            else
                // get new item from queue
                let (ret, eventDQ) = dequeueAction item
                if ret then
                    let (complete, eventProc) = processAction(!item)
                    if (complete) then
                        x.ReleaseItem(item)
                        item := null
                    x.ProcWaitHandle <- eventProc
                    x.InProcessing <- false
                    (eventProc, false)
                else if (isClosed()) then
                    Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "ThreadPool Function Work item %A terminates" (infoFunc(tpKey))))
                    x.ProcWaitHandle <- null
                    x.InProcessing <- false
                    closeAction()
                    // no more running on thread pool
                    SharedComponentState.Remove(x.CompBase.ComponentId, x.CompBase.SharedStateObj) |> ignore
                    (null, true)
                else
                    x.ProcWaitHandle <- eventDQ
                    x.InProcessing <- false
                    (eventDQ, false)
        with e ->
            x.ProcWaitHandle <- null
            x.InProcessing <- false
            Logger.LogF( LogLevel.Error, (fun () -> sprintf "Exception in Component.Process:%A" e))
            (null, true)

    // processing with internal wait if desired
    static member private ProcessW (item : 'T ref) 
                                   (dequeueAction : 'T ref -> bool*ManualResetEvent) 
                                   (processAction : 'T -> bool*ManualResetEvent) 
                                   (isClosed : unit -> bool)
                                   (closeAction : unit -> unit)
                                   (tpKey : 'TP)
                                   (infoFunc : 'TP -> string)
                                   (x : Component<'T>)  () : 
                                   ManualResetEvent * bool =
        let mutable count = 0
        let mutable ev : ManualResetEvent = null
        let mutable closed : bool = false
        while (count < 2) do
            let (ev1, closed1) = Component.Process item dequeueAction processAction isClosed closeAction tpKey infoFunc x ()
            ev <- ev1
            closed <- closed1
            if Utils.IsNull ev || closed then
                count <- 2
            else
                if count < 1 then
                    ev.WaitOne(x.WaitTimeMs) |> ignore
                count <- count + 1
        (ev, closed)

    // process Component of obj which is unit->ManualResetEvent functions - can be used as Proc action
    static member internal ProcessFn (fn : obj) : bool*ManualResetEvent =
        let fnExec = fn :?> (unit->ManualResetEvent)
        let event = fnExec()
        (Utils.IsNull event, event)

// component which only processes when either of two conditions are met:
// 1. the amount of data buffered exceeds a certain threshold
// 2. the time since the last processing exceeds a certain threshold
type internal ComponentThr<'T when 'T:null and 'T:equality>(desiredMaxSize : int64, maxSize : int64, 
                                                            desiredMinSize : int64, minSize : int64,
                                                            maxQWaitTime : int64) as x =
    inherit Component<'T>()
    do x.Q <- new FixedSizeMinSizeQ<'T>(desiredMaxSize, maxSize, desiredMinSize, minSize)
    let q = x.Q :?> FixedSizeMinSizeQ<'T>

    member x.QThr with get() = q

    override x.SelfClose() =
        // make sure everything dequeues and processes and full event does not reset
        q.DesiredMinSize <- 0L
        q.MinSize <- 0L
        base.SelfClose()

    member x.StartUsingDQ(threadPool : ThreadPoolWithWaitHandles<'TP>)
                         (cts : CancellationToken)
                         (tpKey : 'TP)
                         (infoFunc : 'TP -> string) 
                         (infoFuncTimer : unit -> string) : unit =
        q.InitTimer infoFuncTimer maxQWaitTime
        let proc = Component.Process x.Item x.Dequeue x.Proc x.IsClosed x.Close tpKey infoFunc x
        Component<'T>.StartOnThreadPool threadPool proc cts tpKey infoFunc

    abstract Start : ThreadPoolWithWaitHandles<'TP>->CancellationToken->'TP->('TP->string)->(unit->string)->unit
    default x.Start(threadPool : ThreadPoolWithWaitHandles<'TP>)
                   (cts : CancellationToken)
                   (tpKey : 'TP)
                   (infoFunc : 'TP -> string) 
                   (infoFuncTimer : unit -> string) : unit =
        x.Dequeue <- q.DequeueWaitSizeRemProcess
        x.StartUsingDQ threadPool cts tpKey infoFunc infoFuncTimer

