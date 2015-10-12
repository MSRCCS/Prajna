(*---------------------------------------------------------------------------
    Copyright 2014-, Microsoft.	All rights reserved                                                      

    File: 
        clientinfo.fs
  
    Description: 
        Information sent by clients to prajnacontroller

    Author:																	
        Sanjeev Mehrotra
    Date:
        May 2014
 ---------------------------------------------------------------------------*)
 
namespace Prajna.Core

open System
open System.IO
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open System.Net
open System.Net.Sockets
open Prajna.Tools
open Prajna.Tools.FSharp
open LanguagePrimitives
open System.Text.RegularExpressions

[<FlagsAttribute>]
type internal ClientInfoType =
    | None = 0u
    | Basic = 1u
    | DSet = 2u
    | Assembly = 4u
    | Dependency = 16u
    | Cluster = 8u                                              
    | All = 31u
    
type internal AssemblyPack() =                               // for information regarding assemblies
    member val Name = "" with get,set
    member val date = DateTime() with get,set
    member val size = new int64() with get,set

type internal DependencyPack() =                             // for information regarding dependencies                           // "Packs" are flexible for future needs
    member val Name = "" with get,set
    member val date = DateTime() with get,set
    member val size = new int64() with get,set

type internal StoragePack() =                                    // for DSets
     member val Name = "" with get,set
     member val date = DateTime() with get,set
     member val vers = [|""|] with get,set

type internal SocketEx() =
    static member SendRawAsync(x : NetworkSocket, sendBuf:byte[]) =
        x.NetStream.WriteAsync( sendBuf, 0, sendBuf.Length)
    static member SendRawStringAsync(x : NetworkSocket, s:string) =
        x.SendRawAsync( System.Text.Encoding.ASCII.GetBytes( s ) )
    static member SendAsync(x : NetworkSocket, sendBuf : byte[]) = async {
        let bClosed = ref false
        if not x.Shutdown then
            try
                do! x.NetStream.AsyncWrite(BitConverter.GetBytes(IPAddress.HostToNetworkOrder(sendBuf.Length)))
                do! x.NetStream.AsyncWrite(sendBuf)
            with e->
                bClosed := true
        return !bClosed
    }
    static member RcvdAsync(x : NetworkSocket, len : int) = async {
        let buf = Array.zeroCreate<byte> len
        let rem = ref len
        let offset = ref 0
        let bClosed = ref false
        while (!rem > 0 && (not !bClosed)) do
            try
                // Either of following should be same probably
                //let! read = x.NetStream.AsyncRead(buf, !offset, !rem)
                let! read = x.NetStream.ReadAsync(buf, !offset, !rem) |> Async.AwaitTask
                if (read <= 0) then
                    bClosed := true
                else
                    rem := !rem - read
                    offset := !offset + read
            with e ->
                bClosed := true
        return (!bClosed, buf)
    }
    static member RcvdAsync(x : NetworkSocket) = async {
        let! (bClosed, rcvdBuffer) = SocketEx.RcvdAsync(x, 4)
        if (not bClosed) then
            let len = IPAddress.NetworkToHostOrder(BitConverter.ToInt32(rcvdBuffer, 0))
            return! SocketEx.RcvdAsync(x, len)
        else
            return (bClosed, [||])
    }
    static member RcvdAsyncString(x : NetworkSocket) = async {
        let! (bClosed, buf) = SocketEx.RcvdAsync(x)
        if (not bClosed) then
            return (false, System.Text.ASCIIEncoding.ASCII.GetString(buf))
        else
            return (true, "")
    }

[<AllowNullLiteral>]
type internal ClientStatusEx() =
    // properties
    // to interface with network
    member val SendEvent = new Threading.AutoResetEvent(false)
    member val SendQ = ConcurrentQueue<byte[]>() with get
    member val Connected = false with get, set
    member val ConnectionTo = "" with get, set

    // Client Info to interface with controllerk
    member val ClientRefresh = ClientInfoType.All with get, set
    member val DSetName = List<string>()
    member val MaxOutstandingRequests = 10
    member val name = System.Environment.GetEnvironmentVariable("computername")

    member val AssemblyList = List<AssemblyPack>()
    //Dependency declarations
    member val DependencyList = List<DependencyPack>()
    member val StorageList = List<StoragePack>()
    member val metadict = Dictionary<String,byte[]>() with get,set
    member val clusterFiles = Dictionary<String,byte[]>() //List<String>() with get,set

    // methods
    // to interface with network
    member x.SendInfo(desiredInfo : ClientInfoType, info : byte[]) =
        if (x.SendQ.Count < x.MaxOutstandingRequests) then
            use ms = new MemStream()
            let cmd = ControllerCommand(ControllerVerb.Report, ControllerNoun.ClientInfo)
            ms.Serialize(cmd)
            ms.WriteUInt32(uint32 desiredInfo)
            ms.Serialize(info)
            x.SendQ.Enqueue(ms.GetBuffer())
            x.SendEvent.Set() |> ignore

    // server request client node for information
    member x.ReqInfo(desiredInfo : ClientInfoType) =
        if (x.SendQ.Count < x.MaxOutstandingRequests) then
            use ms = new MemStream()
            let cmd = ControllerCommand(ControllerVerb.Get, ControllerNoun.ClientInfo)
            ms.Serialize(cmd)
            ms.WriteUInt32(uint32 desiredInfo)
            x.SendQ.Enqueue(ms.GetBuffer())
            x.SendEvent.Set() |> ignore

    member x.ReqOperation (opNoun:ControllerNoun) (target : string) =                   // currently supports delete, flexible for future needs
        if (x.SendQ.Count < x.MaxOutstandingRequests) then
            use ms = new MemStream()
            let cmd = ControllerCommand(ControllerVerb.Delete, opNoun)
            ms.Serialize(cmd)
            ms.WriteString(target)
           // ms.WriteUInt32(uint32 ClientInfoType.Assembly)
            x.SendQ.Enqueue(ms.GetBuffer())
            x.SendEvent.Set() |> ignore

    member x.ReqValidate(desiredInfo : ClientInfoType) = 
        if (x.SendQ.Count < x.MaxOutstandingRequests) then
            use ms = new MemStream()
            let cmd = ControllerCommand(ControllerVerb.Get, ControllerNoun.ClientInfo)
            ms.Serialize(cmd)
            ms.WriteUInt32(uint32 desiredInfo)
            x.SendQ.Enqueue(ms.GetBuffer())
            x.SendEvent.Set() |> ignore 
        ()

    member x.StartReq(name : string) =
        use ms = new MemStream()
        let cmd = ControllerCommand(ControllerVerb.Report, ControllerNoun.ClientInfo)
        ms.Serialize(cmd)
        ms.WriteString(name)
        x.SendQ.Enqueue(ms.GetBuffer())
        x.SendEvent.Set() |> ignore

    // parse receive command (at both client and server)
    member x.ParseRecv(buf : byte[]) =
        use ms = new MemStream(buf)
        let cmd = ms.Deserialize() :?> ControllerCommand
        match (cmd.Verb, cmd.Noun) with
            | (ControllerVerb.Get, ControllerNoun.ClientInfo) ->
                // client parses controller command
                let desiredInfo = LanguagePrimitives.EnumOfValue<uint32, ClientInfoType>(ms.ReadUInt32())
                x.GetStatus(desiredInfo)
                x.SendInfo(desiredInfo, x.Pack(desiredInfo))
            | (ControllerVerb.Report, ControllerNoun.ClientInfo) ->
                // controller parses client information
                try
                    let reportedInfo = LanguagePrimitives.EnumOfValue<uint32, ClientInfoType>(ms.ReadUInt32())
                    let info = ms.Deserialize() :?> byte[]
                    x.UpdateInfo(reportedInfo, info)
                with
                    |_ -> ()

                // delete assemblies
            | (ControllerVerb.Delete, ControllerNoun.Assembly) ->
                let target = ms.ReadString()
                x.DelAssembly(target)
                // delete dependencies
            | (ControllerVerb.Delete, ControllerNoun.Dependency) ->
                let target = ms.ReadString()
                x.DelDependency(target)
                // delete DSets
            | (ControllerVerb.Delete, ControllerNoun.DSet) ->
                let target = ms.ReadString()
                x.DelDSet(target)
            | _ -> Logger.Log( LogLevel.Error, sprintf "Error: unknown controller command %A" cmd )

    // code running on client nodes ====
    /// Update  of client node
    member x.GetStorageStatus() =
        x.StorageList.Clear()                                                   // go to all 3 possible drives and look for dat files. Then collect DSet directories

        let initArray =
            DeploymentSettings.StorageDrives
            |> Array.collect (fun d -> 
                                try
                                    let dir = DirectoryInfo(d)
                                    let dirs = ( (dir.GetFiles("*.*", SearchOption.AllDirectories)) |> Array.filter (fun item ->  Regex.IsMatch(item.Name, "[a-z,1-9]*.dat" )) |> Array.map ( fun item -> item.Directory ) |> Array.map (fun item -> item.Parent) )
                                    dirs
                                with
                                    |_ -> [||]
                          )

        let uniqueArray = initArray |> Seq.map (fun item -> item) |> Seq.sortBy(fun item -> item.LastWriteTime) |> Seq.distinctBy (fun elem -> 
                                                                                                let idx = elem.FullName.IndexOf(DeploymentSettings.DataFolder)
                                                                                                let comparer = elem.FullName.Substring(idx+9)
                                                                                                comparer
                                                                                    ) |> Seq.toArray                    // filter for unique DSets

        for dircs in uniqueArray do
            let pack = StoragePack()
            pack.Name <- dircs.FullName
            pack.date <- dircs.LastWriteTime
            pack.vers <- (dircs.GetDirectories() |> Array.map ( fun item -> 
                                                                            let verSizes = item.GetFiles() |> Array.map( fun var -> var.Length)
                                                                            let size = Array.fold( fun acc num -> acc + num ) (int64 0) verSizes
                                                                            item.FullName + "#" + size.ToString() ))
            x.StorageList.Add(pack)
        ()
    
    member x.GetAssemblyStatus() =
        x.AssemblyList.Clear()
        let dir = DirectoryInfo(DeploymentSettings.AssemblyFolder)          // gets assembly files
        let AssemblyFiles = dir.GetFiles()
        for file in AssemblyFiles do
            let mutable info = AssemblyPack() 
            info.Name <- file.Name
            info.size <- file.Length
            info.date <- file.LastWriteTime
            x.AssemblyList.Add(info)
        ()

    member x.GetDependencyStatus() =
        x.DependencyList.Clear()
        let dir = DirectoryInfo(DeploymentSettings.JobDependencyFolder)         // gets dependenct files
        let DependencyFiles = dir.GetFiles()
        for file in DependencyFiles do
            let mutable info = DependencyPack() 
            info.Name <- file.Name
            info.size <- file.Length
            info.date <- file.LastWriteTime
            x.DependencyList.Add(info)
        ()

    

    member x.GetClusterStatus() =                                                   // gets cluster files and meta files for validation
        x.metadict.Clear()
        x.clusterFiles.Clear()
       // dictionary dsetver and meta file
        for item in x.StorageList do
            for vers in item.vers do
                let path = (vers.Substring(3).Split('#')).[0]

                let collection =
                    DeploymentSettings.StorageDrives
                    |> Array.collect ( fun d ->
                                           try
                                               let dirs = (Directory.GetFiles(Path.Combine(d, path) ,"DSet*.meta"))
                                               dirs
                                           with
                                               |_ -> [||]
                                     )

                if(collection.Length > 1) then
                    let metafile = ( collection |> Array.sort )
                    let file = metafile.[metafile.Length - 1] // get metafile which is lexographically greatest
                    let stream = File.ReadAllBytes(file)
               // let dict = Dictionary<String,byte[]>
                    x.metadict.Add(path,stream)

        Directory.GetFiles(ClusterInfo.ClusterInfoFolder(),"*.inf") |> Array.map ( fun item -> (x.clusterFiles.Add((item.Substring(18),File.ReadAllBytes(item))))) |> ignore
                       
       //collection of cluster files
    member x.GetStatus(reqInfoType : ClientInfoType) =
        if (reqInfoType.HasFlag(ClientInfoType.DSet)) then
            x.GetStorageStatus()
        else if (reqInfoType.HasFlag(ClientInfoType.Assembly)) then
            x.GetAssemblyStatus()
        else if (reqInfoType.HasFlag(ClientInfoType.Dependency)) then
            x.GetDependencyStatus()
        else if (reqInfoType.HasFlag(ClientInfoType.Cluster)) then
            x.GetClusterStatus()

    /// Packet status information to send to controller
    member x.Pack(reqInfoType : ClientInfoType) =
        use ms = new MemStream()
        if (reqInfoType.HasFlag(ClientInfoType.DSet)) then
           ms.WriteInt32(x.StorageList.Count)
           ms.Serialize(x.StorageList)
        elif (reqInfoType.HasFlag(ClientInfoType.Assembly)) then
           ms.WriteInt32(x.AssemblyList.Count)
           ms.Serialize(x.AssemblyList)
        elif (reqInfoType.HasFlag(ClientInfoType.Dependency)) then
           ms.WriteInt32(x.DependencyList.Count)
           ms.Serialize(x.DependencyList)
        elif (reqInfoType.HasFlag(ClientInfoType.Cluster)) then
           ms.WriteInt32(x.metadict.Count)
           ms.Serialize(x.metadict)
           ms.WriteInt32(x.clusterFiles.Count)
           ms.Serialize(x.clusterFiles)
        let outParam = ms.GetBuffer()
        outParam

    // code running on controller ====
    /// Compute which information needs to be sent by client nodes?
    member x.RefreshNeeded(clientInfoDesired : ClientInfoType) =
        clientInfoDesired &&& x.ClientRefresh

    /// Unpack status information for display by controller
    member x.UpdateInfo(reqInfoType : ClientInfoType, buf : byte[]) =
        use ms = new MemStream(buf)
        if (reqInfoType.HasFlag(ClientInfoType.DSet)) then
            x.StorageList.Clear()
            let num = ms.ReadInt32()
            let packs = (ms.Deserialize() :?> List<StoragePack>)
            for file in packs do
                let info = StoragePack() 
                info.Name <- file.Name
                info.date <- file.date 
                info.vers <- file.vers
                x.StorageList.Add(info) 
            x.ClientRefresh <- x.ClientRefresh &&& (~~~ClientInfoType.DSet)
        elif(reqInfoType.HasFlag(ClientInfoType.Assembly)) then
            x.AssemblyList.Clear()
            let num = ms.ReadInt32()
            let packs = (ms.Deserialize() :?> List<AssemblyPack>)
            for file in packs do
                let info = AssemblyPack() 
                info.Name <- file.Name
                info.size <- file.size
                info.date <- file.date 
                x.AssemblyList.Add(info) 
                x.ClientRefresh <- x.ClientRefresh &&& (~~~ClientInfoType.Assembly)
        elif(reqInfoType.HasFlag(ClientInfoType.Dependency)) then
            x.DependencyList.Clear()
            let num = ms.ReadInt32()
            let packs = (ms.Deserialize() :?> List<DependencyPack>)
            for file in packs do
                let info = DependencyPack() 
                info.Name <- file.Name
                info.size <- file.size
                info.date <- file.date 
                x.DependencyList.Add(info) 
                x.ClientRefresh <- x.ClientRefresh &&& (~~~ClientInfoType.Dependency)
        elif (reqInfoType.HasFlag(ClientInfoType.Cluster)) then
           x.clusterFiles.Clear()
           x.metadict.Clear()
           let metanum = ms.ReadInt32()
           let metafileDict = ms.Deserialize() :?> Dictionary<String,byte[]>
           x.metadict <- metafileDict   //NOT SURE !!!!!!!!!!!!!!!!!!!!one
           let clusternum = ms.ReadInt32()
           let clusterFileList =  ms.Deserialize() :?> Dictionary<String,byte[]>
           for file in clusterFileList do
                x.clusterFiles.Add(file.Key,file.Value)
           x.ClientRefresh <- x.ClientRefresh &&& (~~~ClientInfoType.Cluster)

        else
            ()

//following delete functions get their targets via x.ReqOperation(). Target is decoded to be a version or full entity and appropriate action is taken
    member x.DelAssembly (target:string) =
        if (target.Chars(0) <> '*' ) then
            if (target.Contains("_")) then
                let path = Path.Combine(DeploymentSettings.AssemblyFolder, target) 
                let fi = FileInfo(path)
                fi.Delete()
            else
                let dirc = DirectoryInfo(DeploymentSettings.AssemblyFolder)
                let targetList = dirc.GetFiles("*.*", SearchOption.AllDirectories) |> Array.filter ( fun elem -> Regex.IsMatch( elem.Name, target + "_" + "*"))
                for mem in targetList do
                    mem.Delete()
        else
            let date = Convert.ToDateTime(target.Split('*').[1])
            let dirc = DirectoryInfo(DeploymentSettings.AssemblyFolder)
            let targetList = dirc.GetFiles("*.*", SearchOption.AllDirectories) |> Array.filter ( fun elem -> (elem.LastWriteTime.CompareTo(date)<0))
            for mem in targetList do
                    mem.Delete()

    member x.DelDependency (target:string) =
        if (target.Chars(0) <> '*' ) then
            let dirc = DirectoryInfo(DeploymentSettings.JobDependencyFolder)
            if (target.Contains("_")) then
                let files = dirc.GetFiles(target,SearchOption.AllDirectories)
                for file in files do
                    file.Delete()
            else
                let targetList = dirc.GetFiles("*.*", SearchOption.AllDirectories) |> Array.filter ( fun elem -> Regex.IsMatch( elem.Name, target + "_" + "*"))
                for mem in targetList do
                    mem.Delete()
        else
            let date = Convert.ToDateTime(target.Split('*').[1])
            let dirc = DirectoryInfo(DeploymentSettings.JobDependencyFolder)
            let targetList = dirc.GetFiles("*.*", SearchOption.AllDirectories) |> Array.filter ( fun elem -> (elem.LastWriteTime.CompareTo(date)<0))
            for mem in targetList do
                    mem.Delete()    

    member x.DelDSet (target: string) =

        let drives = DeploymentSettings.StorageDrives

        if(target.Chars(0) <> '*' ) then
            drives |> Array.iter (fun d -> 
                                   try 
                                     let fullTarget = DirectoryInfo(Path.Combine(d, target))
                                     fullTarget.Delete(true)
                                   with
                                     |_ -> ()
                                 )
        else
            let date = Convert.ToDateTime(target.Split('*').[1])

            drives |> Array.iter ( fun d -> 
                                    try
                                        let dir = DirectoryInfo(Path.Combine(d, DeploymentSettings.DataFolder))
                                        let targets = dir.GetDirectories("*.*", SearchOption.AllDirectories) |> Array.filter ( fun item -> item.LastWriteTime.CompareTo(date)<0) 
                                        targets |> Array.iter ( fun item -> item.Delete(true) ) |>ignore
                                    with
                                        |_ -> ()
                               )

    interface IDisposable with
        member x.Dispose() = 
            x.SendEvent.Dispose()
            GC.SuppressFinalize(x)


/// Networking between Client Nodes and Controller
type internal ClientController() =
    static member val ClientInfoDesired = ClientInfoType.Basic with get, set
    static member val ListOfClients = SortedDictionary<string, ClientStatusEx>() with get

    // lightweight "thread", should only contain non-blocking or async operations so as to not block thread in thread-pool
    // note: Begin** are non-blocling, and Async.** are all async
    static member ConnectToController(x : HomeInClient, controlPort : int) = async {
        let connected = Dictionary<string*int, ClientStatusEx>()
        let name = x.Config.MachineName
        //while (connected.Count <> x.ReportingServers.Count) do
        while (true) do
            for pair in x.ReportingServers do
                let servername, port = pair
                let bNew = ref false
                if not (connected.ContainsKey(servername, port)) then
                    connected.Add((servername, port), new ClientStatusEx())
                    bNew := true
                else
                    let statusEx = connected.[(servername, port)]
                    lock (statusEx) (fun() ->
                        if not statusEx.Connected then
                            connected.Remove((servername, port)) |> ignore
                            connected.Add((servername, port), new ClientStatusEx())
                            bNew := true
                    )
                if (!bNew) then
                    let statusEx = connected.[(servername, port)]
                    let ipv4Addr = NetworkSocket.GetIpv4Addr(servername)
                    try
                        let tcpClient = new TcpClient( AddressFamily.InterNetwork )
                        tcpClient.Connect(ipv4Addr, controlPort)
                        use socket = new NetworkSocket(tcpClient)
                        statusEx.Connected <- true
                        statusEx.ConnectionTo <- servername+":"+controlPort.ToString()
                        do! ClientController.ClientControllerRecv(socket, statusEx)
                        do! ClientController.ClientControllerSend(socket, statusEx)
                        statusEx.StartReq(name)
                    with
                        e -> ()
            do! Async.Sleep(5000) // without do! (similar to "await" keyword, this won't do anything)
    }

    static member ControllerHomeIn( o: Object ) = async {
        try
            let tcpClient = o :?> TcpClient
            use socket = new NetworkSocket( tcpClient )
            try
                let rcvdInfo = socket.Rcvd()
                use ms = new MemStream(rcvdInfo)
                let o = ms.Deserialize()
                //let o = Deserialize rcvdInfo
                match o with
                | :? ControllerCommand as cmd -> 
                    match (cmd.Verb, cmd.Noun) with 
                    | ( ControllerVerb.Get, ControllerNoun.ClusterInfo ) ->
                        if HomeInServer.CurrentCluster.Version = DateTime.MinValue then 
                            HomeInServer.TakeSnapShot( DateTime.Now )
                        let sendInfo = HomeInServer.CurrentCluster.PackToBytes()
                        socket.Send( sendInfo )
                    | ( ControllerVerb.Report, ControllerNoun.ClientInfo ) ->
                        let name = ms.ReadString()
                        Logger.Log( LogLevel.Info, (sprintf "Recv client controller connection request from %s at %s" name (tcpClient.Client.RemoteEndPoint.ToString())))
                        lock (ClientController.ListOfClients) (fun() ->
                            if not (ClientController.ListOfClients.ContainsKey(name)) then
                                ClientController.ListOfClients.Add(name, new ClientStatusEx())
                            else
                                ClientController.ListOfClients.[name] <- new ClientStatusEx()
                        )
                        let statusEx = ClientController.ListOfClients.[name]
                        statusEx.ConnectionTo <- name+":"+tcpClient.Client.RemoteEndPoint.ToString()
                        statusEx.Connected <- true
                        do! ClientController.ClientControllerSend(socket, statusEx)
                        do! ClientController.ClientControllerRecv(socket, statusEx)
                        // make initial request for info
                        statusEx.ReqInfo(ClientController.ClientInfoDesired)
                    | _ ->
                        Logger.Log( LogLevel.Error, (sprintf "Error: unknown controller command %A" cmd ))
                | _ -> 
                    Logger.Log( LogLevel.Error, (sprintf "Error: unknown received info from cluster controller %A" rcvdInfo ))
            with
            | e -> Logger.Log( LogLevel.Info, sprintf "Error: fail to communicate with cluster controller %A" e  )
        with 
        | e -> Logger.Log( LogLevel.Info, sprintf "Exception in ControllerHomeIn() %A" e  )
    }

    static member StartController( o: Object ) =
        let x = o :?> HomeInServer
        let listener = TcpListener( x.IpAddress, x.cPort )
        listener.Start()
        Logger.Log( LogLevel.MildVerbose, (sprintf "Listening on port %d" x.cPort ))
        try
            while true do
                Logger.Log( LogLevel.MildVerbose, "Waiting for request ..." )
                let tcpClient = listener.AcceptTcpClient()
                Async.StartAsTask(ClientController.ControllerHomeIn(tcpClient)) |> ignore
        finally
            listener.Stop()
        ()

    // recv "thread"
    static member ClientControllerRecv(socket : NetworkSocket, statusEx : ClientStatusEx) = async {
        try
            while statusEx.Connected do
                let! (bClosed, rcvdBuf) = SocketEx.RcvdAsync(socket)
                if (bClosed) then
                    statusEx.Connected <- false
                else
                    statusEx.ParseRecv(rcvdBuf)
        with e ->
            statusEx.Connected <- false
            Logger.Log( LogLevel.Info, (sprintf "Connection to %s broken" statusEx.ConnectionTo))
    }

    // send "thread"
    static member ClientControllerSend(socket : NetworkSocket, statusEx : ClientStatusEx) = async {
        try
            while statusEx.Connected do
                let! ret = Async.AwaitWaitHandle(statusEx.SendEvent) // use async instead of blocking using WaitOne
                //statusEx.SendEvent.WaitOne() |> ignore
                let deQ = ref true
                let sendBuf = ref null
                while (!deQ) do
                    deQ := statusEx.SendQ.TryDequeue(sendBuf)
                    if (!deQ) then
                        let! bClosed = SocketEx.SendAsync(socket, !sendBuf)
                        if (bClosed) then
                            statusEx.Connected <- false
        with e ->
            statusEx.Connected <- false
            Logger.Log( LogLevel.Info, (sprintf "Connection to %s broken" statusEx.ConnectionTo))
    }

    /// Set DSet refresh
    static member SetRefreshDSet() =
        ClientController.ClientInfoDesired <- ClientController.ClientInfoDesired ||| ClientInfoType.DSet
        lock (ClientController.ListOfClients) (fun() ->
            for client in ClientController.ListOfClients do
                let statusEx = client.Value
                statusEx.ClientRefresh <- statusEx.ClientRefresh ||| ClientInfoType.DSet
                // make a request for DSet Info
                if (statusEx.Connected) then
                    statusEx.ReqInfo(ClientInfoType.DSet)
        )
    /// UnSet DSet refresh
    static member UnSetRefreshDSet() =
        ClientController.ClientInfoDesired <- ClientController.ClientInfoDesired &&& (~~~ClientInfoType.DSet)
        lock (ClientController.ListOfClients) (fun() ->
            for client in ClientController.ListOfClients do
                let statusEx = client.Value
                statusEx.ClientRefresh <- statusEx.ClientRefresh &&& (~~~ClientInfoType.DSet)
        )

    static member SetDSetOperations(act:string)(target:string) =             // currently supports "D" (delete)
        match act with 
            |"D" ->
                    lock (ClientController.ListOfClients) (fun() ->
                        for client in ClientController.ListOfClients do
                            let statusEx = client.Value
                            //req for delete
                            if (statusEx.Connected) then
                                statusEx.ReqOperation (ControllerNoun.DSet)(target)
                    )
            |_ -> 
                    ()


    static member SetValidateDSet() =
        ClientController.ClientInfoDesired <- ClientController.ClientInfoDesired ||| ClientInfoType.DSet
        lock (ClientController.ListOfClients) (fun() ->
            for client in ClientController.ListOfClients do
                let statusEx = client.Value
                if (statusEx.Connected) then
                    statusEx.ReqInfo(ClientInfoType.Cluster)
        )
        ()

//****************************************** Assembly *****************************************************
    /// Set assembly refresh
    static member SetRefreshAssembly() =
        ClientController.ClientInfoDesired <- ClientController.ClientInfoDesired ||| ClientInfoType.Assembly
        lock (ClientController.ListOfClients) (fun() ->
            for client in ClientController.ListOfClients do
                let statusEx = client.Value
                statusEx.ClientRefresh <- statusEx.ClientRefresh ||| ClientInfoType.Assembly
                // make a request for assembly Info
                if (statusEx.Connected) then
                    statusEx.ReqInfo(ClientInfoType.Assembly)
        )
    /// UnSet assembly refresh
    static member UnSetRefreshAssembly() =
        ClientController.ClientInfoDesired <- ClientController.ClientInfoDesired &&& (~~~ClientInfoType.Assembly)
        lock (ClientController.ListOfClients) (fun() ->
            for client in ClientController.ListOfClients do
                let statusEx = client.Value
                statusEx.ClientRefresh <- statusEx.ClientRefresh &&& (~~~ClientInfoType.Assembly)
        )

    static member SetAssemblyOperations (act:string) (target:string) =
        match act with 
            |"D" ->
                    lock (ClientController.ListOfClients) (fun() ->
                        for client in ClientController.ListOfClients do
                            let statusEx = client.Value
                            //req for delete
                            if (statusEx.Connected) then
                                statusEx.ReqOperation (ControllerNoun.Assembly) (target)
                    )
            | _ -> ()

        
//**************************************************Dependency View****************************************************************

    /// Set dep refresh
    static member SetRefreshDependency() =
        ClientController.ClientInfoDesired <- ClientController.ClientInfoDesired ||| ClientInfoType.Dependency
        lock (ClientController.ListOfClients) (fun() ->
            for client in ClientController.ListOfClients do
                let statusEx = client.Value
                statusEx.ClientRefresh <- statusEx.ClientRefresh ||| ClientInfoType.Dependency
                // make a request for assembly Info
                if (statusEx.Connected) then
                    statusEx.ReqInfo(ClientInfoType.Dependency)
        )
    /// UnSet dep refresh
    static member UnSetRefreshDependency() =
        ClientController.ClientInfoDesired <- ClientController.ClientInfoDesired &&& (~~~ClientInfoType.Dependency)
        lock (ClientController.ListOfClients) (fun() ->
            for client in ClientController.ListOfClients do
                let statusEx = client.Value
                statusEx.ClientRefresh <- statusEx.ClientRefresh &&& (~~~ClientInfoType.Dependency)
        )

    static member SetDependencyOperations (act:string) (target:string) =
        match act with 
            |"D" ->
                    lock (ClientController.ListOfClients) (fun() ->
                        for client in ClientController.ListOfClients do
                            let statusEx = client.Value
                            //req for delete
                            if (statusEx.Connected) then
                                statusEx.ReqOperation (ControllerNoun.Dependency)(target)
                    )
            |_ -> 
                    ()

