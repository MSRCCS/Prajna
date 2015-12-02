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
    Date:
        July. 2013
        Split from network.fs on Dec. 2014.
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Collections.Concurrent
open System.Collections.Generic
open System.IO
open System.Net
open System.Net.Sockets
open System.Threading
open System.Threading.Tasks
open System.Runtime.Serialization
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Tools.FileTools
open Prajna.Tools.BytesTools
open Prajna.Tools.Network

// ===================================================================================
// following code is only used by clusterinfo.fs for reporting to PrajnaController and is
// not part of the core networking code
type internal NetworkSocket(socket:TcpClient) =
    let mutable bShutdownCalled = false 
    let mutable bCloseCalled = false
    member val NetStream = socket.GetStream() with get
    member val Socket = socket with get
    member val CurSendInfo: byte[] = null with get, set
    member val CurRcvdInfo: byte[] = null with get, set
    member x.Shutdown with get() = bShutdownCalled
    member x.SendRaw( sendBuf ) =
        if not bShutdownCalled then 
            x.CurSendInfo <- sendBuf
            x.NetStream.Write( x.CurSendInfo, 0, x.CurSendInfo.Length)
    member x.SendRawString( s:string ) =
        if not bShutdownCalled then 
            x.SendRaw( System.Text.Encoding.ASCII.GetBytes( s ) )
    member x.SendRawAsync( sendBuf:byte[] ) =
        x.NetStream.WriteAsync( sendBuf, 0, sendBuf.Length)
    member x.SendRawStringAsync( s:string ) =
        x.SendRawAsync( System.Text.Encoding.ASCII.GetBytes( s ) )
    member x.Send( sendBuf, offset, count ) =
        if not bShutdownCalled then 
            x.CurSendInfo <- sendBuf
            let sendL = IPAddress.HostToNetworkOrder( sendBuf.Length )
            let bytes = BitConverter.GetBytes( sendL )
            x.NetStream.Write( bytes, 0, bytes.Length)
            x.NetStream.Write( x.CurSendInfo, offset, count)
    member x.Send( sendBuf ) =
        x.Send(sendBuf, 0, sendBuf.Length)
    member x.SendString( s:string ) =
        x.Send( System.Text.Encoding.ASCII.GetBytes( s ) )
    member x.RcvdRaw(len) =
        if not bShutdownCalled then 
            let tmpbuf = Array.zeroCreate<byte> len
            let rcvdSize = x.NetStream.Read( tmpbuf, 0, tmpbuf.Length)
            let rcvd = Array.zeroCreate<byte> rcvdSize
            Buffer.BlockCopy( tmpbuf, 0, rcvd, 0, rcvdSize)
            x.CurRcvdInfo <- rcvd
            rcvd
        else
            null
    member x.RcvdRawString( len ) =
        let rcvd = x.RcvdRaw( len )
        if Utils.IsNull rcvd then 
            ""
        else
            System.Text.ASCIIEncoding.ASCII.GetString( rcvd )
    member x.Rcvd( len ) = 
        let rcvd = Array.zeroCreate<byte> len
        x.CurRcvdInfo <- rcvd
        if len > 0 then
            let mutable offset = 0
            while offset < len && x.Socket.Connected && not bShutdownCalled do
                let add_read = x.NetStream.Read( rcvd, offset, len-offset )
                offset <- offset + add_read 
            if offset < len then
                let newrcvd = Array.zeroCreate<byte> offset
                Buffer.BlockCopy( rcvd, 0, newrcvd, 0, offset)
                x.CurRcvdInfo <- newrcvd
                newrcvd 
            else
                rcvd
        else
            rcvd
    member x.Rcvd() =
        let rcvdBuffer = x.Rcvd(4)
        if ( rcvdBuffer.Length<4 ) then 
            Array.zeroCreate<byte> 0
        else
            let rcvdL = BitConverter.ToInt32( rcvdBuffer, 0 )
            let len = IPAddress.NetworkToHostOrder( rcvdL )
            x.Rcvd(len)
    member x.RcvdString() =
        let rcvd = x.Rcvd()
        System.Text.ASCIIEncoding.ASCII.GetString( rcvd )
    static member GetIpv4Addr( name:string ) = 
        LocalDNS.GetAnyIPAddress( name )

    /// This function is used to replace new TcpClient(name, port) 
    /// to use IPv4 address, hopefully to speed up things. 
    static member GetTCPClient( name:string, port ) = 
        let ipv4Addr = NetworkSocket.GetIpv4Addr( name )
        let tcpClient = new TcpClient( AddressFamily.InterNetwork )
        tcpClient.Connect( ipv4Addr, port )
        tcpClient
    member x.Close() = 
        bShutdownCalled <- true
        if not bCloseCalled then 
            bCloseCalled <- true    
            x.NetStream.Close()
            x.Socket.Close()
    override x.Finalize() =
        x.Close()
    interface IDisposable with
        member x.Dispose() = 
            x.Close()
            x.NetStream.Dispose()
            (x.Socket :> IDisposable).Dispose()
            GC.SuppressFinalize(x)

[<Serializable>]
type internal ClientStatus() =
    member val internal CurConfig = DetailedConfig() with get, set
    member val CurDrive = "" with get, set
    member val Name = "" with get, set
    member val InternalPort = 0 with get, set
    member val JobPortMin = 0 with get, set
    member val JobPortMax = 0 with get, set
    member val InternalIPs = null with get, set
    member val ClientVersionInfo = "" with get, set
    member val LastHomeIn = (PerfDateTime.UtcNow()) with get, set
    member val ProcessorCount = 0 with get, set
    member val MemorySpace = 0 with get, set
    member val DriveSpace = 0 with get, set
    member val MAC = "" with get, set
    member val MachineID = 0UL with get, set
    member val RamCapacity = 0 with get, set
    member val DiskCapacity = 0 with get, set
    member val NetworkSpeed = 0L with get, set
    member val NetworkMtu = 0 with get, set
    member val Selected = true with get, set
    member val CpuCounter =
        new System.Diagnostics.PerformanceCounter("Process", "% Processor Time", "_Total" )
    member val RamCounter =
        new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
    member val CpuUsage = "" with get, set
    member x.GetCpuUsageByCounter() =
        use cpuCounter = new System.Diagnostics.PerformanceCounter("Process", "% Processor Time", "_Total" )
        cpuCounter.NextValue() |> ignore
        Threading.Thread.Sleep(200)
        let readOut = cpuCounter.NextValue()
        let avgCpuUsage = readOut / (float32 (x.ProcessorCount))
        x.CpuUsage <- avgCpuUsage.ToString("F2")
        x.CpuUsage
    member x.GetCpuUsage() =
        use processor = new Management.ManagementObject("Win32_PerfFormattedData_PerfOS_Processor.Name='_Total'");
        processor.Get()
        let wmi = processor.GetPropertyValue( "PercentProcessorTime" ) :?> UInt64 
        x.CpuUsage <- wmi.ToString()
        x.CpuUsage
    member x.GetRamUsage() =
        use ramCounter = new System.Diagnostics.PerformanceCounter("Memory", "Available MBytes")
        x.MemorySpace <- (int (ramCounter.NextValue()))
        x.MemorySpace
    member x.GetRamCapacity() = 
        x.RamCapacity <- int (x.CurConfig.MemorySpace / 1024UL / 1024UL )
        x.RamCapacity
    member x.GetDiskCapactiy() = 
        x.DiskCapacity <- int (x.CurConfig.DriveCapacity( "" ) / 1024L / 1024L / 1024L)
        x.DiskCapacity
    member x.GetDriveUsage() =
        x.DriveSpace <- int (x.CurConfig.DriveSpace( "" ) / 1024L / 1024L / 1024L)
        x.DriveSpace
    member x.GetMachineID() = 
        x.MachineID <- x.CurConfig.MachineID
        x.MachineID
    member x.GetNetworkSpeed() = 
        x.NetworkSpeed <- x.CurConfig.NetworkSpeed()
        x.NetworkSpeed
    member x.GetNetworkMTU() = 
        x.NetworkMtu <- x.CurConfig.NetworkMTU()
        x.NetworkMtu        
    override x.ToString() =
        sprintf "%s\t%s\t%d\t%d\t%d\t%x\t%s\t%d\t%d\t%d\t%d" x.Name x.ClientVersionInfo x.ProcessorCount (x.GetRamUsage()) (x.GetDriveUsage()) (x.GetMachineID()) (x.GetCpuUsage()) (x.GetRamCapacity()) (x.GetDiskCapactiy()) (x.GetNetworkMTU()) (x.GetNetworkSpeed())
    static member val LocalIPs = 
        let seqLocalIP = 
//            seq {
//                let host = Dns.GetHostEntry( Dns.GetHostName() )
//                for ip in host.AddressList do 
//                    if ip.AddressFamily = System.Net.Sockets.AddressFamily.InterNetwork then 
//                        yield ip.GetAddressBytes()
//                }
            seq {
                for nic in System.Net.NetworkInformation.NetworkInterface.GetAllNetworkInterfaces() do
                    let ipProps = nic.GetIPProperties()
//                    printfn "Number of gateays: %d" ipProps.GatewayAddresses.Count
                    for addr in ipProps.UnicastAddresses do
                        if addr.Address.AddressFamily = System.Net.Sockets.AddressFamily.InterNetwork && not (IPAddress.IsLoopback(addr.Address)) && ipProps.GatewayAddresses.Count>0 then 
                            yield addr.Address.GetAddressBytes()    
            }
        seqLocalIP |> Seq.toArray
    static member internal Create( config: DetailedConfig, vInfo, drive ) =
        new ClientStatus( Name = config.MachineName, 
            InternalPort = DeploymentSettings.ClientPort,
            JobPortMin = DeploymentSettings.JobPortMin, 
            JobPortMax = DeploymentSettings.JobPortMax, 
            InternalIPs = ClientStatus.LocalIPs,          
            CurDrive = drive, 
            ClientVersionInfo = vInfo,
            ProcessorCount = config.ProcessorCount, 
            MemorySpace = (int (config.MemorySpace / 1024UL / 1024UL )), 
            DriveSpace = int (config.DriveSpace( "" ) / 1024L / 1024L / 1024L), 
            MAC = config.MAC.Item(0).ToString() )
    static member Parse(s:string) =
        let x = ClientStatus()
        try
            if s.Length>0 then 
                let arr = s.Split('\t')
                if arr.Length > 0 then x.Name <- arr.[0]
                if arr.Length > 1 then x.ClientVersionInfo <- arr.[1]
                if arr.Length > 2 then x.ProcessorCount <- Int32.Parse( arr.[2] )
                if arr.Length > 3 then x.MemorySpace <- Int32.Parse( arr.[3] )
                if arr.Length > 4 then x.DriveSpace <- Int32.Parse( arr.[4] )
                if arr.Length > 5 then x.MachineID <- UInt64.Parse( arr.[5], Globalization.NumberStyles.HexNumber )
                if arr.Length > 6 then x.CpuUsage <- arr.[6]
                if arr.Length > 7 then x.RamCapacity <- Int32.Parse( arr.[7] ) else x.RamCapacity <- x.MemorySpace
                if arr.Length > 8 then x.DiskCapacity <- Int32.Parse( arr.[8] ) else x.DiskCapacity <- x.DriveSpace
                if arr.Length > 9 then x.NetworkMtu <- Int32.Parse( arr.[9] ) 
                if arr.Length > 10 then x.NetworkSpeed <- Int64.Parse( arr.[10] ) 
        with
        | _ -> ()
        x
    member x.Pack(ms:MemStream) = 
        ms.WriteString( x.Name )
        ms.WriteString( x.ClientVersionInfo )
        ms.WriteVInt32( x.ProcessorCount ) 
        ms.WriteInt32( x.MemorySpace ) 
        ms.WriteInt32( x.DriveSpace ) 
        ms.WriteUInt64( x.MachineID ) 
        ms.WriteString( x.CpuUsage ) 
        ms.WriteInt32( x.RamCapacity ) 
        ms.WriteInt32( x.DiskCapacity ) 
        ms.WriteInt32( x.NetworkMtu ) 
        ms.WriteInt64( x.NetworkSpeed ) 
        ms.WriteUInt16( uint16 x.InternalPort )
        ms.WriteUInt16( uint16 x.JobPortMin )
        ms.WriteUInt16( uint16 x.JobPortMax )
        ms.WriteVInt32( x.InternalIPs.Length ) 
        for i = 0 to x.InternalIPs.Length - 1 do 
            ms.WriteBytesWLen( x.InternalIPs.[i] )
        // # of external port: 0
        ms.WriteVInt32( 0 ) 
    static member Unpack( ms:MemStream ) = 
        let x = ClientStatus()
        x.Name <- ms.ReadString(  )
        x.ClientVersionInfo <- ms.ReadString(  )
        x.ProcessorCount <- ms.ReadVInt32(  ) 
        x.MemorySpace <- ms.ReadInt32(  ) 
        x.DriveSpace <- ms.ReadInt32(  ) 
        x.MachineID <- ms.ReadUInt64(  ) 
        x.CpuUsage <- ms.ReadString(  ) 
        x.RamCapacity <- ms.ReadInt32(  ) 
        x.DiskCapacity <- ms.ReadInt32(  ) 
        x.NetworkMtu <- ms.ReadInt32(  ) 
        x.NetworkSpeed <- ms.ReadInt64(  ) 
        x.InternalPort <- int (ms.ReadUInt16())
        x.JobPortMin <- int ( ms.ReadUInt16())
        x.JobPortMax <- int ( ms.ReadUInt16())
        let len = ms.ReadVInt32()  
        x.InternalIPs <- Array.zeroCreate<_> len
        for i = 0 to len - 1 do 
            x.InternalIPs.[i] <- ms.ReadBytesWLen()
        let lenExternal = ms.ReadVInt32() 
        x

/// Socket communication module of the PrajnaClient & Controller
[<AllowNullLiteral>]
type internal HomeInClient( versionInfo, datadrive, reportingServer:List<string*int>, ?masterName, ?port ) = 
    member val Updated = false with get, set
    member val DeployFolder = "" with get, set
    member val DeployExe = "" with get, set
    member val MainExe = "" with get, set
    member val VersionInfo = versionInfo with get
    member val Datadrive = datadrive with get
    member val SendInfo:string = "" with get, set
    member val RcvdInfo:string = "" with get, set
    member val internal Config = DetailedConfig() with get, set
    member val PrajnaDir = "" with get, set
    member val MasterName = defaultArg masterName "" with get, set
    member val Port = defaultArg port 1080 with get, set
    member val ReportingServers = reportingServer with get, set
    member val ExecutingHomeIn = false with get, set
    member val Shutdown = false with get, set

    static member HomeInClient( s: Object ) =
        let x = s :?> HomeInClient

        // Otherwise, other thread is already executing home in, no need to do again. 
        let config = DetailedConfig()
        let clientStatus = ClientStatus.Create( config, x.VersionInfo, x.Datadrive)
        x.Config <- config

        let homeInCall servername port bUpdate = 
            async {
                let t1 = ref DateTime.MinValue
                while not x.Shutdown do 
                    let t2 = (PerfDateTime.UtcNow())
                    let elapse = t2 - !t1
                    let elapseMS = elapse.TotalMilliseconds
                    if elapseMS <= 10000. && elapseMS>=0. then 
//                        do! Async.Sleep( 10000 - int elapseMS ) 
                        Threading.Thread.Sleep( 10000 - int elapseMS ) 
                        t1 := (PerfDateTime.UtcNow())
                    else
                        t1 := t2
                    try                       
                        let ipv4Addr = NetworkSocket.GetIpv4Addr( servername )
                        use tcpClient = new TcpClient( AddressFamily.InterNetwork )
                        do! tcpClient.ConnectAsync( ipv4Addr, port ) |> Async.AwaitIAsyncResult |> Async.Ignore
                        if tcpClient.Connected then 
                            use socket = new NetworkSocket(tcpClient)
                            let sendInfo = clientStatus.ToString()
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Home in server at %s:%d with %s" servername port sendInfo))
                            use sendStream = new MemStream( 1024 )
                            clientStatus.Pack(sendStream)
                            let _, strmPos, strmCnt = sendStream.GetBufferPosLength()
                            socket.Send(sendStream.GetBuffer(), strmPos, strmCnt)
                            try
                                let readstr = socket.RcvdString()
                                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "From server at %s:%d, returned %s" servername port readstr ) )
                                if bUpdate then 
                                    let st = x.RcvdInfo.Split("\t".ToCharArray(), StringSplitOptions.RemoveEmptyEntries )
                                    if st.Length>0 then x.Updated <- ( String.Compare( st.[0], x.VersionInfo, true )<>0 )
                                    if st.Length>1 then x.DeployFolder <- st.[1]
                                    if st.Length>2 then x.DeployExe <- st.[2]
                                    if st.Length>3 then x.MainExe <- st.[3]
                            with
                            | e ->
                                // Failing in read is OK. 
                                //let readstr = System.Text.ASCIIEncoding.ASCII.GetString( bytebuf )
                                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Exception read from server at %s:%d" servername port) )
                                ()
                        else
                            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Home in server at %s:%d failed." servername port ))
                    with
                    | e ->
                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Home in server at %s:%d failed with exception %A " servername port e ))
            }
        for pair in x.ReportingServers do
            let servername, port = pair
            let bUpdate = ( servername=x.MasterName)
            Async.Start( homeInCall servername port bUpdate ) 

    member val CurrentReportingClient = ref null with get
    member x.CloseAll() = 
        x.Shutdown <- true


type internal PartitionByKind =
    | Uniform = 0
    | NumberOfCores = 1
    | RAMCapacity = 2
    | DiskCapacity = 3
    | AvailableDiskSpace = 4

[<StructuralEquality;StructuralComparison>]
/// ClientInfo contains information of a Prajna remote node and its daemon. 
/// Information below is exposed as F# is not capable of suppress comment on internal/private structure member. 
type ClientInfo = 
    struct
        /// A number uniquely identify a machine
        val mutable internal MachineID: uint64
        /// Machine Host Name
        val mutable MachineName: string
        /// Machine Daemon Port
        val mutable MachinePort: int
        /// Machine Container Port range, minimum
        val mutable internal JobPortMin: int
        /// Machine Container Port range, maximum
        val mutable internal JobPortMax: int
        /// Machine Daemon Port
        val mutable internal JobPortCur: int
        /// Globally visibe IP addresses of the remote node
        val mutable internal ExternalIPAddress : byte[][]
        /// Internally visibe IP addresses of the remote node
        val mutable internal InternalIPAddress : byte[][]
        /// Number of cores of the remote node
        val mutable internal ProcessorCount: int
        /// RAM capacity of the remote node
        val mutable internal RamCapacity: int
        /// Disk capacity of the remote node
        val mutable internal DiskCapacity: int
        /// Usuable RAM of the remote node
        val mutable internal MemorySpace: int
        /// Usuable Disk of the remote node
        val mutable internal DriveSpace: int
        /// the maximum transmission unit (MTU) of networkk 
        val mutable internal NetworkMTU: int
        /// Network speed 
        val mutable internal NetworkSpeed: int64 
        /// type of SSD, if exist of the remote node
        val mutable internal SSDType: int
        /// Capacity of SSD of the remote node. 
        val mutable internal SSDCapacity : int64
        /// type of GPU, if exist of the remote node
        val mutable internal GPUType: int
        /// number of GPU cores of the remote node
        val mutable internal GPUCores : int 
        /// GPU memory capacity of the remote node
        val mutable internal GPUMemoryCapacity : int64
        internal new ( status: ClientStatus ) = {
            MachineID = status.MachineID;
            MachineName = status.Name;
            MachinePort = status.InternalPort; 
            JobPortMin = status.JobPortMin; 
            JobPortMax = status.JobPortMax;
            JobPortCur = 0; 
            ExternalIPAddress = null;
            InternalIPAddress = status.InternalIPs;
            ProcessorCount = status.ProcessorCount;
            RamCapacity = status.RamCapacity; 
            DiskCapacity = status.DiskCapacity;
            MemorySpace = status.MemorySpace; 
            DriveSpace = status.DriveSpace;
            NetworkMTU = status.NetworkMtu;
            NetworkSpeed = status.NetworkSpeed; 
            SSDType = 0;
            SSDCapacity = 0L; 
            GPUType = 0;
            GPUCores = 0; 
            GPUMemoryCapacity = 0L
            }
        internal new ( name: string ) = {
            MachineID = 0UL;
            MachineName = name;
            MachinePort = DeploymentSettings.ClientPort;
            JobPortMin = 0;
            JobPortMax = 0;
            JobPortCur = 0; 
            ExternalIPAddress = null;
            InternalIPAddress = null;
            ProcessorCount = 0;
            RamCapacity = 0; 
            DiskCapacity = 0;
            MemorySpace = 0; 
            DriveSpace = 0;
            NetworkMTU = 0;
            NetworkSpeed = 0L; 
            SSDType = 0;
            SSDCapacity = 0L; 
            GPUType = 0;
            GPUCores = 0; 
            GPUMemoryCapacity = 0L
            }
        override x.ToString() = 
            sprintf "%x\t%s:%d\t%d\t%d/%d\t%d/%d" x.MachineID x.MachineName x.MachinePort x.ProcessorCount x.MemorySpace x.RamCapacity x.DriveSpace x.DiskCapacity
        member internal x.Resource( partitionBy: PartitionByKind ) = 
            match partitionBy with
            | PartitionByKind.Uniform -> 1
            | PartitionByKind.NumberOfCores -> x.ProcessorCount
            | PartitionByKind.RAMCapacity -> x.RamCapacity
            | PartitionByKind.DiskCapacity -> x.DiskCapacity
            | PartitionByKind.AvailableDiskSpace -> x.DriveSpace
            | _ -> 1
        member internal x.ConvertToClientStatus() =
            ClientStatus( MachineID=x.MachineID, Name=x.MachineName, ProcessorCount=x.ProcessorCount, RamCapacity=x.RamCapacity,
                                DiskCapacity=x.DiskCapacity, MemorySpace=x.MemorySpace, DriveSpace=x.DriveSpace, NetworkMtu=x.NetworkMTU, NetworkSpeed=x.NetworkSpeed, LastHomeIn=(PerfDateTime.UtcNow()).Subtract( TimeSpan( 22, 22, 22) ) )
    end  

type internal ClusterType = 
    | StandAlone = 0                        //  StandAlone cluster, admin access is available to the running user
    | Azure = 1                             //  Azure Cluster
    | StandAloneNoAdminForExternal = 2      //  Admin access is not available to process in machine that runs external to the cluster 
    | StandAloneNoAdmin = 3                 //  Admin access is not available to the machine.
    | StandAloneNoSMB = 4                   //  Cluster with no SMB access

type internal ClusterReplicationType = 
    | DirectReplicate = 0
    | P2PReplicate = 1
    | ClusterReplicate = 2

[<Serializable; AllowNullLiteral>]
type internal ClusterInfoBase( clusterType, version, listOfClients ) = 
    member val ClusterType: ClusterType = clusterType with get, set
    member val Name = "Cluster" with get, set
    member val Version : DateTime = version with get, set
    member val ListOfClients : ClientInfo[] = listOfClients with get, set
    static member SetReplicationType( clType ) = 
        match clType with 
        | ClusterType.StandAlone -> 
            ClusterReplicationType.DirectReplicate
        | ClusterType.Azure -> 
            ClusterReplicationType.ClusterReplicate
        | _ ->
            let msg = sprintf "Undefined Cluster Type %A, can't resolve in SetReplicationType." clType
            Logger.Log( LogLevel.Error, msg )
            failwith msg        
    member val ReplicationType = ClusterInfoBase.SetReplicationType( clusterType ) with get, set
    new ( ) = 
        ClusterInfoBase( ClusterType.StandAlone, DateTime.MinValue, null )
    new ( y: ClusterInfoBase ) as cl =
        ClusterInfoBase( y.ClusterType, y.Version, y.ListOfClients )
        then
            cl.Name <- y.Name
    new ( y: ClusterInfoBase, nodeName: string ) as cl = 
        ClusterInfoBase( y.ClusterType, y.Version, y.ListOfClients |> Array.filter( fun cl -> String.Compare( cl.MachineName, nodeName, StringComparison.OrdinalIgnoreCase)=0 ) )
        then 
            cl.Name <- nodeName
    new ( y: ClusterInfoBase, idx:int ) as cl =
        ClusterInfoBase( y.ClusterType, y.Version, [| y.ListOfClients.[idx] |] )
        then 
            cl.Name <- y.ListOfClients.[idx].MachineName
        
    override x.ToString() = 
        sprintf "%A, %s:%s, with %d nodes" x.ClusterType x.Name (VersionToString(x.Version)) (if Utils.IsNull x.ListOfClients then 0 else x.ListOfClients.Length) // ( x.ListOfClients |> Array.map( fun info -> info.MachineName ) )


[<AllowNullLiteral>]
type internal ClusterInfo( b:  ClusterInfoBase ) = 
    inherit ClusterInfoBase( b )
    member val ControllerName = "" with get, set
    member val ControllerPort = 1081 with get, set
    member val RemoteStoragePath = null with get, set
    member val RemoteStorageFreeSize = null with get, set
    member val RemoteStorageCapacity = null with get, set
    member val GetRemoteStorageInfo = false with get, set
    new ( name, port ) as x = 
        ClusterInfo( new ClusterInfoBase( ) ) 
        then 
            x.ControllerName <- name
            x.ControllerPort <- port 
    new () = 
        ClusterInfo( new ClusterInfoBase( ) ) 
    new ( b: ClusterInfo, nodeName : string ) = 
        ClusterInfo( ClusterInfoBase( b, nodeName ) )
    new ( b: ClusterInfo, idx: int ) = 
        ClusterInfo( ClusterInfoBase( b, idx ))
    member x.IntializeRemoteStorageInfo() = 
        if Utils.IsNull x.RemoteStoragePath then 
            lock (x ) ( fun _ -> if Utils.IsNull x.RemoteStoragePath then 
                                    x.RemoteStoragePath <- Array.zeroCreate<_> x.ListOfClients.Length
                                    x.RemoteStorageCapacity <- Array.zeroCreate<_> x.ListOfClients.Length
                                    x.RemoteStorageFreeSize <- Array.zeroCreate<_> x.ListOfClients.Length
            )
    member x.SetRemoteStorageInfo( peeri, driveInfo: (string*uint64*uint64)[] ) =
        // let excludeCDrive = driveInfo |> Array.filter ( fun (dr, _, _) -> not (dr.ToUpper().Contains( "C:" )) )
        let excludeCDrive = driveInfo |> Array.filter ( fun (dr, _, _) -> DeploymentSettings.IsDataDrive(dr) )
        x.RemoteStoragePath.[peeri] <- excludeCDrive |> Array.mapi ( fun i (dr, _, _ ) -> """\\"""+x.ListOfClients.[peeri].MachineName+"""\"""+dr.Substring(0,1)+"""\""" )
        x.RemoteStorageFreeSize.[peeri] <- excludeCDrive |> Array.map ( fun ( _, free, _ ) -> free )
        x.RemoteStorageCapacity.[peeri] <- excludeCDrive |> Array.map ( fun ( _, _, capacity ) -> capacity )
    
    member x.SetRemoteStorageInfo( drivesInfo: (string*uint64*uint64)[][] ) = 
        x.IntializeRemoteStorageInfo()
        if drivesInfo.Length <> x.ListOfClients.Length then 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "SetRemoteStorageInfo: cluser has %d node, the storage information %A has %d elements" x.ListOfClients.Length drivesInfo drivesInfo.Length ))
        for i = 0 to x.ListOfClients.Length - 1 do 
            if drivesInfo.[i].Length > 0 then 
                // Do not set drive information for empty drive 
                x.SetRemoteStorageInfo( i, drivesInfo.[i] ) 

    member x.RemoteStorageInfoToString() = 
        seq {
            for i = 0 to x.ListOfClients.Length - 1 do 
                yield seq {
                    yield i.ToString()
                    let numDrives = if Utils.IsNull x.RemoteStoragePath.[i] then 0 else x.RemoteStoragePath.[i].Length
                    for j = 0 to numDrives - 1 do 
                        yield x.RemoteStoragePath.[i].[j] + "," + x.RemoteStorageFreeSize.[i].[j].ToString() + "," + x.RemoteStorageCapacity.[i].[j].ToString()
                } |> String.concat "\t"
        } |> String.concat Environment.NewLine
    member x.ParseRemoteStorageInfo( info:string ) = 
        let lines = info.Split( [| Environment.NewLine |], StringSplitOptions.RemoveEmptyEntries ) 
        if lines.Length <> x.ListOfClients.Length then 
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Failed to parse storage information, cluser has %d node, the storage information %s has %d lines" x.ListOfClients.Length info info.Length ))
        for i = 0 to lines.Length - 1 do 
            let drInfo = lines.[i].Split( [| '\t' |], StringSplitOptions.RemoveEmptyEntries ) 
            if drInfo.Length > 1 then 
                // it should have at least 2 columns to be a valid information. 
                let peeri = Int32.Parse ( drInfo.[0] )
                let driveInfo = Array.sub drInfo 1 ( drInfo.Length - 1 ) |> Array.choose ( fun drinfo -> let st = drinfo.Split( [| ',' |], StringSplitOptions.RemoveEmptyEntries ) 
                                                                                                         if Utils.IsNull st || st.Length<>3 then 
                                                                                                            None
                                                                                                         else
                                                                                                            Some ( st.[0], UInt64.Parse( st.[1] ), UInt64.Parse( st.[2] ) ) )
                x.SetRemoteStorageInfo( peeri, driveInfo ) 
    member x.SetAndSaveRemoteStorageInfo( drivesInfo ) =
        x.SetRemoteStorageInfo( drivesInfo ) 
        let remoteInfo = x.RemoteStorageInfoToString()
        let clFileName:string = ClusterInfo.ConstructClusterInfoFileNameWithVersion( x.Name, x.Version ) 
        let storageInfoName = clFileName.Replace( ".inf", ".storage" )
        FileTools.SaveToFile storageInfoName remoteInfo
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Save remote storage information for Cluster %s to %s" x.Name storageInfoName ))
        FileTools.MakeFileAccessible( storageInfoName )
    static member val readFileLock = Object() with get
    member x.LoadRemoteStorageInfo( ) = 
        x.IntializeRemoteStorageInfo() 
        let clFileName:string = ClusterInfo.ConstructClusterInfoFileNameWithVersion( x.Name, x.Version ) 
        let storageInfoName = clFileName.Replace( ".inf", ".storage" )
        if File.Exists( storageInfoName ) then 
            let infoString = lock (ClusterInfo.readFileLock) (fun () -> FileTools.ReadFromFile storageInfoName)
            x.ParseRemoteStorageInfo( infoString ) 
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Load remote storage information for Cluster %s \n %s" x.Name (x.RemoteStorageInfoToString()) ))
            true
        else
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "No remote storage information for Cluster %s" x.Name ))
            false
    member x.GetCurrentPeerIndex() = 
        let hostname = Dns.GetHostName()
        let peerIndex = ref -1 
        for i = 0 to x.ListOfClients.Length - 1 do 
            if String.Compare( hostname, x.ListOfClients.[i].MachineName, StringComparison.CurrentCultureIgnoreCase )=0 then 
                peerIndex := i
        !peerIndex
    member x.ClusterUpdate() =
        if x.ControllerName.Length>0 then 
            let command = ControllerCommand( ControllerVerb.Get, ControllerNoun.ClusterInfo )
            let sendInfo = Serialize command 256
            Logger.Log( LogLevel.MildVerbose, (sprintf "Retrieve Cluster Info at %s:%d with %A" x.ControllerName x.ControllerPort sendInfo))
            try
                use socket = new NetworkSocket( NetworkSocket.GetTCPClient( x.ControllerName, x.ControllerPort ) )
                socket.Send( sendInfo )
                let rcvdInfo = socket.Rcvd()
                let o = ClusterInfo.Unpack( rcvdInfo )
                match o with
                | Some ( clus ) ->
                    let cl = clus :> ClusterInfoBase
                    x.ClusterType <- ( cl.ClusterType )
                    x.Version <- ( cl.Version )
                    x.ListOfClients <- ( cl.ListOfClients )
                | None ->
                    Logger.Log( LogLevel.Error, (sprintf "Error: fail to parse cluster information returned by controller %A" rcvdInfo ))
            with
            | e -> Logger.Log( LogLevel.Error, sprintf "Error: fail to communicate with cluster controller %A" e  )
    member internal x.GetNodeIDsWithResource(usePartitionBy) =
        lock ( x ) ( 
            fun () ->
                let resourceArray = Array.zeroCreate<int> x.ListOfClients.Length
                let minResource = 
                    x.ListOfClients 
                    |> Array.fold ( fun minVal node -> Math.Min( minVal, node.Resource( usePartitionBy ) ) ) Int32.MaxValue

                for i = 0 to x.ListOfClients.Length-1 do
                    let node = x.ListOfClients.[i]
                    let resource = node.Resource( usePartitionBy ) / minResource
                    resourceArray.[i] <- resource
                ( x.ListOfClients, resourceArray )
            )
    static member ClusterInfoFolder () = 
        Path.Combine( DeploymentSettings.LocalFolder, DeploymentSettings.ClusterFolder )

    // find the latest cluster info file for cluster with name 'name'
    static member private FindLatestClusterInfoFile (name) =
        let folder = ClusterInfo.ClusterInfoFolder()
        if Directory.Exists folder then
            // The pattern is "ClusterName_Cluster_150917_045810.019677"
            let files = Directory.GetFiles(folder, name + "_Cluster_*_*.*.inf") |> Array.sort
            if not (Array.isEmpty files) then
                files.[files.Length - 1] |> Some
            else
                None
        else
            None

    /// ver = DateTime.MinValue, just use a shorter filename for ease of remembering (don't save ver into filename)s
    static member ConstructClusterInfoFileNameWithVersion( name, ver ) = 
        let folder = ClusterInfo.ClusterInfoFolder()
        DirectoryInfoCreateIfNotExists folder |> ignore
        let fname1 = 
            if ver<>DateTime.MinValue then 
                Path.Combine( folder, name + "_" + DeploymentSettings.ClusterInfoName( ver) )
            else
                match ClusterInfo.FindLatestClusterInfoFile(name) with
                | Some f -> f
                | None -> Path.Combine( folder, name + ".inf" )
        fname1

    /// Construct the name to save the cluster info
    member x.ConstructClusterInfoFilename() = 
        ClusterInfo.ConstructClusterInfoFileNameWithVersion( x.Name, x.Version ) 
    /// Construct the name to save the cluster list
    member x.ConstructLstFilename() = 
        Path.Combine( DeploymentSettings.LocalFolder, DeploymentSettings.ClusterFolder, DeploymentSettings.ClusterLstName(x.Name, x.Version) )
    /// Save the current ClusterInfoBase to file
    member x.SaveOld(name) = 
        use ms = new MemStream( 102400 )
        ms.Write( DeploymentSettings.ClusterInfoPlainGuid.ToByteArray(), 0, 16 )
        ms.Serialize( x :> ClusterInfoBase )
        WriteBytesToFileConcurrentP name (ms.GetBuffer()) 0 (int ms.Length)
    member x.DumpString() =
        let mutable str = ""
        str <- sprintf "%sClusterInfoPlainV2Guid: %A\n" str DeploymentSettings.ClusterInfoPlainV2Guid
        str <- sprintf "%sName: %A\n" str x.Name
        str <- sprintf "%sClusterType: %A\n" str x.ClusterType
        str <- sprintf "%sVersion.Ticks: %A\n" str x.Version.Ticks
        str <- sprintf "%sListOfClients.Length: %A\n" str x.ListOfClients.Length
        for i=0 to x.ListOfClients.Length-1 do
            let node = x.ListOfClients.[i]
            str <- sprintf "%sNode Index: %A\n" str i
            str <- sprintf "%s\tMachine ID: %A\n" str node.MachineID
            str <- sprintf "%s\tMachine Name: %A\n" str node.MachineName
            str <- sprintf "%s\tMachine Port: %A\n" str node.MachinePort
            if Utils.IsNotNull node.ExternalIPAddress then
                for i=0 to node.ExternalIPAddress.Length - 1 do
                    str <- sprintf "%s\External IP: %A\n" str (Net.IPAddress( node.ExternalIPAddress.[i] ))
            if Utils.IsNotNull node.InternalIPAddress then
                for i=0 to node.InternalIPAddress.Length - 1 do
                    str <- sprintf "%s\tInternal IP: %A\n" str (Net.IPAddress( node.InternalIPAddress.[i] ))
            str <- sprintf "%s\tProcessor Count: %A\n" str node.ProcessorCount
            str <- sprintf "%s\tRAM Capacity: %A\n" str node.RamCapacity
            str <- sprintf "%s\tDisk Capacity: %A\n" str node.DiskCapacity
            str <- sprintf "%s\tMemory Space: %A\n" str node.MemorySpace
            str <- sprintf "%s\tDrive Space: %A\n" str node.DriveSpace
            str <- sprintf "%s\tNetwork MTU: %A\n" str node.NetworkMTU
            str <- sprintf "%s\tNetwork Speed: %A\n" str node.NetworkSpeed
            str <- sprintf "%s\tSSD Type: %A\n" str node.SSDType
            str <- sprintf "%s\tSSD Capacity: %A\n" str node.SSDCapacity
            str <- sprintf "%s\tGPU Type: %A\n" str node.GPUType
            str <- sprintf "%s\tGPU Cores: %A\n" str node.GPUCores
            str <- sprintf "%s\tGPU Memory Capacity: %A\n" str node.GPUMemoryCapacity
        str <- sprintf "%sClusterInfoEndV2Guid: %A" str DeploymentSettings.ClusterInfoEndV2Guid
        str
    member x.IPV4ToInt32 (ipv4 : string) =
        let m = System.Text.RegularExpressions.Regex.Match(ipv4, """^(.*?)\.(.*?)\.(.*?)\.(.*?)$""")
        if (not m.Success) then
            0
        else
            let b0 = Convert.ToInt32(m.Groups.[1].Value)
            let b1 = Convert.ToInt32(m.Groups.[2].Value)
            let b2 = Convert.ToInt32(m.Groups.[3].Value)
            let b3 = Convert.ToInt32(m.Groups.[4].Value)
            if (b0 < 0 || b0 >= 256 ||
                b1 < 0 || b1 >= 256 ||
                b2 < 0 || b2 >= 256 ||
                b3 < 0 || b3 >= 256) then
                raise (Exception("Not valid IPV4 address"))
                0
            else
                (b0 <<< 24) ||| (b1 <<< 16) ||| (b2 <<< 8) ||| b3
    member x.Int32ToIPV4 (addr : int32) =
        let mutable b0 = (addr &&& 0xff000000) >>> 24
        let b1 = (addr &&& 0x00ff0000) >>> 16
        let b2 = (addr &&& 0x0000ff00) >>> 8
        let b3 = (addr &&& 0x000000ff)
        if (b0 < 0) then
            b0 <- b0 + 256
        sprintf "%d.%d.%d.%d" b0 b1 b2 b3
    /// Pack Cluster Information        
    member x.Pack(ms:StreamBase<byte>) = 
        ms.Write( DeploymentSettings.ClusterInfoPlainV2Guid.ToByteArray(), 0, 16 )
        ms.WriteString( x.Name )
        ms.WriteVInt32( int x.ClusterType )
        ms.WriteInt64( x.Version.Ticks )
        ms.WriteVInt32( x.ListOfClients.Length )
        for i=0 to x.ListOfClients.Length-1 do
            let node = x.ListOfClients.[i] 
            ms.WriteUInt64( node.MachineID )
            ms.WriteString( node.MachineName ) 
            ms.WriteUInt16( uint16 node.MachinePort )
            ms.WriteUInt16( uint16 node.JobPortMin )
            ms.WriteUInt16( uint16 node.JobPortMax )
            let lenInternal = if Utils.IsNotNull node.InternalIPAddress then node.InternalIPAddress.Length else 0
            ms.WriteVInt32( lenInternal ) 
            for i = 0 to lenInternal - 1 do
                ms.WriteBytesWLen( node.InternalIPAddress.[i] )  // Internal address & port, no use here
            let lenExternal = if Utils.IsNotNull node.ExternalIPAddress then node.ExternalIPAddress.Length else 0
            ms.WriteVInt32( lenExternal ) 
            for i = 0 to lenExternal - 1 do
                ms.WriteBytesWLen( node.ExternalIPAddress.[i] )  // External address information, no use here
            ms.WriteVInt32( node.ProcessorCount )
            ms.WriteInt32( node.RamCapacity )
            ms.WriteInt32 ( node.DiskCapacity )
            ms.WriteInt32( node.MemorySpace )
            ms.WriteInt32( node.DriveSpace )
            ms.WriteInt32( node.NetworkMTU )
            ms.WriteInt64( node.NetworkSpeed )
            ms.WriteVInt32( node.SSDType )
            ms.WriteInt64( node.SSDCapacity )
            ms.WriteVInt32( node.GPUType )
            ms.WriteVInt32( node.GPUCores )
            ms.WriteInt64( node.GPUMemoryCapacity )
        ms.Write( DeploymentSettings.ClusterInfoEndV2Guid.ToByteArray(), 0, 16 )
    member x.PackToBytes() = 
        use ms = new MemStream( 102400 )
        x.Pack( ms )
        let bytearray = Array.zeroCreate<byte> (int ms.Length)
        Array.Copy( ms.GetBuffer(), bytearray, int ms.Length ) 
        bytearray
    /// New Cluster Format
    member x.Save(name) = 
        use ms = new MemStream( 102400 )
        x.Pack( ms ) 
        WriteBytesToFileConcurrentP name (ms.GetBuffer()) 0 (int ms.Length)
    /// Read the current cluster from file 
    static member ReadOld (name:string) = 
        try
            use stream = new FileStream( name, FileMode.Open, FileAccess.Read )
            let guid = Array.zeroCreate<byte> 16
            stream.Read( guid, 0, 16 ) |> ignore
            match Guid(guid) with 
            | a when a.CompareTo( DeploymentSettings.ClusterInfoPlainGuid ) = 0 ->
                let fmt = Runtime.Serialization.Formatters.Binary.BinaryFormatter()
                let ret = ClusterInfo( fmt.Deserialize( stream ) :?> ClusterInfoBase ) 
                Some( ret )
            | _ ->
                Logger.Log( LogLevel.Error, ( sprintf "The file %s has Guid %A that is not ClusterInfo file (normally with .inf extension)" name (Guid(guid)) ))
                None              
        with
        | e ->
            Logger.Log( LogLevel.Error, ( sprintf "Fail to open file %s or parse it correctly with %A" name e ))
            None
    /// Unpack the current cluster from MemStream
    static member Unpack( ms:StreamBase<byte> ) = 
        try
            let guid = Array.zeroCreate<byte> 16
            ms.Read( guid, 0, 16 ) |> ignore
            match Guid(guid) with 
            | a when a.CompareTo( DeploymentSettings.ClusterInfoPlainGuid ) = 0 ->
                let fmt = Runtime.Serialization.Formatters.Binary.BinaryFormatter()
                let ret = ClusterInfo( fmt.Deserialize( ms ) :?> ClusterInfoBase ) 
                failwith "Cluster with V1 ClusterInfo, no longer supported"
                Some( ret )
            | b when b.CompareTo( DeploymentSettings.ClusterInfoPlainV2Guid ) = 0 ->
                let x = ClusterInfo()
                try
                    x.Name <- ms.ReadString()
                    x.ClusterType <- enum<_> ( ms.ReadVInt32( ) )
                    x.Version <- DateTime( ms.ReadInt64( ) )
                    // Determine the replication type of a cluster
                    x.ReplicationType <- ClusterInfoBase.SetReplicationType( x.ClusterType )
                    x.ListOfClients <- Array.zeroCreate<ClientInfo> (ms.ReadVInt32())
                    for i=0 to x.ListOfClients.Length-1 do
                        let mutable node = x.ListOfClients.[i] 
                        node.MachineID <- ms.ReadUInt64(  )
                        node.MachineName <- ms.ReadString(  ) 
                        node.MachinePort <- int (ms.ReadUInt16())    
                        node.JobPortMin <- int (ms.ReadUInt16() ) 
                        node.JobPortMax <- int (ms.ReadUInt16() )

                        let lenInternal = ms.ReadVInt32() 
                        if lenInternal > 0 then
                            node.InternalIPAddress <- Array.zeroCreate<_> lenInternal
                            for i = 0 to lenInternal - 1 do
                                node.InternalIPAddress.[i] <- ms.ReadBytesWLen() // Internal address & port, no use here
                        let lenExternal = ms.ReadVInt32( ) 
                        if lenExternal > 0 then
                            node.ExternalIPAddress <- Array.zeroCreate<_> lenExternal
                            for i = 0 to lenExternal - 1 do
                                node.ExternalIPAddress.[i] <- ms.ReadBytesWLen(  )  // External address information, no use here
                        node.ProcessorCount <- ms.ReadVInt32()
                        node.RamCapacity <- ms.ReadInt32()
                        node.DiskCapacity <- ms.ReadInt32()
                        node.MemorySpace <- ms.ReadInt32()
                        node.DriveSpace <- ms.ReadInt32()
                        node.NetworkMTU <- ms.ReadInt32()
                        node.NetworkSpeed <- ms.ReadInt64()
                        node.SSDType <- ms.ReadVInt32()
                        node.SSDCapacity <- ms.ReadInt64()
                        node.GPUType <- ms.ReadVInt32()
                        node.GPUCores <- ms.ReadVInt32()
                        node.GPUMemoryCapacity <- ms.ReadInt64() 
                        x.ListOfClients.[i] <- node 
                    ms.Read( guid, 0, 16 ) |> ignore
                    if Guid(guid).CompareTo( DeploymentSettings.ClusterInfoEndV2Guid )=0 then 
                        Some( x )
                    else
                        Logger.Log( LogLevel.Error, ( sprintf "End of Guid verification failed for the ClusterInfo"  ))
                        None
                with
                | e ->
                    Logger.Log( LogLevel.Error, ( sprintf "Failed to parse V2 cluster file: content %A" (ms.GetBuffer()) ))
                    None
            | _ ->
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "The ClusterInfo has Guid %A that is not ClusterInfo (normally with .inf extension)" (Guid(guid)) ))
                None              
        with
        | e ->
            Logger.Log( LogLevel.Error, ( sprintf "Cluster Information File is shorter than 16B, with content %A" (ms.GetBuffer()) ))
            None
    
    static member private ParseListFile(file : string ) =
        let lines = File.ReadAllLines(file)
        if lines.Length < 2 then
            failwith (sprintf "Cluster list file is invalid, it needs to specify the cluster information at line 1, and at least specify one node")

        // The first line:   Name,Port,Passwd   (Port and Passwd is optional)
        let clusterItems = lines.[0].Split([|','|])
        if clusterItems.Length < 2 && clusterItems.Length > 3 then
            failwith (sprintf "Cluster list file is invalid, the first line on cluster information is invalid : %s" lines.[0])

        let clusterName = clusterItems.[0]
        if clusterName.IndexOfAny(Path.GetInvalidFileNameChars()) >= 0 then
            // Must be able to be a file name
            failwith (sprintf "Cluster Name '%s' is not valid." clusterName)

        let b, port = Int32.TryParse(clusterItems.[1])
        if not b then 
            failwith (sprintf "Cluster list file has specified an invalid port at line 1: %s" clusterItems.[1])

        let passwd = if clusterItems.Length = 3 then clusterItems.[2] |> Some else None

        let clients = [|
                for i in 2..lines.Length do
                    // each line has the format  machine-name
                    let items = lines.[i - 1].Split([|','|])
                    if items.Length <> 1 then
                        failwith (sprintf "Cluster list file has a invalid line (line %d): %s" i lines.[i - 1])
                    let machineName = items.[0]
                    
                    // Generate machine id from machine name
                    let ba = let arr = System.Text.Encoding.ASCII.GetBytes(machineName)
                             if arr.Length >= 8 then arr
                             else Array.zeroCreate<byte> 8 |> Array.mapi ( fun i v -> if i < arr.Length then arr.[i] else v )
                    let id = BitConverter.ToUInt64(ba, 0) 
                    yield ClientInfo(ClientStatus(Name = machineName, InternalPort = port, MachineID = id))
            |]
        ClusterInfo(ListOfClients = clients, Name = clusterName) |> Some, passwd

    /// Unpack to cluster information. 
    static member Unpack( bytearray:byte[] ) = 
        use ms = new MemStream( bytearray, 0, bytearray.Length, false, true )
        let outParam = ClusterInfo.Unpack( ms )   
        outParam
    /// Read the current cluster from file 
    static member Read (name:string) = 
        try
            let ext = Path.GetExtension(name)
            if String.Compare(ext, ".lst", StringComparison.InvariantCultureIgnoreCase) = 0 then
                // It's a lst file
                ClusterInfo.ParseListFile(name)
            else
                let bytearray = ReadBytesFromFile( name ) 
                use ms = new MemStream( bytearray, 0, bytearray.Length, false, true )
                let outParam = ClusterInfo.Unpack( ms )
                let passwd = None // inf file does not support password yet
                outParam, passwd
        with
        | e ->
            let msg = sprintf "Fail to open file %s or parse it correctly with %A" name e
            Logger.Log( LogLevel.Error, msg)
            failwith msg

    /// Read the current cluster 
    static member Read (name, ver:DateTime ) = 
        let name = ClusterInfo.ConstructClusterInfoFileNameWithVersion(name, ver)      
        if File.Exists name then 
            ClusterInfo.Read( name ) 
        else
            None, None  

    /// Query available clusters 
    static member GetClusters( name: string ) = 
        let folder = ClusterInfo.ClusterInfoFolder()
        if Directory.Exists folder then 
            let searchName = if StringTools.IsNullOrEmpty name then "*" else name + "_" + "*"
            let dic = ConcurrentDictionary<string, DateTime>(StringComparer.OrdinalIgnoreCase)
            for file in Directory.GetFiles( folder, name, SearchOption.TopDirectoryOnly ) do        
                try 
                    let clusterInfoOpt, _ = ClusterInfo.Read( file )
                    match clusterInfoOpt with 
                    | Some clusterInfo -> 
                        dic.AddOrUpdate( clusterInfo.Name, clusterInfo.Version, fun name ver -> if ver.Ticks > clusterInfo.Version.Ticks then ver else clusterInfo.Version ) |> ignore 
                    | None -> 
                        ()
                with 
                | ex -> 
                    /// It is OK if some of the cluster file can't be parsed
                    () 
            dic |> Seq.map( fun pair -> pair.Key, pair.Value ) |> Seq.toArray
        else
            Array.empty


    /// Save the current ClusterInfoBase to file, with default name
    member x.Save() = 
        x.Save( x.ConstructClusterInfoFilename() )
    /// Save the current ClusterInfoBase to file, with a shorter filename for easier manipulation. 
    member x.SaveShort( ) = 
        x.Save( ClusterInfo.ConstructClusterInfoFileNameWithVersion( x.Name, DateTime.MinValue )  )

    /// Save the current ClusterInfoBase if the current cluster doesn't exist 
    /// if the version is not specified (DateTime.MinValue), then search for the latst version of inf file for the same cluster (by name)
    /// If the info is the same, nothing to persist. Otherwise, create a new version. 
    /// If the version is specified, save it.
    member x.Persist() =
        let fname = 
            if x.Version = DateTime.MinValue then
                let latestInfoFile = ClusterInfo.FindLatestClusterInfoFile(x.Name)
                let isSame = 
                    match latestInfoFile with
                    | Some infoFile -> let clusterInfo, _ = ClusterInfo.Read(infoFile)
                                       match clusterInfo with
                                       | Some info -> Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ClusterInfo.Persist (%s) - Read - %s:%i:%i" x.Name info.Name info.Version.Ticks (info.ListOfClients |> Array.length)))
                                                      info.Version <- DateTime.MinValue
                                                      info = x
                                       | None -> Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ClusterInfo.Persist (%s) - Read %s failed" x.Name infoFile))
                                                 false
                    | None -> Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ClusterInfo.Persist (%s) - not found" x.Name))
                              false
                if not isSame then 
                    ClusterInfo.ConstructClusterInfoFileNameWithVersion( x.Name, DateTime.UtcNow ) |> Some
                else 
                    None
            else                
                x.ConstructClusterInfoFilename() |> Some
            
        match fname with
        | Some f ->   if not ( File.Exists f ) then 
                          x.Save( f )
        | None -> ()

    /// Save the current Client list 
    member x.SaveLst( fname:string ) = 
        use file = new StreamWriter(fname)
        for cl in x.ListOfClients do
            file.WriteLine( cl.MachineName )
        file.Close()
    /// Save the current Client list, with default name
    member x.SaveLst() = 
        x.SaveLst( x.ConstructLstFilename() )
    /// Should not be called, this feature is been deprecated. 
    static member internal PeriodicClusterUpdate( s: Object ) =
        let x = s :?> ClusterInfo
        x.ClusterUpdate()
    interface IComparable<ClusterInfo> with
        member x.CompareTo y = 
            if System.Object.ReferenceEquals( x, y ) then 
                0
            else
                let mutable retVal = x.ClusterType.CompareTo( y.ClusterType )
                if retVal=0 then retVal <- x.Version.CompareTo( y.Version )
                if retVal=0 then 
                    let xResources = x.ListOfClients
                    let yResources = y.ListOfClients
                    retVal <- xResources.Length.CompareTo( yResources.Length )
                    if retVal = 0 then 
                        let mutable idx = 0
                        while retVal = 0 && idx < xResources.Length do
                            let xinfo = xResources.[idx]
                            let yinfo = yResources.[idx]
                            retVal <- (xinfo :> IComparable).CompareTo( yinfo )
                            idx <- idx + 1
                retVal                                  
    interface IComparable with 
        member x.CompareTo other = 
            match other with 
            | :? ClusterInfo as y ->
                ( x :> IComparable<_> ).CompareTo y
            | _ -> 
                InvalidOperationException() |> raise  
    interface IEquatable<ClusterInfo> with
        member x.Equals y = 
            ( ( x :> IComparable<_> ).CompareTo y ) = 0
    override x.Equals other = 
        ( x :> IComparable).CompareTo( other ) = 0
    override x.GetHashCode() = 
        hash ( x.ClusterType, x.Version, x.ListOfClients  )

type internal HomeInServer(info, ?serverInfo, ?ipAddress, ?port, ?cport) =
    // Don't expect contention for home in server. OK to use Dictionary
    static member val ExcludeList = Dictionary<string,bool>() with get, set
    static member val OnlyList = Dictionary<string,int>() with get, set
    static member val ListOfClusters = Array.zeroCreate<string*ClusterInfo*int> 0  with get, set
    static member val CurrentCluster = ClusterInfo() with get, set
    static member val ListOfClients = SortedDictionary<string, ClientStatus>() with get, set
    static member val Updated = true with get, set
    static member LoadAllClusters() = 
        let path = ClusterInfo.ClusterInfoFolder()
        let sortlist = Dictionary< string, ClusterInfo > ()
        let mutable useCluster : ClusterInfo = null
        let mutable useClusterTime = DateTime.MinValue
        try
            let dir = DirectoryInfo( path )
            let files = dir.GetFiles( "*.lst" )
            for file in files do 
                let cluster = ClusterInfo()
                let lines = File.ReadAllLines( file.FullName )
                cluster.ListOfClients <- lines |> Array.map( fun name -> ClientInfo(name) )
                let mutable cname = file.Name
                if cname.EndsWith( ".lst" ) then 
                    cname <- cname.Substring( 0, cname.Length - ".lst".Length )
                if cname.StartsWith( "Cluster_" ) then 
                    cname <- cname.Substring( "Cluster_".Length ) 
                sortlist.Item(cname) <- cluster

            let files = dir.GetFiles( "*.inf" )
            for file in files do 
                try 
                    // HomeInServer is not dealing with passwd yet
                    let cluster, _ = ClusterInfo.Read( file.FullName )
                    match cluster with 
                    | Some ( cl ) ->
                        let mutable cname = file.Name
                        if cname.EndsWith( ".inf" ) then 
                            cname <- cname.Substring( 0, cname.Length - ".inf".Length )
                        if cname.StartsWith( "Cluster_" ) then 
                            cname <- cname.Substring( "Cluster_".Length ) 
                        sortlist.Item(cname) <- cl
                        if file.CreationTime > useClusterTime then 
                            useClusterTime <- file.CreationTime
                            useCluster <- cl
                    | None ->
                        Logger.Log( LogLevel.Info, ( sprintf "Error in Reading cluster file %s" file.Name ))
                with
                | e ->
                    Logger.Log( LogLevel.Info, ( sprintf "Error in Parsing cluster file %s" file.Name ))
                ()
        with
        | e ->
            ()
        if Utils.IsNotNull useCluster then 
            HomeInServer.CurrentCluster <- useCluster
        HomeInServer.ListOfClusters <- Seq.toArray( sortlist |> Seq.map( fun pair -> ( pair.Key, pair.Value, 0 ) ) )
    static member UpdateListOfClients() =
//        lock ( HomeInServer.ListOfClients ) ( fun () ->
            let mutable numOnly = 0
            let mutable useCluster : ClusterInfo = null
    //                let cnt = HomeInServer.ListOfClients.Count
    //                let newArray = Array.zeroCreate<_> cnt
    //                HomeInServer.ListOfClients.CopyTo( newArray, 0 )
    //                newArray )
            // Create Exclude List and Only List
            let newExcludeList = Dictionary<string,bool>()
            let newOnlyList = Dictionary<string,int>() 
            for clusterEntry in HomeInServer.ListOfClusters do
                let cname, cl, status = clusterEntry
                if status = 2 then 
                    for client in cl.ListOfClients do 
                        newExcludeList.Item( client.MachineName.ToUpper() ) <- true
                if status = 3 then
                    numOnly <- numOnly + 1 
                    useCluster <- cl
                    for client in cl.ListOfClients do 
                        if newOnlyList.ContainsKey( client.MachineName.ToUpper() ) then
                            newOnlyList.Item( client.MachineName.ToUpper() ) <- (newOnlyList.Item( client.MachineName.ToUpper() )) + 1
                        else
                            newOnlyList.Item( client.MachineName.ToUpper() ) <- 1
                
            HomeInServer.ExcludeList <- newExcludeList
            HomeInServer.OnlyList <- newOnlyList

            let workListOfClients = Dictionary<_,_>( ) 
            let workListName = Dictionary<string,bool>()
            HomeInServer.ListOfClients
            |> Seq.iter( fun pair -> 
                if HomeInServer.OnlyList.Count<=0 || HomeInServer.OnlyList.ContainsKey( pair.Key.ToUpper() ) then 
                    if not (HomeInServer.ExcludeList.ContainsKey( pair.Key.ToUpper() )) then 
                        workListOfClients.Add( pair.Key, pair.Value )
                        workListName.Add( pair.Key.ToUpper(), true )
            )    


            // Add entry of all client in the inclusion list. 
            for clusterEntry in HomeInServer.ListOfClusters do
                let cname, cl, status = clusterEntry
                if status = 1 || status = 3 then 
                    for client in cl.ListOfClients do 
                        if not (HomeInServer.ExcludeList.ContainsKey( client.MachineName.ToUpper() )) then  
                            if not ( workListName.ContainsKey( client.MachineName.ToUpper() ) ) then 
                                let clusterStatus = client.ConvertToClientStatus()
                                if status=3 then 
                                    // item is included by only list
                                    clusterStatus.ProcessorCount <- HomeInServer.OnlyList.Item( client.MachineName.ToUpper() )
                                workListOfClients.Item( client.MachineName ) <- clusterStatus
                                workListName.Item( client.MachineName.ToUpper() ) <- true
                            else if status=3 && HomeInServer.OnlyList.Item( client.MachineName.ToUpper() ) > 1 then 
                                let clusterStatus = workListOfClients.Item( client.MachineName.ToUpper() )
                                if not ( clusterStatus.ClientVersionInfo.Contains( "(" ) ) then 
                                    clusterStatus.ClientVersionInfo <- clusterStatus.ClientVersionInfo + "(" + HomeInServer.OnlyList.Item( client.MachineName.ToUpper() ).ToString() + ")"
                                    workListOfClients.Item( client.MachineName.ToUpper() ) <- clusterStatus

//            // Remove Entry in ExcludeList
//            for pair in HomeInServer.ExcludeList do 
//                if workListOfClients.ContainsKey( pair.Key) then 
//                    workListOfClients.Remove( pair.Key ) |> ignore
//            
//            // Only Entry in the OnlyList will be retained. 
//            if HomeInServer.OnlyList.Count>0 then 
//                let nameList = Seq.toArray( workListOfClients |> Seq.map( fun pair -> pair.Key ) )
//                for name in nameList do 
//                    if not ( HomeInServer.OnlyList.ContainsKey( name ) ) then 
//                        workListOfClients.Remove( name ) |> ignore 
                      
            let resortListOfClients = SortedDictionary<_, _> (workListOfClients)
            HomeInServer.ListOfClients <- resortListOfClients
            if numOnly = 1 then 
                HomeInServer.CurrentCluster <- useCluster
//            )
    member val ServerInfo = info with get
    member val IpAddress = defaultArg ipAddress IPAddress.Any with get
    member val Port = defaultArg port 1080 with get
    member val cPort =  defaultArg cport 1081 with get
    member val Listener : TcpListener = null with get, set
    /// Take a snap shot of the current valid clients. 
    static member TakeSnapShot( ver ) = 
        lock ( HomeInServer.ListOfClients ) ( fun() -> 
            HomeInServer.CurrentCluster.ListOfClients <- 
                HomeInServer.ListOfClients 
                |> Seq.filter ( fun pair -> pair.Value.Selected )
                |> Seq.map ( fun pair -> ClientInfo(pair.Value) ) 
                |> Seq.toArray 
            HomeInServer.CurrentCluster.Version <- ver )
    /// Take a snap shot of the current valid clients, and put them in a snap shot file. 
    static member SaveSnapShot() = 
        HomeInServer.TakeSnapShot( (PerfDateTime.UtcNow()) )
        HomeInServer.CurrentCluster.Save()
        HomeInServer.CurrentCluster.SaveLst()
    static member StartServer( o: Object ) =
        let x = o :?> HomeInServer
        x.Listener <- new TcpListener( x.IpAddress, x.Port )
        x.Listener.Start( 50 )
        Logger.Log( LogLevel.MildVerbose, (sprintf "Listening on port %d" x.Port ))
        try
            x.Listener.BeginAcceptTcpClient( AsyncCallback( HomeInServer.ClientHomeIn ), x) |> ignore
            while true do
                Threading.Thread.Sleep( 1000 )
        finally
            x.Listener.Stop()
        ()

    static member ControllerHomeIn( o: Object ) = 
        try
            // Note: "ControllerHomeIn" was invoked as an async task, it is responsible for releasing the passed in tcpClient
            use tcpClient = o :?> TcpClient 
            use socket = new NetworkSocket( tcpClient )
            try
                let rcvdInfo = socket.Rcvd()
                let o = Deserialize rcvdInfo
                match o with
                | :? ControllerCommand as cmd -> 
                    match (cmd.Verb, cmd.Noun) with 
                    | ( ControllerVerb.Get, ControllerNoun.ClusterInfo ) ->
                        if HomeInServer.CurrentCluster.Version = DateTime.MinValue then 
                            HomeInServer.TakeSnapShot( (PerfDateTime.UtcNow()) )
//                        let sendInfo = Serialize (HomeInServer.CurrentCluster) 40960
                        let sendInfo = HomeInServer.CurrentCluster.PackToBytes()
                        socket.Send( sendInfo )
                    | _ ->
                        Logger.Log( LogLevel.Error, (sprintf "Error: unknown controller command %A" cmd ))
                | _ -> 
                    Logger.Log( LogLevel.Error, (sprintf "Error: unknown received info from cluster controller %A" rcvdInfo ))
            with
            | e -> Logger.Log( LogLevel.Info, sprintf "Error: fail to communicate with cluster controller %A" e  )
        with 
        | e -> Logger.Log( LogLevel.Info, sprintf "Exception in ControllerHomeIn() %A" e  )
        

    static member StartController( o: Object ) =
        let x = o :?> HomeInServer
        let listener = TcpListener( x.IpAddress, x.cPort )
        listener.Start()
        Logger.Log( LogLevel.MildVerbose, (sprintf "Listening on port %d" x.cPort ))
        try
            while true do
                Logger.Log( LogLevel.MildVerbose, "Waiting for request ..." )
                let tcpClient = listener.AcceptTcpClient()
//                let ControllerHomeInStart = Threading.ParameterizedThreadStart( HomeInServer.ControllerHomeIn )
//                let ControllerHomeInThread = Threading.Thread( ControllerHomeInStart )
//                // IsBackground Flag true: thread will abort when main thread is killed. 
//                ControllerHomeInThread.IsBackground <- true
//                ControllerHomeInThread.Start( tcpClient )
//                Threading.Thread.Sleep( 1 )
                let ControllerHomeInThread = ThreadTracking.StartThreadForFunction ( fun _ -> "Home In Processing") ( fun _ -> HomeInServer.ControllerHomeIn tcpClient)
                ()
        finally
            listener.Stop()
        ()

    static member ClientHomeIn( ar ) = 
        let x = ar.AsyncState :?> HomeInServer
        let tcpClient = x.Listener.EndAcceptTcpClient( ar )
        // Post another
        x.Listener.BeginAcceptTcpClient( AsyncCallback( HomeInServer.ClientHomeIn ), x) |> ignore
        use socket = new NetworkSocket( tcpClient  )
        try
            socket.SendString( x.ServerInfo )
            let rcvdBuf = socket.Rcvd()
            use rcvdMemStream = new MemStream( rcvdBuf )
            let c = ClientStatus.Unpack( rcvdMemStream )
            let mutable bInclude = true
            if HomeInServer.ExcludeList.ContainsKey( c.Name.ToUpper() ) then 
                bInclude <- false
            if HomeInServer.OnlyList.Count > 0 && not ( HomeInServer.OnlyList.ContainsKey( c.Name.ToUpper() ) ) then 
                bInclude <- false
            if bInclude then 
                lock ( HomeInServer.ListOfClients ) ( fun () ->
                    if HomeInServer.ListOfClients.ContainsKey( c.Name ) then 
                        c.Selected <- HomeInServer.ListOfClients.[c.Name].Selected
                        HomeInServer.ListOfClients.Remove( c.Name ) |> ignore
                        HomeInServer.ListOfClients.Add( c.Name, c )
                        HomeInServer.Updated <- true
                    else 
                        HomeInServer.ListOfClients.Add( c.Name, c )
                        HomeInServer.Updated <- true
            )
            Logger.Log( LogLevel.MildVerbose, (sprintf "Rcvd %dB from %A..." rcvdBuf.Length socket ))
        with
        | _ -> () 


