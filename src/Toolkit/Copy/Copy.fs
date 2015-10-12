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
        PrajnaCopy.fs
  
    Description: 
        Copy a set of files into (and out of Prajna) 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        July. 2013
    
 ---------------------------------------------------------------------------*)
open System
open System.Collections.Generic
open System.Threading
open System.Diagnostics
open System.IO
open System.Net
open System.Runtime.Serialization
open System.Threading.Tasks
open Prajna.Tools
open Prajna.Tools.Network

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp
open System.Xml

module AssemblyProperties =
// Signs the assembly in F#
    open System
    open System.Reflection;
    open System.Runtime.InteropServices;

    do()

module PrajnaCopy = 
    /// Helper function on double array. 
    type internal ArrayFunc() = 
        /// calculate the distance of two double array. 
        static member Distance (arr1:float[]) (arr2:float[]) = 
            Math.Sqrt( Array.fold2 ( fun sum xi yi -> sum + ( xi - yi ) * ( xi - yi ) ) 0.0 arr1 arr2 )

    type internal UniformFunc() = 
        static member Random (rnd:Random) dimn =
            Array.init dimn ( fun _ -> rnd.NextDouble() )

    type internal LaplacianFunc() = 
        static member Random (rnd:Random) dimn = 
            Array.init dimn ( fun _ -> - Math.Log( 1.0 - rnd.NextDouble() )  )
                                   

    type internal GaussianFunc() = 
        /// Generate a pair of gaussian random variable with 0 means and variance 1
        static member BoxMuller2 (rnd:Random) =
            let mutable x1 = 0.0
            let mutable x2 = 0.0
            let mutable w = 2.0
            while w >= 1.0 do 
                x1 <- rnd.NextDouble() * 2.0 - 1.0
                x2 <- rnd.NextDouble() * 2.0 - 1.0
                w <- x1 * x1 + x2 * x2
            let r = Math.Sqrt( -2.0 * Math.Log(w) / w )
            x1 * r, x2 * r

        static member Random2 (rnd:Random) dimn =
            let v = Array.zeroCreate dimn
            for i = 0 to ( v.Length - 1 ) / 2 do
                let x1, x2 = GaussianFunc.BoxMuller2 rnd
                v.[ i*2 ] <- x1
                v.[ i*2 + 1 ] <- x1
            if v.Length%2 <> 0 then 
                let x1, _ = GaussianFunc.BoxMuller2 rnd
                v.[ v.Length - 1 ] <- x1
            v

        static member BoxMuller (rnd:Random) =
            let u1 = rnd.NextDouble()
            let u2 = rnd.NextDouble()
            Math.Sqrt( -2.0 * Math.Log( u1 ) ) * Math.Sin( 2.0 * Math.PI * u2 )

        static member Random (rnd:Random) dimn =
            Array.init dimn ( fun _ -> GaussianFunc.BoxMuller rnd )

        static member RandomMV (rnd:Random) (mean:float[]) variance =
            let var = variance / Math.Sqrt( double mean.Length )
            let v = GaussianFunc.Random rnd (mean.Length)
            Array.map2 ( fun v m -> (v * var) + m ) v mean
    //        for i = 0 to v.Length - 1 do 
    //            v.[i] <- ( v.[i] * var + mean.[i] )  
    //        v

        static member RandomMV2 (rnd:Random) (mean:float[]) variance =
            let var = variance / Math.Sqrt( double mean.Length )
            let v = GaussianFunc.Random2 rnd (mean.Length)
            Array.map2 ( fun v m -> (v * var) + m ) v mean
    let findClosestFunc( genV, meanArray ) = 
        let distArray = meanArray |> Array.map( fun cl -> ArrayFunc.Distance genV cl ) 
        let smalli = ref -1 
        let smalld = ref Double.MaxValue
        distArray |> Array.iteri( fun i d -> if !smalli<0 || d < !smalld then 
                                                smalli:=i
                                                smalld:=d
                                                )
        !smalli  

    let Usage = "PrajnaClient \n\
        Command line arguments:\n\
        -master     Name of the Prajna controller \n\
        -local      Local directory \n\
        -remote     Name of the distributed Prajna folder\n\
        -rep REP    Number of Replication \n\
        -ver V      Version string: in format yyMMdd_HHmmss.fff \n\
        -slimit S   # of record to serialize \n\
        -password P Password \n\
        -balancer B Type of load balancer \n\
        -nump N     Number of partitions \n\
        -in         Copy into Prajna \n\
        -out        Copy outof Prajna \n\
        -echo N     Network Echo test of Prajna (use first N machines) \n\
        -len  L     Length of echos, default=2^20\n\
        -count 1000 Count of echos, default=1024\n\
        -ecount N   Count of echo arrays, default=count\n\
        -flag FLAG  DKVFlag \n\
        -hash       Return hash of the file stored \n\
        -aggregate  Calculate for aggregation\n\
        -queue 100  Allowed Receiving Command Queue Length (in MegaByte) \n\
        -speed S    Limiting speed of each peer to S bps\n\
        -bucket     Leaky Bucket Size\n\
        -spattern   Search pattern for the local directory, for example, *.jpg \n\
        -ram        Make DKV persist to speed up followup execution (only used with -hash flag for testing). \n\
        -rec        if present, search through all directories\n\
        -speed S    Peer network speed limit \n\
        -azure      Use Azure Blob Storage\n\
        -gen        Generate a random set of vectors (to be sorted/k-mean)\n\
        -test       Local test of Gaussian variable generation \n\
        -num        Number of vectors to be generated\n\
        -dim        Dimension of vectors to be generated\n\
        -class      Number of classes to generate or classify \n\
        -var        Variance of the Gaussian variable. \n\
        -noise      Add some small noise. \n\
        -mapreduce  Add map reduce operation. \n\
        -verify     Verify mapreduce computation. \n\
        -upload     Upload a text file as DKV to the cluster. Each line of is split to a string[], the key is the #uploadKey field\n\
        -uploadKey  #uploadKey field\n\
        -uploadSplit Characters used to split the upload\n\
        -download LIMIT  Download files to the cluster. Using key as URL. LIMIT indicates # of parallel download allowed \n\
        -seed SEED  seed of random generator \n\
        -exe        Execute function in a separate exe\n\
        -task       Execute function in parallel tasks\n\
        -downloadChunk Chunk of Bytes to download\n\
        -encrypt    Encrypt messages\n\
        -sort KEYLENGTH Test of sorting, use length of key in bytes as input\n\
        "

    let watch = Stopwatch.StartNew()

    let parseEcho (queue : NetworkCommandQueue) (countEcho : int) (countEchoArr : int) (countRcvd : int[]) (nTotalRcvd : uint64[])
                  (echoArrays : uint64[][]) (bError : bool ref) (lenEcho : int) (n : int) 
                  (command : NetworkCommand) =
        let cmd = command.cmd
        let ms = command.ms

        match ( cmd.Verb, cmd.Noun ) with 
        | ( ControllerVerb.EchoReturn, ControllerNoun.Message ) ->
            let reachTime = DateTime(ms.ReadInt64())
            let sendTime = ms.ReadInt64()
            let diffTicks = PerfADateTime.UtcNowTicks() - sendTime
            let diff = TimeSpan(diffTicks).TotalMilliseconds
            let mutable bMatch = false
            let mutable pos = -1 
            let cnt = countEcho - countRcvd.[n]
            let ecnt = cnt % countEchoArr
            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Packet %d echo from peer %d %s RTT(ms): %f, Ticks = %d " cnt n queue.EPInfo diff diffTicks))
            let len = ms.ReadInt32()
            if len = echoArrays.[ecnt].Length then
                pos <- 0                                    
                while pos<len && ms.ReadUInt64()=echoArrays.[ecnt].[pos] do
                    pos <- pos+1
                if pos >= echoArrays.[ecnt].Length then 
                    bMatch <- true
            if not bMatch then 
                Logger.Log( LogLevel.Error, (sprintf "Echoback fail to match at position %d" pos ))
                bError := true
            else
                countRcvd.[n] <- countRcvd.[n] - 1
                let nCurRcvd = ( uint64 ( countEcho - countRcvd.[n] ) ) * (uint64 lenEcho)
                if ( nCurRcvd - nTotalRcvd.[n] >= 1000000UL ) then 
                    nTotalRcvd.[n] <- nCurRcvd
                Logger.Log( LogLevel.Info, (sprintf "Successful echo peer %d message %d with Queue %d MB" n (countEcho-countRcvd.[n]) queue.UnProcessedCmdInMB ))
        | _ ->
            failwith "Receive command other than EchoReturn, Message"
            bError := true
        null

    // Define your library scripting code here
    [<EntryPoint>]
    let main orgargs = 
        let args = Array.copy orgargs
        let parse = ArgumentParser(args)
        let PrajnaMasterFile = parse.ParseString( "-master", "" )
        let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
        let localdir = parse.ParseString( "-local", "" )
        let remoteDKVname = parse.ParseString( "-remote", "" )
        let bIn = parse.ParseBoolean( "-in", false )
        let bOut = parse.ParseBoolean( "-out", false )
        let uploadFile = parse.ParseString( "-upload", "" )
        let uploadKey = parse.ParseInt( "-uploadKey", 0 )
        let uploadSplit = parse.ParseString( "-uploadSplit", "\t" )
        let downloadLimit = parse.ParseInt( "-download", -1 )
        let bRetrieve = parse.ParseBoolean( "-retrieve", false )
        let bGen = parse.ParseBoolean( "-gen", false )
        let bTest = parse.ParseBoolean( "-test", false )
        let num = parse.ParseInt( "-num", 1000000 )
        let dim = parse.ParseInt( "-dim", 2048 )
        let nClasses = parse.ParseInt( "-class", 100 )
        let var = parse.ParseFloat( "-var", 0.1 )
        let noise = parse.ParseFloat( "-noise", Double.MinValue )
        let bMapReduce = parse.ParseBoolean( "-mapreduce", false )
        let bVerify = parse.ParseBoolean( "-verify", false )
        let bCacheRAM = parse.ParseBoolean( "-ram", false )
        let nrep = parse.ParseInt( "-rep", 3 )
        let typeOf = enum<LoadBalanceAlgorithm>(parse.ParseInt( "-balancer", 0) )
        let mutable nEcho = parse.ParseInt( "-echo", 0 )
        let lenEcho = parse.ParseInt( "-len", (1<<<20) )
        let sleepBetweenEchoMs = parse.ParseInt( "-echosleep", 0 )
        let countEcho = parse.ParseInt( "-count", 1024 )
        let mutable countEchoArr = parse.ParseInt( "-ecount", 0 )
        let queueLen = parse.ParseInt( "-queue", (DeploymentSettings.MaxSendingQueueLimit >>> 20))
        let bucketSize = parse.ParseInt( "-bucket", 32768 )
        let versionInfo = parse.ParseString( "-ver", "" )
        let slimit = parse.ParseInt( "-slimit", 10 )
        let password = parse.ParseString( "-password", "" )
        let rcvdSpeedLimit = parse.ParseInt64( "-speed", 40000000000L )
        let flag = parse.ParseInt( "-flag", -100 )
        let bHash = parse.ParseBoolean( "-hash", false )
        let bAggregate = parse.ParseBoolean( "-aggregate", false )
        let nump = parse.ParseInt( "-nump", 0 )
        let bUseAzure = parse.ParseBoolean( "-azure", false )
        let bExe = parse.ParseBoolean( "-exe", false )
        let seed = parse.ParseInt( "-seed", 0 )
        let bTask = parse.ParseBoolean( "-task", false )
        let downloadChunk = parse.ParseInt( "-downloadChunk", (1<<<20) )
        let nSort = parse.ParseInt( "-sort", -1 )
        let ver = if versionInfo.Length=0 then (DateTime.UtcNow) else StringTools.VersionFromString( versionInfo) 
        let mutable bExecute = false
        let keyfile = parse.ParseString( "-keyfile", "" )
        let keyfilepwd = parse.ParseString( "-keypwd", "" )
        let pwd = parse.ParseString( "-pwd", "" )
        let encrypt = parse.ParseBoolean( "-encrypt", false )
        let mutable bNoEcho = parse.ParseBoolean( "-noechoenc", false )

        if (countEchoArr = 0) then
            countEchoArr <- countEcho
        else if (countEchoArr < 0) then
            countEchoArr <- 1
            bNoEcho <- true

        DeploymentSettings.MaxSendingQueueLimit <- 1024 * 1024 * queueLen

        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))
    //    let job = Job()
    //    job.PopulateLoadedAssems()
    //    Logger.Log( LogLevel.Info, job.ToString() )

        //Added by XSHUA: two more parameters
        let defaultSearchPattern = "*.*"
        let searchPattern = parse.ParseString( "-spattern", defaultSearchPattern )
        let searchOption = if parse.ParseBoolean( "-rec", false ) then SearchOption.AllDirectories else SearchOption.TopDirectoryOnly
    
        let sha512hash (bytearray: byte[]) = 
            let sha512Hash = System.Security.Cryptography.SHA512.Create()
            let res = sha512Hash.ComputeHash( bytearray )
            BitConverter.ToInt64( res, 0 )

        let bAllParsed = parse.AllParsed Usage

        if bAllParsed then 
            Cluster.Start( PrajnaMasterFile, PrajnaClusterFile )
    //        match Cluster.Current with 
    //        | None ->
    //            Logger.Log( LogLevel.Error, sprintf "Can't find a valid cluster Master File=%s, Cluster File = %s" PrajnaMasterFile PrajnaClusterFile  )
    //            ()
    //        | Some( cluster ) ->
            let connects = Cluster.Connects
            if not (keyfilepwd.Equals("")) then
                connects.InitializeAuthentication(pwd, keyfile, keyfilepwd)
            let cluster = Cluster.GetCurrent()
            if true then
                if bExe then 
                    JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask
                let mutable nPeerSucceeded = 0
                let peerFailed = List<string>()
                let mutable t1 = (DateTime.UtcNow)
                if ( not bGen && not bVerify && bIn && localdir.Length>0 && remoteDKVname.Length>0 ) then 
                    if localdir.EndsWith(".xml") then 
                        // copy in file is an xml file
                        use file = new StreamReader( localdir )
                        let xdoc = new XmlDocument()  
                        xdoc.Load( file )
                    else
                        let store = DSet<string*byte[]>( Name = remoteDKVname, NumReplications = nrep, 
                                        Version = ver, SerializationLimit = slimit, 
                                        Password = password, 
                                        TypeOfLoadBalancer = typeOf, 
                                        PeerRcvdSpeedLimit = rcvdSpeedLimit )
                        if (not bUseAzure) then
                            store.StorageType <- StorageKind.HDD
                        else
                            store.StorageType <- StorageKind.Azure
                        if flag <> -100 then 
                            store.Flag <- enum<DSetFlag>(flag)
                        if nump<>0 then 
                            store.NumPartitions <- nump
                        let t1 = (DateTime.UtcNow)
                        let fseq, total, files = DSet.FolderRecursiveSeq( localdir, searchPattern, searchOption ) 
                        if not bHash then 
                            store.Store( fseq )
                        else
                            fseq |> 
                            Seq.map( fun (filename, bytearray) -> 
                                Logger.Log( LogLevel.Info, ( sprintf "File %s ... %dB (Hash %08x) " filename (filename.Length+bytearray.Length) ( sha512hash bytearray) )   )
                                filename, bytearray
                                )
                            |> store.Store
                        let t2 = (DateTime.UtcNow)
                        let elapse = t2.Subtract(t1)
                        Logger.Log( LogLevel.Info, ( sprintf "Processed %d Files with total %dB in %f sec, throughput = %f MB/s" files.Length !total elapse.TotalSeconds ((float !total)/elapse.TotalSeconds/1000000.) ))

                        ()
     //                   printfn "Partition mapping = %A" dkv.GetPartitionMapping()
                                            
    //                DKV.store localdir remotedir
                    bExecute <- true
                elif ( not bVerify && not bGen && bOut && localdir.Length>0 && remoteDKVname.Length>0 ) then 
                    let t1 = (DateTime.UtcNow)
                    let mutable curDKV = DSet<string*byte[]>( Name = remoteDKVname,
                                                      Version = ver, 
                                                      Password = password, 
                                                      PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                    if (not bUseAzure) then
                        curDKV.StorageType <- StorageKind.HDD
                    else
                        curDKV.StorageType <- StorageKind.Azure
                    if flag <> -100 then 
                        curDKV.Flag <- enum<DSetFlag>(flag)
                    // add search pattern first. 
                    if searchPattern <> defaultSearchPattern then 
                        curDKV <- curDKV |> DKV.filterByKey( fun fname -> System.Text.RegularExpressions.Regex.Match( fname , searchPattern ).Success )
                    let numFiles, total = 
                        if bAggregate then 
                            let count, total_size = curDKV 
                                                    |> DSet.map ( fun (fname, bytearray ) -> (1, uint64 bytearray.Length) )  
                                                    |> DSet.reduce ( fun (v1,l1) (v2,l2) -> (v1+v2), (l1+l2) )
                            count, total_size
                        elif not bHash then 
                            let o = curDKV.ToSeq()
                            DSet.RetrieveFolderRecursive( localdir, o )
                        else
                            let mutable hDKV = curDKV.MapByValue ( fun bytearray -> ( sha512hash bytearray), (bytearray.Length) ) 
                            if bCacheRAM then 
                                hDKV <- hDKV.CacheInMemory()
                            let hseq = hDKV.ToSeq()
                            let nFiles = ref 0
                            let nTotal = ref 0UL
                            hseq 
                            |> Seq.iter ( 
                                fun (filename, payload ) -> 
                                    let hash, flen = payload
                                    nTotal := !nTotal + (uint64 filename.Length) + (uint64 flen ) 
                                    nFiles := !nFiles + 1
                                    Logger.Log( LogLevel.Info, ( sprintf "File %s ... %dB (Hash %08x) " filename (filename.Length+flen) hash ))
                               )
                            ( !nFiles, !nTotal )
                    let t2 = (DateTime.UtcNow)
                    let elapse = t2.Subtract(t1)
                    Logger.Log( LogLevel.Info, ( sprintf "Processed %d Files with total %dB in %f sec, throughput = %f MB/s" numFiles total elapse.TotalSeconds ((float total)/elapse.TotalSeconds/1000000.) ))
                    bExecute <- true
                elif bGen && bTest then 
                    let rnd = System.Random(seed)
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> 
                       let arr = GaussianFunc.Random rnd 1000000
                       let sum = Array.fold ( fun sum x -> sum + x ) 0.0 arr
                       let sum2 = Array.fold ( fun sum x -> sum + x * x ) 0.0 arr
                       sprintf "Test Gaussian, sum of avg, variance of 1000000 vectors %f, %f" sum sum2 ))

                    let meanArray = Array.init nClasses ( fun _ -> UniformFunc.Random rnd dim )
                    let varArray = Array.create nClasses var
                    let probArray = LaplacianFunc.Random rnd nClasses
                    let cumArrayPre = probArray |> Array.scan ( fun sum x -> sum + x ) 0.0 
                    let cumValue = cumArrayPre.[ cumArrayPre.Length - 1 ]
                    let cumArray = Array.sub(cumArrayPre) 1 ( cumArrayPre.Length - 1 ) |> Array.map ( fun x -> x/cumValue ) 
                    let t1 = (DateTime.UtcNow)
                    let genVector = Array.init num ( fun p -> let v = rnd.NextDouble() 
                                                              let idx = ref 0
                                                              for i = 0 to cumArray.Length - 1 do
                                                                if v > cumArray.[i] then 
                                                                    idx := i + 1 
                                                              let genV = GaussianFunc.RandomMV2 rnd meanArray.[!idx] varArray.[!idx]
                                                              Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Pt %d, distance to generated vector: %f, variance: %f" p (ArrayFunc.Distance genV meanArray.[!idx]) (varArray.[!idx]) ))
                                                              genV
                                                   )
                    let t2 = (DateTime.UtcNow)
                    let elapse = t2.Subtract(t1)
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Generate %d gaussian vector of dimension %d with %f secs" num dim elapse.TotalSeconds ))
                    for i = 0 to meanArray.Length - 1 do
                        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Mean of class %d : %A" i meanArray.[i]  ))
                        let distArray = Array.map ( fun mean -> ArrayFunc.Distance mean meanArray.[i] ) meanArray 
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Mean of class %d distance matrix:%A " i distArray ))

                    for i = 0 to genVector.Length - 1 do
                        let distArray = Array.map ( fun mean -> ArrayFunc.Distance genVector.[i] mean) meanArray 
                        printfn "Vector %d : distance %A" i distArray 
                    bExecute <- true
                elif uploadFile.Length>0 && remoteDKVname.Length>0 then 
                    let store = DSet<string*string[]>( Name = remoteDKVname, NumReplications = nrep, 
                                    Version = ver, SerializationLimit = slimit, 
                                    Password = password, 
                                    TypeOfLoadBalancer = typeOf, 
                                    PeerRcvdSpeedLimit = rcvdSpeedLimit )
                    if (not bUseAzure) then
                        store.StorageType <- StorageKind.HDD
                    else
                        store.StorageType <- StorageKind.Azure
                    if flag <> -100 then 
                        store.Flag <- enum<DSetFlag>(flag)
                    if nump<>0 then 
                        store.NumPartitions <- nump
                    let t1 = (DateTime.UtcNow)
                    let total = ref 0L
                    let lines = ref 0 
                    use reader = new StreamReader( uploadFile )
                    let toSave = 
                        seq { 
                            while not reader.EndOfStream do 
                                let line = reader.ReadLine()
                                let strArray = line.Split( uploadSplit.ToCharArray(), StringSplitOptions.RemoveEmptyEntries )
                                if strArray.Length> uploadKey then 
                                    yield ( strArray.[uploadKey], strArray )
                                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Upload %s" strArray.[uploadKey] ))
                                    lines := !lines + 1
                                    total := !total + ( int64 ( strArray.[uploadKey].Length + line.Length ))
                                    if ( !lines % 1000 = 0 ) then 
                                        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Upload %s" strArray.[uploadKey] ))
                        }
                    toSave |> store.Store
                    let t2 = (DateTime.UtcNow)
                    let elapse = t2.Subtract(t1)
                    Logger.Log( LogLevel.Info, ( sprintf "Processed %d Lines with total %dB in %f sec, throughput = %f MB/s" !lines !total elapse.TotalSeconds ((float !total)/elapse.TotalSeconds/1000000.) ))
                    bExecute <- true
                elif downloadLimit>0 && remoteDKVname.Length>0 then 
                    let downloadHttp parti serial (url:string, value) = 
                        async {
                            try 
                                let req = WebRequest.Create(url)
                                let! respAsync = req.AsyncGetResponse()
                                // To automatically dispose respAsync when it is out of scope
                                use resp = respAsync
                                let contentLength = int resp.ContentLength
                                let stream = resp.GetResponseStream()
                                let rcvdBuffer = new ExpandableBuffer( (if contentLength < 0 then 1<<<20 else contentLength), 0 )
                                let pos = ref 0
                                let lastRead = ref 1024 
                                let maxReadLen = downloadChunk
                                while (!lastRead)>0 do
                                    let readLen = if contentLength < 0 then maxReadLen else Math.Min( contentLength - rcvdBuffer.Tail, maxReadLen ) 
                                    // It is guarantteed that we can read readLen bytes afterwards.
                                    rcvdBuffer.TryExtendTail( readLen ) 
                                    let buf, pos, _ = rcvdBuffer.GetBufferAfterTail()
                                    let! readin = stream.AsyncRead( buf, pos, readLen ) 
                                    if readin > 0 then 
                                        rcvdBuffer.ExtendTail( readin )
                                    lastRead := readin
                                let bytearr = rcvdBuffer.GetTruncatedBuffer()
                                let msg = sprintf "%d, %d, download %s with %dB" parti serial url bytearr.Length
                                return url, ( value, bytearr, msg)
                            with
                            | e ->
                                let errorMsg = sprintf "%d, %d, download %s error, with exception %A" parti serial url e
                                Logger.Log( LogLevel.WildVerbose, errorMsg )
                                return url, ( value, null, errorMsg )
                        }
                    let srcDKV = DSet<string*string[]>( Name = remoteDKVname, 
                                                            PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                    // Calculating download limit
                    let httpDKV = srcDKV |> DSet.reorgWDegree downloadLimit |> DSet.asyncMapi downloadHttp
                    // Split the DKV into 2, one for retrieve status information, one to save to destination. 
                    let infoDKV, saveDKV = httpDKV |> DSet.bypass2             
                    let saveDKV1 = saveDKV |> DSet.choose ( fun x -> let url, tuple = x 
                                                                     let value, bytearr, msg = tuple
                                                                     if Utils.IsNotNull bytearr then 
                                                                        Some ( url, (bytearr, value) )
                                                                     else
                                                                        None )
                    let imgDKV, metaDKV = saveDKV1 |> DSet.split2 ( fun (url, (bytearray, value )) -> url, bytearray )
                                                                  ( fun (url, (bytearray, value )) -> url, value )
                    let imgStream = imgDKV |> DSet.lazySaveToHDDByName (remoteDKVname + ".download.async") 
                    let metaStream = metaDKV |> DSet.lazySaveToHDDByName ( remoteDKVname + ".meta.async" ) 
                    let msgDKV = infoDKV |> DSet.map ( fun (url, tuple) -> let _, bytearr, msg = tuple
                                                                           msg, if Utils.IsNull bytearr then 0 else bytearr.Length )
                    let msgSeq = msgDKV.ToSeq()
                    let nDownloads = ref 0
                    let nTotal = ref 0UL
                    let t1 = (DateTime.UtcNow)
                    msgSeq 
                    |> Seq.iter ( 
                        fun (msg, len ) -> 
                            nTotal := !nTotal + (uint64 len ) 
                            if len > 0 then 
                                nDownloads := !nDownloads + 1
                            Logger.Log( LogLevel.Info, ( sprintf "%d,%dMB %s" !nDownloads (!nTotal>>>20) msg ))
                        )
                    let t2 = (DateTime.UtcNow)
                    let elapseSecond = t2.Subtract(t1).TotalSeconds
                    Logger.Log( LogLevel.Info, ( sprintf "Total download %d files, %dMB in %f sec, throughput=%fMB/s" !nDownloads (!nTotal>>>20) elapseSecond (float (!nTotal>>>20)/elapseSecond) ))
                    bExecute <- true
                elif bGen && bIn then 
                    let t1 = (DateTime.UtcNow)
                    let startDKV = DSet<_>( Name = "VectorGen", NumReplications = nrep, 
                                            Version = ver, SerializationLimit = slimit, 
                                            Password = password, 
                                            TypeOfLoadBalancer = typeOf, 
                                            PeerRcvdSpeedLimit = rcvdSpeedLimit )
                    let numPartitions = if nump > 0 then startDKV.NumPartitions <- nump 
                                                         nump 
                                                    else startDKV.SetupPartitionMapping()
                                                         startDKV.NumPartitions
                    let partitionSizeFunc( parti ) = 
                        let ba = num / numPartitions
                        if parti < ( num % numPartitions ) then ba + 1 else ba
                    let rnd = Random(seed)
                    let meanArray = Array.init nClasses ( fun _ -> UniformFunc.Random rnd dim )
                    let varArray = Array.create nClasses var
                    let probArray = LaplacianFunc.Random rnd nClasses
                    let cumArrayPre = probArray |> Array.scan ( fun sum x -> sum + x ) 0.0 
                    let cumValue = cumArrayPre.[ cumArrayPre.Length - 1 ]
                    let cumArray = Array.sub(cumArrayPre) 1 ( cumArrayPre.Length - 1 ) |> Array.map ( fun x -> x/cumValue ) 
                    if Utils.IsNotNull localdir && localdir.Length>0 then 
                        use writeFile = FileTools.CreateFileStreamForWrite( localdir ) 
                        let fmt = Formatters.Binary.BinaryFormatter()
                        fmt.Serialize( writeFile, meanArray ) 
                        fmt.Serialize( writeFile, varArray )
                        fmt.Serialize( writeFile, cumArray )
                        writeFile.Flush()
                    for i = 0 to meanArray.Length - 1 do
                        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Mean of class %d : %A" i meanArray.[i]  ))
                        let distArray = Array.map ( fun mean -> ArrayFunc.Distance mean meanArray.[i] ) meanArray 
                        if nClasses < 10 then 
                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Mean of class %d distance matrix:%A " i distArray ))
                        else
                            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Mean of class %d distance matrix:%A " i distArray ))

                    let initFunc( parti, serial ) = 
                        // each generated vector is with a different random seed
                        let seed = serial * numPartitions + parti
                        let rnd = Random( serial * numPartitions + parti )
                        let v = rnd.NextDouble() 
                        let idx = ref 0
                        for i = 0 to cumArray.Length - 1 do
                            if v > cumArray.[i] then 
                                idx := i + 1 
                        let genV = GaussianFunc.RandomMV2 rnd meanArray.[!idx] varArray.[!idx]
                        seed, genV   
                                      
                    let dkvGen0 = startDKV |> DSet.init initFunc partitionSizeFunc 
                    let dkvGen = dkvGen0 |> DSet.rowsReorg ( 55 )
    //                Logger.LogF( LogLevel.Info,  fun _ -> sprintf "Mean Array : %A" meanArray  )
                    let reportDKV = 
                        if Utils.IsNotNull remoteDKVname && remoteDKVname.Length>0 then 
                            let mDKV = dkvGen |> DSet.map( fun (seed, genV) -> (seed, findClosestFunc( genV, meanArray)), genV )
                            let rDKV, s1 = mDKV |> DSet.split2 ( fun ((seed,cl), genV) -> seed, cl )
                                                                  ( fun tuple -> tuple )
                            let saveDKV = 
                                if bMapReduce then 
                                    let m1, m2 = s1 |> DSet.bypass2 
                                    let mapReduce = m2 |> DSet.mapReduce ( fun (info, genV) -> let _, classi = info
                                                                                               // Logger.LogF( LogLevel.Info,  fun _ -> sprintf "map %A to class %d" genV classi  )
                                                                                               Seq.singleton( classi, genV ) )
                                                            ( fun (cl, vecList) -> (cl, vecList.ToArray() ) ) 
                                    mapReduce |> DSet.lazySaveToHDDByName (remoteDKVname+"_MapReduce") |> ignore
                                    m1
                                else
                                    s1
                            if noise < 0.0 then 
                                saveDKV |> DSet.lazySaveToHDDByName remoteDKVname |> ignore
    //                        saveDKV.Name <- remoteDKVname
    //                        let stream = saveDKV.Save()
                            else 
                                let s1, s2 = saveDKV |>  DSet.split2 ( fun ((seed,cl), genV) -> seed, genV ) 
                                                                    ( fun ((seed,cl), genV) -> let rnd = RandomWithSalt( seed ) 
                                                                                               ( seed, genV |> Array.map( fun v -> v + ( rnd.NextDouble() - 0.5) * noise ) ) )
                                s1 |> DSet.lazySaveToHDDByName remoteDKVname |> ignore
                                s2 |> DSet.lazySaveToHDDByName (remoteDKVname+"_Noise") |> ignore
                            rDKV
                        else
                            dkvGen |> DSet.map( fun (seed, genV) -> (seed, findClosestFunc( genV, meanArray) ) )
                    // Ease debugging by allow only one execution. 
    //                reportDKV.NumParallelExecution <- 1
                    let msgSeq = reportDKV.ToSeq()
                    let nTotal = ref 0
                    let nClassCount = Array.zeroCreate<_> meanArray.Length
                    msgSeq 
                    |> Seq.iter ( 
                        fun (seed, classi ) -> 
                            nTotal := !nTotal + 1 
                            nClassCount.[classi] <- nClassCount.[classi] + 1 
                            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Seed %d -> class %d" seed classi ))
                        )
                    let t2 = (DateTime.UtcNow)
                    let elapse = t2.Subtract(t1)
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Generate %d gaussian vector of dimension %d with %f secs, class distribution %A, cumulative prob %A" !nTotal dim elapse.TotalSeconds nClassCount cumArray))
                    bExecute <- true
                    ()
                elif bGen && bOut && Utils.IsNotNull localdir && localdir.Length>0 then 
                    let t1 = (DateTime.UtcNow)
                    let startDKV = DSet<int*float[]>( Name = remoteDKVname, PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                    let readFile = FileTools.CreateFileStreamForRead( localdir ) 
                    let fmt = Formatters.Binary.BinaryFormatter()
                    let meanArray = fmt.Deserialize( readFile ) :?> float[][]
                    let varArray = fmt.Deserialize( readFile ) :?> float[]
                    let cumArray = fmt.Deserialize( readFile ) :?> float[]
                    readFile.Close()
                                    
                    let reportArr = startDKV 
                                    |> DSet.map ( fun (seed, genV) -> let countarray = Array.zeroCreate<int> meanArray.Length
                                                                      let id = findClosestFunc( genV, meanArray)
                                                                      countarray.[id] <- countarray.[id] + 1 
                                                                      countarray )
                                    |> DSet.reduce ( fun (c1:int[]) (c2:int[]) -> Array.map2( fun v1 v2 -> v1 + v2 ) c1 c2 )
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Gaussian vector of dimension %d class distribution %A" meanArray.Length reportArr))
                    bExecute <- true
                elif bVerify && bOut && Utils.IsNotNull localdir && localdir.Length>0 then 
                    let t1 = (DateTime.UtcNow)
                    let startDKV = DSet<int*float[][]>( Name = remoteDKVname, PeerRcvdSpeedLimit = rcvdSpeedLimit ) |> DSet.loadSource
                    let readFile = FileTools.CreateFileStreamForRead( localdir ) 
                    let fmt = Formatters.Binary.BinaryFormatter()
                    let meanArray = fmt.Deserialize( readFile ) :?> float[][]
                    let varArray = fmt.Deserialize( readFile ) :?> float[]
                    let cumArray = fmt.Deserialize( readFile ) :?> float[]
                    readFile.Close()
                    let reportArr = startDKV.Fold( ( fun (c:Dictionary<_,_>) (cl, genVarr) ->  let countarray = if Utils.IsNull c then Dictionary<_,_>() else c
                                                                                               let idArr = genVarr |> Array.map ( fun genV -> findClosestFunc( genV, meanArray) )
                                                                                               let total = idArr.Length
                                                                                               let inCorrect = idArr |> Array.filter ( fun id -> id<>cl ) 
                                                                                               if not (countarray.ContainsKey( cl )) then 
                                                                                                  countarray.Item(cl) <- (total, inCorrect, 1)
                                                                                               else
                                                                                                  let total1, arr1, aggCount = countarray.Item(cl)
                                                                                                  countarray.Item(cl) <- (total + total1, ( Seq.append arr1 inCorrect) |> Seq.toArray, aggCount + 1  ) 
                                                                                               countarray ),
                                                     ( fun c1 c2 ->     if Utils.IsNotNull c1 && Utils.IsNotNull c2 then 
                                                                            for item in c2 do 
                                                                              let cl = item.Key
                                                                              let total1, incor1, agg1 = item.Value
                                                                              if c1.ContainsKey(cl) then 
                                                                                 let total2, incor2, agg2 = c1.Item(cl) 
                                                                                 c1.Item( cl ) <- ( total1+total2, (Seq.append incor1 incor2) |> Seq.toArray, agg1 + agg2 )
                                                                              else
                                                                                 c1.Item( cl ) <- ( total1, incor1, agg1 )
                                                                            c1
                                                                        elif Utils.IsNotNull c1 then 
                                                                            c1
                                                                        elif Utils.IsNotNull c2 then 
                                                                            c2
                                                                        else 
                                                                            null ),
                                                     (null))
                    startDKV.NumParallelExecution <- 1                                                                                                                                
                    let distribution = Array.zeroCreate<_> meanArray.Length
                    let sortArr = SortedDictionary<_,_>( reportArr )
                    for pair in sortArr do 
                        let cl = pair.Key
                        let total, incor, aggcount = pair.Value
                        distribution.[cl] <- total
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Gaussian vector of dimension %d, distribution: %A " meanArray.Length distribution))
                    for pair in sortArr do 
                        let cl = pair.Key
                        let total, incor, aggcount = pair.Value
                        if incor.Length > 0 || aggcount <> 1 then 
                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Class %d: Total %d, Incorrect mapping to: %A, with aggregation count: %d" cl total incor aggcount ))
                    bExecute <- true
                else 
                    ()
            
                let numNodes = if Utils.IsNull cluster then 0 else cluster.ClusterInfo.ListOfClients.Length
                nEcho <- Math.Min( nEcho, numNodes )
                if nEcho>0 then 
                    let listOfClients = cluster.ClusterInfo.ListOfClients
                    // Echo Testing ...
                    // Prepare echo array
                    // round up lenEcho to nearest multiple of 8
                    let lenEcho = (lenEcho + 7) &&& 0xfffffff8
                    nPeerSucceeded <- nEcho
                    let cmd = 
                        if (bNoEcho) then
                            // send a message which is ignored
                            new ControllerCommand( ControllerVerb.Nothing, ControllerNoun.Message )
                        else
                            new ControllerCommand( ControllerVerb.Echo, ControllerNoun.Message ) 
                    let echoArrays = Array.zeroCreate<uint64[]> countEchoArr
                    let offset = (sizeof<int>)+2*(sizeof<int64>)
                    let msArrays = Array.init<MemStream> 
                                    countEchoArr
                                    ( fun _ ->
                                        let buffer = Array.zeroCreate<byte>(offset+lenEcho) 
                                        new MemStream(buffer, 0, buffer.Length, true, true) )
                    for i = 0 to countEchoArr-1 do
                        echoArrays.[i] <- Array.zeroCreate<uint64> (lenEcho/8)
                        let mutable h = 0UL
                        msArrays.[i].Seek(2L*(int64 sizeof<int64>), SeekOrigin.Begin) |> ignore
                        msArrays.[i].WriteInt32(echoArrays.[i].Length)
                        for n = 0 to echoArrays.[i].Length-1 do
            //                h <- MurmurHash.MurmurHash64OneStep( h, uint64 n )
                            h <- uint64 ( i ) * uint64 ( lenEcho/8) + uint64( n )
                            echoArrays.[i].[n] <- h
                            msArrays.[i].WriteUInt64(h)
                    let t2 = (DateTime.UtcNow)
                    let elapse = t2.Subtract(t1)
                    printfn "Data preparation time after connection = %A" elapse
                    t1 <- t2
                    // Echo
                    // Prepare Echo Channels
                    let connects = Cluster.Connects
                    connects.Initialize()
                    let nSize = nEcho
                    let queues = Array.zeroCreate<NetworkCommandQueue> nSize
                    let countSend = Array.create<int> nEcho countEcho 
                    let countRcvd = Array.create<int> nEcho countEcho 
                    let nTotalRcvd = Array.zeroCreate<uint64> nEcho 
                    let bError = ref false
                    for n = 0 to nSize-1 do
                        let client = listOfClients.[n]
                        queues.[n] <- connects.AddConnect( client.MachineName, client.MachinePort )
                        // processing happens below, leave it here for now
                        queues.[n].AddRecvProc (parseEcho queues.[n] countEcho countEchoArr countRcvd nTotalRcvd echoArrays bError lenEcho n) |> ignore
                        queues.[n].Initialize()
                        if Utils.IsNotNull queues.[n] then 
                            queues.[n].MaxTokenSize <- int64 bucketSize
                    let t2 = (DateTime.UtcNow)
                    let elapse = t2.Subtract(t1)
                    printfn "Execution Time after connection = %A" elapse
                    t1 <- t2
                    let mutable maxRemainingSend = countEcho
                    let mutable maxRemainingRcvd = countEcho
                    while not !bError && ( maxRemainingSend>0 || maxRemainingRcvd>0 ) do
                        if (connects.ChannelsCollection.Count = 0) then
                            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "All channels closed prior to finishing"))
                            bError := true
                        let lastMaxRemainingSend = maxRemainingSend
                        let lastMaxRemainingRcvd = maxRemainingRcvd
                        let allowDiscrepency = ( 100000 + lenEcho ) / lenEcho
                        maxRemainingSend <- Int32.MinValue
                        maxRemainingRcvd <- Int32.MinValue
                        if (sleepBetweenEchoMs <> 0) then
                            Thread.Sleep(sleepBetweenEchoMs)
                        for n = 0 to nEcho-1 do
                            let queue = queues.[n]
                            if queue.CanSend && queue.SendQueueLength<5 && countSend.[n]>0  then  
                                let sendQ = queue.CompSend.Q :?> Queue.FixedSizeQ<NetworkCommand>
                                let cnt = countEcho - countSend.[n]
                                let ecnt = cnt % countEchoArr
                                if queue.UnProcessedCmdInBytes + 8L + msArrays.[ecnt].Length <= sendQ.MaxSize / 2L ||
                                    queue.SendCommandQueueLength = 0
    //                                && (lastMaxRemainingSend - countSend.[n] ) <= allowDiscrepency 
    //                                && (lastMaxRemainingRcvd - countRcvd.[n] ) <= allowDiscrepency
                                    then 
                                        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Send packet %d to %s" cnt queue.EPInfo))
                                        msArrays.[ecnt].Seek(int64 sizeof<int64>, SeekOrigin.Begin) |> ignore
                                        msArrays.[ecnt].WriteInt64(PerfADateTime.UtcNowTicks())
                                        msArrays.[ecnt].Seek(0L, SeekOrigin.End) |> ignore
                                        if (encrypt) then
                                            queue.ToSendEncrypt(cmd, msArrays.[ecnt].GetBuffer(), 0, int msArrays.[ecnt].Length)
                                        else
                                            queue.ToSend( cmd, msArrays.[ecnt] ) |> ignore
                                        countSend.[n] <- countSend.[n] - 1
                                //else
                                //    Thread.Sleep(1) // spend some time if can't send
                            if countSend.[n]=0 && (countRcvd.[n]=0 || bNoEcho) then 
                                // Make sure queue is removed only once
                                Logger.Log( LogLevel.Info, (sprintf "Peer %d Client %s bytes sent & received = %d, %d" n queue.EPInfo queue.TotalBytesSent queue.TotalBytesRcvd ))
                                queue.Close()
                                connects.RemoveConnect( queue )
                                countRcvd.[n] <- -1
                            maxRemainingSend <- Math.Max( countSend.[n], maxRemainingSend )
                            maxRemainingRcvd <- Math.Max( countRcvd.[n], maxRemainingRcvd )
                    bExecute <- true
                    Logger.Log( LogLevel.Info, (sprintf "Number of Peers that succeeds echo %d" nPeerSucceeded ))
                    if peerFailed.Count > 0 then 
                        Logger.Log( LogLevel.Info, (sprintf "Peer failed are: %A" peerFailed ))
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.Log( LogLevel.Info, (sprintf "Execution Time for the last segment = %A" elapse ))
                ()
            Cluster.Stop()
        // Make sure we don't print the usage information twice. 
        if not bExecute && bAllParsed then 
            parse.PrintUsage Usage
        0
