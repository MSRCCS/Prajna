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
        DistributedSort.fs
  
    Description: 
        Distributed Sort. Distributedly generating random vector of bytearray and then sort the bytearray distributedly. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        June. 2014
    
 ---------------------------------------------------------------------------*)
open System
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading
open System.Diagnostics
open System.IO
open System.Net
open System.Runtime.Serialization
open System.Threading.Tasks
open System.Runtime.InteropServices
open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp

let Usage = "
    Usage: Distributed Sort. Distributedly generating random vector of bytearray and then sort the bytearray distributedly.  \n\
    Command line arguments:\n\
    -cluster    Name of cluster used \n\
    -clusterlst The cluster list file \n\
    -in         Copy into Prajna \n\
    -out        Copy outof Prajna \n\
    -local      Local directory. All files in the directories will be copy to (or from) remote \n\
    -remote     Name of the distributed Prajna folder\n\
    -ver V      Select a particular DKV with Version string: in format yyMMdd_HHmmss.fff \n\
    -rep REP    Number of Replication \n\
    -slimit S   # of record to serialize \n\
    -balancer B Type of load balancer \n\
    -nump N     Number of partitions \n\
    -flag FLAG  DKVFlag \n\
    -speed S    Limiting speed of each peer to S bps\n\
    -num        Number of vectors to be generated\n\
    -dim        Dimension of vectors to be generated\n\
    -sort       Executing Sort. \n\
    -seed SEED  seed of random generator \n\
    -parallel P # of parallel execution \n\
    -mapreduce  sort via mapreduce \n\
    "

module AssemblyProperties =
// Signs the assembly in F#
    open System
    open System.Reflection;
    open System.Runtime.InteropServices;
    
#if DEBUG
    [<assembly: AssemblyConfiguration("Debug")>]
#else
    [<assembly: AssemblyConfiguration("Release")>]
#endif 
    do()

module InteropWithMsvcrt =
    [<DllImport("msvcrt.dll", CallingConvention=CallingConvention.Cdecl)>]
    /// Return: >0 if b1 > b2
    ///         =0 if b1 = b2 
    ///         <0 if b1 < b2
    extern int memcmp(byte[] b1, byte[] b2, int64 count);
    
    let compareBytes (b1:byte[]) (b2:byte[]) =
        let ret = memcmp( b1, b2, int64 b1.Length )
        if ret<>0 then ret else (b1.Length - b2.Length )

    let BytesComparer = 
        { new IComparer<byte[]> with 
            member self.Compare( b1, b2 ) = 
                compareBytes b1 b2 
        }

type RndGeneration() = 
    static member val RndGen = ConcurrentDictionary<_,Random>() with get
    static member GetRandomGen( parti: int, seedi ) = 
        RndGeneration.RndGen.GetOrAdd( parti, fun parti -> Random( seedi ) )


[<AllowNullLiteral>]
type VerifySort (nSort:int) = 
    member val Count = 0L with get, set
    member val MinBytes = ( Array.create nSort 255uy) with get, set
    member val MaxBytes = (Array.zeroCreate<byte> nSort) with get, set
    member val LastBytes = (Array.zeroCreate<byte> nSort) with get, set
    member val NumOutOfOrder = 0L with get, set
    member val Range = SortedDictionary<_,_>( InteropWithMsvcrt.BytesComparer ) with get
    member x.VerifyOne (buf:byte[]) = 
        let nCompareToLast = InteropWithMsvcrt.compareBytes buf x.LastBytes  
        if nCompareToLast < 0 then 
            // New variable should be larger than last one. 
            x.NumOutOfOrder <- x.NumOutOfOrder + 1L
        if InteropWithMsvcrt.compareBytes buf x.MinBytes < 0 then 
            x.MinBytes <- buf
        if InteropWithMsvcrt.compareBytes buf x.MaxBytes > 0 then 
            x.MaxBytes <- buf
        x.LastBytes <- buf
    static member Fold nSortConstruct (x:VerifySort) (buf:byte[]) = 
        let y = if Utils.IsNull x then VerifySort( nSortConstruct ) else x
        y.VerifyOne( buf ) 
        y.Count <- y.Count + 1L
        y
    static member Aggregate (x1:VerifySort) (x2:VerifySort) = 
        if x1.Range.Count = 0 then 
            x1.Range.Item( x1.MinBytes) <- x1.MaxBytes
        if x2.Range.Count = 0 then 
            x2.Range.Item( x2.MinBytes) <- x2.MaxBytes
        for pair in x2.Range do
            x1.Range.Item( pair.Key ) <- pair.Value
        x1.NumOutOfOrder <- x1.NumOutOfOrder + x2.NumOutOfOrder
        x1.Count <- x1.Count + x2.Count
        x1
    member x.Validate() = 
        let mutable lastBytes = Array.zeroCreate<byte> nSort
        let mutable nOverlappedRange = 0 
        for pair in x.Range do 
            if InteropWithMsvcrt.compareBytes pair.Key lastBytes < 0 then 
                nOverlappedRange <- nOverlappedRange + 1
            lastBytes <- pair.Value
        x.Count, nOverlappedRange, x.NumOutOfOrder    
    member x.ShowOverlappedRange() = 
        let zeroBytes = Array.zeroCreate<byte> nSort
        let lastRange = ref (KeyValuePair(zeroBytes, zeroBytes ))
        seq {
            for pair in x.Range do 
                if InteropWithMsvcrt.compareBytes pair.Key (!lastRange).Value < 0 then 
                    yield (sprintf "Overlapped Partitions %A-%A with %A-%A" (!lastRange).Key (!lastRange).Value pair.Key pair.Value )    
                lastRange := pair
            } |> String.concat Environment.NewLine

// Define your library scripting code here
[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let PrajnaClusterListFile = parse.ParseString( "-clusterlst", "")
    let localdir = parse.ParseString( "-local", "" )
    let remoteDKVname = parse.ParseString( "-remote", "" )
    let nrep = parse.ParseInt( "-rep", 3 )
    let typeOf = enum<LoadBalanceAlgorithm>(parse.ParseInt( "-balancer", 0) )
    let slimit = parse.ParseInt( "-slimit", 10 )
    let nParallel = parse.ParseInt( "-parallel", 0 )
    let password = parse.ParseString( "-password", "" )
    let rcvdSpeedLimit = parse.ParseInt64( "-speed", 40000000000L )
    let flag = parse.ParseInt( "-flag", -100 )
    let nump = parse.ParseInt( "-nump", 80 )
    let bExe = parse.ParseBoolean( "-exe", false )
    let versionInfo = parse.ParseString( "-ver", "" )
    let ver = if versionInfo.Length=0 then (DateTime.UtcNow) else StringTools.VersionFromString( versionInfo) 
    let nDim = parse.ParseInt( "-dim", 80 )
    let bSort = parse.ParseBoolean( "-sort", false )
    let bRepartition = parse.ParseBoolean( "-rpart", false )
    let bMapReduce = parse.ParseBoolean( "-mapreduce", false )
    let seed = parse.ParseInt( "-seed", 0 )
    let num = parse.ParseInt64( "-num", 1000000L )
    let bIn = parse.ParseBoolean( "-in", false )
    let bOut = parse.ParseBoolean( "-out", false )
    let nRand = parse.ParseInt( "-nrand", 16 )
    
    let mutable bExecute = false

    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))

    let defaultSearchPattern = "*.*"
    let searchPattern = parse.ParseString( "-spattern", defaultSearchPattern )
    let searchOption = if ( parse.ParseBoolean( "-rec", false ) ) then SearchOption.AllDirectories else SearchOption.TopDirectoryOnly
    
    let sha512hash (bytearray: byte[]) = 
        let sha512Hash = System.Security.Cryptography.SHA512.Create()
        let res = sha512Hash.ComputeHash( bytearray )
        BitConverter.ToInt64( res, 0 )

    let allArray = Array.init<byte[]> 256 (fun i ->
        let arr = Array.zeroCreate<byte>(nDim)
        arr.[0] <- byte i
        arr
    )

    // network test
    let repartitionTest() =
        let t1 = (DateTime.UtcNow)
        let startDSet = DSet<_>( Name = "SortGen", NumReplications = nrep, 
                                Version = ver, SerializationLimit = slimit, 
                                Password = password, 
                                TypeOfLoadBalancer = typeOf, 
                                PeerRcvdSpeedLimit = rcvdSpeedLimit )
        let finalPartitions = 256
        if nParallel > 0 then 
            startDSet.NumParallelExecution <- nParallel
            startDSet.NumPartitions <- startDSet.Cluster.NumNodes*startDSet.NumParallelExecution
        if nump > 0 then
            startDSet.NumPartitions <- nump
            startDSet.NumParallelExecution <- nump / startDSet.Cluster.NumNodes
        let numPartitions = startDSet.NumPartitions
        let partitionSizeFunc (numPartitions) ( parti ) = 
            let ba = int (num / int64 numPartitions) 
            if parti < int ( num % int64 numPartitions ) then ba + 1 else ba
        let initVectorFunc (parti, serial) =
            allArray.[serial % 256]
//            let key = Array.zeroCreate<byte> nDim
//            key.[0] <- byte(serial)
//            key
        let partitionFunc (kv:byte[]) = 
            int kv.[0]
        let dkvGen = startDSet.InitN(initVectorFunc, partitionSizeFunc)
        //let dkvGen = startDSet |> DSet.init initVectorFunc partitionSizeFunc 
        //let dkvGenRAM = dkvGen |> DSet.cacheInRAM
        let repartParam = DParam ( PreGroupByReserialization = 0, NumPartitions = finalPartitions )
        let dkvRepart = dkvGen.RepartitionP(repartParam, partitionFunc)
        let t3 = DateTime.UtcNow
        //let verifyCount = dkvGen |> DSet.fold (fun cnt x -> cnt + 1) (fun a b -> a + b) 0
        // allow us to set breakpoint inside
//        let foldFunc = (fun cnt x ->
//            cnt + 1
//        )
        let verifyCount = dkvRepart |> DSet.fold (fun cnt x -> cnt + 1) (fun a b -> a + b) 0
        //let verifyCount = dkvRepart |> DSet.fold foldFunc foldFunc 0
        let t2 = DateTime.UtcNow
        let elapse = t2.Subtract(t3)
        let throughput = (float num * float nDim / 1000000. / elapse.TotalSeconds)
        let perNodeThr = throughput * 8.0 / (float startDSet.Cluster.NumNodes) / 1000.0
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Repartition %d vectors of size %d with %f secs, throughput = %f MB/s pernode = %f Gbit/sec validation = %A" 
                                                   num nDim elapse.TotalSeconds throughput perNodeThr verifyCount ))
        // repeat on RAM cached
//        let t3 = DateTime.UtcNow
//        let dkvRepart = dkvGenRAM.RepartitionParam repartParam partitionFunc
//        let verifyCount = dkvRepart |> DSet.fold (fun cnt x -> cnt + 1) (fun a b -> a + b) 0
//        let elapse = t2.Subtract(t3)
//        Logger.LogF(LogLevel.Info, ( fun _ -> sprintf "Repartition %d vectors of size %d with %f secs, throughput = %f MB/s validation = %A" 
//                                                    num nDim elapse.TotalSeconds (float num * float nDim / 1000000. / elapse.TotalSeconds) verifyCount ))
//

//    let rnd = new Random()
//    let indexarr = Array.init 100000000 ( fun i -> rnd.Next(10000) )
//    let t1 = (DateTime.UtcNow)
//    let arr = Array.zeroCreate<int> 10000
//    indexarr |> Array.iter ( fun i -> arr.[i] <- arr.[i] + 1 )
//    let t2 = (DateTime.UtcNow)
//    let elapse1 = t2.Subtract(t1).TotalSeconds
//    let arr1 = System.Collections.Concurrent.ConcurrentDictionary<int,int>()
//    indexarr |> Array.iter ( fun i -> arr1.AddOrUpdate( i, 1, fun k v -> v + 1 ) |>ignore )
//    let t3 = System.(DateTime.UtcNow)
//    let elapse2 = t3.Subtract(t2).TotalSeconds
//    printfn "Using array time : %f sec, concurrent array time %f sec" elapse1 elapse2


    let bAllParsed = parse.AllParsed Usage

    if bAllParsed then 
        if not (String.IsNullOrEmpty PrajnaClusterListFile) then
            Cluster.StartCluster( PrajnaClusterListFile )
        else
            Cluster.StartCluster( PrajnaClusterFile )
        let cluster = Cluster.GetCurrent()
        if true then
            if bExe then 
                JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask

            let mutable t1 = (DateTime.UtcNow)
            if bRepartition then
                repartitionTest()
                bExecute <- true
            if bSort then 
                let t1 = (DateTime.UtcNow)
                let startDSet = DSet<_>( Name = "SortGen", NumReplications = nrep, 
                                        Version = ver, SerializationLimit = slimit, 
                                        Password = password, 
                                        TypeOfLoadBalancer = typeOf, 
                                        PeerRcvdSpeedLimit = rcvdSpeedLimit )
                let numPartitions = cluster.NumNodes // Partition is fixed by DSet.init
                let PartitionsAfterSort = 256
                if nParallel > 0 then 
                    startDSet.NumParallelExecution <- nParallel    
//                    if nump > 0 then 
//                        startDSet.NumPartitions <- nump
                                                     
                let partitionSizeSortFunc( parti ) = 
                    let ba = int (num / int64 numPartitions) 
                    if parti < int ( num % int64 numPartitions ) then ba + 1 else ba
                let initSortFunc( parti, serial ) = 
                    // each generated vector is with a different random seed
                    let rnd = RndGeneration.GetRandomGen( parti, serial )
                    let key = Array.zeroCreate<byte> nDim
                    if nDim < nRand then 
                        rnd.NextBytes( key )    
                    else    
                        let rndArr = Array.zeroCreate<byte> nRand
                        rnd.NextBytes( rndArr )
                        let mutable len = nRand
                        Buffer.BlockCopy( rndArr, 0, key, 0, len ) 
                        len <- len * 2
                        while len < nDim do 
                            let copylen = Math.Min( len * 2 , nDim ) 
                            let cnt = copylen - len 
                            Buffer.BlockCopy( key, 0, key, len, cnt ) 
                            len <- copylen 
                    key
                let partitionSortFunc (kv:byte[]) = 
                    int kv.[0]
                let dkvGen0 = startDSet |> DSet.init initSortFunc partitionSizeSortFunc 
                // overwrite num partitions
                dkvGen0.NumPartitions <- startDSet.NumPartitions
                let sortParam = DParam ( PreGroupByReserialization = 1000000, NumPartitions = PartitionsAfterSort )
                let dkvSorted = dkvGen0 |> DSet.binSortP sortParam partitionSortFunc InteropWithMsvcrt.BytesComparer
                let verify = dkvSorted |> DSet.fold (VerifySort.Fold nDim) (VerifySort.Aggregate) null
                let count, numOverlap, numOutOfOrder = verify.Validate()
                let validation = numOverlap=0 && numOutOfOrder=0L && count = num
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Sorted %d vectors of size %d with %f secs, throughput = %f MB/s validation = %A" 
                                                           num nDim elapse.TotalSeconds (float num * float nDim / 1000000. / elapse.TotalSeconds) validation ))
                if not validation then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "There are %d sorrted vectors %d overlapped ranges and %d out of order vectors in partitions" 
                                                               count numOverlap numOutOfOrder ))
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Overlapped ranges are: %s" (verify.ShowOverlappedRange()) ))
                            
                bExecute <- true
                ()
            if bMapReduce then 
                let t1 = (DateTime.UtcNow)
                let startDSet = DSet<_>( Name = "SortGen", NumReplications = nrep, 
                                        Version = ver, SerializationLimit = slimit, 
                                        Password = password, 
                                        TypeOfLoadBalancer = typeOf, 
                                        PeerRcvdSpeedLimit = rcvdSpeedLimit )
                let numPartitions = nump // Hard coded partition
                let PartitionsAfterSort = 256
                if nParallel > 0 then 
                    startDSet.NumParallelExecution <- nParallel    
                if nump > 0 then 
                    startDSet.NumPartitions <- nump
                                                     
                let partitionSizeSortFunc( parti ) = 
                    let ba = int (num / int64 numPartitions) 
                    if parti < int ( num % int64 numPartitions ) then ba + 1 else ba
                let initSortFunc( parti, serial ) = 
                    // each generated vector is with a different random seed
                    let rnd = RndGeneration.GetRandomGen( parti, parti )
                    let key = Array.zeroCreate<byte> nDim
                    rnd.NextBytes( key )
                    key
                let partitionSortFunc (kv:byte[]) = 
                    int kv.[0]
                let mapFunc (kv:byte[]) = 
                    Seq.singleton( int kv.[0], kv )
                let reduceFunc (key:int, lst:System.Collections.Generic.List<byte[]>) = 
                    lst.Sort( InteropWithMsvcrt.BytesComparer )
                    lst
                let dkvGen0 = startDSet |> DSet.init initSortFunc partitionSizeSortFunc 
                let sortParam = DParam ( PreGroupByReserialization = 1000000, NumPartitions = PartitionsAfterSort )
                let dkvSorted1 = dkvGen0 |> DSet.mapReducePWithPartitionFunction sortParam mapFunc Operators.id reduceFunc
                let dkvSorted = dkvSorted1 |> DSet.collect ( fun lst -> lst :> seq<_>)
                let verify = dkvSorted |> DSet.fold (VerifySort.Fold nDim) (VerifySort.Aggregate) null
                let count, numOverlap, numOutOfOrder = verify.Validate()
                let validation = numOverlap=0 && numOutOfOrder=0L && count = num
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Sorted %d vectors of size %d with %f secs, throughput = %f MB/s validation = %A" 
                                                           num nDim elapse.TotalSeconds (float num * float nDim / 1000000. / elapse.TotalSeconds) validation ))
                if not validation then 
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "There are %d sorrted vectors %d overlapped ranges and %d out of order vectors in partitions" 
                                                               count numOverlap numOutOfOrder ))
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Overlapped ranges are: %s" (verify.ShowOverlappedRange()) ))
                            
                bExecute <- true
                ()
                

        Cluster.Stop()
    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
