open System
open System.IO
open System.Diagnostics
open System.Runtime.InteropServices
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open System.Threading

open Microsoft.FSharp.NativeInterop

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp

let Usage = "
    Usage: Benchmark performance for distributed sort. \n\
    Command line arguments:\n\
    -cluster    Cluster to use\n\
    -dim        Dimension of each record\n\
    -records N  Number of records in total is N\n\
    -nfile N    Number of input partitions per node (# of files per node) is N\n\
    -nump N     Number of output partitions per node is N\n\
    -fnump N    Further binning per partition to improve sort performance when using bin sort\n\
    "

[<Serializable>]
// create an instance of this class at remote side
type Remote(dim : int, numNodes : int, numInPartPerNode : int, numOutPartPerNode : int, furtherPartition : int, recordsPerNode : int64) =
    let totalRecords = int64(numNodes) * recordsPerNode
    let totalInPartitions = int64(numNodes) * int64(numInPartPerNode)
    let totalOutPartitions = int64(numNodes) * int64(numOutPartPerNode)
    let outSegmentsPerNode = int64(numOutPartPerNode) * int64(furtherPartition)
    let totalOutSegments = totalOutPartitions * int64(furtherPartition)
    let recordsPerInPartition = totalRecords / totalInPartitions
    let perFileInLen = recordsPerInPartition * int64(dim)
    let totalSizeInByte = perFileInLen * int64(totalInPartitions)
    let maxSubPartitionLen = totalSizeInByte * 3L / (totalOutSegments * 2L) // 150% of avg size per partition
    let mutable partBoundary : int[] = null
    let mutable segBoundary : int[] = null
    let mutable minSegVal : int[] = null

    // for file I/O
    let alignLen = (dim + 7)/8*8
    let cacheLenPerSegment =  Remote.NumCacheRecords * int64(alignLen)
    let inLenPerPartition = Remote.ReadRecords * dim

    static member val ReadRecords = 1000*1024 with get, set
    static member val NumCacheRecords = (1000000L/16L) with get, set

    // properties
    member x.Dim with get() = dim
    member x.InPartitions with get() = totalInPartitions
    member x.OutPartitions with get() = totalOutPartitions
    member x.TotalSizeInByte with get() = totalSizeInByte
    member x.NumNodes with get() = numNodes
    member x.FurtherPartition with get() = furtherPartition

    static member val Current : Remote = Unchecked.defaultof<Remote> with get, set

    member val private SegmentCnt = -1 with get, set
    member val internal InMemory : bool = false with get, set
    member val internal Allocate : bool = false with get, set
    member val internal ReadPool : SharedMemoryChunkPool<byte> = null with get, set // used for reading records
    member val internal MemoryPool : SharedMemoryChunkPool<byte> = null with get, set // used for storing repartition records coming in
    member val internal SortPool : SharedMemoryChunkPool<byte> = null with get, set // used for sorting each segment
    member val internal WriteStream = ConcurrentDictionary<uint32, List<int*int*int>*ConcurrentDictionary<int, bool*int*BufferListStream<byte>>>() with get
    member val internal SortStrm = ConcurrentDictionary<int, BufferListStreamWithBackingStream<byte,RefCntBufChunkAlign<byte>>>() with get

    // init and start of remote instance ===================   
    member x.InitInstance(inMemory : bool) =
        lock (x) (fun _ ->
            if (not x.Allocate) then
                RefCntBufChunkAlign<byte>.AlignBytes <- 16
                let maxSubPartitionLenAdjust = maxSubPartitionLen * int64(alignLen) / int64(dim)
                let memoryPoolLen =
                    if (inMemory) then
                        maxSubPartitionLenAdjust
                    else
                        cacheLenPerSegment
                x.ReadPool <- new SharedMemoryChunkPool<byte>(2*numInPartPerNode, 2*numInPartPerNode, inLenPerPartition, (fun _ -> ()), "ReadPool")
                x.MemoryPool <- new SharedMemoryChunkPool<byte>(2*(int outSegmentsPerNode), 2*(int outSegmentsPerNode), int memoryPoolLen, (fun _ -> ()), "RepartitionPool")
                if (not x.InMemory) then
                    // to reuse memory for sort pool, would have to close all streams first
                    //x.SortPool <- SharedMemoryChunkPool<byte>.ReusePool(x.MemoryPool, numOutPartPerNode*2, numOutPartPerNode*2, int maxSubPartitionLen, (fun _ -> ()), "SortPool")
                    x.SortPool <- new SharedMemoryChunkPool<byte>(numOutPartPerNode*2, numOutPartPerNode*2, int maxSubPartitionLenAdjust*2, (fun _ -> ()), "SortPool")
                // boundaries for repartitioning (Shuffling)
                partBoundary <- Array.init 65536 (fun i -> Math.Min(int(totalOutPartitions)-1,(int)(((int64 i)*(int64 totalOutPartitions))/65536L)))
                // boundaries for further binning at each node
                let maxPerPartition = (65536 + int(totalOutPartitions) - 1) / int(totalOutPartitions)
                let maxValPartition = (maxPerPartition <<< 8) + 256
                minSegVal <- Array.init (int(totalOutPartitions)) (fun i -> int((65536L * (int64 i) + (totalOutPartitions - 1L))/totalOutPartitions))
                segBoundary <- Array.init maxValPartition (fun i -> (int)(((int64 i)*(int64 furtherPartition))/(int64 maxValPartition)))
                x.InMemory <- inMemory
        )

    member x.StopInstance() =
        (x :> IDisposable).Dispose()

    static member StartRemoteInstance(rawDir : string[], partDir : string[], sortDir : string[], readRecords : int, cacheRecords : int64, 
                                      dim : int, numNodes : int, numInPartPerNode : int, numOutPartPerNode : int, furtherPartition : int, 
                                      recordsPerNode : int64, inMemory : bool) () =
        Remote.ReadRecords <- readRecords
        Remote.NumCacheRecords <- cacheRecords
        Remote.Current <- new Remote(dim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode)
        // overwrite partdir and sortdir with arguments from job
        Remote.Current.RawDataDir <- rawDir
        Remote.Current.PartDataDir <- partDir
        Remote.Current.SortDataDir <- sortDir
        Remote.Current.InitInstance(inMemory)

    static member StopRemoteInstance() =
        Remote.Current.StopInstance()

    // transfer a local instance to remote - only works if every member serializable
    static member TransferInstance (rmt) () =
        Remote.Current <- rmt

    member val IsDisposed = ref 0 with get
    member x.Dispose(bDisposing : bool) =
        if (Interlocked.CompareExchange(x.IsDisposed, 1, 0) = 0) then
            // unmanaged stuff always release
            if bDisposing then
                (x.MemoryPool :> IDisposable).Dispose()

    interface IDisposable with
        override x.Dispose() =
            x.Dispose(true)
            GC.SuppressFinalize(x)

    // =======================================================================
    // Generic partition getters to perform sort
    member x.GetCachePtr(parti : int) : seq<uint32> =
        Seq.singleton(uint32 parti)

    static member GetCachePtr parti =
        Remote.Current.GetCachePtr(parti)

    // ================================================================
    member x.FurtherPartitionAndDispose(ms : StreamBase<byte>) =
        ms.Seek(0L, SeekOrigin.Begin) |> ignore
        let parti = ms.ReadUInt32()
        let alignLen = (dim + 7)/8*8
        let vec = Array.zeroCreate<byte>(alignLen)
        while (ms.Read(vec, 0, dim) = dim) do
            let index0 = ((int vec.[0]) <<< 8) ||| (int vec.[1])
            let index1 = ((index0 - minSegVal.[int parti]) <<< 8) ||| (int vec.[2])
            let segment = segBoundary.[index1]
            let (segmentList, segmentDic) = x.WriteStream.GetOrAdd(parti, fun _ -> (List<int*int*int>(), ConcurrentDictionary<int,(bool*int*BufferListStream<byte>)>()))
            let (init, segIndex, bls) = segmentDic.GetOrAdd(segment, fun _ ->
                // following code may be called multiple times, only one will return
                // so do not allocte something that needs IDisposable here
                let segIndex = int(parti)*furtherPartition + segment
                if (x.InMemory) then
                    (false, segIndex, null)
                else
                    (false, segIndex, null)
            )
            if (not init) then
                // prevent orphaned BufferListStream from hanging around by using lock
                lock (segmentDic.[segment]) (fun _ ->
                    let (init, segIndex, bls) = segmentDic.[segment] // get current state of initialization
                    if (not init) then
                        segmentList.Add(int parti, segmentList.Count, segment)
                        if x.InMemory then
                            segmentDic.[segment] <- (true, segIndex, new BufferListStream<byte>())
                        else
                            //let dirIndex = segIndex % x.PartDataDir.Length
                            let dirIndex = int(parti) % x.PartDataDir.Length
                            let fileName = Path.Combine(x.PartDataDir.[dirIndex], sprintf "%d.bin" segIndex)
                            Logger.LogF(LogLevel.Info, fun _ -> sprintf "Create file %s" fileName)
                            let strm = DiskIO.OpenFileWrite(fileName, FileOptions.Asynchronous ||| FileOptions.WriteThrough, false)
                            let bls = new BufferListStreamWithCache<byte,RefCntBufChunkAlign<byte>>(strm, x.MemoryPool)
                            bls.FileName <- fileName
                            //let bls = new BufferListStreamWithCache<byte,RefCntBufChunkAlign<byte>>(fileName, x.MemoryPool) // does not open file
                            //bls.BufferLess <- true
                            bls.SequentialWrite <- true
                            segmentDic.[segment] <- (true, segIndex, bls :> BufferListStream<byte>)
                )
            let (init, segIndex, bls) = segmentDic.[segment]
            //bls.Write(vec, 0, vec.Length)
            bls.WriteConcurrent(vec, 0, vec.Length, 1)
        (ms :> IDisposable).Dispose()

    static member FurtherPartitionAndDispose(ms) =
        Remote.Current.FurtherPartitionAndDispose ms

    static member ToSeq(enum : IEnumerable) =
        seq {
            for e in enum do
                yield e
        }

    member internal x.GetCacheMemSubPartN(parti : int) : seq<_> =
        if (x.WriteStream.ContainsKey(uint32 parti)) then 
            //Remote.ToSeq(x.WriteStream.[uint32 parti])
            seq (fst x.WriteStream.[uint32 parti])
        else
            Seq.empty

    static member internal GetCacheMemSubPartN parti =
        Remote.Current.GetCacheMemSubPartN(parti)

    member x.ClearCacheMemSubPartN(parti : uint32) =
        if (x.WriteStream.ContainsKey(parti)) then
            if (Utils.IsNotNull x.WriteStream.[parti]) then
                for elem in (snd x.WriteStream.[parti]) do
                    let (init, segIndex, bls) = elem.Value
                    (bls :> IDisposable).Dispose()
            x.WriteStream.Clear()

    static member ClearCacheMemSubPartN parti =
        Remote.Current.ClearCacheMemSubPartN(parti)    

    member x.ClearSortStrm() =
        for elem in x.SortStrm do
            if (Utils.IsNotNull elem.Value) then
                (elem.Value :> IDisposable).Dispose()
        x.SortStrm.Clear()

    static member ClearSortStream() =
        Remote.Current.ClearSortStrm()

    // ================================================================================
    member val private ReadCnt = ref -1 with get
    member val RawDataDir : string[] = [|@"c:\sort\raw"; @"d:\sort\raw"; @"e:\sort\raw"; @"f:\sort\raw"|] with get, set
    member val PartDataDir : string[] = [|@"c:\sort\part"; @"d:\sort\part"; @"e:\sort\part"; @"f:\sort\part"|] with get, set
    member val SortDataDir : string[] = [|@"c:\sort\sort"; @"d:\sort\sort"; @"e:\sort\sort"; @"f:\sort\sort"|] with get, set
//    member val RawDataDir : string[] = [|@"e:\sort\raw"; @"f:\sort\raw"|] with get, set
//    member val PartDataDir : string[] = [|@"e:\sort\part"; @"f:\sort\part"|] with get, set
//    member val SortDataDir : string[] = [|@"e:\sort\sort"; @"f:\sort\sort"|] with get, set

    member x.ReadFilesToMemStream dim parti =
        let readBlockSize = Remote.ReadRecords * dim       
        let counter = ref 0
        let totalReadLen = ref 0L
        let ret =
            seq {
                let instCnt = Interlocked.Increment(x.ReadCnt)
                let dirIndex = instCnt % x.RawDataDir.Length // pick up directory in round-robin
                let fh = File.Open(Path.Combine(x.RawDataDir.[dirIndex], "raw.bin"), FileMode.Open, FileAccess.Read, FileShare.Read)
                while !totalReadLen < perFileInLen do 
                    let toRead = int32 (Math.Min(int64 readBlockSize, perFileInLen - !totalReadLen))
                    if toRead > 0 then
                        let memBuf = new BufferListStreamWithPool<byte,RefCntBufChunkAlign<byte>>(x.ReadPool) // take from read pool
                        memBuf.WriteFromFileStream(fh, int64 toRead, dim)
                        totalReadLen := !totalReadLen + (int64) toRead
                        fh.Seek(0L, SeekOrigin.Begin) |> ignore
                        counter := !counter + 1
                        yield memBuf :> BufferListStream<byte>
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "All data from file has been read"))  
                fh.Close()
            }
        ret

    static member ReadFilesToMemStreamS dim parti =
        Remote.Current.ReadFilesToMemStream dim parti

    member x.ReadFilesToMemStreamF dim parti =
        let readBlockSize = Remote.ReadRecords * dim
        let tbuf = Array.zeroCreate<byte> readBlockSize
        let rand = new Random()
        rand.NextBytes(tbuf)            
        let counter = ref 0
        let totalReadLen = ref 0L
        let ret =
            seq {
                let instCnt = Interlocked.Increment(x.ReadCnt)
                while !totalReadLen < perFileInLen do 
                    let toRead = int32 (Math.Min(int64 readBlockSize, perFileInLen - !totalReadLen))
                    if toRead > 0 then
                        let memBuf = new BufferListStreamWithPool<byte,RefCntBufChunkAlign<byte>>(x.ReadPool)
                        memBuf.WriteArrAlign(tbuf, 0, toRead, dim)
                        totalReadLen := !totalReadLen + (int64) toRead
                        counter := !counter + 1
                        //if (!counter % 100 = 0) then
                        //    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read %d bytes from file" !totalReadLen) )
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%d Read %d bytes from file total %d - rem %d" instCnt toRead !totalReadLen (perFileInLen-(!totalReadLen))) )
                        yield memBuf :> BufferListStream<byte>
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "All data from file has been read"))  
            }
        ret

    static member ReadFilesToMemStreamFS dim parti =
        Remote.Current.ReadFilesToMemStreamF dim parti

    member internal x.RepartitionMemStream (buffer:BufferListStream<byte>) = 
        if buffer.Length > 0L then
            let retseq = seq {
                let partstream = Array.init<StreamBase<byte>> (int(totalOutPartitions)) (fun i -> null)
                let t1 = DateTime.UtcNow
                let bHasBuf = ref true
                let sr = new StreamReader<byte>(buffer, 0L)

                while !bHasBuf do
                    let (buf, pos, len) = sr.GetMoreBuffer()
                    if (Utils.IsNotNull buf) then
                        let idx = ref pos
                        while (!idx + dim <= pos + len) do
                            let index = (((int) buf.[!idx]) <<< 8) + ((int) buf.[!idx + 1])
                            let parti = partBoundary.[index]

                            if Utils.IsNull partstream.[parti] then
                                let ms = new MemoryStreamB()
                                ms.WriteUInt32(uint32 parti)
                                partstream.[parti] <- ms :> StreamBase<byte>
                            partstream.[parti].Write(buf, !idx, dim)
                            idx := !idx + dim
                    else
                        bHasBuf := false

                (buffer :> IDisposable).Dispose()
                let t2 = DateTime.UtcNow

                for i = 0 to int(totalOutPartitions) - 1 do
                    if Utils.IsNotNull partstream.[i] then
                        if (partstream).[i].Length > 0L then
                            (partstream).[i].Seek(0L, SeekOrigin.Begin) |> ignore
                            yield (partstream).[i]
                        else 
                            ((partstream).[i] :> IDisposable).Dispose()
                        
                Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "repartition: %d records, takes %f s" (buffer.Length / 100L) ((t2-t1).TotalSeconds) ) )          
            }
            retseq
        else
            Seq.empty

    static member RepartitionMemStream (buffer) =
        Remote.Current.RepartitionMemStream (buffer)

    // =========================================================
    member x.LoadForSort(segIndex : int, bls : BufferListStream<byte>) =
        let blsC = bls :?> BufferListStreamWithCache<byte,RefCntBufChunkAlign<byte>>
        let fileName = blsC.FileName
        //blsC.Flush()  // will start async write operations which need to be waited upon
        //blsC.WaitForIOFinish()
        (blsC :> IDisposable).Dispose()
        let sortFile = Native.AsyncStreamIO.OpenFile(fileName, FileAccess.Read, FileOptions.Asynchronous ||| FileOptions.SequentialScan, false)
        let sortStrm = new BufferListStreamWithCache<byte,RefCntBufChunkAlign<byte>>(sortFile, x.SortPool :> SharedPool<string,RefCntBufChunkAlign<byte>>)
        sortStrm.SetPrefetch(1)
        x.SortStrm.[segIndex] <- sortStrm        

    member x.DoSortFile(parti : int, posInList : int, segment : int) =
        let (segList, segDic) = x.WriteStream.[uint32 parti]
        let (init, segIndex, bls) = segDic.[segment]
        let (rpart, strmLen) =
            if (x.InMemory) then
                (bls.GetMoreBufferPart(0L), bls.Length)
            else
                if (0 = posInList) then
                    x.LoadForSort(segIndex, bls)
                if (posInList+1 < segList.Count) then
                    // start load of next one asynchronously
                    let (nextParti, nextPos, nextSegment) = segList.[posInList+1]
                    let (nexInit, nextSegIndex, nextBls) = segDic.[nextSegment]
                    x.LoadForSort(nextSegIndex, nextBls)
                (x.SortStrm.[segIndex].GetMoreBufferPart(0L), x.SortStrm.[segIndex].Length)
        // now sort
        let buf = rpart.Elem :?> RefCntBufChunkAlign<byte>
        NativeSort.Sort.AlignSort64(buf.Ptr, alignLen>>>3, int rpart.Count / alignLen)
        let cnt = rpart.Count
        if (cnt <> strmLen) then
            failwith "Full stream is not being sorted"
        if (x.InMemory) then
            (rpart :> IDisposable).Dispose()
        else
            // write out to file which will dispose rpart
            //let filename = (bls :?> BufferListStreamWithCache<byte,RefCntBufAlign<byte>>).FileName + ".sorted"
            //let dirIndex = segIndex % x.PartDataDir.Length
            let dirIndex = parti % x.PartDataDir.Length
            let filename = Path.Combine(x.SortDataDir.[dirIndex], sprintf "%d.bin" segIndex)
            let diskIO = DiskIO.OpenFileWrite(filename, FileOptions.Asynchronous ||| FileOptions.WriteThrough, false)
            rpart.Type <- RBufPartType.MakeVirtual
            DiskIOFn<byte>.WriteBuffer(rpart, diskIO, 0L)
            // dispose sort stream
            (x.SortStrm.[segIndex] :> IDisposable).Dispose()
            // wait for other to finish
            diskIO.WaitForIOFinish()
            (diskIO :> IDisposable).Dispose()
        // dispose stream
        //(bls :> IDisposable).Dispose()
        cnt

    static member DoSortFile(parti, posInList, segIndex) =
        Remote.Current.DoSortFile(parti, posInList, segIndex)

// ====================================================

module Interop =
    let inline AlignSort(buffer : byte[], align : int, num : int) =
        let bufferHandle = GCHandle.Alloc(buffer, GCHandleType.Pinned)
        try
            NativeSort.Sort.AlignSort64(bufferHandle.AddrOfPinnedObject(), (align+7)/8*8, num)
        finally
            bufferHandle.Free()

let repartitionFn (ms : StreamBase<byte>) =
    ms.Seek(0L, SeekOrigin.Begin) |> ignore
    let index = ms.ReadUInt32()
    int index

let aggrFn (cnt1 : int64) (cnt2 : int64) =
    cnt1 + cnt2

let cntLenByteArr (alignLen : int) (cnt : int64) (newCnt : int64) =
    cnt + newCnt/(int64 alignLen)

// Full sort
let fullSort(sort : Remote, remote : DSet<_>, inMemory : bool) =
    let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
    startDSet.NumParallelExecution <- 16 

    let watch = Stopwatch.StartNew()

    // Read Data into DSet
    let dset1 = 
        //if (inMemory || true) then
        if (inMemory) then
            startDSet |> DSet.sourceI (int sort.InPartitions) (Remote.ReadFilesToMemStreamFS sort.Dim)
        else
            startDSet |> DSet.sourceI (int sort.InPartitions) (Remote.ReadFilesToMemStreamS sort.Dim)
    dset1.NumParallelExecution <- 16 
    dset1.SerializationLimit <- 1

    // Map to find new partition index
    let dset3 = dset1 |> DSet.map Remote.RepartitionMemStream
    dset3.NumParallelExecution <- 16 
    dset3.SerializationLimit <- 1

    // Collect all memstream into new DSet                
    let dset4 = dset3 |> DSet.collect Operators.id
    dset4.NumParallelExecution <- 16 
                
    // Repartition using index
    let param = new DParam()
    param.NumPartitions <- int sort.OutPartitions
    let dset5 = dset4 |> DSet.repartitionP param repartitionFn

    // Iterate through and cache in RAM
    dset5 |> DSet.iter Remote.FurtherPartitionAndDispose
    let cnt = sort.TotalSizeInByte / (int64 sort.Dim)
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition + cacheInRam stream takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    // now sort
    let startRepart = DSet<_>(Name = "SortVec", SerializationLimit = 1)
    startRepart.NumParallelExecution <- 16

    // count # of sorted vectors to verify result
    let dset6 = startRepart |> DSet.sourceI dset5.NumPartitions Remote.GetCacheMemSubPartN
    let alignLen = (sort.Dim + 7)/8*8
    let dset7 = dset6 |> DSet.map Remote.DoSortFile
    let cnt = dset7 |> DSet.fold (cntLenByteArr alignLen) aggrFn 0L
    Logger.LogF(LogLevel.Info, fun _ -> sprintf "Creating remap + repartition stream + cache + sort takes: %f seconds num: %d rate per node: %f Gbps" watch.Elapsed.TotalSeconds cnt ((double cnt)*(double sort.Dim)*8.0/1.0e9/(double sort.NumNodes)/watch.Elapsed.TotalSeconds))

    // now clear the memory cache
    let dset8 = DSet<_>(Name = "ClearCache", SerializationLimit = 1) |> DSet.sourceI dset5.NumPartitions Remote.GetCachePtr
    dset8 |> DSet.iter Remote.ClearCacheMemSubPartN

    
[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let mutable PrajnaClusterFile = parse.ParseString( "-cluster", "c:\onenet\cluster\onenet21-25.inf" )
    let mutable nDim = parse.ParseInt( "-dim", 100 )
    let mutable recordsPerNode = parse.ParseInt64( "-records", 250000000L ) // records per node
    //let recordsPerNode = parse.ParseInt64("-records", 100000000L)
    let mutable numInPartPerNode = parse.ParseInt( "-nfile", 8 ) // number of partitions (input)
    let mutable numOutPartPerNode = parse.ParseInt( "-nump", 8 ) // number of partitions (output)
    let mutable furtherPartition = parse.ParseInt("-fnump", 2500) // further binning for improved sort performance
    let mutable inMemory = parse.ParseBoolean("-inmem", false)
    let mutable bSimpleTest = parse.ParseBoolean("-simple", false)

    // simple test case
    if (bSimpleTest) then
        Console.WriteLine("Simple local test")
        PrajnaClusterFile <- "local[2]"
        nDim <- 10
        recordsPerNode <- 160L
        numInPartPerNode <- 2
        numOutPartPerNode <- 2
        furtherPartition <- 4
        inMemory <- false
        Remote.ReadRecords <- 40
        Remote.NumCacheRecords <- 5L
        //MemoryStreamB.InitMemStack(100, 50)

    let bAllParsed = parse.AllParsed Usage
    let mutable bExecute = false

    if (bAllParsed) then
        Environment.Init()
        // start cluster
        let cluster = new Cluster(PrajnaClusterFile)
        Cluster.SetCurrent(cluster)

        // add other dependencies
        let curJob = JobDependencies.setCurrentJob "SortGen"
        JobDependencies.Current.Add([|"nativesort.dll"; "native.dll"|])
        let proc = Process.GetCurrentProcess()
        let dir = Path.GetDirectoryName(proc.MainModule.FileName)
        curJob.AddDataDirectory( dir ) |> ignore

        let numNodes = cluster.NumNodes

        // create local copy
        let sort = new Remote(nDim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode)
        if (bSimpleTest) then
            sort.RawDataDir <- [|@"e:\sort\raw"; @"f:\sort\raw"|]
            sort.PartDataDir <- [|@"e:\sort\part"; @"f:\sort\part"|]
            sort.SortDataDir <- [|@"e:\sort\sort"; @"f:\sort\sort"|]

        // do sort
        let remoteExec = DSet<_>(Name = "Remote")
        let watch = Stopwatch.StartNew()

        remoteExec.Execute(fun () -> ())
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Init takes %f seconds" watch.Elapsed.TotalSeconds)

        //remoteExec.Execute(RemoteFunc.TransferInstance(sort)) // transfer local to remote via serialization
        remoteExec.Execute(Remote.StartRemoteInstance(sort.RawDataDir, sort.PartDataDir, sort.SortDataDir, Remote.ReadRecords, Remote.NumCacheRecords, 
                                                      nDim, numNodes, numInPartPerNode, numOutPartPerNode, furtherPartition, recordsPerNode, inMemory))
        Logger.LogF(LogLevel.Info, fun _ -> sprintf "Init plus alloc takes %f seconds" watch.Elapsed.TotalSeconds)

        fullSort(sort, remoteExec, inMemory)
        remoteExec.Execute(Remote.ClearSortStream)
        fullSort(sort, remoteExec, inMemory)
        remoteExec.Execute(Remote.ClearSortStream)

        // stop remote instances
        remoteExec.Execute(Remote.StopRemoteInstance)

        Cluster.Stop()

        bExecute <- true

    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage

    0
