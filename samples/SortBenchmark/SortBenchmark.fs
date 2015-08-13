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
        SortBenchmark.fs
  
    Description: 
        Benchmark performance for distributed sort. 

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


open sortbenchmark
open Microsoft.FSharp.NativeInterop
open Prajna.Service.FSharp

let Usage = "
    Usage: Benchmark performance for distributed sort. \n\
    Command line arguments:\n\
    -in         Copy into Prajna \n\
    -out        Copy outof Prajna \n\
    -dir        Directory where the sort gen file stays \n\
    -num        Number of remote instances running \n\
    -sort       Executing Sort (1: 1-pass sort, 2-2 pass sort) \n\
    -nump N     Number of partitions \n\
    -records N  Number of records in total \n\
    -networktest Launch a network test on throughput \n\
    -close      Close network monitor service \n\

    -local      Local directory. All files in the directories will be copy to (or from) remote \n\
    -remote     Name of the distributed Prajna folder\n\
    -ver V      Select a particular DKV with Version string: in format yyMMdd_HHmmss.fff \n\
    -rep REP    Number of Replication \n\
    -slimit S   # of record to serialize \n\
    -balancer B Type of load balancer \n\

    -flag FLAG  DKVFlag \n\
    -speed S    Limiting speed of each peer to S bps\n\

    -dim        Dimension of vectors to be generated\n\
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

module Interop =
    [<DllImport(@"qsort.dll", CallingConvention=CallingConvention.StdCall)>]
    extern void stdsort(nativeint buf, int len, int dim);
    [<DllImport(@"qsort.dll", CallingConvention=CallingConvention.StdCall)>]
    extern void stdqsort(nativeint buf, int len, int dim);
    let STLqsort (buffer:byte[], len:int, dim:int) = 
        let bufferHandle = GCHandle.Alloc(buffer,GCHandleType.Pinned);
        let pinedbuffer = bufferHandle.AddrOfPinnedObject()        
        stdqsort(pinedbuffer, len / dim, dim)
        bufferHandle.Free()

    [<DllImport(@"qsort.dll", CallingConvention=CallingConvention.StdCall)>]
    extern void bin(nativeint buf, int len, int dim, int binNum, nativeint boundary, nativeint sPos, nativeint oBuf);
    let NativeBin (buffer:byte[], size:int, dim:int, binNum:int, boundary:int[], oBuf:byte[]) = 
        let bufHandle = GCHandle.Alloc(buffer,GCHandleType.Pinned);
        let pinedBuf = bufHandle.AddrOfPinnedObject()

        let obufHandle = GCHandle.Alloc(oBuf,GCHandleType.Pinned);
        let pinedoBuf = obufHandle.AddrOfPinnedObject()

        let boundaryHandle = GCHandle.Alloc(boundary,GCHandleType.Pinned);
        let pinedBoundary = boundaryHandle.AddrOfPinnedObject()


        
        let res = Array.zeroCreate<int> binNum
        let resHandle = GCHandle.Alloc(res,GCHandleType.Pinned);
        let pinedres = resHandle.AddrOfPinnedObject()
        bin(pinedBuf, size/dim,dim,binNum,pinedBoundary,pinedres,pinedoBuf)
        bufHandle.Free()
        boundaryHandle.Free()
        resHandle.Free()
        obufHandle.Free()

        res

    [<DllImport(@"qsort.dll", CallingConvention=CallingConvention.StdCall)>]
    extern void Mymemcpy(nativeint srcBuf, int srcOff, nativeint dest, int destOff, int size);
    let NativeMemcpy (srcBuf:byte[], srcOff:int, destBuf:byte[], destOff:int, len:int) = 
        let sbufHandle = GCHandle.Alloc(srcBuf,GCHandleType.Pinned);
        let pinedsBuf = sbufHandle.AddrOfPinnedObject()
        
        let dbufHandle = GCHandle.Alloc(destBuf,GCHandleType.Pinned);
        let pineddBuf = dbufHandle.AddrOfPinnedObject()

        Mymemcpy(pinedsBuf,srcOff,pineddBuf,destOff,len)
        dbufHandle.Free()
        sbufHandle.Free()

/// <summary>
/// This class contains the parameter used to start the monitor network service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. Please fill in all class members that are not default. 
/// </summary> 
[<AllowNullLiteral; Serializable>]
type WriteStorageParam() =
    inherit WorkerRoleInstanceStartParam()
    member val NumFiles = 1024 with get, set
    member val WriteBlockSize = 1024*1024*10 with get, set
/// <summary>
/// This class represent a instance to monitor the network interface. The developer may extend MonitorInstance class, to implement the missing functions.
/// </summary> 
[<AllowNullLiteral>]
type ReadStorageInstance() =
    inherit WorkerRoleInstance<WriteStorageParam>()
    let totalDrives = DriveInfo.GetDrives() 
    let evDrives = ConcurrentDictionary<string,int> ()
    let dataBuf = ConcurrentDictionary<string, ConcurrentQueue<byte[]>>()
    let dataBufSize = ConcurrentDictionary<string, int ref>()
    
    member val WriteBlockSize = 1024*1024*10 with get, set
    member val NumFiles = 1024 with get, set
    /// OnStart is run once during the start of the  MonitorInstance
    /// It is generally a good idea in OnStart to copy the specific startup parameter to the local instance.
    override x.OnStart param =
        // Setup distribution policy
        let mutable bSuccess = true 
        // Find a collection of local disk 
        param.NumThreads <- totalDrives.Length
        x.NumFiles <- param.NumFiles
        x.WriteBlockSize <- param.WriteBlockSize
        bSuccess 
    override x.Run() = 
        //let bExist, myDrive = totalDrives.TryDequeue()
        //if bExist then 
        x.CheckAndWriteToFile()
        ()    
    override x.OnStop() = 
        // Cancel all pending jobs. 
        x.EvTerminated.Set() |> ignore 
    override x.IsRunning() = 
        not (x.EvTerminated.WaitOne(0))

    member x.WriteToBuffer(filename:string, buf:byte[]) =
        let updateFunc (key,queue:ConcurrentQueue<byte[]>) =
            queue.Enqueue(buf)
            queue
        dataBuf.GetOrAdd(filename, new ConcurrentQueue<byte[]>()).Enqueue(buf)

        let bufSize = dataBufSize.GetOrAdd(filename,ref 0)
        Interlocked.Add(bufSize,buf.Length) |> ignore


    member internal x.WriteToDisk(files:string[]) = 
        let writebuf = Array.zeroCreate<byte> x.WriteBlockSize
        files |> Array.iter (fun filename ->
                                    let bf, bufQueue = dataBuf.TryGetValue(filename)
                                    if bf then
                                        use fileHandle = new FileStream(filename,FileMode.Append)
                                        
                                        let qlen = ref (ref 0)
                                        
                                        while dataBufSize.TryGetValue(filename, qlen) && !(!qlen) > x.WriteBlockSize do
                                            let dequeuedSize = ref 0
                                            let mutable bHasEle = true
                                            while !dequeuedSize < x.WriteBlockSize && bHasEle do
                                                let bPeeked, buf = bufQueue.TryPeek()
                                                bHasEle <- false
                                                if (bPeeked && buf.Length + !dequeuedSize <= x.WriteBlockSize) then
                                                    let bDequeued, buf = bufQueue.TryDequeue()
                                                    if bDequeued then
                                                        Interlocked.Add((!qlen),0-buf.Length) |> ignore
                                                        Buffer.BlockCopy(writebuf,!dequeuedSize,buf,0,buf.Length)
                                                        dequeuedSize := !dequeuedSize + buf.Length
                                                        bHasEle <- true
                                            fileHandle.Write(writebuf,0,!dequeuedSize)
                                            



                                        fileHandle.Flush()
                                        fileHandle.Close()
                            )
        ()


    member internal x.FlushToDisk(files:string[]) = 
        files |> Array.iter (fun filename ->
                                    let bf, bufQueue = dataBuf.TryGetValue(filename)
                                    if bf then
                                        let qlen = ref( ref 0 )
                                        use fileHandle = new FileStream(filename,FileMode.Append)
                                        while (dataBufSize.TryGetValue(filename,qlen)) && !(!qlen) > 0 do
                                            let len = !(!qlen)
                                            let writebuf = Array.zeroCreate<byte> len
                                            let dequeuedSize = ref 0
                                            while !dequeuedSize < len do
                                                let bDequeued, buf = bufQueue.TryDequeue()
                                                if bDequeued then
                                                    Interlocked.Add((!qlen),0-buf.Length) |> ignore
                                                    Buffer.BlockCopy(writebuf,!dequeuedSize,buf,0,buf.Length)
                                                    dequeuedSize := !dequeuedSize + buf.Length
                                            fileHandle.Write(writebuf,0,!dequeuedSize)
                                        fileHandle.Flush()
                                        fileHandle.Close()
                            )
        ()

    member internal x.CheckAndWriteToFile() = 

        let fileReadyToWrite = dataBufSize.ToArray() 
                                |> Seq.filter (fun kv -> !(kv.Value) > x.WriteBlockSize  ) 
                                |> Seq.map (fun kv -> kv.Key)

        let workQueue = totalDrives |> Seq.map (fun di ->
                                                    fileReadyToWrite 
                                                        |> Seq.filter (fun filename -> filename.ToLower().Contains(di.Name.ToLower()))
                                                        |> Seq.toArray
                                                    )
                                    |> Seq.toArray

        workQueue |> Array.Parallel.iter (fun files -> x.WriteToDisk (files) )

        ()
/// records is total number of records
[<Serializable>]
type RemoteFunc( filePartNum:int, records:int64, _dim:int , partNumS1:int, partNumS2:int, stageOnePartionBoundary:int[], stageTwoPartionBoundary:int[]) =    
    member val dim = _dim with get 
    member val diskHelper = new DiskHelper(records) with get
    //member val HDReaderLocker = [|ref 0,ref 0,ref 0,ref 0|]
    member val HDReaderLocker = [|ref 0;ref 0;ref 0;ref 0|]
    member val HDIndex = [|"c:\\";"d:\\";"e:\\";"f:\\"|]

    static member val sharedMem = new ConcurrentQueue<byte[]>()
    static member val sharedMemSize = ref 0

    member val blockSizeReadFromFile = 1024*1000*100

    static member val partiSharedMem = new ConcurrentQueue<byte[]>()
    static member val partiSharedMemSize = ref 0
    static member val leftOverShareMem = new ConcurrentQueue<byte[][]>()
    member val repartitionBlockSize = (1024*1000*100 / partNumS1 / 100) * 100


    member x.Validate (parti) = 
        
        let filename = x.diskHelper.GenerateSortedFilePath(parti)
        let dir = @"C:\sortbenchmark\val_"+(string) records
        if not (Directory.Exists(dir)) then
            Directory.CreateDirectory(dir)  |> ignore
            
        let outputFilename = Path.Combine(dir,Path.GetFileNameWithoutExtension(filename)+".sum")
        if File.Exists(outputFilename) then
            File.Delete(outputFilename)

        let argstr = (sprintf " -o %s %s" outputFilename filename)  
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Generate records for partition %d, using cmd valsort.exe %s" parti argstr))
        let procStartInfo = new ProcessStartInfo(@"valsort.exe", argstr) 

        procStartInfo.RedirectStandardError <- true
        procStartInfo.UseShellExecute <- false
        procStartInfo.CreateNoWindow <- true

        use proc = new Process();
        proc.StartInfo <- procStartInfo;

        let t = (DateTime.UtcNow)
        if proc.Start() then
            while not (proc.WaitForExit(10000)) do
                ()
        else
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Fail to call gensort." ))

        let output = proc.StandardError.ReadToEnd()

        proc.Dispose()
        
        if not (File.Exists(outputFilename)) then
            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Fail to validate partition %d: %s" parti filename))


        let fi = new FileInfo(outputFilename)
        let len = (int32) fi.Length        
        let buffer = Array.zeroCreate<byte> len
        let mutable bSucessRead = false
        while not bSucessRead do
            try

                use file = new FileStream(outputFilename,FileMode.Open)
                let readLen = ref Int32.MaxValue
                readLen := file.Read( buffer, 0, len )
                if (!readLen <> len) then
                    Logger.LogF( LogLevel.Error, (fun _ -> sprintf "read file error"))
                file.Close()
                bSucessRead <- true
            with e->
                Logger.LogF( LogLevel.Error, (fun _ -> sprintf "read file error: %A" e))
        (parti,buffer,len,output)

    member x.GenerateDataFiles parti serial kv = 
        let mutable genLength = 0L
        if serial = 0L then 
            // Only need to write for the first key
            let beginRecord = records  * int64 parti  / int64 filePartNum 
            let endRecord = records  * int64 (parti + 1) / int64 filePartNum
            let recordsPerPartition = endRecord - beginRecord 
            // call sortGen, generate record from beginRecord ... endRecord
            //let datafileName = sprintf "d:\\sortbenchmark\\data\\gensort_%d_%d_%d.bin" batchi beginRecord endRecord
            let datafileName = x.diskHelper.GenerateDataFilePath(parti)
            
            let mutable bGenNew = true

            if (File.Exists(datafileName)) then
                let fi = new FileInfo(datafileName)
                if fi.Length = (recordsPerPartition * (int64) x.dim ) then
                    bGenNew <- false
                    genLength <- fi.Length
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Data file %s is existing, skipping generation" datafileName))

            if bGenNew then
                let argstr = (sprintf " -b%d %d %s" beginRecord recordsPerPartition datafileName)  
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Generate records for partition %d, using cmd gensort.exe %s" parti argstr))
                let procStartInfo = new ProcessStartInfo(@"gensort.exe", argstr) 

                procStartInfo.RedirectStandardOutput <- false
                procStartInfo.UseShellExecute <- false
                procStartInfo.CreateNoWindow <- true

                use proc = new Process();
                proc.StartInfo <- procStartInfo;

                let t = (DateTime.UtcNow)
                if proc.Start() then
                    while not (proc.WaitForExit(10000)) do
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Generating records for partition %d. time spent:%A seconds" parti ((DateTime.UtcNow)-t).TotalSeconds ))
                else
                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!!Error!!! Fail to call gensort." ))
                proc.Dispose()
                
                genLength <-
                    if File.Exists(datafileName) then
                        let fi = new FileInfo(datafileName)
                        fi.Length
                    else 
                        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!!Error!!! Fail to generate data records." ))
                        0L

        (parti, genLength)

    member val repartitionThread = ref 0


    member x.Repartitionfake parti serial (buffer:byte[], size:int)=
        
        Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "!!!repartition data!"))
        let r = seq {
                        //let t1 = DateTime.UtcNow
                        for i = 0 to partNumS1 do
                            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "repartition data!"))
                            let t = Array.zeroCreate<byte> (size / 1024)
                            Buffer.BlockCopy(buffer,0,t,0,t.Length)
                            yield i,t
                        //let t2 = DateTime.UtcNow
                        //Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "repartition: %d records, takes %f s; # repartition threads: %d" (size / 100) ((t2-t1).TotalSeconds) !x.repartitionThread))

                        }
        r

    member x.ReadFilesfake parti= 
            let toRead = x.blockSizeReadFromFile
            let rand = new Random(DateTime.Now.Millisecond)

            

            let readLen = ref Int32.MaxValue
                
            let ret =
                seq {
                    while true do
                        Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "read data!"))
                        let byt = Array.zeroCreate<byte> (toRead)
                        rand.NextBytes(byt)
                        yield byt, toRead
                       
                }
            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "UTC %s, all data from file %s has been read" (UtcNowToString()) "adf"))
            ret


    member x.WritePreSortFuncFakeNativeRepSeq ( data ) = 
        data |> Seq.iter(fun (i,buf,len) -> 
        

                        let oBuf:byte[] = x.GetRepartitionMemBuf()
                        let r = Interop.NativeBin(buf,len,x.dim,partNumS1,stageTwoPartionBoundary,oBuf)
        
                        RemoteFunc.partiSharedMem.Enqueue(buf)
                        RemoteFunc.partiSharedMem.Enqueue(oBuf)
                        
                        
                        )
        //data |> Seq.iter(fun (i,d) -> ())
        ()



    member x.WritePreSortFuncFakeSeq ( data ) = 
        let tbuf = Array.zeroCreate<byte> x.dim
        data |> Seq.iter(fun (i,buf:byte[],len) -> 

                        let nump = partNumS1
                        let posi = Array.zeroCreate<int> nump

                        let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (nump-1),2.0)) + 1)) 
                        let hashByteSize = (hashBitSize - 1 ) / 8 + 1

                        let t1 = DateTime.UtcNow

                        for i in [|0..(len/x.dim - 1)|] do
                            let indexV = ref 0
                            for p = 0 to hashByteSize - 1 do
                                indexV := (!indexV <<< 8) + int buf.[i*x.dim+p]
                            //let parti = x.GetParti(indexV) 

                            let parti = stageTwoPartionBoundary.[!indexV]


                            if parti >= nump then
                                    Logger.LogF( LogLevel.WildVerbose,  ( fun _ -> sprintf " partition index %d is bigger then the number of partitions %d" parti nump ))
             
                            Buffer.BlockCopy (buf, i*x.dim,tbuf, 0,x.dim)   
                                

                        RemoteFunc.partiSharedMem.Enqueue(buf)
                        )
        //data |> Seq.iter(fun (i,d) -> ())
        ()

    member x.WritePreSortFuncFake ( _ ) = 
        ()

    member x.NativeRepartition (stage:int) (buffer:byte[], size:int)=
        if size > 0 then
            let retseq = seq {
                            let partionBoundary =
                                if stage = 1 then
                                    stageOnePartionBoundary
                                else 
                                    stageTwoPartionBoundary
                            let nump = 
                                if stage = 1 then
                                    partNumS1
                                else 
                                    partNumS2

                            
                            let t1 = DateTime.UtcNow
                            Interlocked.Increment(x.repartitionThread) |> ignore

                            let oBuf = ref Unchecked.defaultof<_>
                            let newmem = ref false
                            if not (RemoteFunc.sharedMem.TryDequeue(oBuf)) then
                                if (!RemoteFunc.sharedMemSize <= 300) then   // 50GB shared memory
                                    oBuf := Array.zeroCreate<_> (x.blockSizeReadFromFile)
                                    Interlocked.Increment(RemoteFunc.sharedMemSize) |>  ignore
                                    newmem := true
                                else 
                                    
                                    while not (RemoteFunc.sharedMem.TryDequeue(oBuf)) do
                                        ()


                            let r = Interop.NativeBin(buffer,size,x.dim,nump,partionBoundary,!oBuf)

                            RemoteFunc.sharedMem.Enqueue(buffer)

                            let t2 = DateTime.UtcNow

                            if r.[0] > 0 then
                                let toCopyLen = r.[0]
                                let tl = ref 0
                                while (!tl < toCopyLen) do
                                    let iBuf:byte[] = x.GetRepartitionMemBuf()
                                    let cpLen = Math.Min(toCopyLen - !tl, (iBuf.Length))
                                    Buffer.BlockCopy(buffer,0,iBuf,0,cpLen)
                                    tl := !tl + cpLen
                                    yield (0,iBuf,cpLen)
                        
                            for i = 1 to nump-1 do
                                if r.[i] <> r.[i-1] then
                                    let toCopyLen = (r.[i]-r.[i-1])
                                    let tl = ref 0
                                    while (!tl < toCopyLen) do
                                        let iBuf:byte[] = x.GetRepartitionMemBuf()
                                        let cpLen = Math.Min(toCopyLen - !tl, (iBuf.Length))
                                        Buffer.BlockCopy(buffer,0,iBuf,0,cpLen)
                                        tl := !tl + cpLen
                                        yield (i,iBuf,cpLen)


                            let t3 = DateTime.UtcNow
                            Interlocked.Decrement(x.repartitionThread) |> ignore
                            RemoteFunc.sharedMem.Enqueue(!oBuf)
                            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "repartition: %d records, takes %f / %f s; # repartition threads: %d; shareMem Len: %d; new allocated mem: %A" (size / 100) ((t2-t1).TotalSeconds) ((t3-t1).TotalSeconds) !x.repartitionThread RemoteFunc.sharedMem.Count !newmem)  )                  

                        }
            retseq
        else 
            Seq.empty


    member x.GetRepartitionMemBuf() = 
        let oBuf = ref Unchecked.defaultof<byte[]>

        if not (RemoteFunc.partiSharedMem.TryDequeue(oBuf)) then
            if (!RemoteFunc.partiSharedMemSize <= 800000) then   // 24GB shared memory
                oBuf := Array.zeroCreate<_> (x.repartitionBlockSize)
                Interlocked.Increment(RemoteFunc.partiSharedMemSize) |>  ignore
            else 

                while not (RemoteFunc.partiSharedMem.TryDequeue(oBuf)) do
                    ()    
        !oBuf

    member x.Repartition (stage:int) (buffer:byte[], size:int) = 
        if size > 0 then
            
            let retseq = seq {
                        let partionBoundary =
                            if stage = 1 then
                                stageOnePartionBoundary
                            else 
                                stageTwoPartionBoundary        
                        let nump = 
                            if stage = 1 then
                                partNumS1
                            else 
                                partNumS2

                        let hashByteSize = 
                            if stage = 1 then
                                let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (partNumS1-1),2.0)) + 1)) 
                                (hashBitSize - 1 ) / 8 + 1
                            else 
                                let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (partNumS2-1),2.0)) + 1)) 
                                (hashBitSize - 1 ) / 8 + 1


                        let partstream = Array.init nump (fun _ -> x.GetRepartitionMemBuf())

//                        let partstream = ref Unchecked.defaultof<_>
//                        if not (RemoteFunc.leftOverShareMem.TryDequeue(partstream)) then
//                            partstream := Array.init nump (fun _ -> x.GetRepartitionMemBuf())


                        let posi = Array.zeroCreate<int> nump
                        let t1 = DateTime.UtcNow

                        for i in [|0..(size/x.dim - 1)|] do
                            let indexV = ref 0
                            for p = 0 to hashByteSize - 1 do
                                indexV := (!indexV <<< 8) + int buffer.[i*x.dim+p]
                            //let parti = x.GetParti(indexV) 

                            let parti = partionBoundary.[!indexV]


                            if parti >= nump then
                                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf " partition index %d is bigger then the number of partitions %d" parti nump ))
                                            
                            Buffer.BlockCopy (buffer, i*x.dim,(partstream).[parti], posi.[parti],x.dim)   
                                
                            posi.[parti] <- posi.[parti] + x.dim
                            if posi.[parti]  = x.repartitionBlockSize then
                                yield parti, (partstream).[parti], posi.[parti] 
                                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "repartition: %d records " (posi.[parti] / 100) ) )
                                posi.[parti] <- 0
                                (partstream).[parti] <- x.GetRepartitionMemBuf()


                        let t2 = DateTime.UtcNow

                        for i = 0 to posi.Length - 1 do
                            if posi.[i] > 0 then
                                yield i,(partstream).[i],posi.[i] 

//                        for i = 0 to posi.Length - 1 do
//                            if posi.[i] > 0 then
//                                let ibuf = Array.zeroCreate<byte> posi.[i]
//                                Buffer.BlockCopy(partstream, i*flushLen, ibuf,0,posi.[i] )
//                                yield i,ibuf
//                                FTraceFLine FTraceLevel.WildVerbose (fun _ -> sprintf "repartition: %d records " (posi.[i] / 100) )                    
                        
                        Logger.LogF( LogLevel.MildVerbose ,(fun _ -> sprintf "repartition: %d records, takes %f s, !RemoteFunc.partiSharedMemSize %d; RemoteFunc.partiSharedMem.length %d " (size / 100) ((t2-t1).TotalSeconds) !RemoteFunc.partiSharedMemSize RemoteFunc.partiSharedMem.Count)                    )
                        RemoteFunc.sharedMem.Enqueue(buffer)
                    }
            retseq

        else 
            Seq.empty


    member x.ReadFilesToSeq parti = 
            let toRead = 1024*1000*100

            let _, filename = x.diskHelper.GetDataFilePath()
            
            if true then
            //if Utils.IsNotNull filename then
                let readLen = ref Int32.MaxValue
                //let fi = new FileInfo(filename)
                //let len = fi.Length
                let len = 25000000000L
                let totalReadLen = ref 0L
                let rand = new Random(DateTime.UtcNow.Millisecond)
                let ret =
                    seq {
                        //use file = new FileStream(filename, FileMode.Open)
                        let counter = ref 0
                        while !readLen > 0 do 

                            let byt = ref Unchecked.defaultof<_>
        
                            if not (RemoteFunc.sharedMem.TryDequeue(byt)) then
                                if (!RemoteFunc.sharedMemSize <= 300) then   // 50GB shared memory
                                    byt := Array.zeroCreate<_> (toRead)
                                    rand.NextBytes(!byt)
                                    Interlocked.Increment(RemoteFunc.sharedMemSize) |>  ignore
                                else 

                                    while not (RemoteFunc.sharedMem.TryDequeue(byt)) do
                                        ()
                            //readLen := file.Read( !byt, 0, toRead)
                            totalReadLen := !totalReadLen + (int64) toRead
                            if !totalReadLen >= len then
                                readLen := 0
                            else   
                                readLen := toRead

                            counter := !counter + 1
                            if (!counter % 10 = 0) then
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read %d bytes from file" !totalReadLen) )
                            if (!readLen<>0 && ((!readLen) % x.dim)<>0) then
                                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "read an incomplete record" ))
                                x.diskHelper.ReportReadBytes((int64)!readLen)
                                yield !byt, !readLen
                        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "UTC %s, all data from file %s has been read" (UtcNowToString()) filename)            )
                    }

                ret
            else 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "!!!!!Cannot Get Data file " ))
                Seq.empty



    member x.ReadFilesToMemStream parti = 
            let defaultReadBlock = 1024*1000*100
            let tbuf = Array.zeroCreate<byte> defaultReadBlock
            let _, filename = x.diskHelper.GetDataFilePath()

            if Utils.IsNotNull filename then
                let fi = new FileInfo(filename)
                let len = fi.Length
                let totalReadLen = ref 0L
                let ret =
                    seq {
                        use file = new FileStream(filename, FileMode.Open)
                        let counter = ref 0
                        while !totalReadLen < len do 
                            let toRead = int32 (Math.Min(int64 defaultReadBlock, len - !totalReadLen))
                            if toRead > 0 then
                                let memBuf = new MemStream(toRead)
                                //memBuf.WriteFromFile(file,toRead)

                                file.Read(tbuf,0,toRead) |> ignore
                                memBuf.Write(tbuf,0,toRead)
            
                                totalReadLen := !totalReadLen + (int64) toRead

                                counter := !counter + 1
                                if (!counter % 100 = 0) then
                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Read %d bytes from file" !totalReadLen) )
                
                            
                                //if (toRead<>0 && (toRead % x.dim)<>0) then
                                //    FTraceFLine FTraceLevel.Error ( fun _ -> sprintf "read an incomplete record" )
                                //FTraceFLine FTraceLevel.WildVerbose (fun _ -> sprintf "UTC %s, %d records are read from file %s " (UtcNowToString()) (toRead/ 100) filename)                    
                                x.diskHelper.ReportReadBytes((int64)toRead)
                                yield memBuf
                        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "UTC %s, all data from file %s has been read" (UtcNowToString()) filename)    )        
                    }
                
                ret
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "!!!!!Cannot Get Data file " ))
                Seq.empty

    member x.RepartitionMemStream (stage:int) (buffer:MemStream) = 
        if buffer.Length > 0L then
            let retseq = seq {

                        let partionBoundary =
                            if stage = 1 then
                                stageOnePartionBoundary
                            else 
                                stageTwoPartionBoundary    

                        let nump = 
                            if stage = 1 then
                                partNumS1
                            else 
                                partNumS2

                        let hashByteSize = 
                            if stage = 1 then
                                let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (partNumS1-1),2.0)) + 1)) 
                                (hashBitSize - 1 ) / 8 + 1
                            else 
                                let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (partNumS2-1),2.0)) + 1)) 
                                (hashBitSize - 1 ) / 8 + 1
                        
                        let partstream = Array.init<MemStream> nump (fun _ -> null)

                        let t1 = DateTime.UtcNow
                        let rBuf = Array.zeroCreate<byte> x.dim
                        for i in [|0..((int32)buffer.Length/x.dim - 1)|] do
                            let indexV = ref 0

                            buffer.Read(rBuf,0,x.dim) |> ignore
                            
                            for p = 0 to hashByteSize - 1 do
                                indexV := (!indexV <<< 8) + (int) rBuf.[p]

                            //buffer.Seek((int64(0-hashByteSize)),SeekOrigin.Current) |> ignore
                                //let parti = x.GetParti(indexV) 

                            let parti = partionBoundary.[!indexV]


                            if parti > nump || parti < 0 then
                                    Logger.LogF( LogLevel.Error ,( fun _ -> sprintf " partition index %d is invalid. bigger then the number of partitions %d?" parti nump ))
             
                            //should check the actually read len, but simply ignored here :)

                            //!!!!!! should changed to ReadInternal   buffer.ReadInternal: int -> buf -> int -> int (toreadlen, output buf, buf offsite, len)
                            //how to deal with multiple internal buf block?
                            

                            if Utils.IsNull partstream.[parti] then
                                partstream.[parti] <- new MemStream()
                            partstream.[parti].Write(rBuf,0,x.dim)

                                            
                        let t2 = DateTime.UtcNow

                        for i = 0 to nump - 1 do
                            if Utils.IsNotNull partstream.[i] then
                                yield i, (partstream).[i]

                        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "repartition: %d records, takes %f s, !RemoteFunc.partiSharedMemSize %d; RemoteFunc.partiSharedMem.length %d " (buffer.Length / 100L) ((t2-t1).TotalSeconds) !RemoteFunc.partiSharedMemSize RemoteFunc.partiSharedMem.Count)          )          
                }
            retseq

        else
            Seq.empty



    member x.writeFileLock = Array.create stageTwoPartionBoundary.Length (ref 0)
    member val minparti = Int32.MaxValue with get, set
    member val writeCache = new ConcurrentDictionary<int,MemStream>()

    member x.RepartitionAndWriteToFile ( parti:int, ms:MemStream ) = 

        //let t1 = DateTime.UtcNow
        let mutable len = 0

        let newPartition = x.RepartitionMemStream 2 ms

        newPartition |> Seq.iter ( fun (i, (buf:MemStream)) ->
                                let filename = x.diskHelper.GeneratePartitionFilePath(i)
                                let mutable b = false
                                while not b do 
                                    
                                    if Interlocked.CompareExchange(x.writeFileLock.[parti],1,0) = 0 then
                                        use file = new FileStream(filename,FileMode.Append)    
                                        let mutable pos = 0
                                        //ms.ReadToFile(file,(int) buf.Length)
                                        x.diskHelper.ReportWriteBytes(buf.Length)
                                        file.Flush()
                                        file.Close()

                                        x.writeFileLock.[parti] := 0
                                        b <- true
                                    )

//        let t2 = DateTime.UtcNow
//        let rcount = len / 100
//        if parti <= x.minparti then
//            x.minparti <- parti
//            FTraceFLine FTraceLevel.WildVerbose ( fun _ -> sprintf "UTC %s, received data in partition %d, write %d records to file %s, takes %f s" (UtcNowToString()) parti rcount filename (t2-t1).TotalSeconds)
//

        //(parti,len,filename)
        ()


    member x.WritePreSortFunc ( parti:int, lstMemStream:byte[] ) = 

        let t1 = DateTime.UtcNow
        let mutable len = 0
        let filename = x.diskHelper.GeneratePartitionFilePath(parti)

        let mutable b = false
        while not b do 
            if Interlocked.CompareExchange(x.writeFileLock.[parti],1,0) = 0 then
                use file = new FileStream(filename,FileMode.Append)    
                let mutable pos = 0
                while (pos < (int)lstMemStream.Length) do
                    let count = Math.Min((int)lstMemStream.Length-pos,1024*1024*10)
                    file.Write(lstMemStream, pos, count)
                    x.diskHelper.ReportWriteBytes(int64 count)
                    pos <- pos + count
                len <- len + (int)lstMemStream.Length
                    //ms.Dispose()
                file.Flush()
                file.Close()

                x.writeFileLock.[parti] := 0
                b <- true


        let t2 = DateTime.UtcNow
        let rcount = len / 100
        if parti <= x.minparti then
            x.minparti <- parti
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "UTC %s, received data in partition %d, write %d records to file %s, takes %f s" (UtcNowToString()) parti rcount filename (t2-t1).TotalSeconds))


        //(parti,len,filename)
        ()







    member val readMRELock = ref 0
    member val readMRE:Object = null with set, get

    member val sortLock = ref 0
    member val readSemaphore = ref 0
    member val sortSemaphore = ref 0
    member val writeSemaphore = ref 0
    member val totalSemaphore = ref 0
    member val sortMRE:Object = null with set, get
    member val sortedPartitionCounter = ref 0

    static member val sortSharedMem = new ConcurrentQueue<byte[]>()
    static member val sortSharedMemSize = ref 0


    member x.PipelineSort (threads : int) parti serial kv = 
        let mutable t = -1.
        if Utils.IsNull x.sortMRE && (Interlocked.CompareExchange(x.sortLock, 1, 0) = 0) then
            if Utils.IsNull x.sortMRE then
                let init = 
                    fun _ -> x.sortMRE <- (new ManualResetEvent (false)) :> Object
                init()
            x.sortLock := 0


        if Utils.IsNull x.readMRE && (Interlocked.CompareExchange(x.readMRELock, 1, 0) = 0) then
            if Utils.IsNull x.readMRE then
                let init = 
                    fun _ -> x.readMRE <- (new ManualResetEvent (false)) :> Object
                init()
            x.readMRELock := 0
        

        let filename = x.diskHelper.GeneratePartitionFilePath(parti)

        if (File.Exists(filename)) then

            let fi = new FileInfo(filename)
            if (fi.Length >= (int64) Int32.MaxValue) then
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!!ERROR!!! data in a single partition %d is bigger then 2GB" parti ))
            //let len = (int32) fi.Length
            let len = 500000000


            let buffer = ref Unchecked.defaultof<byte[]>
        


            let read () = 

                if not (RemoteFunc.sortSharedMem.TryDequeue(buffer)) then
                    if (!RemoteFunc.sortSharedMemSize <= threads*3) then 
                        buffer := Array.zeroCreate<byte> (500000000)
                        Interlocked.Increment(RemoteFunc.sortSharedMemSize) |>  ignore
                    else 

                        while not (RemoteFunc.sortSharedMem.TryDequeue(buffer)) do
                            ()

                let bRead = ref false
                let HDi = Array.IndexOf(x.HDIndex, Path.GetPathRoot(filename).ToLower())

                while not !bRead do
                    (x.readMRE :?> ManualResetEvent).Reset() |> ignore
                    if Interlocked.CompareExchange(x.HDReaderLocker.[HDi],1,0) = 0 then
                        let t1 = DateTime.UtcNow
                        use file = new FileStream(filename,FileMode.Open)
                        let readLen = ref Int32.MaxValue
                        readLen := file.Read( !buffer, 0, len )
                        if (!readLen <> len) then
                            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "read file error"))
                        file.Close()      
                        //File.Delete(filename);          
                        let t2 = DateTime.UtcNow
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf " Read data in partition %d  ( %s ), takes %f ms"  parti filename ((t2-t1).TotalMilliseconds)))

                        bRead := true


                        // the order of the following two lines are important. we should unlock before Set()
                        x.HDReaderLocker.[HDi] := 0
                        (x.readMRE :?> ManualResetEvent).Set() |> ignore

                    if (!bRead) then
                        (x.readMRE :?> ManualResetEvent).WaitOne() |> ignore
                ()
            

            let sort () =
                let t1 = DateTime.UtcNow
                Interop.STLqsort(!buffer,len,x.dim)
                let t2 = DateTime.UtcNow
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "sort data in memory takes %f ms" ((t2-t1).TotalMilliseconds)))
                ()


            let write () = 

                let resultfilename = x.diskHelper.GenerateSortedFilePath(parti)

                use wfile = new FileStream(resultfilename,FileMode.Create)
                let wt1 = DateTime.UtcNow
                let mutable pos = 0
                while (pos < len) do
                    let count = Math.Min(len-pos,1024*1024*10)
                    wfile.Write(!buffer, pos, count)
                    x.diskHelper.ReportWriteBytes(int64 count)
                    pos <- pos + count
                wfile.Flush()
                wfile.Close()
                RemoteFunc.sortSharedMem.Enqueue(!buffer)
                File.Delete(resultfilename)
                let wt2 = DateTime.UtcNow
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "write %d records to file %s, takes %f ms" (len/100) filename ((wt2-wt1).TotalMilliseconds)))
                ()

            let pst1 = DateTime.UtcNow
            

            let mutable canrun = false
            while (not canrun) do
                if (Interlocked.CompareExchange(x.sortLock, 1, 0) = 0) then
                    if (!x.readSemaphore < threads && !x.totalSemaphore < threads * 3) then
                        canrun <- true
                        Interlocked.Increment(x.readSemaphore)  |> ignore
                        Interlocked.Increment(x.totalSemaphore) |> ignore
                    else
                        (x.sortMRE :?> ManualResetEvent).Reset() |> ignore
                    x.sortLock := 0
            
                if not canrun then
                    (x.sortMRE :?> ManualResetEvent).WaitOne() |> ignore
        
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "UTC %s, reading threads:%d" (UtcNowToString()) (!x.readSemaphore)))
            read()
            
            (x.sortMRE :?> ManualResetEvent).Set() |> ignore


            canrun <- false
            while (not canrun) do
                if (Interlocked.CompareExchange(x.sortLock, 1, 0) = 0) then
                    if (!x.sortSemaphore < threads) then
                        canrun <- true
                        Interlocked.Increment(x.sortSemaphore)  |> ignore
                    else
                        (x.sortMRE :?> ManualResetEvent).Reset() |> ignore
                    x.sortLock := 0
            
                if not canrun then
                    (x.sortMRE :?> ManualResetEvent).WaitOne() |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "UTC %s, sorting threads:%d" (UtcNowToString()) (!x.sortSemaphore)))
            Interlocked.Decrement(x.readSemaphore) |> ignore
            sort()
            Interlocked.Decrement(x.sortSemaphore) |> ignore
            (x.sortMRE :?> ManualResetEvent).Set() |> ignore


            canrun <- false
            while (not canrun) do
                if (Interlocked.CompareExchange(x.sortLock, 1, 0) = 0) then
                    if (!x.writeSemaphore < threads) then
                        canrun <- true
                        Interlocked.Increment(x.writeSemaphore )  |> ignore
                    else
                        (x.sortMRE :?> ManualResetEvent).Reset() |> ignore
                    x.sortLock := 0
            
                if not canrun then
                    (x.sortMRE :?> ManualResetEvent).WaitOne() |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "writing threads:%d" (!x.writeSemaphore)))
            write()
            Interlocked.Decrement(x.writeSemaphore) |> ignore
            Interlocked.Decrement(x.totalSemaphore) |> ignore

            (x.sortMRE :?> ManualResetEvent).Set() |> ignore

            Interlocked.Increment(x.sortedPartitionCounter) |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf " %d partitions have been sorted" (!x.sortedPartitionCounter)))
            let pst2 = DateTime.UtcNow
            t <- ((pst2-pst1).TotalSeconds)
            (parti,t)
        else 
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "Cannot find file %s for sorting" filename))
            (parti, -1.)


        
[<Serializable>]
type SamplingFunc( filePartNum:int, records:int64, dim:int, sampleRate:int, keyLen:int ) = 
    member x.ReadFiles parti serial kv  = 
        if serial = 0L then 
            // Only need to write for the first key
            // Recommend to open a file with buffer read 

            let binarr = Array.zeroCreate<int64> (1 <<< (keyLen*8))
            let byt = Array.zeroCreate<_> (keyLen)
            let beginRecord = records  * int64 parti / int64 filePartNum
            let endRecord = records  * int64 (parti+1) / int64 filePartNum

            let filename = (sprintf "D:\\sortbenchmark\\data\\gensort_%d_%d.bin" beginRecord endRecord)
            let readLen = ref Int32.MaxValue
            use file = new FileStream(filename, FileMode.Open)
            while !readLen > 0 do 
                readLen := file.Read( byt, 0, keyLen )
                    
                if !readLen = keyLen then
                    let mutable key = (int) byt.[0]
                    for i = 1 to keyLen - 1 do
                        key <- ( key <<< 8 ) + (int) byt.[i]
                    binarr.[key] <- binarr.[key] + 1L
                if file.CanSeek then
                    file.Seek(int64 ((sampleRate - 1) * dim + (dim - keyLen)), SeekOrigin.Current) |> ignore
                else
                    Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!!Error!!! try to read samples from file, but cannot seek" ))
            Seq.singleton(binarr)                    
        else
            Seq.empty



// Define your library scripting code here
[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let localdir = parse.ParseString( "-local", "" )
    let remoteDKVname = parse.ParseString( "-remote", "" )
    let nrep = parse.ParseInt( "-rep", 3 )
    let typeOf = enum<LoadBalanceAlgorithm>(parse.ParseInt( "-balancer", 0) )
    let slimit = parse.ParseInt( "-slimit", 10 )
    let nParallel = parse.ParseInt( "-parallel", 0 )
    let password = parse.ParseString( "-password", "" )
    let rcvdSpeedLimit = parse.ParseInt64( "-speed", 40000000000L )
    let flag = parse.ParseInt( "-flag", -100 )
    let bNetworkTest = parse.ParseBoolean( "-networktest", false )
    let bClose = parse.ParseBoolean( "-close", false )

//    let bExe = parse.ParseBoolean( "-exe", false )
//    let versionInfo = parse.ParseString( "-ver", "" )
//    let ver = if versionInfo.Length=0 then (DateTime.UtcNow) else StringTools.VersionFromString( versionInfo) 
//    let nDim = parse.ParseInt( "-dim", 80 )
//    let bRepartition = parse.ParseBoolean( "-rpart", false )
//    let bMapReduce = parse.ParseBoolean( "-mapreduce", false )
//    let seed = parse.ParseInt( "-seed", 0 )

    let nDim = parse.ParseInt( "-dim", 100 )
    let records = parse.ParseInt64( "-records", 1000000L ) // number of total records
    let bIn = parse.ParseBoolean( "-in", false )
    let bVal = parse.ParseBoolean( "-val", false )
    
    let bOut = parse.ParseBoolean( "-out", false )
    let bSample = parse.ParseBoolean( "-sample", false )
    let sampleRate = parse.ParseInt( "-samplerate", 100 ) // number of partitions
    let dirSortGen = parse.ParseString( "-dir", "." )
    let num = parse.ParseInt( "-nump", 100 ) // number of partitions
    let num2 = parse.ParseInt( "-nump", 100 ) // number of partitions
    let nSort = parse.ParseInt( "-sort", 0 )
    let nRand = parse.ParseInt( "-nrand", 16 )
    let nFilePN = parse.ParseInt( "-nfile", 8 )
    
    let mutable bExecute = false

    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))

    let defaultSearchPattern = "*.*"
    let searchPattern = parse.ParseString( "-spattern", defaultSearchPattern )
    let searchOption = if ( parse.ParseBoolean( "-rec", false ) ) then SearchOption.AllDirectories else SearchOption.TopDirectoryOnly



    let bAllParsed = parse.AllParsed Usage

    if bAllParsed then 
        Cluster.Start( null, PrajnaClusterFile )
        
        let cluster = Cluster.GetCurrent()
        
        


        // number of data files generated by each node
        let dataFileNumPerNode = nFilePN
        
        //let recordPerBatch = 500L * 1024L * 1024L

        //let recordsPerFilePerBatch = recordPerBatch / int64 cluster.NumNodes / int64 dataFileNumPerNode

        //let dataBatchNum = (int) ((records - 1L ) / recordPerBatch + 1L)
        

        //number of partition of input DKV; total number of data files
        let dataFileNum = dataFileNumPerNode * cluster.NumNodes

        //let recordsPerBatch = records / (int64) dataBatchNum


        //num should be bigger than 1
        let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (num-1),2.0)) + 1)) 
        let hashByteSize = (hashBitSize - 1 ) / 8 + 1
        
        let maxHashValue = 1 <<< (hashByteSize * 8) 




        let hashBitSize2 = (Math.Max(8, (int) (Math.Log((float) (num2*num-1),2.0)) + 1)) 
        let hashByteSize2 = (hashBitSize2 - 1 ) / 8 + 1
        
        let maxHashValue2 = 1 <<< (hashByteSize2 * 8) 
        //let rmtPart = RemotePartitionFunc( num, records, nDim,null)
        
        
        

        if bIn then 
            /// Distributed executing N gensort function each at each remote node
            
            let curJob = JobDependencies.setCurrentJob "SortGen"
            // Map local directory of sort Gen to a remote directory 
            curJob.AddDataDirectory( dirSortGen ) |> ignore 
            
            let t = (DateTime.UtcNow)
            

            let totalLen = ref 0L


            let startDKV = DSet<_>( Name = "SortGen", SerializationLimit = 1 ) 
            let dkv1 = startDKV.SourceN (dataFileNumPerNode, ( fun i -> Seq.singleton 1 ))
            let rmt = RemoteFunc( dataFileNum, records, nDim,num, num2,null,null)
            dkv1.NumParallelExecution <- 10
            let dkv2= dkv1 |> DSet.mapi rmt.GenerateDataFiles 

            dkv2.ToSeq() |> Seq.iter ( fun (parti, len) ->  totalLen := !totalLen + len
                                                            (Logger.LogF(LogLevel.MildVerbose, ( fun _ -> sprintf "Generated %d records for partition %d" (len/100L) parti )) )) //each record is 100 bytes
            





            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Data generation done! Generated %d records in total; Time cost:%A second" (!totalLen/100L) (DateTime.UtcNow - t).TotalSeconds ))
             
            bExecute <- true  

        if not bExecute && bVal then 
            /// Distributed executing N gensort function each at each remote node
            
            let curJob = JobDependencies.setCurrentJob "SortGen"
            // Map local directory of sort Gen to a remote directory 
            curJob.AddDataDirectory( dirSortGen ) |> ignore 

            let rmt = RemoteFunc(  dataFileNum, records, nDim,num, num2,null,null)
            
            let sortDSet = DSet<_>( Name = "SortSet", NumPartitions = num) 

            let resSet = sortDSet |> DSet.initS (fun (p,s) -> p) 1
            resSet.NumPartitions <- num
            resSet.SerializationLimit <- 1
            resSet.NumParallelExecution <- 10
            let valSet = resSet.Map rmt.Validate


            let failedIds = new List<_>()
            let valms = new MemStream()
            let valdata = new List<_>()
            valSet |> DSet.localIter (fun (parti,buf,len,output) ->     
                                                                    valdata.Add((parti,buf,len))
                                                                    printf "valsort on partition %d: \n " parti 
                                                                    if (output.Contains ("SUCCESS")) then
                                                                        printf "SUCCESS!\n%s\n" output
                                                                    else 
                                                                        printf "%s\n" output
                                                                        failedIds.Add((parti,output))
                                                                    Logger.LogF( LogLevel.Info, (fun _ -> sprintf "verified %d partitions, %d partitions are failed. %d" valdata.Count failedIds.Count len))
                                                                    )
            valdata.ToArray() 
                |> Array.sortBy (fun (parti,buf,len) -> parti) 
                |> Array.iter (fun (parti,buf,len) -> 
                                valms.Write(buf,0,280)
                                printf "%d\n" parti
                                )


            Logger.LogF( LogLevel.Info, (fun _ -> sprintf "# of Failure partitions: %d" failedIds.Count))

            failedIds.ToArray() |> Array.iter (fun (id,msg) -> 
                                                    Logger.LogF( LogLevel.Info, (fun _ -> sprintf "Failure partitions %d:\n message from valsort: %s" id msg))
                                    )


            use valfile = new FileStream(@"D:\MSR\sortbenchmark\val_"+(string) records+".sum",FileMode.Create)
            valfile.Write(valms.GetBuffer(),0, (int) valms.Length)
            valfile.Close()
            


            bExecute <- true  


        if not bExecute && bSample then
            
            let rmt1 = SamplingFunc(dataFileNum, records, nDim, sampleRate, hashByteSize + 1)

            let curJob = JobDependencies.setCurrentJob "SortGen"


            let startDKV = DSet<_>( Name = "SortGen") 
            let dkv1 = startDKV |> DSet.sourceN dataFileNumPerNode ( fun i -> Seq.singleton 1 ) 
            dkv1.NumParallelExecution <- 7
            let dkv2 = dkv1 |> DSet.mapi rmt1.ReadFiles  |> DSet.collect (Operators.id )

            let aggrFun (a:int64[]) (item:int64[]) =
                if (Utils.IsNotNull a) && (Utils.IsNotNull item) then
                    for i = 0 to a.Length - 1 do
                        a.[i] <- a.[i] + item.[i]
                    a
                else if (Utils.IsNotNull a) then
                    a
                else if (Utils.IsNotNull item) then
                    item
                else
                    null


            let accArr = 
                dkv2 |> DSet.reduce aggrFun 

            let totalRecords = ( accArr |> Array.toList<int64> |> List.sum  )
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "sampled %d records" totalRecords ))

            
            let binBoundary = Array.zeroCreate num
            let binSize = totalRecords / (int64 (binBoundary.Length))
            let mutable acc = 0L
            let mutable idx = 0
            for i = 0 to binBoundary.Length - 1 do
                while idx < accArr.Length && acc <= ((int64) (i + 1) * binSize) do
                    acc <- acc + accArr.[idx]
                    idx <- idx + 1
                binBoundary.[i] <- (idx >>> 8)

            binBoundary.[binBoundary.Length - 1] <- maxHashValue


            let uniBoundary = Array.init num (fun i -> (i+1)*(maxHashValue/num))
            Array.iter2 (fun a1 a2 -> if (a1 <> a2) then (printf "%d " (a1 - a2 ))  )  binBoundary uniBoundary


            bExecute <- true  
            ()

 
        if not bExecute && nSort = 1 then 
            let curJob = JobDependencies.setCurrentJob "SortGen"
            curJob.AddDataDirectory( dirSortGen ) |> ignore 
//            if bNetworkTest then 
//                // network test is inserted during sorting ...  
//                let networkParam = MonitorNetworkParam( )
//                RemoteInstance.Start( MonitorNetworkParam.MonitorNetworkServiceName, networkParam, ( fun _ -> MonitorNetworkInstance<MonitorNetworkParam>() ) )
//        
            let binBoundary = Array.init maxHashValue (fun i -> Math.Min(num-1,i/(maxHashValue/num)) )
            let numStage2 = num*num2
            let binBoundary2 = Array.init maxHashValue2 (fun i -> Math.Min(numStage2-1,i/(maxHashValue2/numStage2)) )
            let rmtPart = RemoteFunc( dataFileNum, records, nDim,num, num*num2,binBoundary,binBoundary2)

                

            let conf5() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- dataFileNumPerNode 

                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToSeq)
                dset1.NumParallelExecution <- dataFileNumPerNode 

                let dset3 = dset1 |> DSet.map (rmtPart.Repartition 1)
                dset3.NumParallelExecution <- dataFileNumPerNode 
                dset3.SerializationLimit <- 1

                let dset4 = dset3 |> DSet.collect Operators.id

                dset4.SerializationLimit <- 1
                dset4.NumParallelExecution <- dataFileNumPerNode

                //let dset4_re = dset4 |> DSet.rowsReorg (100)
                //dset4_re.NumParallelExecution <- dataFileNumPerNode

                let dset5 = dset4 |> DSet.repartitionN num (fun (i,ms,len) -> 
                                                                    i
                                                                ) 
                dset5.NumParallelExecution <- dataFileNumPerNode

            
//                let dset5_re = dset5 |> DSet.rowsReorg (100)
//                dset5_re.NumParallelExecution <- dataFileNumPerNode
            

                //Todo, cache the received data and the write to disk
                dset5 |> DSet.iter rmtPart.WritePreSortFuncFake
    
                ()


            let conf6() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- dataFileNumPerNode 
                
                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToSeq)
                dset1.NumParallelExecution <- dataFileNumPerNode 

                let dset3 = dset1 |> DSet.map (rmtPart.Repartition 1)
                dset3.NumParallelExecution <- dataFileNumPerNode 
                dset3.SerializationLimit <- 1


                dset3 |> DSet.iter rmtPart.WritePreSortFuncFakeSeq



//                let dset4 = dset3 |> DSet.collect Operators.id
//
//                dset4.SerializationLimit <- 1
//                dset4.NumParallelExecution <- dataFileNumPerNode
//
//                //let dset4_re = dset4 |> DSet.rowsReorg (100)
//                //dset4_re.NumParallelExecution <- dataFileNumPerNode
//
//                let dset5 = dset4 |> DSet.repartitionN num (fun (i,ms) -> 
//                                                                    i
//                                                                ) 
//                dset5.NumParallelExecution <- dataFileNumPerNode
//                
//                
//                let dset5_re = dset5 |> DSet.rowsReorg (100)
//                dset5_re.NumParallelExecution <- dataFileNumPerNode
//            
//
//                //Todo, cache the received data and the write to disk
//                dset5 |> DSet.iter rmtPart.WritePreSortFuncFake

                ()

            let conf7() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- dataFileNumPerNode 

                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToSeq)
                dset1.NumParallelExecution <- dataFileNumPerNode 

                let dset3 = dset1 |> DSet.map (rmtPart.NativeRepartition 1)
                dset3.NumParallelExecution <- dataFileNumPerNode
                dset3.SerializationLimit <- 1


                dset3 |> DSet.iter rmtPart.WritePreSortFuncFakeNativeRepSeq
//
//
//            let s = rmtPart.ReadFilesToMemStream(0)
//            let sa = s |> Seq.toArray 
//
//
//            let memBuf = new MemStream(100000)
//            let buf = Array.zeroCreate<byte> 100000
//            let rand = new Random()
//            rand.NextBytes(buf)
//            memBuf.WriteBytes(buf)
//
//            let p = rmtPart.RepartitionMemStream 1 memBuf
//            p |> Seq.iter(fun _ ->())

            //test memstream
            let conf8() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- dataFileNumPerNode 
                
                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToMemStream)
                dset1.NumParallelExecution <- dataFileNumPerNode 

                let dset3 = dset1 |> DSet.map (rmtPart.RepartitionMemStream 1)
                dset3.NumParallelExecution <- dataFileNumPerNode 
                dset3.SerializationLimit <- 1

                let dset4 = dset3 |> DSet.collect Operators.id

                dset4 |> DSet.iter rmtPart.RepartitionAndWriteToFile

            let t1 = (DateTime.UtcNow)
            conf8()
            let t2= (DateTime.UtcNow)
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Data is distributed, takes %f ms"  ((DateTime.UtcNow - t1).TotalMilliseconds) ))

//            let sortDSet = DSet<_>( Name = "SortSet", NumPartitions = num) 
//            
//
//            let resSet = sortDSet |> DSet.initS (fun (p,s) -> p) 1
//            resSet.NumPartitions <- num
//            
//            resSet.NumParallelExecution <- 60
//            
//            resSet  |> DSet.mapi (rmtPart.PipelineSort 20)
//                    |> DSet.toSeq 
//                    |> Seq.iter (fun (i,t) -> FTraceFLine FTraceLevel.Info ( fun _ -> sprintf "Sorted partition %d takes %f ms" i t))
//            
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Done, takes %f ms: stage 1 takes: %f ms, stage 2 takes: %f ms"  ((DateTime.UtcNow - t1).TotalMilliseconds) ((t2-t1).TotalMilliseconds)  ((DateTime.UtcNow - t2).TotalMilliseconds) ))
//

            bExecute <- true  
            () 
 
          
//        if bClose then 
//            RemoteInstance.Stop( MonitorNetworkParam.MonitorNetworkServiceName )
//            bExecute <- true  
//                

        Cluster.Stop()
    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
