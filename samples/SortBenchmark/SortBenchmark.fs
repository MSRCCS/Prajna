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
//open Prajna.Service.CoreServices
open Prajna.Api.FSharp
open Prajna.Api.FSharp

open sortbenchmark
open Microsoft.FSharp.NativeInterop
//open Prajna.Service.FSharp

let Usage = "
    Usage: Benchmark performance for distributed sort. \n\
    Command line arguments:\n\
    -in         Copy into Prajna \n\
    -dir        Directory where the sort gen file stays \n\
    -nump       Number of partitions in the second stage repartition\n\
    -sort       Executing Sort (1: gray sort) \n\
    -nfile      Number of data files in ***each node ***
    -records N  Number of records in total \n\
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

    let STLqsortwithLen (buffer:byte[], dim:int, len:int) = 
        let bufferHandle = GCHandle.Alloc(buffer,GCHandleType.Pinned);
        let pinedbuffer = bufferHandle.AddrOfPinnedObject()        
        stdqsort(pinedbuffer, len / dim, dim)
        bufferHandle.Free()

    let STLqsort (buffer:byte[], dim:int) = 
        let bufferHandle = GCHandle.Alloc(buffer,GCHandleType.Pinned);
        let pinedbuffer = bufferHandle.AddrOfPinnedObject()        
        stdqsort(pinedbuffer, buffer.Length / dim, dim)
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
    extern void MyMemoryCopy(nativeint srcBuf, int srcOff, nativeint dest, int destOff, int size);
    let NativeMemoryCopy (srcBuf:byte[], srcOff:int, destBuf:byte[], destOff:int, len:int) = 
        let sbufHandle = GCHandle.Alloc(srcBuf,GCHandleType.Pinned);
        let pinedsBuf = sbufHandle.AddrOfPinnedObject()
        
        let dbufHandle = GCHandle.Alloc(destBuf,GCHandleType.Pinned);
        let pineddBuf = dbufHandle.AddrOfPinnedObject()

        MyMemoryCopy(pinedsBuf,srcOff,pineddBuf,destOff,len)
        dbufHandle.Free()
        sbufHandle.Free()

type RepartitionStage = StageOne=1 | StageTwo=2

/// records is total number of records
[<Serializable>]
type RemoteFunc( filePartNum:int, records:int64, _dim:int , partNumS1:int, partNumS2:int, stageOnePartionBoundary:int[], stageTwoPartionBoundary:int[]) =    
    member val dim = _dim with get 
    member val diskHelper = new DiskHelper(records) with get
    


    static member val sharedMem = new ConcurrentQueue<byte[]>()
    static member val sharedMemSize = ref 0


    static member val partiSharedMem = new ConcurrentQueue<byte[]>()
    static member val partiSharedMemSize = ref 0
    
    member val blockSizeReadFromFile = 1024*1000*100
    member x.repartitionBlockSize = (x.blockSizeReadFromFile / partNumS1 / 1000) * 100

    
    member x.HDIndex = [|"c:\\";"d:\\";"e:\\";"f:\\"|]
    member x.HDReaderLocker = Array.init (x.HDIndex.Length) (fun _ -> ref 0)


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






    member x.GetReadFileBuf() = 
            let byt = ref Unchecked.defaultof<_>
            let bnewbuf = ref false
            if not (RemoteFunc.sharedMem.TryDequeue(byt)) then
                if (Interlocked.Increment(RemoteFunc.sharedMemSize) < 300) then
                    byt := Array.zeroCreate<_> (x.blockSizeReadFromFile)
                    bnewbuf := true
                else 
                    Interlocked.Decrement(RemoteFunc.sharedMemSize) |> ignore
                    while not (RemoteFunc.sharedMem.TryDequeue(byt)) do
                        ()
            !byt, !bnewbuf


    member x.ReadFilesToSeq parti = 
            let toRead = x.blockSizeReadFromFile

            let _, filename = x.diskHelper.GetDataFilePath()
            if Utils.IsNotNull filename then
                let readLen = ref Int32.MaxValue
                let fi = new FileInfo(filename)
                let len = fi.Length

                let totalReadLen = ref 0L
                

                let ret =
                    seq {
                        use file = new FileStream(filename, FileMode.Open)
                        let counter = ref 0
                        while !readLen > 0 do 

                            let byt, bnewbuf = x.GetReadFileBuf() 

                            readLen := file.Read( byt, 0, toRead)
                
                            totalReadLen := !totalReadLen + (int64) toRead

                            if !totalReadLen >= len then
                                readLen := 0
                            else   
                                readLen := toRead

                            counter := !counter + 1
                            if (!counter % 10 = 0) then
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read %d bytes from file, new buf: %b" !totalReadLen bnewbuf)  )
                            if (!readLen<>0 && ((!readLen) % x.dim)<>0) then
                                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "read an incomplete record" ))
                            x.diskHelper.ReportReadBytes((int64)!readLen)
                            yield byt, !readLen

                        let flushBuf = Array.init<byte> 1 (fun _ -> byte 0)
                        yield flushBuf, 1
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Send Flush Buffer signal from read file"))
                        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "UTC %s, all data from file %s has been read" (UtcNowToString()) filename)            )
                    }

                ret
            else 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "!!!!!Cannot Get Data file " ))
                Seq.empty

    member x.ReadFilesToSeqF parti = 
            let toRead = x.blockSizeReadFromFile
            if true then
                let readLen = ref Int32.MaxValue
                let len = 62500000000L

                let totalReadLen = ref 0L
                let rand = new Random(DateTime.UtcNow.Millisecond)
                let rndBuffer = Array.zeroCreate<_> (toRead)
                rand.NextBytes(rndBuffer)
                let ret =
                    seq {
                        let counter = ref 0
                        while !readLen > 0 do 

                            let byt, bnewbuf = x.GetReadFileBuf() 
                            Buffer.BlockCopy(rndBuffer,0,byt,0,toRead)
                            totalReadLen := !totalReadLen + (int64) toRead
                            if !totalReadLen >= len then
                                readLen := 0
                            else   
                                readLen := toRead
                            counter := !counter + 1
                            if (!counter % 10 = 0) then
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read %d bytes from file, new buf: %b" !totalReadLen bnewbuf)  )
                            if (!readLen<>0 && ((!readLen) % x.dim)<>0) then
                                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "read an incomplete record" ))
                            x.diskHelper.ReportReadBytes((int64)!readLen)
                            yield byt, !readLen
                    }
                ret
            else 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "!!!!!Cannot Get Data file " ))
                Seq.empty


    member internal x.ReadFilesToMemStream parti = 
            let defaultReadBlock = x.blockSizeReadFromFile
            let tbuf = Array.zeroCreate<byte> defaultReadBlock
            let _, filename = x.diskHelper.GetDataFilePath()
            if Utils.IsNotNull filename then
                let fi = new FileInfo(filename)
                let len = fi.Length
                let totalReadLen = ref 0L
                let readLen = ref Int32.MaxValue
                let ret =
                    seq {
                        use file = new FileStream(filename, FileMode.Open)
                        let counter = ref 0
                        while !readLen > 0 do 
                                let memBuf = new MemoryStreamB()

                                readLen := file.Read( tbuf, 0, defaultReadBlock)
                                if !readLen > 0 then
                                    memBuf.Write(tbuf,0,!readLen)
                                    totalReadLen := !totalReadLen + (int64) !readLen

                                    counter := !counter + 1
                                    if (!counter % 100 = 0) then
                                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read %d bytes from file" !totalReadLen) )
                                    x.diskHelper.ReportReadBytes((int64)!readLen)
                                    yield memBuf
                        Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "UTC %s, all data from file %s has been read" (UtcNowToString()) filename)    )        
                    }
                
                ret
            else
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "!!!!!Cannot Get Data file " ))
                Seq.empty


    member internal x.ReadFilesToMemStreamF parti = 
            
            let defaultReadBlock = x.blockSizeReadFromFile
            let tbuf = Array.zeroCreate<byte> defaultReadBlock
            let rand = new Random()
            rand.NextBytes(tbuf)
            
            let counter = ref 0
            let len = 62500000000L
            let totalReadLen = ref 0L
            let ret =
                seq {
                    while !totalReadLen < len do 
                        let toRead = int32 (Math.Min(int64 defaultReadBlock, len - !totalReadLen))
                        if toRead > 0 then
                            let memBuf = new MemoryStreamB()
                            let ttbuf = Array.zeroCreate<byte> tbuf.Length
                            Buffer.BlockCopy(tbuf,0,ttbuf,0,tbuf.Length)
                            memBuf.Write(tbuf,0,toRead)
                            totalReadLen := !totalReadLen + (int64) toRead

                            counter := !counter + 1
                            if (!counter % 100 = 0) then
                                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Read %d bytes from file" !totalReadLen) )
                            x.diskHelper.ReportReadBytes((int64)toRead)
                            yield memBuf
                }
            ret

    member val repartitionThread = ref 0


    member internal x.RepartitionMemStream (stage:RepartitionStage) (buffer:MemoryStreamB) = 
        if buffer.Length > 0L then
            let retseq = seq {

                        let partionBoundary =
                            if stage = RepartitionStage.StageOne then
                                stageOnePartionBoundary
                            else 
                                stageTwoPartionBoundary    

                        let nump = 
                            if stage = RepartitionStage.StageOne then
                                partNumS1
                            else 
                                partNumS2

                        let partstream = Array.init<MemoryStreamB> nump (fun i -> 
                                                                            null
                                                                            )

                        let t1 = DateTime.UtcNow
                        let bHasBuf = ref true
                        let sr = new StreamReader<byte>(buffer,0L)

                        let bRemainRecord = ref false
                        let remainBuf = ref Unchecked.defaultof<byte[]>
                        let remainpos = ref 0
                        let remainLen = ref 0

                        while !bHasBuf do
                            let (buf, pos,len) = sr.GetMoreBuffer()
                            if len > 0 then
                                let idx = ref pos

                                if !bRemainRecord then
                                    if (!remainLen - !remainpos >=2) then
                                        let index = (((int) (!remainBuf).[!remainpos]) <<< 8) + ((int) (!remainBuf).[!remainpos + 1])
                                        let parti = partionBoundary.[index]

                                        if Utils.IsNull partstream.[parti] then
                                            let ms = new MemoryStreamB()
                                            ms.WriteByte((byte)parti)
                                            partstream.[parti] <- ms
                                        partstream.[parti].Write((!remainBuf),!remainpos,(!remainLen - !remainpos))

                                        //there is a bug, need to handle the case of new buffer is shorter than 100-(!remainLen - !remainpos) bytes!!
                                        partstream.[parti].Write(buf,!idx,x.dim - (!remainLen - !remainpos))
                                        idx := !idx + x.dim - (!remainLen - !remainpos)
                                    else
                                        let index = (((int) (!remainBuf).[!remainpos]) <<< 8) + ((int) buf.[!idx])
                                        let parti = partionBoundary.[index]

                                        if Utils.IsNull partstream.[parti] then
                                            let ms = new MemoryStreamB()
                                            ms.WriteByte((byte)parti)
                                            partstream.[parti] <- ms
                                        partstream.[parti].Write((!remainBuf),!remainpos,(!remainLen - !remainpos))

                                        //there is a bug, need to handle the case of new buffer is shorter than 100-(!remainLen - !remainpos) bytes!!
                                        partstream.[parti].Write(buf,!idx,x.dim - (!remainLen - !remainpos))
                                        idx := !idx + x.dim - (!remainLen - !remainpos)

                                while (!idx < pos) do
                                    let index = (((int) buf.[!idx]) <<< 8) + ((int) buf.[!idx + 1])
                                    let parti = partionBoundary.[index]

                                    if Utils.IsNull partstream.[parti] then
                                        let ms = new MemoryStreamB()
                                        ms.WriteByte((byte)parti)
                                        partstream.[parti] <- ms
                                    partstream.[parti].Write(buf,!idx,x.dim)
                                    idx := !idx + x.dim
                                if !idx < len then
                                    bRemainRecord := true
                                    remainBuf := buf
                                    remainpos := !idx
                                    remainLen := len
                            else
                                bHasBuf := false
                        sr.Release()

                        (buffer :> IDisposable).Dispose()
                        let t2 = DateTime.UtcNow

                        for i = 0 to nump - 1 do
                            if Utils.IsNotNull partstream.[i] then
                                if (partstream).[i].Length > 0L then
                                    (partstream).[i].Seek(0L, SeekOrigin.Begin) |> ignore
                                    let tBuf = Array.zeroCreate<byte> ((int)(partstream).[i].Length)
                                    let rand = new Random()
                                    rand.NextBytes(tBuf)
                                    yield (partstream).[i]
                                else 
                                    ((partstream).[i] :> IDisposable).Dispose()
                        
                        Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "repartition: %d records, takes %f s" (buffer.Length / 100L) ((t2-t1).TotalSeconds) )          )          
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


    static member val sharedrepartitionBuf = new ConcurrentQueue<byte[]>()
    static member val sharedrepartitionBufSize = ref 0

    member x.GetSharedrepartitionBuf() = 
        let oBuf = ref Unchecked.defaultof<byte[]>

        if not (RemoteFunc.sharedrepartitionBuf.TryDequeue(oBuf)) then
            if (!RemoteFunc.sharedrepartitionBufSize <= 50) then   
                oBuf := Array.zeroCreate<_> (x.maxDumpFileSize)
                Interlocked.Increment(RemoteFunc.sharedrepartitionBufSize) |>  ignore
            else 

                while not (RemoteFunc.sharedrepartitionBuf.TryDequeue(oBuf)) do
                    ()    
        !oBuf

 // native repartition, use per-allocated memory. For testing and comparing to MemoryStreamB only, cannot be used in remote server
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

                            //let oBuf, bnewbuf = x.GetReadFileBuf() 
                            let oBuf = x.GetSharedrepartitionBuf()
                            let r = Interop.NativeBin(buffer,size,x.dim,nump,partionBoundary,oBuf)

                            //RemoteFunc.sharedMem.Enqueue(buffer)

                            let t2 = DateTime.UtcNow
                            let partitionNum = ref 0
                            if r.[0] > 0 then
                                let toCopyLen = r.[0]
                                let tl = ref 0
                                while (!tl < toCopyLen) do
                                    let iBuf:byte[] = x.GetRepartitionMemBuf()
                                    let cpLen = Math.Min(toCopyLen - !tl, (iBuf.Length))
                                    Buffer.BlockCopy(buffer,0,iBuf,0,cpLen)
                                    tl := !tl + cpLen
                                    yield (0,iBuf,cpLen)
                                partitionNum := !partitionNum + 1
                        
                            for i = 1 to nump-1 do
                                if r.[i] > r.[i-1] then
                                    let toCopyLen = (r.[i]-r.[i-1])
                                    let tl = ref 0
                                    while (!tl < toCopyLen) do
                                        let iBuf:byte[] = x.GetRepartitionMemBuf()
                                        let cpLen = Math.Min(toCopyLen - !tl, (iBuf.Length))
                                        Buffer.BlockCopy(buffer,0,iBuf,0,cpLen)
                                        tl := !tl + cpLen
                                        yield (i ,iBuf,cpLen)
                                    partitionNum := !partitionNum + 1

                            RemoteFunc.sharedrepartitionBuf.Enqueue(oBuf) |> ignore
                            let t3 = DateTime.UtcNow
                            Interlocked.Decrement(x.repartitionThread) |> ignore
                            //RemoteFunc.sharedMem.Enqueue(oBuf)
                            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "native repartition: %d records, into %d partitions, takes %f / %f s; # repartition threads: %d; shareMem Len: %d; " (size / 100) !partitionNum ((t2-t1).TotalSeconds) ((t3-t1).TotalSeconds) !x.repartitionThread RemoteFunc.sharedMem.Count)  )                  

                        }
            retseq
        else 
            Seq.empty


   
    member internal x.NativeRepartitionWithMemStream (stage:RepartitionStage) (buffer:byte[], size:int)=
        if size > 1 then
            let retseq = seq {
                            let partionBoundary =
                                if stage = RepartitionStage.StageOne then
                                    stageOnePartionBoundary
                                else 
                                    stageTwoPartionBoundary
                            let nump = 
                                if stage = RepartitionStage.StageOne then
                                    partNumS1
                                else 
                                    partNumS2

                            
                            let t1 = DateTime.UtcNow
                            Interlocked.Increment(x.repartitionThread) |> ignore

                            let oBuf, bnewbuf = x.GetReadFileBuf() 


                            let r = Interop.NativeBin(buffer,size,x.dim,nump,partionBoundary,oBuf)

                            RemoteFunc.sharedMem.Enqueue(buffer)

                            let t2 = DateTime.UtcNow

                            let partitionNum = ref 0
                            if r.[0] > 0 then
                                
                                let iBuf = new MemoryStreamB()
                                iBuf.WriteByte(byte 0)
                                iBuf.Write(oBuf,0,r.[0])
                                partitionNum := !partitionNum + 1
                                iBuf.Seek(0L,SeekOrigin.Begin) |> ignore
                                yield iBuf
                        
                            for i = 1 to nump-1 do
                                if r.[i] > r.[i-1] then
                                    let toCopyLen = (r.[i]-r.[i-1])
                                    let tl = ref 0

                                    let iBuf = new MemoryStreamB(toCopyLen)
                                    iBuf.WriteByte(byte i)
                                    iBuf.Write(oBuf,r.[i-1],toCopyLen)
                                    partitionNum := !partitionNum + 1
                                    yield iBuf



                            let t3 = DateTime.UtcNow
                            Interlocked.Decrement(x.repartitionThread) |> ignore
                            RemoteFunc.sharedMem.Enqueue(oBuf)
                            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "repartition: %d records, into %d partitions, takes %f / %f s; # repartition threads: %d; shareMem Len: %d; new allocated mem: %A" (size / 100) !partitionNum ((t2-t1).TotalSeconds) ((t3-t1).TotalSeconds) !x.repartitionThread RemoteFunc.sharedMem.Count bnewbuf)  )                  

                        }
            retseq
        elif size = 1 then
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Send Flush Buffer signal from repartition"))
                
                seq {
                    let nump = 
                        if stage = RepartitionStage.StageOne then
                            partNumS1
                        else 
                            partNumS2 
                    for i = 0 to nump-1 do
                        let iBuf = new MemoryStreamB()
                        iBuf.WriteByte((byte) i)
                        iBuf.WriteByte((byte) 1)
                        yield iBuf  
                    }      
        else 
            Seq.empty




    // repartition, use per-allocated memory. For testing and comparing to MemoryStreamB only, cannot be used in remote server
    member x.RepartitionSharedMemory (stage:RepartitionStage) (buffer:byte[], size:int) = 
        if size > 0 then
            
            let retseq = seq {
                        let partionBoundary =
                            if stage = RepartitionStage.StageOne then
                                stageOnePartionBoundary
                            else 
                                stageTwoPartionBoundary        
                        let nump = 
                            if stage = RepartitionStage.StageOne then
                                partNumS1
                            else 
                                partNumS2

                        let hashByteSize = 
                            if stage = RepartitionStage.StageOne then
                                let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (partNumS1-1),2.0)) + 1)) 
                                (hashBitSize - 1 ) / 8 + 1
                            else 
                                let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (partNumS2-1),2.0)) + 1)) 
                                (hashBitSize - 1 ) / 8 + 1


                        let partstream = Array.init nump (fun _ -> x.GetRepartitionMemBuf())


                        let posi = Array.zeroCreate<int> nump
                        let t1 = DateTime.UtcNow

                        for i in [|0..(size/x.dim - 1)|] do
                            let indexV = ref 0
                            for p = 0 to hashByteSize - 1 do
                                indexV := (!indexV <<< 8) + int buffer.[i*x.dim+p]

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

                        Logger.LogF( LogLevel.MildVerbose ,(fun _ -> sprintf "repartition: %d records, takes %f s, !RemoteFunc.partiSharedMemSize %d; RemoteFunc.partiSharedMem.length %d " (size / 100) ((t2-t1).TotalSeconds) !RemoteFunc.partiSharedMemSize RemoteFunc.partiSharedMem.Count)                    )
                        RemoteFunc.sharedMem.Enqueue(buffer)
                    }
            retseq

        else 
            Seq.empty















//
//    member val writeFileLock = Array.create stageTwoPartionBoundary.Length (ref 0)
//    member val minparti = Int32.MaxValue with get, set
//    member val writeCacheBlockSize = 10000000
//    member val writeCache = new ConcurrentDictionary<int,byte[]>()
//    member val writeCachePos = new ConcurrentDictionary<int,int>()
//
//
//
//
//    member val memStreamBuf =  new ConcurrentQueue<MemoryStreamB>()
//    
//    member val memStreamBufEmptyHandel:Object = null with get,set
//    member val memStreamBufFullHandel:Object = null with get,set




    member val sortThread = null with get,set
    member x.SortDumpFile() =
        let buf = Array.zeroCreate<byte> x.maxDumpFileSize
        let fn = ref Unchecked.defaultof<_>
        while (true) do
            if ((x.rollingFileMgr :?> RollingFileMgr).readyFileQ.TryDequeue(fn)) then
                let fh = new FileStream(!fn,FileMode.Open)
                let len = fh.Read(buf,0,x.maxDumpFileSize)
                fh.Close()
                if (len > 0) then
                    Interop.STLqsortwithLen(buf,x.dim,len)
                let fh = new FileStream((!fn)+".sorted",FileMode.Create)
                fh.Write(buf,0,len)
                fh.Close()
                File.Delete(!fn)
                

    static member val repartitionBuf = new ConcurrentStack<byte[]*int>()
    static member val repartitionBufReady = new ConcurrentQueue<byte[]*int>()
    member val writeActionQ = Array.init partNumS2 (fun _ ->  null)
    member val writeFileHandle = Array.init<Object> partNumS2 (fun _ ->  null)
    member x.writeAct (parti:int,buf:byte[],off:int,len:int, flush:bool) ()=
                                        if not flush then
                                            if (Utils.IsNull x.writeFileHandle.[parti]) then
                                                x.writeFileHandle.[parti] <- new FileStream( x.diskHelper.GeneratePartitionFilePath(parti),FileMode.Append) :> Object
                                            (x.writeFileHandle.[parti]  :?> FileStream).Write(buf,off,len)
                                            RemoteFunc.partiSharedMem.Enqueue(buf)
                                            ()
                                        else
                                            if (Utils.IsNotNull x.writeFileHandle.[parti]) then
                                                (x.writeFileHandle.[parti]  :?> FileStream).Close()
                                                x.writeFileHandle.[parti]  <- Unchecked.defaultof<_>




    member x.RepartitionMem() =
        RemoteFunc.repartitionBuf.Push(Array.zeroCreate<byte> x.maxDumpFileSize,0)
        RemoteFunc.repartitionBuf.Push(Array.zeroCreate<byte> x.maxDumpFileSize,0)
        RemoteFunc.repartitionBuf.Push(Array.zeroCreate<byte> x.maxDumpFileSize,0)
        let elem = ref Unchecked.defaultof<_>

        while (true) do
            if (RemoteFunc.repartitionBufReady.TryDequeue(elem)) then
                let buf,len = !elem
                x.NativeRepartition 2 (buf,len)
                |> Seq.iter (fun (parti,rbuf,rlen) -> 
                                            if (Utils.IsNull x.writeActionQ.[parti]) then
                                                x.writeActionQ.[parti] <- new SingleThreadExec1()
                                            x.writeActionQ.[parti].ExecQ((x.writeAct (parti,rbuf,0,rlen,false)))
                                            ()
                                )
                if !(x.CleaningUp) = 1 && RemoteFunc.repartitionBufReady.IsEmpty then
                    x.writeActionQ |> Array.iteri (fun i q -> if Utils.IsNotNull q then 
                                                                    q.Flush <- true
                                                                    q.ExecQ((x.writeAct (i,null,0,0,true)))
                                                            
                                                            )

                RemoteFunc.repartitionBuf.Push(buf,0)
            if ((x.WriteEventHandle :?> ManualResetEvent).WaitOne(0)) then
                (x.WriteEventHandle :?> ManualResetEvent).Reset() |> ignore
            else 
                (x.WriteEventHandle :?> ManualResetEvent).WaitOne() |> ignore


    member val rollingFileMgr:Object = null with get,set
    member val internal dumpCache = new ConcurrentQueue<MemoryStreamB>()
    member val maxDumpFileSize = 100000000


    member internal x.RepartitionAndWriteToFileMem ( ms:MemoryStreamB ) = 
        (ms :> IDisposable).Dispose()

    member val WriteEventHandle:Object = null with get,set

    member val CleaningUp = ref 0

    member x.CleanedUp() = 
        let bFinishedCleanup = ref true
        x.writeFileHandle |> Array.iter (fun h -> if Utils.IsNotNull h then bFinishedCleanup := false)
        if x.dumpCache.Count > 0 then 
            bFinishedCleanup := false
        if RemoteFunc.repartitionBufReady.Count > 0 then
            bFinishedCleanup := false

        x.writeActionQ |> Array.iter (fun q -> if Utils.IsNotNull q && not (q.IsEmpty()) then bFinishedCleanup := false)
        !bFinishedCleanup

    member val CleanUpSignalCount = ref 0

    member internal x.RepartitionAndWriteToFileMemuseless ( ms:MemoryStreamB ) = 
        
        let CleanCache(bForceCache:bool) = 
                let elem = ref Unchecked.defaultof<_> 
                let cacheCount = x.dumpCache.Count
                if bForceCache && cacheCount > 0 then
                    if not (RemoteFunc.repartitionBuf.TryPop(elem)) then
                        elem := (Array.zeroCreate<byte> x.maxDumpFileSize,0)
                        ()

                if (bForceCache && cacheCount > 0 || RemoteFunc.repartitionBuf.TryPop(elem)) then
                    let mutable fp, filesize = !elem

                    
                    for i = 0 to cacheCount - 1 do
                        let ms:MemoryStreamB ref = ref Unchecked.defaultof<_>
                        if (x.dumpCache.TryDequeue(ms)) then
                            let bHasBuf = ref true
                            let sr = new StreamReader<byte>((!ms),(!ms).Position)

                            //x.maxDumpFileSize has to be divisible by x.dim!!
                            while !bHasBuf do
                                let (buf, pos,len) = sr.GetMoreBuffer()


                                let mutable rlen = len
                                let mutable rpos = pos
                                while rlen > 0 do
                                    let toWrite = Math.Min(rlen,x.maxDumpFileSize-filesize)
                                    Buffer.BlockCopy(buf,rpos,fp,filesize,toWrite)
                                    rlen <- rlen - toWrite
                                    filesize <- filesize + toWrite
                                    rpos <- rpos + toWrite
                                    if filesize >= x.maxDumpFileSize then
                                        RemoteFunc.repartitionBufReady.Enqueue(fp,filesize)
                                        (x.WriteEventHandle :?> ManualResetEvent).Set() |> ignore
                                        let elem1 = ref Unchecked.defaultof<_> 
                                        if bForceCache then
                                            if not (RemoteFunc.repartitionBuf.TryPop(elem1)) then
                                                elem1 := (Array.zeroCreate<byte> x.maxDumpFileSize,0)
                                                ()
                                        else 
                                            while not (RemoteFunc.repartitionBuf.TryPop(elem1)) do
                                                ()
                                        fp <-fst (!elem1)
                                        filesize <- snd (!elem1)
                                if len = 0 then
                                    bHasBuf := false
                            sr.Release()
                            ((!ms) :> IDisposable).Dispose()
                    RemoteFunc.repartitionBuf.Push(fp,filesize)

        if Utils.IsNull x.sortThread then
            lock(x) (fun _ ->
                                if Utils.IsNull x.sortThread then
                                    x.WriteEventHandle <- (new ManualResetEvent(true)) :> Object
                                    x.sortThread <- Array.init 10 (fun i -> 
                                            let t = new Thread(x.RepartitionMem)
                                            t.Start()
                                            t
                                    )
                            )

        ms.Seek(0L,SeekOrigin.Begin) |> ignore
        let parti = ms.ReadByte()
        let t1 = DateTime.UtcNow        
        
        if (ms.Length = 2L) then
            let cc = Interlocked.Increment(x.CleanUpSignalCount) 
            if cc = filePartNum then
                if (Interlocked.CompareExchange(x.CleaningUp,1,0) = 0) then
                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Flush Buffer"))
                    (ms :> IDisposable).Dispose()
                    CleanCache(true)
                    let elem = ref Unchecked.defaultof<_> 
                    while (RemoteFunc.repartitionBuf.TryPop(elem) && !(x.CleaningUp) = 1) do
                        let mutable fp, filesize = !elem
                        RemoteFunc.repartitionBufReady.Enqueue(fp,filesize)

                    let mutable t1 = DateTime.UtcNow
                    while not (x.CleanedUp()) && !(x.CleaningUp)  = 1 do
                        (x.WriteEventHandle :?> ManualResetEvent).Set() |> ignore
                        if (DateTime.UtcNow - t1).TotalSeconds > 5. then
                            let wfcount = ref 0
                            let wacount = ref 0

                            x.writeFileHandle |> Array.iter (fun h -> if Utils.IsNotNull h then wfcount := !wfcount + 1)
                            x.writeActionQ |> Array.iter (fun q -> if Utils.IsNotNull q && not (q.IsEmpty()) then wacount := !wacount + 1)

                            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "waiting for flush buffer. RemoteFunc.repartitionBufReady %d,x.CleanUpSignalCount: %d, x.dumpCache %d,RemoteFunc.repartitionBuf %d,!wacount %d, !wfcount %d" RemoteFunc.repartitionBufReady.Count !(x.CleanUpSignalCount) x.dumpCache.Count RemoteFunc.repartitionBuf.Count !wacount !wfcount))
                            t1 <- DateTime.UtcNow
                        ()



                    if (!(x.CleaningUp) = 0) then
                        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Flush Buffer is Canceled, because new data arrived"))
                

        else 
            while (x.dumpCache.Count > 100) do
                CleanCache(false)

            x.dumpCache.Enqueue(ms) |> ignore


    member x.ReturnSharedBuf ( parti:int,buf:byte[],len:int ) = 
        RemoteFunc.partiSharedMem.Enqueue(buf)
        
    member internal x.DeRefMemStream ( buf:MemoryStreamB ) = 
        (buf :> IDisposable).Dispose()
        ()

    member internal x.DeRefMemStreamSeq ( data ) = 
//        data |> Seq.iter (fun (parti:int,buf:MemoryStreamB) ->
//                                    buf.DecRef()
//                                    )
        data |> Seq.iter (fun (parti:int,buf:MemoryStreamB) ->
                                    //buf.DecRef()
                                    ()
                                    )
        0

    member val readMRELock = ref 0
    member val readMRE:Object = null with set, get

    member val sortLock = ref 0
    member val readSemaphore = ref 0
    member val sortSemaphore = ref 0
    member val writeSemaphore = ref 0
    member val sortMRE:Object = null with set, get
    member val sortedPartitionCounter = ref 0

    member x.PipelineSort (threads : int) parti serial kv = 

        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!!!!!!!!!!!RemoteFunc.repartitionBufReady.Count %d" RemoteFunc.repartitionBufReady.Count ))
        

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
        

        let pi,filename = x.diskHelper.GetNextPartitionFilePath()

        if (File.Exists(filename)) then

            let fi = new FileInfo(filename)
            if (fi.Length >= (int64) Int32.MaxValue) then
                Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!!ERROR!!! data in a single partition %d is bigger then 2GB" parti ))
            let len = (int32) fi.Length
            let buffer = Array.zeroCreate<byte> len
            let sortCache1 = Array.zeroCreate<uint64> (len/x.dim)
            let sortCache2 = Array.zeroCreate<uint64> (len/x.dim)
            let read () = 


                let bRead = ref false
                let HDi = Array.IndexOf(x.HDIndex, Path.GetPathRoot(filename).ToLower())

                while not !bRead do
                    (x.readMRE :?> ManualResetEvent).Reset() |> ignore
                    if Interlocked.CompareExchange(x.HDReaderLocker.[HDi],1,0) = 0 then
                        let t1 = DateTime.UtcNow
                        use file = new FileStream(filename,FileMode.Open)
                        let readLen = ref Int32.MaxValue
                        readLen := file.Read( buffer, 0, len )
                        if (!readLen <> len) then
                            Logger.LogF( LogLevel.Error, (fun _ -> sprintf "read file error"))
                        file.Close()      
                        File.Delete(filename);          
                        let t2 = DateTime.UtcNow
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "UTC %s, Read data in partition %d  ( %s ), takes %f ms" (UtcNowToString()) parti filename ((t2-t1).TotalMilliseconds)))

                        bRead := true


                        // the order of the following two lines are important. we should unlock before Set()
                        x.HDReaderLocker.[HDi] := 0
                        (x.readMRE :?> ManualResetEvent).Set() |> ignore

                    if (!bRead) then
                        (x.readMRE :?> ManualResetEvent).WaitOne() |> ignore
                ()
            

            let sort () =
                let t1 = DateTime.UtcNow
                Interop.STLqsort(buffer,x.dim)
                let t2 = DateTime.UtcNow
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "UTC %s, sort data in memory takes %f ms" (UtcNowToString()) ((t2-t1).TotalMilliseconds)))
                ()


            let write () = 

                let resultfilename = x.diskHelper.GenerateSortedFilePath(parti)

                use wfile = new FileStream(resultfilename,FileMode.Create)
                let wt1 = DateTime.UtcNow
                let mutable pos = 0
                while (pos < buffer.Length) do
                    let count = Math.Min(buffer.Length-pos,1024*1024*10)
                    wfile.Write(buffer, pos, count)
                    x.diskHelper.ReportWriteBytes(int64 count)
                    pos <- pos + count
                wfile.Flush()
                wfile.Close()
                let wt2 = DateTime.UtcNow
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "UTC %s, write %d records to file %s, takes %f ms" (UtcNowToString()) (buffer.Length/100) filename ((wt2-wt1).TotalMilliseconds)))
                ()

            let pst1 = DateTime.UtcNow
            

            let mutable canrun = false
            while (not canrun) do
                if (Interlocked.CompareExchange(x.sortLock, 1, 0) = 0) then
                    if (!x.readSemaphore < threads) then
                        canrun <- true
                        x.readSemaphore := !x.readSemaphore + 1
                    else
                        (x.sortMRE :?> ManualResetEvent).Reset() |> ignore
                    x.sortLock := 0
            
                if not canrun then
                    (x.sortMRE :?> ManualResetEvent).WaitOne() |> ignore
        
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "UTC %s, reading threads:%d" (UtcNowToString()) (!x.readSemaphore)))
            read()
            Interlocked.Decrement(x.readSemaphore) |> ignore
            (x.sortMRE :?> ManualResetEvent).Set() |> ignore


            canrun <- false
            while (not canrun) do
                if (Interlocked.CompareExchange(x.sortLock, 1, 0) = 0) then
                    if (!x.sortSemaphore < threads) then
                        canrun <- true
                        x.sortSemaphore := !x.sortSemaphore + 1
                    else
                        (x.sortMRE :?> ManualResetEvent).Reset() |> ignore
                    x.sortLock := 0
            
                if not canrun then
                    (x.sortMRE :?> ManualResetEvent).WaitOne() |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "UTC %s, sorting threads:%d" (UtcNowToString()) (!x.sortSemaphore)))
            sort()
            Interlocked.Decrement(x.sortSemaphore) |> ignore
            (x.sortMRE :?> ManualResetEvent).Set() |> ignore


            canrun <- false
            while (not canrun) do
                if (Interlocked.CompareExchange(x.sortLock, 1, 0) = 0) then
                    if (!x.writeSemaphore < threads) then
                        canrun <- true
                        x.writeSemaphore := !x.writeSemaphore + 1
                    else
                        (x.sortMRE :?> ManualResetEvent).Reset() |> ignore
                    x.sortLock := 0
            
                if not canrun then
                    (x.sortMRE :?> ManualResetEvent).WaitOne() |> ignore
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "writing threads:%d" (!x.writeSemaphore)))
            write()
            Interlocked.Decrement(x.writeSemaphore) |> ignore
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
    
    let nDim = parse.ParseInt( "-dim", 100 )
    let records = parse.ParseInt64( "-records", 1000000L ) // number of total records
    let bIn = parse.ParseBoolean( "-in", false )
    let bVal = parse.ParseBoolean( "-val", false )
    
    let bOut = parse.ParseBoolean( "-out", false )
    let bSample = parse.ParseBoolean( "-sample", false )
    let sampleRate = parse.ParseInt( "-samplerate", 100 ) // number of partitions
    let dirSortGen = parse.ParseString( "-dir", "." )
    //let num = parse.ParseInt( "-nump", 200 ) // number of partitions
    let num2 = parse.ParseInt( "-nump", 200 ) // number of partitions
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
        
        let num = cluster.NumNodes
        let numStage2 = num*num2

        let binBoundary = Array.init 65536 (fun i -> Math.Min(num-1,i/(65536/num)) )

        let binBoundary2 = Array.init 65536 (fun i -> Math.Min(numStage2-1,i/(65536/numStage2)) )



        let rmtPart = RemoteFunc( 16, records, nDim,num, num*num2,binBoundary,binBoundary2)
       


        // number of data files generated by each node
        let dataFileNumPerNode = nFilePN
      
        //number of partition of input DKV; total number of data files
        let dataFileNum = dataFileNumPerNode * cluster.NumNodes

        //num should be bigger than 1
        let hashBitSize = (Math.Max(8, (int) (Math.Log((float) (num-1),2.0)) + 1)) 
        let hashByteSize = (hashBitSize - 1 ) / 8 + 1
        
        let maxHashValue = 1 <<< (hashByteSize * 8) 




        let hashBitSize2 = (Math.Max(8, (int) (Math.Log((float) (num2*num-1),2.0)) + 1)) 
        let hashByteSize2 = (hashBitSize2 - 1 ) / 8 + 1
        
        let maxHashValue2 = 1 <<< (hashByteSize2 * 8) 

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
            use valms = new MemStream()
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

            let binBoundary = Array.init maxHashValue2 (fun i -> Math.Min(num-1,i/(maxHashValue2/num)) )
            let numStage2 = num*num2
            let binBoundary2 = Array.init maxHashValue2 (fun i -> Math.Min(numStage2-1,i/(maxHashValue2/numStage2)) )
            let rmtPart = RemoteFunc( dataFileNum, records, nDim,num, num*num2,binBoundary,binBoundary2)

                

            let conf5() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- dataFileNumPerNode 

                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToSeq)
                dset1.NumParallelExecution <- dataFileNumPerNode 

                let dset3 = dset1 |> DSet.map (rmtPart.RepartitionSharedMemory RepartitionStage.StageOne)
                dset3.NumParallelExecution <- dataFileNumPerNode 
                dset3.SerializationLimit <- 1

                let dset4 = dset3 |> DSet.collect Operators.id  |> DSet.rowsReorg 1

                dset4.SerializationLimit <- 1
                dset4.NumParallelExecution <- dataFileNumPerNode


                let dset5 = dset4 |> DSet.repartitionN num (fun (i,ms,len) -> 
                                                                    i
                                                                ) 
                dset5.NumParallelExecution <- dataFileNumPerNode
           

                //Todo, cache the received data and the write to disk
                dset5 |> DSet.iter rmtPart.ReturnSharedBuf
    
                ()



            //test repartition throughput with SharedMemory baseline, user's program manages shared memory for read file and repartition results
            let conf6() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- dataFileNumPerNode 
                
                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToSeq)
                dset1.NumParallelExecution <- dataFileNumPerNode 

                let dset3 = dset1 |> DSet.map (rmtPart.RepartitionSharedMemory RepartitionStage.StageOne)


                dset3 
                    |> DSet.collect Operators.id
                    |> DSet.iter rmtPart.ReturnSharedBuf

                ()

            //test repartition throughput with Native code baseline, user's program manages shared memory for read file and repartition results
            let conf7() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- dataFileNumPerNode 

                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToSeq)
                dset1.NumParallelExecution <- dataFileNumPerNode 

                let dset3 = dset1 |> DSet.map (rmtPart.NativeRepartition 1)
                dset3.NumParallelExecution <- dataFileNumPerNode
                dset3.SerializationLimit <- 1


                dset3 
                    |> DSet.collect Operators.id
                    |> DSet.iter rmtPart.ReturnSharedBuf


            //test memstream
            let MemStream_Fake_conf() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- 16 
                
                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToMemStreamF)
                dset1.NumParallelExecution <- 16 
                dset1.SerializationLimit <- 1
                let dset3 = dset1 |> DSet.map (rmtPart.RepartitionMemStream RepartitionStage.StageOne)
                dset3.NumParallelExecution <- 16 
                dset3.SerializationLimit <- 1
                
                let dset4 = dset3 |> DSet.collect Operators.id
                dset4.NumParallelExecution <- 16 

                //dset4 |> DSet.iter rmtPart.RepartitionAndWriteToFile

                dset4 |> DSet.iter rmtPart.DeRefMemStream


            //test memstream with network
            let MemStream_conf() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- 8
                
                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToMemStream)
                dset1.NumParallelExecution <- 8
                dset1.SerializationLimit <- 1
                let dset3 = dset1 |> DSet.map (rmtPart.RepartitionMemStream RepartitionStage.StageOne)
                dset3.NumParallelExecution <- 8
                dset3.SerializationLimit <- 1


                let dset4 = dset3 |> DSet.collect Operators.id  |> DSet.rowsReorg 1
                dset4.NumParallelExecution <- 8
                dset4.SerializationLimit <- 1

                let param = DParam( )
                param.NumPartitions <- num

                let dset5 = dset4 |> DSet.repartitionP param (fun (ms) -> 
                                                                    ms.Seek(0L,SeekOrigin.Begin) |> ignore
                                                                    ms.ReadByte()
                                                                ) 

                dset5.NumParallelExecution <- 20
                dset5.SerializationLimit <- 1
                dset5 |> DSet.iter rmtPart.RepartitionAndWriteToFileMemuseless

                //let dset6 = dset5 |> DSet.map rmtPart.RepartitionAndWriteToFileOld

                //dset6 |> DSet.toSeq |> Seq.iter(fun _ -> ())

            //native repartition, with memstream
            let NativeRepartitionConf() =
                let startDSet = DSet<_>( Name = "SortGen", SerializationLimit = 1) 
                startDSet.NumParallelExecution <- 8 

                let dset1 = startDSet |> DSet.sourceI dataFileNum (rmtPart.ReadFilesToSeq)
                dset1.NumParallelExecution <- 8 

                let dset3 = dset1 |> DSet.map (rmtPart.NativeRepartitionWithMemStream RepartitionStage.StageOne)
                dset3.NumParallelExecution <- 8 
                dset3.SerializationLimit <- 1

                let dset4 = dset3 |> DSet.collect Operators.id |> DSet.rowsReorg 1
                
                dset4.SerializationLimit <- 1
                dset4.NumParallelExecution <- 8
                
                let param = DParam( )
                param.NumPartitions <- num                

                let dset5 = dset4 |> DSet.repartitionP param (fun (ms) -> 
                                                                    ms.Seek(0L,SeekOrigin.Begin) |> ignore
                                                                    ms.ReadByte()
                                                                ) 
                dset5.NumParallelExecution <- 8
                dset5.SerializationLimit <- 1

                dset5 |> DSet.iter rmtPart.RepartitionAndWriteToFileMemuseless

                ()


            let t1 = (DateTime.UtcNow)
            NativeRepartitionConf()
            let t2= (DateTime.UtcNow)
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Data is distributed, takes %f ms"  ((DateTime.UtcNow - t1).TotalMilliseconds) ))

            let sortDSet = DSet<_>( Name = "SortSet", NumPartitions = num2*num) 
            

            let resSet = sortDSet |> DSet.initS (fun (p,s) -> p) 1
            resSet.NumPartitions <- num2*num
            
            resSet.NumParallelExecution <- 60
            
            resSet  |> DSet.mapi (rmtPart.PipelineSort 20)
                    |> DSet.toSeq 
                    |> Seq.iter (fun (i,t) ->  Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Sorted partition %d takes %f s" i t)))
            
            
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
