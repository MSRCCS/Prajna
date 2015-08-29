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
        DiskHelper.fs
  
    Description: 
        Benchmark performance for distributed sort. 

    Author:																	
        Hongzhi Li
    Date:
        June. 2014
    
 ---------------------------------------------------------------------------*)
namespace sortbenchmark
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
//open OneNet.Service.CoreServices

[<Serializable>]
type StorageProfile (profileName : string) =
    //member val diskProfile = [|"c:\\sortbenchmark\\";"c:\\sortbenchmark\\";"c:\\sortbenchmark\\";"c:\\sortbenchmark\\";"d:\\sortbenchmark\\";"e:\\sortbenchmark\\";"f:\\sortbenchmark\\"|] with get
    //member val drives = DriveInfo.GetDrives()
    member val diskProfile = null with get,set
    //member val diskProfile = [|"c:\\sortbenchmark\\";"d:\\sortbenchmark\\";"e:\\sortbenchmark\\";"f:\\sortbenchmark\\"|] with get

    member val fileListQueue : ConcurrentQueue<(int*string)> = new ConcurrentQueue<(int*string)>()
    //for saving and loading
    member val fileListDict : ConcurrentDictionary<int,string> = new ConcurrentDictionary<int,string>() 
    member val lock = ref 0
    member val diskIndex = ref 0
    member val bInited = false with get,set
    member val profileFileName = Path.Combine("c:\\sortbenchmark\\",profileName+".txt") with get
    member internal x.Save() = 

        let mutable b = false
        while not b do
            if (Interlocked.CompareExchange(x.lock,1,0) = 0) then
//                let ms = new MemStream()
//                ms.SerializeFrom(x.fileListDict.ToArray())
//                use file = new FileStream(x.profileFileName, FileMode.Create)
//                file.Write(ms.GetBuffer(),0,(int) ms.Length)
//                file.Close()

                use sw = new StreamWriter(x.profileFileName)
                x.fileListDict.ToArray() |> Array.iter (fun kv -> (sw.WriteLine((sprintf "%d,%s" kv.Key kv.Value))))
                sw.Close()

                x.lock := 0
                b <- true

        ()

    
    // x.Load should be thread safe, since x.Load should only be called from x.Init, which is thread safe
    // it shouldn't be called from anywhere else
    member internal x.Load() = 
        if  (File.Exists(x.profileFileName)) then
            use sr = new StreamReader(x.profileFileName)
            while not sr.EndOfStream do
                let items = sr.ReadLine().Trim().Split([|','|])
                if items.Length = 2 then
                    x.fileListQueue.Enqueue((Int32.Parse(items.[0]),items.[1].Trim())) |> ignore
                    x.fileListDict.TryAdd(Int32.Parse(items.[0]),items.[1].Trim()) |> ignore
                else
                    let msg = sprintf "!!!Error!!! wrong storage profile file %s" x.profileFileName
                    Logger.Log(LogLevel.Error, msg)
                    failwith msg
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "%d file entries have been loaded from profile file: %s" (x.fileListDict.Count) (x.profileFileName)))
            
        ()

    member internal x.Init () = 
        if not x.bInited then
            let mutable b = false
            while not b && not x.bInited do
                if (Interlocked.CompareExchange(x.lock,1,0) = 0) then
                    if not x.bInited then
                        x.diskProfile <- DriveInfo.GetDrives() 
                            |> Array.filter(fun di -> di.DriveType = DriveType.Fixed)  
                            |> Array.filter(fun di -> di.TotalSize > 100000000000L) 
                            |> Array.map(fun di -> Path.Combine(di.Name,"sortbenchmark").ToLower()+"\\")

                        x.diskProfile
                            |> Array.iter (fun path ->
                                                        try 
                                                            if not (Directory.Exists(path)) then
                                                                Directory.CreateDirectory(path) |> ignore
                                                        with
                                                            | _ -> (Logger.Fail (sprintf "!!!Error!!! Data profile error. Cannot create data dictionary based on provided path" ))   
                                                    )
                        x.Load()
                        x.bInited <- true
                        ()
                    b <- true
                    x.lock := 0


    /// if we have generated a filename for a partition, return the old value, 
    ///otherwise, we generate a new file name, add it to dictionary and return the generated filename
    member x.GenerateFilePath(key:int) =
        x.Init()
        if (x.fileListDict.ContainsKey(key)) then
            x.fileListDict.[key]
        else 
            let i = Interlocked.Increment(x.diskIndex)
            let dir = x.diskProfile.[i % (x.diskProfile.Length)]
            let filename = sprintf "%d.bin" key
            if not (Directory.Exists(Path.Combine(dir,profileName))) then
                Directory.CreateDirectory(Path.Combine(dir,profileName)) |> ignore
            let filepath = Path.Combine([|dir;profileName;filename|])
            x.fileListQueue.Enqueue(key,filepath)
            x.fileListDict.TryAdd(key,filepath) |> ignore
            x.Save()
            filepath

    member x.GetNextFilePath() =
        x.Init()
        let filepath = ref Unchecked.defaultof<_>
        while not (x.fileListQueue.TryDequeue(filepath))  && not (x.fileListQueue.IsEmpty) do
            ()
        if Utils.IsNotNull !filepath then
            !filepath
        else   
            
            (-1,null)
        
    member x.GetFilePath(key:int) =
        x.Init()
        if x.fileListDict.ContainsKey(key) then
            x.fileListDict.[key]
        else 
            null

[<AllowNullLiteral; Serializable>]
type DiskHelper(records:int64) = 
    member val rawdataSP = new StorageProfile("RawData_"+(string) records)
    member val partitiondataSP = new StorageProfile("PartiData_"+(string) records)
    member val sorteddataSP = new StorageProfile("SortedData_"+(string) records)

    // it may be safer if we use concurrentqueue. Add two variable to store current time and current value, 
    // update current value if time doesn't change, otherwise, enqueue curent value and reset current time value
    // to output the performance log, just dequeue from the concurrentqueue until it's empty
    // But for now, as a simple implementation, it's ok to use concurrent dictionary
    member val readRateMonitor = new ConcurrentDictionary<int64,int64>()
    member val writeRateMonitor = new ConcurrentDictionary<int64,int64>()
    member val diskRateMonitor = new ConcurrentDictionary<int64,int64>()

    member val perfSaveLock = ref 0
    member val lastPerfLogUpdatedTime = -1L with get, set
    member x.ReportReadBytes(byt:int64) = 
        if false then
            let time = int64 (DateTime.UtcNow.TimeOfDay.TotalSeconds) 
            x.readRateMonitor.AddOrUpdate(time,byt,(fun i ov -> (ov + byt))) |> ignore
            x.diskRateMonitor.AddOrUpdate(time,byt,(fun i ov -> (ov + byt))) |> ignore
        


            if x.lastPerfLogUpdatedTime = -1L then
                x.lastPerfLogUpdatedTime <- time
            else if (time - x.lastPerfLogUpdatedTime) > 10L then
                x.lastPerfLogUpdatedTime <- time
                let asyncSave = async { x.SaveDiskPerfMonitor() }
                Async.Start asyncSave
        ()

    member x.ReportWriteBytes(byt:int64) = 
        if false then
            let time = int64 (DateTime.UtcNow.TimeOfDay.TotalSeconds) 
            x.writeRateMonitor.AddOrUpdate(time,byt,(fun i ov -> (ov + byt))) |> ignore
            x.diskRateMonitor.AddOrUpdate(time,byt,(fun i ov -> (ov + byt))) |> ignore

            if x.lastPerfLogUpdatedTime = -1L then
                x.lastPerfLogUpdatedTime <- time
            else if (time - x.lastPerfLogUpdatedTime) > 30L then
                x.lastPerfLogUpdatedTime <- time
                let asyncSave = async { x.SaveDiskPerfMonitor() }
                Async.Start asyncSave
        ()


    // we may lost at most 10 seconds performance data at the end of the experiment by using this save policy 
    member x.SaveDiskPerfMonitor() = 
        
        if Interlocked.CompareExchange (x.perfSaveLock,1,0) = 0 then

            if x.readRateMonitor.Count > 0 then
                if File.Exists("c:\\sortbenchmark\\readrate.txt") then
                    use sr = new StreamReader("c:\\sortbenchmark\\readrate.txt")
                    while not sr.EndOfStream do
                        let items = sr.ReadLine().Trim().Split([|'\t'|]) |> Array.map (fun e -> Int64.Parse(e))
                        x.readRateMonitor.AddOrUpdate(items.[0],items.[1], fun k v -> v) |> ignore
                    sr.Close()
                        
                            
                use readPerfFile = new StreamWriter("c:\\sortbenchmark\\readrate.txt")
                x.readRateMonitor.ToArray()|> Array.sortBy(fun kv -> kv.Key) |> Array.iter (fun kv ->
                                                            readPerfFile.WriteLine(sprintf "%d\t%d" kv.Key kv.Value)
                                                            )
                readPerfFile.Close()



            if x.writeRateMonitor.Count > 0 then 
                
                if File.Exists("c:\\sortbenchmark\\writerate.txt") then
                    use sr = new StreamReader("c:\\sortbenchmark\\writerate.txt")
                    while not sr.EndOfStream do
                        let items = sr.ReadLine().Trim().Split([|'\t'|]) |> Array.map (fun e -> Int64.Parse(e))
                        x.writeRateMonitor.AddOrUpdate(items.[0],items.[1], fun k v -> v) |> ignore
                    sr.Close()


                use writePerfFile = new StreamWriter("c:\\sortbenchmark\\writerate.txt")
                x.writeRateMonitor.ToArray() |> Array.sortBy(fun kv -> kv.Key)  |> Array.iter (fun kv ->
                                                            writePerfFile.WriteLine(sprintf "%d\t%d" kv.Key kv.Value)
                                                            )
                writePerfFile.Close()


            if x.diskRateMonitor.Count > 0 then 
                
                if File.Exists("c:\\sortbenchmark\\diskrate.txt") then
                    use sr = new StreamReader("c:\\sortbenchmark\\diskrate.txt")
                    while not sr.EndOfStream do
                        let items = sr.ReadLine().Trim().Split([|'\t'|]) |> Array.map (fun e -> Int64.Parse(e))
                        x.diskRateMonitor.AddOrUpdate(items.[0],items.[1], fun k v -> v) |> ignore
                    sr.Close()


                use writePerfFile = new StreamWriter("c:\\sortbenchmark\\diskrate.txt")
                x.diskRateMonitor.ToArray() |> Array.sortBy(fun kv -> kv.Key)  |> Array.iter (fun kv ->
                                                            writePerfFile.WriteLine(sprintf "%d\t%d" kv.Key kv.Value)
                                                            )
                writePerfFile.Close()

            x.perfSaveLock := 0


    member x.GenerateDataFilePath(parti:int) = 
        x.rawdataSP.GenerateFilePath(parti)
            
    member x.GetDataFilePath() = 
        x.rawdataSP.GetNextFilePath()

    member x.GeneratePartitionFilePath(parti:int) =
        x.partitiondataSP.GenerateFilePath(parti)


    member x.GenerateSortedFilePath(parti:int) = 
        x.sorteddataSP.GenerateFilePath(parti)

    member x.Save() =
        let mutable b = false
        while not b do
            if (Interlocked.CompareExchange(x.rawdataSP.lock ,1,0) = 0) then
                x.rawdataSP.Save() 
                b <- true
                x.rawdataSP.lock := 0

        b <- false
        while not b do
            if (Interlocked.CompareExchange(x.partitiondataSP.lock ,1,0) = 0) then
                x.partitiondataSP.Save() 
                b <- true
                x.partitiondataSP.lock := 0


        b <- false
        while not b do
            if (Interlocked.CompareExchange(x.sorteddataSP.lock ,1,0) = 0) then
                x.sorteddataSP.Save() 
                b <- true
                x.sorteddataSP.lock := 0



[<AllowNullLiteral>]
type RollingFileMgr(records:int64) = 
    let prefix = "sortFile_"
    let dirs = [|@"C:\sortbenchmark";@"D:\sortbenchmark\";@"E:\sortbenchmark\";@"F:\sortbenchmark\"|]
    member val dirIndex = ref -1 with get,set

    member val writeFileLock = Array.create dirs.Length (ref 0)
    member val MaxFileSize = 10000000
    
    member val fileHandleQ = new ConcurrentQueue<(FileStream*int)>()

    member val readyFileQ = new ConcurrentQueue<string>()
    member val fileNameQ = new ConcurrentQueue<string>()


    member x.Init() =
        for i = 0 to dirs.Length - 1 do
            let dir = Path.Combine(dirs.[i],prefix+(string) records)
            if not (Directory.Exists(dir)) then
                Directory.CreateDirectory(dir) |> ignore

            //x.fileHandleQ.Enqueue(x.CreateNewFile() , 0)
            
        ()

    member x.GetNewFilename() =
        let index = Interlocked.Increment(x.dirIndex)
        let dir = Path.Combine(dirs.[index % dirs.Length],prefix+(string) records)
        Path.Combine(dir, (string) index + ".bin")

    member x.CreateNewFile() =
        let fp = x.GetNewFilename()
        x.fileNameQ.Enqueue(fp)
        new FileStream(fp,FileMode.Create)


    member x.EnqueueFileHandle(fh:FileStream, size:int) =
        x.fileHandleQ.Enqueue(fh,size)



[<AllowNullLiteral>]
type SingleThreadExec1() =
    let counter = ref 0
    let q = new ConcurrentQueue<unit->unit>()

    // execute function only on one thread - "counter" number of times
    member x.Exec(f : unit->unit) =
        if (Interlocked.Increment(counter) = 1) then
            let mutable bDone = false
            while (not bDone) do
                f()
                bDone <- (Interlocked.Decrement(counter) = 0)

    // execute function only on one thread - but at least one time after call
    member x.ExecOnce(f : unit->unit) =
        if (Interlocked.Increment(counter) = 1) then
            let mutable bDone = false
            while (not bDone) do
                // get count prior to executing
                let curCount = !counter
                f()
                bDone <- (Interlocked.Add(counter, -curCount) = 0)

    member x.ExecQ(f : unit->unit) =
        q.Enqueue(f)
        if (Interlocked.Increment(counter) = 10) then
            let mutable bDone = false
            let fn = ref (fun () -> ())
            while (not bDone) do
                let ret = q.TryDequeue(fn)
                if (ret) then
                    (!fn)()
                    bDone <- (Interlocked.Decrement(counter) = 0)