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
        DistributedLogAnalysis.fs
  
    Description: 
        Perform a distributed analysis of log. Showcase DSet<_>.source feature.  

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Aug. 2014
    
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

let Usage = "
    Usage: Distributed DistributedLogAnalysis. Analyze logs of all files on the cluster, and perform a regular expression matching. .  \n\
    Command line arguments:\n\
    -cluster    Prajna cluster in which the distributed log analysis is performed \n\
    -dir        Remote directory [default c:\\prajna\\] \n\
    -spattern   Search pattern of the log file [default *.log] \n\
    -rec        Search log file recurrsively \n\
    -slimit S   # of record to serialize \n\
    -exe        Execute Prajna in exe mode \n\
    -num        Number of threads executing in each machine \n\
    -reg        Regular Expression pattern to search for ( pattern, line_before, line_after ) \n\
    -regfile    A file that contains Regular Expression pattern to search for ( pattern, line_before, line_after ) \n\
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

let openFile fname = 
    try 
        let file = new FileStream( fname, FileMode.Open, FileAccess.Read, FileShare.Read, 1<<<20 )
        let reader = new StreamReader( file )
        let readerLen = reader.BaseStream.Length
        Some ( fname, reader)
    with 
    | e -> 
        None

    

let fileSeq (fname:string, reader:StreamReader) = 
    seq {
        let readerLen = ref 0L
        try
            let bEndReached = ref false
            let nLines = ref 0L
            while not (!bEndReached) do 
                let line = reader.ReadLine()
                bEndReached := (Utils.IsNull line)
                if not (!bEndReached) then 
                    nLines := (!nLines) + 1L
                    yield fname, !nLines, line
            readerLen := reader.BaseStream.Length
        finally 
            if Utils.IsNotNull reader then 
                reader.Close()
        yield fname, (!readerLen), null
    }        
 
let sourceSeqFunc dir spattern sOption () = 
    let files = Directory.GetFiles( dir, spattern, sOption )
    files 
    |> Seq.choose ( fun fname -> openFile fname )
    |> Seq.collect ( fun (fname, fstream) -> fileSeq (fname,fstream) ) 

type SourceNSeq(num, dir, spattern, sOption) = 
    member val Files = null with get, set
    member x.GetFileSeq sourcei = 
        if Utils.IsNull x.Files then 
            lock ( x ) ( fun _ -> 
                if Utils.IsNull x.Files then 
                    let files = Directory.GetFiles( dir, spattern, sOption )
                    x.Files <- Array.init num ( fun _ -> List<_>( files.Length / num + 1 ) )
                    let countL = Array.zeroCreate<int64> num
                    for i = 0 to files.Length - 1 do
                        // Add to the partition that has the smallest files. 
                        let minv = ref Int64.MaxValue
                        let mini = ref Int32.MinValue
                        countL |> Array.iteri ( fun i v -> if v < !minv then
                                                                minv := v
                                                                mini := i )
                        try
                            let f = new FileInfo( files.[i] )
                            let flen = f.Length
                            x.Files.[ (!mini) ].Add( files.[i] )
                            countL.[ (!mini) ] <- countL.[ (!mini) ] + flen
                        with 
                        | e ->
                            // Skip the file if length is not available.  
                            ()
                )
        x.Files.[sourcei] 
        |> Seq.choose ( fun fname -> openFile fname )
        |> Seq.collect ( fun (fname, fstream) -> fileSeq (fname,fstream) ) 
    

[<AllowNullLiteral>]
type FileLogAnalysis() =
    member val Buffer = Queue<_>() with get, set
    member val NumLinesAfter = 0 with get, set
    member val UnflushedLines = 0 with get, set

type LogAnalysis( searchReg ) = 
    let maxLinesBuffered = searchReg |> Array.map( fun tuple -> let _, linebefore, _ = tuple 
                                                                linebefore ) 
                                                 |> Array.max
    let mutable regUsed = null 
    let dic = ConcurrentDictionary<_,FileLogAnalysis>()
    member x.LogAnalysisFun( fname, line, content ) = 
        if Utils.IsNull regUsed then 
            lock ( x ) ( fun _ -> 
                // initialized regular expression
                if Utils.IsNull regUsed then 
                    regUsed <- searchReg |> Array.map( fun (pattern, _, _ ) -> new System.Text.RegularExpressions.Regex( pattern ) )
            )
        let mutable retLst = null
        let mutable bDequeue = true
        let mutable bMatch = false
        let mutable linebefore = 0
        let mutable lineafter = 0
        let y = dic.GetOrAdd( fname, fun _ -> FileLogAnalysis() )
        if Utils.IsNotNull content then 
            // Test matching of the current line
            for i = 0 to searchReg.Length - 1 do 
                if regUsed.[i].Match( content ).Success then 
                    bMatch <- true
                    let _, lb, la = searchReg.[i]
                    linebefore <- Math.Max( linebefore, lb ) 
                    lineafter <- Math.Max( lineafter, la )
            if bMatch then 
                if linebefore > 0 && y.Buffer.Count > 0 then 
                    // Mark buffer in lines to be flushed. 
                    let arr = y.Buffer.ToArray()
                    let line0 = Math.Max( 0, arr.Length - linebefore )
                    for l = line0 to arr.Length - 1 do 
                        let _, _, _, bFlush = arr.[l]
                        bFlush := true
                    if y.UnflushedLines <= linebefore then 
                        y.UnflushedLines <- 0 
                y.NumLinesAfter <- Math.Max( y.NumLinesAfter, lineafter+1 )
            else
                linebefore <- 0 
            // bMatch: the status of the current line 
            bMatch <- bMatch || y.NumLinesAfter > 0 
            y.NumLinesAfter <- y.NumLinesAfter - 1
            if bMatch then 
                // current line needs to be shown, dequeue lines in buffer that are not shown until it reaches maxLinesBuffered
                while bDequeue && y.Buffer.Count>0 do 
                    let f1, l1, c1, bF = y.Buffer.Peek()    
                    if not !bF then 
                        bDequeue <- y.Buffer.Count >= maxLinesBuffered
                        if bDequeue then 
                            y.Buffer.Dequeue() |> ignore
                    else
                        // line will be shown, stop dequeueing
                        bDequeue <- false
            else
                // current line needs not to be shown, dequeue lines in buffer that are shown & not shown until it reaches maxLinesBuffered
                let mutable firstLineShown = Int64.MinValue
                let mutable lastLineShown = Int32.MinValue
                let mutable aggLst = null
                while bDequeue && y.Buffer.Count>0 do 
                    let f1, l1, c1, bF = y.Buffer.Peek()
                    if not !bF then 
                        if Utils.IsNotNull aggLst then 
                            // Add one aggregation to return
                            let agg_lines = aggLst |> String.concat Environment.NewLine
                            if Utils.IsNull retLst then 
                                retLst <- List<_>()
                            retLst.Add( fname, firstLineShown, lastLineShown, agg_lines )
                            aggLst <- null
                        bDequeue <- y.Buffer.Count >= maxLinesBuffered
                        if bDequeue then 
                            y.Buffer.Dequeue() |> ignore
                            y.UnflushedLines <- Math.Min( y.UnflushedLines, y.Buffer.Count ) 
                    else
                        if y.UnflushedLines >= y.Buffer.Count then  
                            // Need to reset UnflushedLines, find first unflushed line (
                            let arr = y.Buffer.ToArray()
                            let mutable firstUnflushed = 0
                            let mutable bFind = false
                            while not bFind do 
                                let f1, l1, c1, bF = arr.[firstUnflushed]
                                if not !bF then 
                                    bFind <- true
                                else
                                    firstUnflushed <- firstUnflushed + 1
                                    bFind <- firstUnflushed >= arr.Length
                            y.UnflushedLines <- arr.Length - firstUnflushed                          
                        if y.UnflushedLines>= maxLinesBuffered then 
                            // there is at least one line not flushing that is maxLinesBuffered deep
                            if Utils.IsNull aggLst then 
                                aggLst <- List<_>()
                                firstLineShown <- int64 l1
                            aggLst.Add( c1 ) 
                            lastLineShown <- l1
                            y.Buffer.Dequeue() |> ignore
                        else
                            // Do not flush
                            bDequeue <- false
                if Utils.IsNotNull aggLst then 
                    // Add one aggregation to return
                    let agg_lines = aggLst |> String.concat Environment.NewLine
                    if Utils.IsNull retLst then 
                        retLst <- List<_>()
                    retLst.Add( fname, firstLineShown, lastLineShown, agg_lines )
                    aggLst <- null
            y.Buffer.Enqueue( fname, int line, content, ref bMatch )
            // Always enqueue current 
            if y.UnflushedLines > 0 then 
                // Advancing the line
                y.UnflushedLines <- y.UnflushedLines + 1
            else 
                // First line 
                y.UnflushedLines <- if bMatch then 0 else 1
        else
            // content = null, dequeue everything. 
            y.NumLinesAfter <- 0 
            retLst <- List<_>()
            let mutable firstLineShown = Int64.MinValue
            let mutable lastLineShown = Int32.MinValue
            let mutable aggLst = null
            while y.Buffer.Count > 0 do 
                let f1, l1, c1, bF = y.Buffer.Dequeue()
                if not !bF then 
                    if Utils.IsNotNull aggLst then 
                        // Add one aggregation to return
                        let agg_lines = aggLst |> String.concat Environment.NewLine
                        retLst.Add( fname, firstLineShown, lastLineShown, agg_lines )
                        aggLst <- null
                else
                    if Utils.IsNull aggLst then 
                        aggLst <- List<_>()
                        firstLineShown <- int64 l1
                    aggLst.Add( c1 ) 
                    lastLineShown <- l1
            if Utils.IsNotNull aggLst then 
                // Add one aggregation to return
                let agg_lines = aggLst |> String.concat Environment.NewLine
                retLst.Add( fname, firstLineShown, lastLineShown, agg_lines )
                aggLst <- null
            retLst.Add( fname, line, 0, null )
            let z = ref null
            dic.TryRemove( fname, z ) |> ignore
        if Utils.IsNull retLst then 
            Seq.empty
        else
            retLst :> seq<_>

// Define your library scripting code here
[<EntryPoint>]
let main orgargs = 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let regpatterns = List<_>(100)
    let PrajnaClusterFile = parse.ParseString( "-cluster", null )
    let remotedir = parse.ParseString( "-dir", "c:\Prajna" )
    let searchPattern = parse.ParseString( "-spattern", "*.log" )
    let searchOption = if ( parse.ParseBoolean( "-rec", false ) ) then SearchOption.AllDirectories else SearchOption.TopDirectoryOnly
    let slimit = parse.ParseInt( "-slimit", 10000 )
    let num = parse.ParseInt( "-num", 1 )
    let bExe = parse.ParseBoolean( "-exe", false )
    let seps = "\t,"
    let mutable bDoneAddingReg = false
    while not bDoneAddingReg do 
        let regExp = parse.ParseString( "-reg", null )
        bDoneAddingReg <- ( Utils.IsNull regExp )
        if not bDoneAddingReg then 
            regpatterns.Add( regExp )
    let regFile = parse.ParseString( "-regfile", null )
    if Utils.IsNotNull regFile then 
        let reader = new StreamReader(regFile)
        bDoneAddingReg <- false
        while not bDoneAddingReg do 
            let regExp = reader.ReadLine()
            bDoneAddingReg <- ( Utils.IsNull regExp )
            if not bDoneAddingReg then 
                regpatterns.Add( regExp )
    let searchReg = 
        regpatterns.ToArray()
        |> Array.choose ( fun line -> let items = line.Split( seps.ToCharArray(), StringSplitOptions.RemoveEmptyEntries ) |> Array.map ( fun s -> s.Trim() )
                                      match items.Length with
                                      | a when a = 1 -> 
                                            Some (items.[0], 0, 0)
                                      | a when a = 2 -> 
                                            Some (items.[0], Int32.Parse( items.[1] ), Int32.Parse( items.[1] ))
                                      | a when a >= 3 ->
                                            Some (items.[0], Int32.Parse( items.[1] ), Int32.Parse( items.[2] ))
                                      | _ -> 
                                            None
                         )
    let mutable bExecute = false
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))
    Logger.Do( LogLevel.MildVerbose, ( fun _ -> 
           for searchExp in searchReg do 
               let pattern, linebefore, lineafter = searchExp 
               Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Search for Regex: %s, [%d,%d] " pattern linebefore lineafter ))
       ))
    if bExe then 
        JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask

    let bAllParsed = parse.AllParsed Usage
    if bAllParsed then 
        if Utils.IsNull PrajnaClusterFile then 
                let t1 = (DateTime.UtcNow)
                let srcLog = sourceSeqFunc remotedir searchPattern searchOption ()
                let ana = LogAnalysis( searchReg )
                let resultSeq = srcLog |> Seq.collect( ana.LogAnalysisFun )
                let numLogAnalyzed = ref 0 
                let nBytesAnalyzed = ref 0L
                resultSeq |> Seq.iter ( fun (fname, linestart, lineend, content ) -> 
                    if Utils.IsNull content then 
                        numLogAnalyzed := !numLogAnalyzed + 1
                        nBytesAnalyzed := !nBytesAnalyzed + linestart
                    else
                        Logger.Log( LogLevel.Info, ( sprintf "********************************** %s:%d-%d **********************************************" fname linestart lineend ))
                        Logger.Log( LogLevel.Info, ( content ))
                )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Analyzed %d log files of size %dMB with %f secs, throughput = %f MB/s" 
                                                           !numLogAnalyzed (!nBytesAnalyzed>>>20) elapse.TotalSeconds (float !nBytesAnalyzed / 1000000. / elapse.TotalSeconds) ))
                bExecute <- true
        else
            Cluster.Start( null, PrajnaClusterFile )
            let cluster = Cluster.GetCurrent()
            if true then

                let t1 = (DateTime.UtcNow)
                let startDSet = DSet<_>( Name = "DistributedLogAnalysis", SerializationLimit = slimit )                                                   
                let srcLog = 
                    if num <= 1 then 
                        startDSet |> DSet.source ( sourceSeqFunc remotedir searchPattern searchOption ) 
                    else
                        let sourceNSeq = SourceNSeq( num, remotedir, searchPattern, searchOption )
                        startDSet |> DSet.sourceN num sourceNSeq.GetFileSeq
                let ana = LogAnalysis( searchReg )
                let processedLog = srcLog |> DSet.collect( ana.LogAnalysisFun )
                let resultSeq = processedLog.ToSeq()
                let numLogAnalyzed = ref 0 
                let nBytesAnalyzed = ref 0L
                resultSeq |> Seq.iter ( fun (fname, linestart, lineend, content ) -> 
                    if Utils.IsNull content then 
                        numLogAnalyzed := !numLogAnalyzed + 1
                        nBytesAnalyzed := !nBytesAnalyzed + linestart
                    else
                        Logger.Log( LogLevel.Info, ( sprintf "********************************** %s:%d-%d **********************************************" fname linestart lineend ))
                        Logger.Log( LogLevel.Info, ( content ))
                )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Analyzed %d log files of size %dMB with %f secs, throughput = %f MB/s" 
                                                           !numLogAnalyzed (!nBytesAnalyzed>>>20) elapse.TotalSeconds (float !nBytesAnalyzed / 1000000. / elapse.TotalSeconds) ))
                Cluster.Stop()                            
                bExecute <- true

    // Make sure we don't print the usage information twice. 
    if not bExecute && bAllParsed then 
        parse.PrintUsage Usage
    0
