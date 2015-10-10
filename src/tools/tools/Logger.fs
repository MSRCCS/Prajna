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
		Logger.fs
  
	Description: 
		Logger APIs

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Apr. 2013
	
 ---------------------------------------------------------------------------*)

namespace Prajna.Tools

open System
open System.Collections.Concurrent
open System.Diagnostics
open System.IO
open System.Security.AccessControl
open System.Security.Principal
open System.Threading

open Prajna.Tools.StringTools

/// <summary> 
/// Prajna Trace level, the lower the level, the more important of the message. 
/// </summary>
type LogLevel = 
    /// Fatal error
    | Fatal = 0
    /// Error/exception messgae, should always be outputed. 
    | Error = 1
    /// Warning, should always be outputed. 
    | Warning = 2
    /// Information, normally be outputed (default trace level is Info)
    | Info = 3
    /// MildVerbose, additional information (usually won't seriously impact performance, during test, Prajna is operated at this trace level. )
    | MildVerbose = 4
    /// MildVerbose, extra information (may slightly impact performance.)
    | MediumVerbose = 5
    /// WildVerbose, detailed execution trace (non-trival impact on performance, usually used during intensive debugging of a hard to fix bug. )
    | WildVerbose = 6
    /// WildVerbose, extreme detailed execution trace (serious impact on performance, not commonly used. )
    | ExtremeVerbose = 7


type ILoggerProvider =
    // Typical paramters for a log entry:
    //
    // The "log id" is unique string across all components. Log entries that 
    // use the same log id should be in a related activity. 
    // The "log id" is the unit used for configuring rules for log entries, such as the where 
    // the log entries should go (to certain file, to stdout, etc), and at what level the 
    // log entries should be emitted.
    // If not specified, a default log id "Default" is used, and 
    // a default log level threshold (specified in the Logger type) is used.
    //
    // The "log level" is to specify the level of the log entry.

    /// For the specifed "log id", whether the specified "log level" is enabled
    abstract member IsEnabled : (string*LogLevel) -> bool

    /// Takes two parameters: log level, log Message
    abstract member Log : (LogLevel*string) -> unit

    /// Takes three parameters: log id, log level, log Message
    abstract member Log : (string*LogLevel*string) -> unit

    /// Takes three parameters: JobID, log level, log Message
    abstract member Log : (Guid*LogLevel*string) -> unit

    /// Takes four parameters: log id, JobID, log level, log Message
    abstract member Log : (string*Guid*LogLevel*string) -> unit

    /// Parse arguments that configs the behavior of the LoggerProvider
    abstract member ParseArgs : string[] -> unit

    /// Print the usage information on string arguments that ParseArgs can parse for the LoggerProvider
    abstract member GetArgsUsage : unit -> string

    /// Flush the pending log entries
    abstract member Flush : unit -> unit

    /// Return latest log entries as a file (It's up to the provider to decide whether to return a file and how many entries to return)
    /// Returns "null" if the provider has no such capability
    abstract member GetLogFile : unit -> string

type internal DefaultLogger () =
    // max log file size
    let maxLogFileLength = 10L <<< 20

    // Flush and examination interval. 
    // Flush will launch a new thread and is a relative expensive operation, should not be done often. 
    let logFlushInterval = 10L * TimeSpan.TicksPerSecond

    let flashTicks = ref (PerfDateTime.UtcNowTicks())
    //    let mutable globalLogLevel = LogLevel.Info
    //    let mutable showStackLogLevel = LogLevel.Fatal
    let mutable logFileName = null
    let mutable indexToFlush = -1
    let mutable logCount = 0
    let mutable logFile = null 
    let mutable logListener : TextWriterTraceListener = null
    let mutable shouldShowStackTrace = false
    /// When true, each job will show the elapse time from the first item in the job
    /// Note this feature will consume a small amount of memory for each job (Guid, int64). 
    /// The option should be turned off for all services. 
    let mutable shouldShowTimeForJobID = true
    /// Services
    let jobTimerCollection = ConcurrentDictionary<Guid,int64>()

    let CreateNewLogFile( ) = 
        let extName = Path.GetExtension( logFileName ) 
        let idxExt = logFileName.IndexOf( extName, StringComparison.OrdinalIgnoreCase )
        let useFilename = 
            if String.IsNullOrEmpty extName || idxExt < 0 then 
                logFileName + "_" + logCount.ToString() + ".log"
            else
                logFileName.Substring( 0, idxExt ) + "_" + logCount.ToString() + logFileName.Substring( idxExt )
        try
            File.Delete( useFilename )
        with 
        | e -> 
            ()
        Volatile.Write( flashTicks, (PerfDateTime.UtcNowTicks()) )
        logCount <- logCount + 1
        let useListener = new TextWriterTraceListener(useFilename)
        /// Trigger creation of log file. 
        useListener.WriteLine( sprintf "============== New Log File ======================= "  )
        // File created are given full control by everyone, this eases the job of executing file under multiuser scenario
        if not Runtime.RunningOnMono then
            let fSecurity = File.GetAccessControl( useFilename ) 
            let everyoneSid = SecurityIdentifier( WellKnownSidType.WorldSid, null )
            fSecurity.AddAccessRule( new FileSystemAccessRule( everyoneSid, FileSystemRights.FullControl, AccessControlType.Allow ) )
            File.SetAccessControl( useFilename, fSecurity )
        Trace.Listeners.Add( useListener ) |> ignore 
        if Utils.IsNotNull logListener then 
            Trace.Listeners.Remove( logListener )
            logListener.Flush()
            logListener.Dispose()
        logFile <- useFilename
        logListener <- useListener

    let AddLogFile (filename:string) = 
        let path = Path.GetDirectoryName( filename )
        DirUtils.DirectoryInfoCreateIfNotExists(path) |> ignore
        logFileName <- filename
        CreateNewLogFile()

    let AddConsole() = 
        Trace.Listeners.Add( new ConsoleTraceListener(false) ) |> ignore

    let RemoveConsole() = 
        let mutable toRemove = null
        for listener in Trace.Listeners do 
            match listener with 
            | :? ConsoleTraceListener -> 
                toRemove <- listener
            | _ -> 
                ()
        if Utils.IsNotNull toRemove then 
            Trace.Listeners.Remove( toRemove )

    /// To parse the configuration for the logger
    /// -log FILENAME : add FILENAME to trace log
    /// -con          : show trace on console (default: console will be attached if no log file is specified )
    /// -nocon        : do not show trace on console 
    /// -vnoflush     : turn off auto flush
    /// -vstack       : turn on stack trace
    let ParseArguments (args : string[]) = 
        Trace.AutoFlush <- true
        let mutable i = 0
        let mutable nListeners = Trace.Listeners.Count
        while i < args.Length do
            match args.[i] with 
                | Prefixi "-log" _ 
                    -> args.[i] <- String.Empty
                       if i+1 < args.Length then 
                           // Trace.Listeners.Add( new TextWriterTraceListener(args.[i+1]) ) |> ignore
                           AddLogFile( args.[i+1] )
                           args.[i + 1] <- String.Empty
                       else
                           failwith "incorrect arguments"
                | Prefixi "-con" _ 
                    -> // let monoutput = (sprintf "Argument %s, register console as trace output.... " args.[i])
                       // Console.Out.WriteLine( monoutput )
                       args.[i] <- String.Empty
                       AddConsole()
                | Prefixi "-nocon" _ 
                    -> args.[i] <- String.Empty
                       RemoveConsole()
                | Prefixi "-vnoflush" _ 
                    -> args.[i] <- String.Empty
                       Trace.AutoFlush <- false
                | Prefixi "-vstack" _ 
                    -> args.[i] <- String.Empty
                       shouldShowStackTrace <- true
                | _ 
                    -> ()
            i <- i+1

        if Trace.Listeners.Count = nListeners then
            // JinL: don't add Console.Out due to potential performance penalty. 
            // Trace.Listeners.Add( new TextWriterTraceListener(Console.Out) ) |> ignore
            ()

    /// Return usage information for ParseArguments
    let usage = "-log FILENAME : add FILENAME to trace log.\n\
        -con          : show trace on console(default: console will be attached if no log file is specified ).\n\
        -nocon        : do not show trace on console.\n\
        -vnoflush     : turn off auto flush.\n\
        -vstack       : turn on stack trace.\n"   

    /// Flush operation, and check if the file is long enough. 
    /// The operation is put on a separate thread (not Task), as it may contains blocking operation
    let DoFlush() = 
        let flen = 
            if String.IsNullOrEmpty logFile then 
                0L
            else
                try  
                    System.IO.FileInfo( logFile).Length
                with
                | e -> 
                    Trace.WriteLine( sprintf "!!! Exception to get file length of %s for DefaultLogger.DoFlush" logFile )
                    0L
        if flen > maxLogFileLength then 
            CreateNewLogFile()    
        else
            if Utils.IsNotNull logListener then 
                // let ops = logListener.Writer.FlushAsync()
                // ops.Start() 
                ()

    /// Flush Trace
    let FlushImpl() = 
        let cur = (PerfDateTime.UtcNowTicks())
        let oldValue = !flashTicks
        if cur >= oldValue + logFlushInterval then 
            // Examine every second 
            if Interlocked.CompareExchange( flashTicks, cur, oldValue ) = oldValue then 
                // This is a rare place in the code base that we have an untracked thread. The reason is
                // it is used by ThreadTracking, and can't use ThreadTracking
                // DoFlush contains blocking code, and is not suitable to be launched in Task. 
                let threadDelegate = ThreadStart( DoFlush ) 
                let thread = Thread( threadDelegate ) 
                thread.Start()

    let EmitLogEntry(logLevel : LogLevel, message : string) =
        let sb = Text.StringBuilder()
        // The following field are always present in the log entry
        sb.Append(UtcNowToString())  // timestamp
          .Append(",")
          .Append(Thread.CurrentThread.ManagedThreadId) // thread id
          .Append(",")
          .Append(logLevel) // trace-level
          .Append(",") 
          |> ignore            
        let str =
            if shouldShowStackTrace || logLevel <= LogLevel.Warning then 
                // Debug & release may need to peel of different trace
                let stack = StackTrace (1, true)
                if Utils.IsNotNull stack then
                    let frame = stack.GetFrame(0)
                    if Utils.IsNotNull frame then
                        let m = frame.GetMethod()
                        if Utils.IsNotNull m then
                            sprintf "%s.%s : %s" (m.DeclaringType.FullName) (m.Name) message
                        else
                            message
                    else
                        message
                else
                    message
            else
                message
        sb.Append(str) |> ignore
        Trace.WriteLine <| sb.ToString()
        FlushImpl()

    member this.ShowTimeForJobID with get() = shouldShowTimeForJobID
                                 and set( b ) = shouldShowTimeForJobID <- b

    interface ILoggerProvider with
        member this.ParseArgs(args : string[]) =
            ParseArguments(args)

        member this.GetArgsUsage() =
            usage

        member this.IsEnabled((logId : string, logLevel : LogLevel)) =
            // DefaultLogger does not support "log id" yet, returns true at Info level
            logLevel <= LogLevel.Info

        member this.Log ((logLevel : LogLevel, message : string)) =
            EmitLogEntry(logLevel, message)

        member this.Log ((jobID: Guid, logLevel : LogLevel, message : string)) =
            // DefaultLogger does not support "log id" yet
            if shouldShowTimeForJobID then 
                let curTicks = DateTime.UtcNow.Ticks 
                let firstTicks = jobTimerCollection.GetOrAdd( jobID, curTicks)
                let elapseInMs = (curTicks-firstTicks)/(TimeSpan.TicksPerMillisecond)
                EmitLogEntry(logLevel, sprintf "JobID=%A(%dms),%s" jobID elapseInMs message)
            else
                EmitLogEntry(logLevel, sprintf "JobID=%A,%s" jobID message)

        member this.Log ((logId : string, logLevel : LogLevel, message : string)) =
            // DefaultLogger does not support "log id" yet
            EmitLogEntry(logLevel, message)
        
        member this.Log ((logId : string, jobID: Guid, logLevel : LogLevel, message : string)) =
            // DefaultLogger does not support "log id" yet
            if shouldShowTimeForJobID then 
                let curTicks = DateTime.UtcNow.Ticks 
                let firstTicks = jobTimerCollection.GetOrAdd( jobID, curTicks)
                let elapseInMs = (curTicks-firstTicks)/(TimeSpan.TicksPerMillisecond)
                EmitLogEntry(logLevel, sprintf "JobID=%A(%dms),%s" jobID elapseInMs message)
            else
                EmitLogEntry(logLevel, sprintf "JobID=%A,%s" jobID message)

        member this.Flush () = 
            FlushImpl()

        member this.GetLogFile () =
            logFile

    interface IDisposable with
        member x.Dispose() = 
            if Utils.IsNotNull logListener then
                logListener.Dispose()
            GC.SuppressFinalize(x)

/// Logger
type Logger internal ()=    
    // Note: the type contains APIs that can be shared by both F#/C# APIs

    /// Default log id
    static member val DefaultLogId = "Default" with get

    /// The logger provider that is used for logging
    static member val LoggerProvider : ILoggerProvider =  // Note: DefaultLogger is IDisposable. However, it is assigned to a static member of Logger class
                                                          // thus it will only be finalized/disposed when the appdomain unloads. It should be OK.
                                                          let logger = (new DefaultLogger()) :> ILoggerProvider
                                                          logger 
                                                          with get, set

    /// Global default logging level
    static member val DefaultLogLevel : LogLevel = LogLevel.Info with get, set
    
    static member private ParseLogLevel (s:string) =
        match s with
            | Prefixi "0" _ 
            | Prefixi "fatal" _ -> LogLevel.Fatal
            | Prefixi "1" _
            | Prefixi "err" _ -> LogLevel.Error
            | Prefixi "2" _
            | Prefixi "warn" _ -> LogLevel.Warning
            | Prefixi "3" _
            | Prefixi "info" _ -> LogLevel.Info
            | Prefixi "4" _
            | Prefixi "mild" _ -> LogLevel.MildVerbose
            | Prefixi "5" _
            | Prefixi "med" _  -> LogLevel.MediumVerbose
            | Prefixi "6" _
            | Prefixi "wild" _ -> LogLevel.WildVerbose
            | Prefixi "7" _
            | Prefixi "extreme" _ -> LogLevel.ExtremeVerbose
            | _ ->   failwith (sprintf "Unsupported log level: %s" s)

    static member val CommonUsage = "-verbose logLevel  : set default log level.\n"

    /// Parse the arguments that configure the behavior of the logger
    static member ParseArgs(args : string[]) =
        // Parse the generic arguments
        let mutable i = 0
        while i < args.Length do
            match args.[i] with 
            | Prefixi "-verbose" _ 
                -> args.[i] <- String.Empty
                   if i+1 < args.Length then 
                      Logger.DefaultLogLevel <- Logger.ParseLogLevel (args.[i+1])
                      args.[i + 1] <- String.Empty
                   else
                      failwith "Incorrect Arguments"
            | _ -> ()
            i <- i + 1
        // Parse LoggerProvider's specific arguments
        Logger.LoggerProvider.ParseArgs(args)

    static member PrintArgsUsage() =
        let result = Logger.CommonUsage +  Logger.LoggerProvider.GetArgsUsage()
        printfn "%s" result

    /// Log "message" if logLevel <= Logger.DefaultLogLevel                                                                    
    static member inline Log(logLevel : LogLevel, message : string) =
        if logLevel <= Logger.DefaultLogLevel then
            Logger.LoggerProvider.Log((logLevel, message))

    /// Log "message" using "logId"                                                              
    static member inline Log(logId : string, logLevel : LogLevel, message : string) =
        if Logger.LoggerProvider.IsEnabled(logId, logLevel) then
            Logger.LoggerProvider.Log((logId, logLevel, message))

    /// Log "message" using "jobID"                                                              
    static member inline Log(jobID: Guid, logLevel : LogLevel, message : string) =
        if logLevel <= Logger.DefaultLogLevel then
            Logger.LoggerProvider.Log((jobID, logLevel, message))

    /// Log "message" using "logId" and jobID
    static member inline Log(logId: string, jobID: Guid, logLevel : LogLevel, message : string) =
        if Logger.LoggerProvider.IsEnabled(logId, logLevel) then
            Logger.LoggerProvider.Log((logId, jobID, logLevel, message))

    /// Log stack trace if logLevel <= Logger.DefaultLogLevel
    static member LogStackTrace(logLevel : LogLevel) =
        Logger.Log(logLevel, Environment.StackTrace)        

    /// Log stack trace with "logId"
    static member LogStackTrace(logId : string, logLevel : LogLevel) =
        Logger.Log(logId, logLevel, Environment.StackTrace)  
        
    /// Flush the pending log entries
    static member Flush () = 
        Logger.LoggerProvider.Flush()

    /// Return a file that contains the latest log entries
    static member GetLogFile() =
        Logger.LoggerProvider.GetLogFile()

    // Log an error and throw an exception 
    static member Fail(message : string) =
        Logger.Log(LogLevel.Error, message)
        failwith message

    // Log an error and throw an exception 
    static member Fail(logId : string, message : string) =
        Logger.Log(logId, LogLevel.Error, message)
        failwith message
