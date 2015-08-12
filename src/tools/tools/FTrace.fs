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
        FTrace.fs
  
    Description: 
        Provide customerized trace for F#

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Apr. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System

/// <summary> 
/// A set of helper function for diagnostic trace, using System.Diagnostics
/// </summary>
module FTrace =
    open System
    open System.Diagnostics
    open System.IO
    open System.Security.AccessControl
    open System.Threading

    /// <summary> 
    /// Prajna Trace level, the lower the level, the more important of the message. 
    /// </summary>
    type FTraceLevel = LogLevel
    
    type FLogger = Prajna.Tools.FSharp.Logger

    /// <summary>
    /// To parse the commandline parameter related to traces. The following command line argument is defaultly parsed for every program that uses FTrace. <para/>
    /// -log FILENAME : add FILENAME to trace log<para/>
    /// -con          : show trace on console (default: console will be attached if no log file is specified )<para/>
    /// -nocon        : do not show trace on console <para/>
    /// -v   VERBOSE  : verbose level. <para/>
    /// -vnoflush     : turn off auto flush<para/>
    /// -vstack       : turn on stack trace<para/>
    /// </summary>
    let FTrace (args : string[]) = 
        FLogger.ParseArgs(args)

    let FTracePrintUsage () = 
        FLogger.PrintArgsUsage()

    /// Flush Trace
    let FTraceFlush() = 
        FLogger.Flush()

    /// Flush all traces. 
    let FTraceFlushForce() = 
        for listener in Trace.Listeners do 
            listener.Flush() 
//            let numListeners = Trace.Listeners.Count
//            if numListeners<=0 then 
//                m_FlashTicks <- m_FlashTicks + m_FlushFrequency
//            else
//                m_FlashTicks <- m_FlashTicks + m_FlushFrequency / int64 numListeners
//                m_IndexToFlush <- (m_IndexToFlush+1)%numListeners
//                let listener = Trace.Listeners.Item(m_IndexToFlush)
//                let writer = listener :?> TextWriterTraceListener
//                let ops = writer.Writer.FlushAsync()
//                ops.Start() 

    /// <summary>
    /// Get the current trace Log file
    /// </summary> 
    let FTraceGetLogFile()= 
        FLogger.GetLogFile()

    /// If curTraceLevel is below global trace level, print info in trace/log file
    /// should be used for computational simple info, or non critical loop, as info is always evaluated. 
    let inline FTraceLine curTraceLevel info =
        FLogger.Log(curTraceLevel, info)

    /// If curTraceLevel is below global trace level, print info in trace/log file. 
    /// info() will NOT be evaluated if the current trace level is below curTraceLevel. Thus for cases that the trace does not activate, the only overhead is an inline comparison. 
    let inline FTraceFLine curTraceLevel ( info: unit -> string) =
        FLogger.LogF(curTraceLevel, info)

    /// If curTraceLevel is below global trace level, print info in trace/log file
    /// should be used for computational simple info, or non critical loop, as info is always evaluated. 
    let inline FTraceWrite curTraceLevel info =
        FLogger.Log(curTraceLevel, info)
        // FTraceImpl curTraceLevel (fun () -> info) (Trace.Write)

    /// If curTraceLevel is below global trace level, print info in trace/log file
    /// info() will NOT be evaluated if the current trace level is below curTraceLevel. Thus for cases that the trace does not activate, the only overhead is an inline comparison. 
    let inline FTraceFWrite curTraceLevel info =
        FLogger.LogF(curTraceLevel, info)
        // FTraceImpl curTraceLevel info (Trace.Write)

    // Note: to avoid confusion C# APIs should be provided in a different namespaces, so C# user does not see all the F# only functions
    /// C# and .Net support of functional trace. If curTraceLevel is below global trace level, print info in trace/log file. 
    /// info() will NOT be evaluated if the current trace level  is below curTraceLevel. Thus for cases that the trace does not activate, the only overhead is an inline comparison. 
    //    let inline CSTraceFLine (curTraceLevel, info: Func<string>) =
    //        FTraceImpl curTraceLevel (fun () -> info.Invoke()) (Trace.WriteLine)

    /// If curTraceLevel is below global trace level, print current stack in trace/log file. 
    /// Thus for cases that the trace does not activate, the only overhead is an inline comparison. 
    let inline FTraceFShowStacks curTraceLevel = 
        FLogger.LogStackTrace(curTraceLevel)

    /// FTraceFDo curTraceLevel (action: unit -> unit). 
    /// Execute some action if curTraceLevel is below global trace level. 
    let inline FTraceFDo curTraceLevel ( action: unit -> unit ) = 
        FLogger.Do(curTraceLevel, action)

    /// Print error message in trace file, and throw an exception. 
    let inline FTraceFail info =
        FLogger.Log(FTraceLevel.Error, info)
        //FTraceLine FTraceLevel.Error info
        failwith( info )

    //    /// helper function. Check if curTraceLevel is below global trace level
    //    let inline FTraceExamine curTraceLevel =
    //        curTraceLevel <= m_TraceLevel


            



     
                      


