namespace Prajna.Test.Common

open System
open System.Threading

open NUnit.Framework

open Prajna.Tools
open Prajna.Tools.FSharp

// A common base class for tests that contains common Init and Cleanup
type Tester () = 
    
    let sw = Diagnostics.Stopwatch()
    
    // To be called before each test
    abstract member Init : unit -> unit
    [<SetUp>] 
    default x.Init() =
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "##### Test %s starts (%s) #####" TestContext.CurrentContext.Test.FullName (StringTools.UtcNowToString())))
        sw.Restart()

    // To be called before each test
    abstract member Cleanup : unit -> unit
    [<TearDown>] 
    default x.Cleanup() =
        sw.Stop()
        // Force a GC collection in the process
        // From: http://blogs.msdn.com/b/tess/archive/2008/08/19/questions-on-application-domains-application-pools-and-unhandled-exceptions.aspx,
        // "all appdomains in the process share the same GC, threadpool, finalizer thread etc"
        // GC.Collect()
        // GC.WaitForPendingFinalizers()
        let proc = Diagnostics.Process.GetCurrentProcess()

        let maxThreads, maxIOThreads = ThreadPool.GetMaxThreads()
        let availThreads, availIOThreads = ThreadPool.GetAvailableThreads()

        Logger.LogF( LogLevel.Info, 
                     ( fun _ -> sprintf "##### Test %s ends: %s (%i ms) (%i THs, (%i, %i) ThreadPool THs, GC Heap: %f MB, Private Memory %f MB) #####" 
                                  TestContext.CurrentContext.Test.FullName 
                                  (TestContext.CurrentContext.Result.Status.ToString()) 
                                  sw.ElapsedMilliseconds 
                                  (proc.Threads.Count)
                                  (maxThreads - availThreads)
                                  (maxIOThreads - availIOThreads)
                                  ((float (GC.GetTotalMemory(false))) / 1e6)
                                  ((float proc.PrivateMemorySize64)/1e6)))
