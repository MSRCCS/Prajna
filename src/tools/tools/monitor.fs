(*---------------------------------------------------------------------------
    Copyright 2016 Microsoft

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
        monitor.fs
  
    Description: 
        Use of monitor instead of events

    Author:
        Sanjeev Mehrotra, Principal Software Architect
    Date:
        January 2016	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.IO
open System.Collections.Generic
open System.Collections.Concurrent
open System.Runtime.Serialization
open System.Threading
open System.Runtime.InteropServices

open Prajna.Tools
open Prajna.Tools.FSharp

// =====================================================

type internal DoOnce() =
    let lockObj = new Object()
    let mutable bDone = false
    member x.Run(fn : unit->unit) =
        if (not bDone) then
            lock (lockObj) (fun _ ->
                if (not bDone) then
                    fn()
                    bDone <- true
            )

// ==============================================

[<AllowNullLiteral>]
[<AbstractClass>]
type internal MEventBase() =
    let lockObj = Object()
    member internal x.LockObj with get() = lockObj
    abstract Wait : unit->unit
    abstract Set : unit->unit

[<AllowNullLiteral>]
type internal MEvent(initState : bool) =
    inherit MEventBase()

    let setLock = new Object() // without this, potentially only one thread would wake up, as other would wait for lockObj
    let mutable signalled = initState

    override x.Wait() =
        lock (x.LockObj) (fun _ ->
            while (not signalled) do
                Monitor.Wait(x.LockObj) |> ignore
        )
        // alternately can write (same thing)
//        try
//            Monitor.Enter(lockObj)
//            while (not signalled) do
//                Monitor.Wait(lockObj) |> ignore
//        finally
//            Monitor.Exit(lockObj)

    member x.Reset() =
        lock (setLock) (fun _ ->
            //Thread.MemoryBarrier()
            signalled <- false
            //Thread.MemoryBarrier()
        )

    override x.Set() =
        lock (setLock) (fun _ ->
            //Thread.MemoryBarrier() // write cannot move earlier in thread
            signalled <- true
            lock (x.LockObj) (fun _ ->
                Monitor.PulseAll(x.LockObj)
            )
        )

[<AllowNullLiteral>]
type internal MEventCond(cond : unit->bool) =
    inherit MEventBase()

    member val internal Cond = cond with get, set

    override x.Wait() =
        lock (x.LockObj) (fun _ ->
            while not (x.Cond()) do
                Monitor.Wait(x.LockObj) |> ignore
        )

    // everytime you may set, must pulse
    override x.Set() =
        lock (x.LockObj) (fun _ ->
            Monitor.PulseAll(x.LockObj)
        )

// ===================================================

type [<AbstractClass>] [<AllowNullLiteral>] internal ThreadBase(pool : ThreadPoolBase) =
    let mutable thread : Thread = null
    let mutable terminate = false
    let mutable stop = false

    //static member val BackgroundThreads = false // prevents process from terminating
    static member val BackgroundThreads = true
    static member val LogLevel = LogLevel.WildVerbose with get

    member val ControlHandle : MEvent = null with get, set

    member val StopHandle = new ManualResetEvent(false) with get

    abstract Stop : unit->unit
    default x.Stop() =
        stop <- true
        if (Utils.IsNotNull x.ControlHandle) then
            x.ControlHandle.Set()

    abstract Terminate : unit->unit
    default x.Terminate() =
        terminate <- true
        if (Utils.IsNotNull x.ControlHandle) then
            x.ControlHandle.Set()

    member val Id = -1L with get, set

    member x.Thread with get() = thread

    member x.Start() =
        thread.Start()
    
    abstract Init : unit->unit
    default x.Init() =
        let threadStart = new ThreadStart(x.Process)
        thread <- new Thread(threadStart)
        thread.IsBackground <- ThreadBase.BackgroundThreads

    abstract ToStop : (unit->bool) with get
    abstract ProcessOne : unit->unit

    member x.Process() =
        while not (terminate || (stop && x.ToStop())) do
            x.ProcessOne()
        x.StopHandle.Set() |> ignore

    override x.Finalize() =
        if (ThreadBase.BackgroundThreads = false) then
            x.Terminate()

    interface IDisposable with
        override x.Dispose() =
            x.Finalize()
            GC.SuppressFinalize(x)

and [<AbstractClass>] internal ThreadPoolBase() =
    let id = ref -1L
    let mutable bStop = false
    let mutable bTerminate = false
    let mutable inCreation = false
    let stopList = new List<WaitHandle>()
    let threads = new ConcurrentDictionary<int64, ThreadBase>()
    
    member x.Terminate() =
        bTerminate <- true
        let curId = !id
        let sp = new SpinWait()
        while (inCreation && !id=curId) do
            sp.SpinOnce()
        for t in threads do
            t.Value.Terminate()

    member x.Stop() =
        bStop <- true
        let curId = !id
        let sp = new SpinWait()
        while (inCreation && !id=curId) do
            sp.SpinOnce()
        for t in threads do
            t.Value.Stop()

    member x.StopAndWait(timeout : int) =
        x.Stop()
        let wh = stopList.ToArray()
        WaitHandle.WaitAll(wh, timeout) |> ignore

    member x.Threads with get() = threads

    member val internal ST = new SingleThreadExec() with get

    abstract CreateNew : (ThreadPoolBase -> ThreadBase) with get, set

    member x.CreateNewThread() =        
        let ntId = Interlocked.Increment(id)
        inCreation <- true
        if (not bTerminate && not bStop) then
            let nt = x.CreateNew(x)
            nt.Id <- ntId
            nt.Init()
            threads.[ntId] <- nt
            stopList.Add(nt.StopHandle)
            nt.Start()
        inCreation <- false

// ===================================================================

