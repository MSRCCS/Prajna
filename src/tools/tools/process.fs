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
        Process.fs
  
    Description: 
        Helper function for process

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools
open System
open System.Diagnostics
open System.Threading
open System.IO
open System.Collections.Concurrent
open System.Collections.Generic
open Prajna.Tools.StringTools
open Prajna.Tools.FileTools
open Prajna.Tools.FSharp

type internal SpinLockTask =
    struct
        val private LockValue : int64 ref
        val private Tracking : bool 
        new(bTracking: bool) = { LockValue = ref 0L; Tracking = bTracking }
        member x.IsHeld with get() = (!x.LockValue)<>0L
        member private x.GetID() = 
            if x.Tracking then 
                let currentThreadID = Thread.CurrentThread.ManagedThreadId
                let taskID = Tasks.Task.CurrentId
                let ID = if taskID.HasValue then 
                            ( int64 taskID.Value <<< 32 ) + (int64 currentThreadID )
                            else
                            (int64 currentThreadID )
                if ID = 0L then int64 Int32.MaxValue else ID
            else
                1L
        member x.Enter( lockTaken: bool ref) = 
            let ID = x.GetID() 
            while not ( !lockTaken ) do 
                x.TryEnterInternal lockTaken ID
        member x.TryEnter( lockTaken: bool ref ) = 
            let ID = x.GetID() 
            x.TryEnterInternal lockTaken ID
        member private x.TryEnterInternal ( lockTaken: bool ref ) ID= 
            lockTaken := Interlocked.CompareExchange( x.LockValue, ID, 0L ) = 0L
        member x.Exit( bBarrier: bool ) = 
            if !(x.LockValue) <> x.GetID() then          
                let lockID = !(x.LockValue)   
                let curID = x.GetID()
                Logger.LogF( LogLevel.Warning, ( fun _ -> let lockThreadID = int ( lockID &&& 0xffffffffL )
                                                          let lockTaskID = int ( lockID >>> 32 )
                                                          let curThreadID = int ( curID &&& 0xffffffffL )
                                                          let curTaskID = int ( curID >>> 32 )
                                                          if curTaskID = 0 && lockTaskID = 0 then 
                                                              sprintf "Exit a SpinLock on thread %d, different from thread %d that holds the lock "
                                                                       curThreadID lockThreadID 
                                                          else                                                           
                                                              sprintf "Exit a SpinLock on thread %d, task %d, different from thread %d, task %d that holds the lock "
                                                                       curThreadID curTaskID lockThreadID lockTaskID 
                                                          ))
                Logger.LogStackTrace( LogLevel.Warning )
            x.LockValue := 0L
    end 

/// <summary>
/// SpinLockSlim is similar to SpinLock, but with lower overhead. It also corrects some bugs in SpinLock in which it fails to lock the area of access. 
/// </summary>
type internal SpinLockSlim =
    struct
        /// Lock Value used by SpinLockSlim, using 1 if not tracking thread, and managed thread ID if tracking thread
        val private LockValue : int ref
        /// Whether tracking thread
        val private Tracking : bool 
        /// Initialize a SpinLockSlim, if bTracking is true, the threadID of the locking thread will be used in tracking
        new(bTracking: bool) = { LockValue = ref 0; Tracking = bTracking }
        /// ture if lock is being held
        member x.IsHeld with get() = (!x.LockValue)<>0
        /// if tracking thread, return the ManagedThreadId, otherwise, always return 1
        member x.GetID() = 
            if x.Tracking then 
                Thread.CurrentThread.ManagedThreadId
            else
                1
        /// <summary>
        /// Internal function of acquiring a lock
        /// </summary>
        member x.TryEnterInternal ID= 
            if ID = 0 then 
                Logger.Log( LogLevel.Warning, ( sprintf "Attempt to lock SpinLockSlim with ID 0 " ))
                Logger.LogStackTrace( LogLevel.Warning )
            (Interlocked.CompareExchange( x.LockValue, ID, 0 ) = 0)
        /// <summary>
        /// Acquire a lock 
        /// </summary>
        member x.Enter() = 
            let ID = x.GetID() 
            let mutable lockTaken = false
            while not ( lockTaken ) do 
                lockTaken <- x.TryEnterInternal ID
        /// <summary>
        /// Attempts to acquire the lock. The return value indicates whether it has been acquired or not
        /// </summary>
        member x.TryEnter() = 
            let ID = x.GetID() 
            x.TryEnterInternal ID
        /// <summary>
        /// Releases the lock.
        /// </summary>
        member x.Exit() = 
            if !(x.LockValue) <> x.GetID() then          
                Logger.Log( LogLevel.Warning, ( sprintf "Exit a SpinLockSlim on thread %d, different from thread %d that holds the lock "
                                                        (x.GetID()) (!x.LockValue)
                                                          ))
                Logger.LogStackTrace( LogLevel.Warning )
            x.LockValue := 0
    end 

/// ExecuteOnceWrapper garantee that the function wrapped is executed only once when the wrapper action is called. 
/// Also, each execution only completes when the instance of execution has been completed. 
/// The syntax is similar to Lazy, though no result is required. 
type internal ExecuteOnceWrapper<'T> private (func:'T -> unit) = 
    let nExecuted = ref 0 
    let evExecuted = new ManualResetEvent(false)
    member private x.Execute inp = 
        if Interlocked.CompareExchange( nExecuted, 1, 0)=0 then 
            func inp
            evExecuted.Set() |> ignore 
        evExecuted.WaitOne() |> ignore
    /// Warp a function, garantee that it is executed only once when the wrapped function is called. Also, each function 
    /// only completes when the instance of execution has been completed. 
    static member Wrap (f) = 
        let x = ExecuteOnceWrapper(f)
        x.Execute
    /// Warp a action, garantee that it is executed only once when the wrapped action is called. Also, each action 
    /// only completes when the instance of execution has been completed. 
    static member WrapAction(f: Action<'T>) = 
        let x = ExecuteOnceWrapper<'T>( f.Invoke )
        Action<'T>( fun inp -> x.Execute inp )

/// ExecuteOnceUnit garantee that the function wrapped is executed only once when the wrapper action is called. 
/// Also, each execution only completes when the instance of execution has been completed. 
/// The syntax is similar to Lazy, though no result is required. 
type internal ExecuteOnceUnit private (func) = 
    let nExecuted = ref 0 
    let evExecuted = new ManualResetEvent(false)
    member private x.Execute() = 
        if Interlocked.CompareExchange( nExecuted, 1, 0)=0 then 
            func()
            evExecuted.Set() |> ignore 
        evExecuted.WaitOne() |> ignore
    /// Warp a function, garantee that it is executed only once when the wrapped function is called. Also, each function 
    /// only completes when the instance of execution has been completed. 
    static member Wrap (f) = 
        let x = ExecuteOnceUnit(f)
        x.Execute
    /// Warp a action, garantee that it is executed only once when the wrapped action is called. Also, each action 
    /// only completes when the instance of execution has been completed. 
    static member WrapAction(f: Action) = 
        let x = ExecuteOnceUnit( f.Invoke )
        Action( x.Execute )

/// Structure associated with CleanUp, only used if the class need to clean up the class early. 
/// If that is the case, call CleanUpThisOnly()
[<AllowNullLiteral>]
type internal OneCleanUp ( o:Object, infoFunc, f, earlyCleanUp: unit -> unit ) =
    let wrappedCleanUp() = 
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "==== CleanUp %s ==== " (infoFunc()) ))
        f()
    member val InfoFunc = infoFunc with get
    /// Providing information of the cleanup object
    member val CleanUpFunc = ExecuteOnceUnit.Wrap( wrappedCleanUp ) with get
    member val CleanUpObject = o with get
    /// CleanUpThisOnly() should be called if the object o needs to be disposed early 
    member val CleanUpThisOnly = earlyCleanUp with get

// In Prajna, the order of clean up is 
// ThreadPoolTimerCollection:       300
// Cluster:                   1000
// ThreadPoolWithWaitHandles:       1500
// ThreadPoolWait:                  2000
// LocalCluster:        2500
// ThreadTracking:      3000


/// CleanUp structure is used for system maintainenance, allow multiple systemwide disposable object to be disposed in an ordered way 
/// (e.g., threadpool is closed at last, after network queue has been shutdown, etc..)
/// The participated disposable object is usually systemwide static class (e.g., all queues, all wait handles, all thread pools, etc..)
/// Each systemwide disposable object (e.g., the static valuable attached to the object) should use Dispoable interface, and register its 
/// disposing function during construction at CleanUp.Register( orderNumber, o, f), where o is the object and f is the disposing function to be executed. 
/// The object's own disposing function then should contain 
/// CleanUp.CleanUpAll(), which garantees all objects to be cleaned up in an orderly fashion. 
type internal CleanUp private () = 
    let cleanUpStore = ConcurrentDictionary<int64,OneCleanUp>()
    /// The default CleanUp structure to be used. 
    static member val Current = CleanUp() with get, set
    member private x.FindObject( o: Object ) = 
        let mutable retCleanUp = null
        let mutable retKey = Int64.MinValue
        for pair in cleanUpStore do 
            if Object.ReferenceEquals( pair.Value.CleanUpObject, o) then 
                retCleanUp <- pair.Value
                retKey <- pair.Key
        retKey, retCleanUp
    /// Find a key for the current disposable object. If already exist, find another key. 
    member private x.FindKey( orderNumber:int, o: Object ) = 
        let mutable useNumber = ((int64 orderNumber)<<<32) + int64 (o.GetHashCode())
        let refValue = ref Unchecked.defaultof<_>
        while cleanUpStore.TryGetValue( useNumber, refValue ) do 
            useNumber <- useNumber + 1L
        useNumber
                
    /// <summary> 
    /// Register a clean up function, with the associated object, the cleanup function will be garanteed to called once. 
    /// </summary> 
    /// <param name="orderNumber"> An integer indicates that the object and its cleanup function will be called, the smaller the number, 
    ///     the earlier it will be disposed. </param>
    /// <param name="o"> An object to be disposed. </param>
    /// <param name="f"> The inner function of the object to be called for disposing the object. </param> 
    /// <param name="infoFunc"> The information of the object being disposed. </param> 
    member x.Register( orderNumber, o, f, infoFunc ) =
        let mutable oneCleanUp = OneCleanUp(o,infoFunc, f, fun _ -> x.CleanUpOneObject(o) )
        // Find if the object exists 
        let mutable bRegistered = false
        while not bRegistered do 
            let retKey, retCleanUp = x.FindObject( o )
            if Utils.IsNotNull retCleanUp then 
                // Object already registered, do nothing
                oneCleanUp <- retCleanUp
                bRegistered <- true
            else
                let useNumber = x.FindKey( orderNumber, o )
                let retCleanUp = cleanUpStore.GetOrAdd( useNumber, oneCleanUp )
                if Object.ReferenceEquals( retCleanUp, oneCleanUp ) || 
                    Object.ReferenceEquals( retCleanUp.CleanUpObject, oneCleanUp.CleanUpObject ) then 
                        oneCleanUp <- retCleanUp
                        bRegistered <- true
                else
                    // Failed to get the lock, retry
                    ()
        oneCleanUp
            
    /// <summary> 
    /// Specifically clean up one object before hand. Object o should be registered previously. The function also doesn't trigger 
    /// disposing of all other object. 
    /// </summary> 
    member x.CleanUpOneObject (o:Object) = 
        let retKey, retCleanUp = x.FindObject( o )
        if Utils.IsNotNull retCleanUp then 
            cleanUpStore.TryRemove( retKey ) |> ignore 
            retCleanUp.CleanUpFunc()

    member private x.FlushAllListeners () =
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

    /// Orderly clean up all objects. 
    member x.CleanUpAll()=
        let allKeys = cleanUpStore.Keys |> Seq.sort |> Seq.toArray
        for key in allKeys do 
            let bExist, oneCleanUp = cleanUpStore.TryRemove( key )
            if bExist then 
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "::: Enter to clean up %s" (oneCleanUp.InfoFunc()) ))
                oneCleanUp.CleanUpFunc();
        x.FlushAllListeners()

module internal Process =

    let WaitProcessToEnd name timesInMilliseconds = 
        let mutable procs = Process.GetProcessesByName( name )
        let mutable wait = timesInMilliseconds
        while procs.Length>0 && wait>0 do
            Thread.Sleep( 1000 )
            procs <- Process.GetProcessesByName( name )
        procs.Length <= 0 
    
    let ProcessClose name =
        let procs = Process.GetProcessesByName( name )
        for proc in procs do
            proc.CloseMainWindow() |> ignore
        procs.Length
    let ProcessKill name =
        let procs = Process.GetProcessesByName( name )
        for proc in procs do
            proc.Kill() |> ignore
        procs.Length

    let EnsureKillBegin exesName =
        // wait for main program to end
        let nproc = Array.map (ProcessClose) exesName
        nproc

    let EnsureKillEnd exesName nproc = 
        // Ensure killing multiple processes
        let mutable retry = 10
        try
            // Copy deploy files 
            let mutable maxnproc = nproc |> Array.max
            while maxnproc > 0 && retry > 0 do
                // Wait for process to terminate
                Thread.Sleep(3000)
                let newproc = 
                    Array.map2 ( fun name n -> 
                                    if n>0 then 
                                        ProcessKill name
                                    else
                                        n
                                ) exesName nproc
                Array.Copy( newproc, nproc, nproc.Length )
                maxnproc <- ( nproc |> Array.max )
                retry <- retry - 1
        with
        | _ -> ()

    let EnsureKill exesName = 
        let nproc = EnsureKillBegin exesName 
        EnsureKillEnd exesName nproc
        nproc

    let EnsureKillOne exeName =
        let res = EnsureKill [|exeName|]
        res.[0]

    let CopyAllFiles srcDir dstDir =
        try
            let srcDirInfo = new DirectoryInfo(srcDir)
            if srcDirInfo.Exists then 
                let files = srcDirInfo.GetFiles()
                let dstDirInfo = DirectoryInfoCreateIfNotExists( dstDir )
                // if dstDir was not created, an exception should have been thrown
                for file in files do
                    let dstFName = Path.Combine( dstDir, file.Name )
                    try 
                        file.CopyTo( dstFName, true ) |> ignore
                        Logger.Log( LogLevel.MildVerbose, ( sprintf "Copy %s to %s" file.FullName dstFName ))
                    with
                    | _ -> ()
                    ()
        with
        | _ -> ()
        ()
        

    // Find executable
    let FindExecutableAtLeast localFolder exeName (maxVer:string) = 
        let mutable runDeploy = Path.Combine( localFolder, exeName )
        let mutable bFind = false
        let mutable dir = Char.MaxValue.ToString()

        if not ( File.Exists( runDeploy )) || maxVer.Length>0 then 
            let sortedVerDirs = SubDirectories localFolder
            Array.Sort( sortedVerDirs )
            let mutable i = sortedVerDirs.Length-1
            while i>=0 && not bFind do
                dir <- sortedVerDirs.[i]
                if maxVer.Length=0 || dir < maxVer then 
                    runDeploy <- Path.Combine( dir, exeName )
                    if File.Exists( runDeploy ) then 
                        bFind <- true
                i <- i - 1
        else
            bFind <- true
        if bFind then 
            ( runDeploy, dir )
        else
            ( "", "" )

    let FindExecutable localFolder exeName =
        FindExecutableAtLeast localFolder exeName ""

    let EnsureExecute localFolder exeName shortExeName (param:string) = 
        let mutable runDeploy, maxVer = FindExecutable localFolder exeName

        if runDeploy.Length>0 then
            if param.Length>0 then 
                Process.Start( runDeploy, param ) |> ignore
            else
                Process.Start( runDeploy ) |> ignore
            Thread.Sleep( 2000 )
            let mutable nprocs = Process.GetProcessesByName( shortExeName )
            let mutable retry = 10

            Logger.Log( LogLevel.Info, (sprintf "Find Processes %s with %A" shortExeName nprocs ))
            true
   
//            while nprocs.Length = 0 && retry>0 do 
//                // Process execute in too short time, try to start a new process
//                let cExe, cVer = FindExecutableAtLeast localFolder exeName maxVer
//                runDeploy <- cExe
//                maxVer <- cVer
//                if runDeploy.Length>0 then 
//                    if param.Length>0 then 
//                        Process.Start( runDeploy, param ) |> ignore
//                    else
//                        Process.Start( runDeploy ) |> ignore
//                    Thread.Sleep( 3000 )
//                    nprocs <- Process.GetProcessesByName( shortExeName )
//                    retry <- retry - 1 
//                else
//                    retry <- 0
//            nprocs.Length > 0
        else
            false


type internal ExecutionMode = 
    | ByTask = 0
    | ByLongRunningTask = 1
    | ByThread = 2

/// Try implementing an array that can concurrently add/remove items 
/// Implementation hasn't worked out all thread safety issues. 
type internal ConcurrentArray<'T>() =
    let refCount = ref 0
    let mutable internalArray : ('T)[] ref = ref null 
    let newCapacity (n:int) (curCapacity:int)= 
        Math.Max( n, curCapacity * 2 )
    static member val DefaultCapacity = 8 with get, set
    member x.Count with get() = !refCount
    member x.Capacity with get() = if Utils.IsNull !internalArray then 0 else (!internalArray).Length
                       and set( c ) = x.Expand c
    member val NewCapacityFunc = newCapacity with get, set
    member x.ExpandTo() =
        let n = !refCount
        let curCapacity = x.Capacity
        let num = if curCapacity=0 then Math.Max( n, ConcurrentArray<'T>.DefaultCapacity) else n       
        if num > (curCapacity) then 
            x.NewCapacityFunc num curCapacity * 2 
        else 
            num
    member x.Expand desiredCapacty = 
        let bDone = ref false
        while not (!bDone) do 
            let curCapacity = x.Capacity
            if desiredCapacty <= curCapacity then 
                bDone := true
            else
                // Need to expand
                let newCapacity = x.ExpandTo() 
                assert ( newCapacity > curCapacity )
                lock (x) ( fun _ -> 
                    if x.Capacity = curCapacity then 
                        Array.Resize( internalArray, newCapacity )
                        bDone := true
                    else
                        () // Spin again, some thread already executes the expansion
                    )
    member x.BoundedAdd (t:'T) = 
        let idx = Interlocked.Increment( refCount ) 
        if idx >= x.Capacity then 
            Interlocked.Decrement( refCount ) |> ignore 
            false
        else
            (!internalArray).[idx - 1] <- t
            true
    member x.Add (t:'T) = 
        let idx = Interlocked.Increment( refCount ) - 1
        x.Expand (idx+1)
        (!internalArray).[idx] <- t
//    member x.Item with get(i:int) = (!internalArray).[i]
//                   and set(i:int, t ) =  (!internalArray).[i] <- t
//    interface IEnumerable<'T> with 
//        member x.GetEnumerator() = 
//            (!internalArray).GetEnumerator()
    // Remove Item, but don't shrink Array
    member x.Remove( idx ) = 
        let bDone = ref false
        let moveArray = (!internalArray)
        while not (!bDone) do 
            let lastIdx = Interlocked.Decrement( refCount ) 
            if lastIdx > idx then 
                moveArray.[ idx ] <- moveArray.[ lastIdx ]
            if Object.ReferenceEquals( moveArray, !internalArray) && lastIdx=(!refCount) then 
                // Not expanded 
                bDone := true

// ==============================================================

type internal SingleThreadExec() =
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
        if (Interlocked.Increment(counter) = 1) then
            let mutable bDone = false
            let fn = ref (fun () -> ())
            while (not bDone) do
                let ret = q.TryDequeue(fn)
                if (ret) then
                    (!fn)()
                    bDone <- (Interlocked.Decrement(counter) = 0)

// ===========================================================

// callbacks return next time
type private TimerPool<'K>() as x =
    static let instanceCount = ref -1
    let timer = new Timer(x.OnFire, null, int64 Timeout.Infinite, int64 Timeout.Infinite)
    let todo = new ConcurrentDictionary<'K, int64*(unit->int64)>()
    let timeQ = new ConcurrentQueue<int64>()
    let count = ref -1L
    let toEnqueue = new SingleThreadExec()
    let toFire = new SingleThreadExec()
    let instance = Interlocked.Increment(instanceCount)
    let stopwatch = new Stopwatch()
    let mutable firingTime = Int64.MaxValue
    do stopwatch.Start()

    member x.Instance with get() = instance
    member x.Count with get() = count

    member x.UpdateTime() = 
        let fireTime = ref 0L
        while (not timeQ.IsEmpty) do
            let ret = timeQ.TryDequeue(fireTime)
            if (ret) then
                firingTime <- Math.Min(firingTime, !fireTime)
        let waitTimeMs = 
            if (firingTime = Int64.MaxValue) then
                int64 Timeout.Infinite
            else
                firingTime - stopwatch.ElapsedMilliseconds
        if (waitTimeMs <= 0L) then
            toFire.ExecOnce(x.Fire)
        else
            if not (timer.Change(waitTimeMs, int64 Timeout.Infinite)) then
                toFire.ExecOnce(x.Fire)

    member x.AddTimer(key : 'K, cb : unit->int64, timeMs : int64) =
        if (timeMs <> int64 Timeout.Infinite) then
            let timeFire = stopwatch.ElapsedMilliseconds + timeMs
            todo.[key] <- (timeFire, cb)
            timeQ.Enqueue(timeFire)
            toEnqueue.ExecOnce(x.UpdateTime)

    member x.AddTimer(key : 'K, cb : unit->unit, timeMs: int64, periodMs : int64) =
        let wrappedFunc() =
            cb()
            periodMs
        x.AddTimer(key, wrappedFunc, timeMs)

    member x.AddTimer(key : 'K, cb : unit->unit, timeMs : int64) =
        let wrappedFunc() =
            cb()
            int64 Timeout.Infinite
        x.AddTimer(key, wrappedFunc, timeMs)

    member x.Fire() =
        let mutable nextFiringTime = Int64.MaxValue
        let curTime = stopwatch.ElapsedMilliseconds
        // reset the firing time
        firingTime <- Int64.MaxValue
        for pair in todo do
            let (timeFire, cb) = pair.Value
            if (timeFire + 1L <= curTime) then
                let nextWaitTime =
                    try
                        cb()
                    with e ->
                        Logger.LogF( LogLevel.Error, (fun _ -> sprintf "Timerpool callback with key %A throws Exception %A" pair.Key e))
                        int64 Timeout.Infinite
                if (nextWaitTime = int64 Timeout.Infinite) then
                    todo.TryRemove(pair.Key) |> ignore
                else
                    let timeFire = curTime + nextWaitTime
                    nextFiringTime <- Math.Min(nextFiringTime, timeFire)
                    todo.[pair.Key] <- (timeFire, cb)
            else
                nextFiringTime <- Math.Min(nextFiringTime, timeFire)
        if (nextFiringTime < Int64.MaxValue) then
            timeQ.Enqueue(nextFiringTime)
            toEnqueue.ExecOnce(x.UpdateTime)

    member x.OnFire(o) =
        toFire.ExecOnce(x.Fire)

type internal PoolTimer() =
    static member val private TimerPoolInt = new TimerPool<int64>() with get

    static member GetTimerKey() =
        Interlocked.Increment(PoolTimer.TimerPoolInt.Count)

    static member AddTimer(cb : unit->int64, timeMs : int64) =
        let key = Interlocked.Increment(PoolTimer.TimerPoolInt.Count)
        PoolTimer.TimerPoolInt.AddTimer(key, cb, timeMs)

    static member AddTimer(cb : unit->unit, timeMs : int64, periodMs : int64) =
        let key = Interlocked.Increment(PoolTimer.TimerPoolInt.Count)
        PoolTimer.TimerPoolInt.AddTimer(key, cb, timeMs, periodMs)

    static member AddTimer(cb : unit->unit, timeMs : int64) =
        let key = Interlocked.Increment(PoolTimer.TimerPoolInt.Count)
        PoolTimer.TimerPoolInt.AddTimer(key, cb, timeMs)

// ============================================

/// <summary>
/// UnitAction represent one function to be checked during the wake up of ThreadPool wait. 
/// For performance reason, it is important to only put light function in this loop. Any heavy operation should be forked to be executed on another thread/task. 
/// </summary>
type UnitAction = System.Action

/// <summary>
/// UnitAction represent a set of function to be checked during the wake up of ThreadPool wait. 
/// For performance reason, it is important to only put light function in this loop. Any heavy operation should be forked to be executed on another thread. 
/// </summary>
type internal ThreadPoolWaitCheck() = 
    static member val Collection = ConcurrentQueue<UnitAction>() with get
                
                        
type internal ThreadStartParam = 
    struct
        val Th : Thread
        val Name : string
        val DoFunc : Action<unit>
        val CancelFunc: unit -> unit
        val ThreadAffinity: IntPtr
        new ( th, name, doFunc, cancelFunc, threadAffinity ) = { Th = th; Name = name; DoFunc = doFunc; CancelFunc = cancelFunc; ThreadAffinity=threadAffinity }
    end    

/// <summary> 
/// Tracking Execution Threads, this is the preferred way to start thread, as it will make sure that threads get terminated when 
/// unexpected things happen (e.g., Daemon dies)
/// </summary>
type internal ThreadTracking private () as this = 
    do 
        CleanUp.Current.Register( 3000, this, ThreadTracking.CloseAllActiveThreads, fun _ -> "ThreadTracking" ) |> ignore 
    static member val internal Current = new ThreadTracking() with get
    /// Timer to Wait for all threads to termiante
    static member val internal ThreadJoinTimeOut = 10000 with get, set
    static member val TrackingThreads = ConcurrentQueue<_>() with get
    /// <summary> 
    /// This is the preferred way to start a thread. 
    /// apartmentState: ApartmentState
    /// threadAffinity: IntPtr(-1) if on any processor, otherwise, assigned to a particular thread
    /// cancelFunc: unit-> unit, this callback func is called if external process request the thread to terminate
    /// nameFunc: unit-> string, give information on what thread is running
    /// action: the main body of the thread. 
    /// </summary>
    static member StartThreadForActionWithCancelationAndApartment (apartmentState:ApartmentState) (threadAffinity:IntPtr) (cancelFunc:unit->unit) (nameFunc:unit -> string) (action:Action<unit>) = 
        if (!ThreadTracking.nCloseAllCalled)=0 then 
            let threadStart = new Threading.ParameterizedThreadStart( ThreadTracking.ExecuteAction )
            let thread = new Threading.Thread( threadStart )
            thread.SetApartmentState( apartmentState )
            thread.IsBackground <- true
            // Storing name, instead of function as some parameter of the nameFunc() may not be available at the end of the thread. 
            let param = ThreadStartParam( thread, nameFunc(), action, cancelFunc, threadAffinity  )
            thread.Start( param )
            ThreadTracking.TrackingThreads.Enqueue( (thread, nameFunc(), cancelFunc, threadAffinity) )  
            thread
        else
            let msg = sprintf "ThreadTracking, launching a thread when CloseAllActiveThreads have been called ."
            Logger.Log( LogLevel.Error, msg )
            failwith msg
    /// <summary> 
    /// This is the preferred way to start a thread. 
    /// threadAffinity: IntPtr(-1) if on any processor, otherwise, assigned to a particular thread
    /// cancelFunc: unit-> unit, this callback func is called if external process request the thread to terminate
    /// nameFunc: unit-> string, give information on what thread is running
    /// action: the main body of the thread. 
    /// </summary>
    static member StartThreadForActionWithCancelation = 
        ThreadTracking.StartThreadForActionWithCancelationAndApartment ApartmentState.Unknown
    /// <summary> 
    /// nameFunc: unit-> string, give information on what thread is running
    /// action: the main body of the thread. 
    /// </summary>
    static member StartThreadForAction = 
        let cancelFunc() = 
            ()
        ThreadTracking.StartThreadForActionWithCancelation (IntPtr(-1)) cancelFunc
    /// <summary> 
    /// This is the preferred way to start a thread. 
    /// cancelFunc: unit-> unit, this callback func is called if external process request the thread to terminate
    /// nameFunc: unit-> string, give information on what thread is running
    /// func: unit->unit the main body of the thread. 
    /// </summary>
    static member StartThreadForFunctionWithCancelation (cancelFunc:unit->unit) nameFunc (func: unit->unit ) = 
        let action = Action<unit>( func )
        ThreadTracking.StartThreadForActionWithCancelation (IntPtr(-1)) cancelFunc nameFunc action
    /// <summary> 
    /// This is the preferred way to start a thread. 
    /// nameFunc: unit-> string, give information on what thread is running
    /// func: unit->unit the main body of the thread. 
    /// </summary>
    static member StartThreadForFunction = 
        let cancelFunc() = 
            ()
        ThreadTracking.StartThreadForFunctionWithCancelation cancelFunc
    /// <summary> 
    /// This is the preferred way to start a STA thread (usually for Windows UI)
    /// nameFunc: unit-> string, give information on what thread is running
    /// func: unit->unit the main body of the thread. 
    /// </summary>
    static member StartThreadForFunctionSTA nameFunc ( func: unit-> unit) = 
        let cancelFunc() = 
            ()
        let action = Action<unit>( func )
        ThreadTracking.StartThreadForActionWithCancelationAndApartment (ApartmentState.STA) (IntPtr(-1)) cancelFunc nameFunc action
    /// TraceLevel to Monitor the life of threads. If you would like to monitor thread life cycle in Prajna, please set the Property to a lower trace level, e.g., LogLevel.Info. 
    static member val ThreadLifeMonitorTraceLevel = LogLevel.MildVerbose with get, set
    static member val NumThreadsAlive = ref 0 with get
    static member private ExecuteAction o = 
        match o with 
        | :? ThreadStartParam as x -> 
            let name = ref null 
            try 
                Logger.LogF(ThreadTracking.ThreadLifeMonitorTraceLevel, ( fun _ -> name := x.Name
                                                                                   let cnt = Interlocked.Increment( ThreadTracking.NumThreadsAlive)
                                                                                   sprintf "ThreadTracking, %s started (%d) .... " !name cnt  ))
                let action = x.DoFunc
                action.Invoke()
                Logger.LogF(ThreadTracking.ThreadLifeMonitorTraceLevel, ( fun _ -> let cnt = Interlocked.Decrement( ThreadTracking.NumThreadsAlive)
                                                                                   sprintf "ThreadTracking, %s terminated (%d) .... " !name cnt ))
            with 
            | e -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "thread %s has an exception %A .... " !name e ))
        | _ -> 
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "ThreadTracking.ExecuteAction is called with parameter other than ThreadStartParam .... " ))

    static member private CheckActiveThreadsInternal() = 
        let nThreads = ThreadTracking.TrackingThreads.Count
        let tuple = ref Unchecked.defaultof<_>
        for i = 0 to nThreads - 1 do 
            let bDequeue = ThreadTracking.TrackingThreads.TryDequeue( tuple )
            if bDequeue then 
                let thread, name, cancelFunc, threadAffinity = !tuple
                if thread.ThreadState &&& ThreadState.Stopped = enum<Threading.ThreadState>(0) then 
                    // Thread is still alive. 
                    ThreadTracking.TrackingThreads.Enqueue( !tuple )
    static member val private LastCheck = (PerfADateTime.UtcNow()) with get, set
    static member val private nCloseAllCalled = ref 0 with get, set
    static member CheckActiveThreads () = 
        let cur = (PerfADateTime.UtcNow())
        if cur.Subtract( ThreadTracking.LastCheck ).TotalSeconds > 1. then 
            ThreadTracking.LastCheck <- cur
            ThreadTracking.CheckActiveThreadsInternal() 
    static member private TryCloseAllActiveThreads() = 
        if Interlocked.CompareExchange( ThreadTracking.nCloseAllCalled, 1, 0 ) = 0 then 
            ThreadTracking.CheckActiveThreadsInternal() 
            for tuple in ThreadTracking.TrackingThreads do 
                let thread, name, cancelFunc, threadAffinity = tuple
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ThreadTracking, calling the cancelation function on thread %s, ThreadState is %A .... " name thread.ThreadState ))
                cancelFunc()
    /// <summary>
    /// Shutdown all active threads tracked by ThreadTracking by calling their cancellation functions. 
    /// </summary>
    /// <param name="millisecondTimeout"> Timeout value (in milliseconds) for the cancelled thread to join. </param>
    static member CloseAllActiveThreads () = 
        let millisecondTimeout = ThreadTracking.ThreadJoinTimeOut
        let startTime = (PerfADateTime.UtcNow())
        ThreadTracking.TryCloseAllActiveThreads()
        let refTuple = ref Unchecked.defaultof<_>
        while ThreadTracking.TrackingThreads.TryDequeue( refTuple ) do
            let thread, name, cancelFunc, threadAffinity = !refTuple
            let curTime = (PerfADateTime.UtcNow())
            let elapseMs = curTime.Subtract( startTime ).TotalMilliseconds
            let waitMs = Math.Max( 0, millisecondTimeout - int elapseMs )
            let bTerminate = thread.Join( waitMs ) 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ThreadTracking, waiting for thread %s to join is %A" name bTerminate ))
        ThreadTracking.nCloseAllCalled := 0
    /// Standard form for all class that use CleanUp service
    override x.Finalize() =
        /// Close All Active Connection, to be called when the program gets shutdown.
        CleanUp.Current.CleanUpAll()
    /// Standard form for all class that use CleanUp service
    interface IDisposable with
        /// Close All Active Connection, to be called when the program gets shutdown.
        member x.Dispose() = 
            CleanUp.Current.CleanUpAll()
            GC.SuppressFinalize(x)

/// <summary> 
/// One common thread that once start, will continue to find items to work until jobs run out. 
/// This is like Task(), except we are implementing using our own Customized Thread Pool which we can control threadAffinity (don't have that in Task). 
/// </summary>
type internal CommonThread(evWakeUp:ManualResetEvent, threadAffinity:IntPtr, taskQueue: ConcurrentQueue<Action<unit>>) = 
    member val bCancellationCalled = false with get, set
    member x.ThreadStart() = 
        while not x.bCancellationCalled do 
            // I am working on items, if something just added, the thread can work on it. 
            evWakeUp.Reset() |> ignore
            let refWork = ref Unchecked.defaultof<_>
            while not x.bCancellationCalled && taskQueue.TryDequeue( refWork ) do 
                // Some work 
                let work = !refWork
                work.Invoke()
            if not x.bCancellationCalled then 
                evWakeUp.WaitOne() |> ignore 
    member x.Cancel() = 
        x.bCancellationCalled <- true
        evWakeUp.Set() |>ignore 

/// <summary>
/// Customized thread pool.  
/// </summary>
type internal ThreadPoolWithAffinityMask() =
    static member val Current = new ThreadPoolWithAffinityMask() with get
    /// # of thread launched per affinity
    static member val NumThreadsPerAffinity = 1 with get
    member val CommonThreadPool = ConcurrentDictionary<_,_>() with get
    member val ActionItems = ConcurrentDictionary<_,ConcurrentQueue<Action<unit>>>() with get
    member val EvWakeUp = ConcurrentDictionary<_,ManualResetEvent>() with get
    member x.ExecuteActionOnce (threadAffinity) action =
        let refValueAddByThis = ref Unchecked.defaultof<_>
        let addFunc (threadAffinity:IntPtr) = 
            refValueAddByThis := ConcurrentQueue<Action<unit>>()
            !refValueAddByThis
        let evWakeUp = x.EvWakeUp.GetOrAdd( threadAffinity, fun _ -> new ManualResetEvent( false ) )
        let taskQueue = x.ActionItems.GetOrAdd( threadAffinity, addFunc )
        if Object.ReferenceEquals( taskQueue, !refValueAddByThis ) then 
            /// First time an affinity mask is seen, we will need to create a thread
            for i = 0 to ThreadPoolWithAffinityMask.NumThreadsPerAffinity do 
                let commonThread = CommonThread( evWakeUp, threadAffinity, taskQueue )
                ThreadTracking.StartThreadForActionWithCancelationAndApartment 
                    (ApartmentState.MTA) threadAffinity (commonThread.Cancel) 
                    ( fun _ -> sprintf "Thread pool %d for affinity mask %d" i (threadAffinity.ToInt64() ) )
                    (Action<_>( commonThread.ThreadStart )) |> ignore
        taskQueue.Enqueue( action ) 
        evWakeUp.Set() |> ignore
    static member ExecuteShortActionOnce (threadAffinity) action = 
        ThreadPoolWithAffinityMask.Current.ExecuteActionOnce threadAffinity action
    static member ExecuteShortFunctionOnce (threadAffinity) func = 
        ThreadPoolWithAffinityMask.Current.ExecuteActionOnce threadAffinity (Action<_>(func))


/// <summary>
/// Customized thread pool.  
/// </summary>
type internal ThreadPoolCustomized() =
    static member ExecuteShortActionOnce action = 
        ThreadPoolWithAffinityMask.Current.ExecuteActionOnce (IntPtr(-1)) action
    static member ExecuteShortFunctionOnce func = 
        ThreadPoolWithAffinityMask.Current.ExecuteActionOnce (IntPtr(-1)) (Action<_>(func))
    

type internal ExecutionTasks = 
    static member internal ExecuteAsyncTask( job, cts, mode ) = 
        match mode with 
        | ExecutionMode.ByTask -> 
            Async.StartAsTask( job, cancellationToken=cts )
        | ExecutionMode.ByLongRunningTask -> 
            Async.StartAsTask( job, taskCreationOptions=Tasks.TaskCreationOptions.LongRunning, cancellationToken=cts )
        | _ -> 
            let msg = sprintf "ExecuteAsyncTask:Unexpected %A for exeuction mode" mode
            failwith msg
    static member StartThreadForAction nameFunc (action:Action<unit>) = 
        ThreadTracking.StartThreadForAction nameFunc action
    static member StartThread nameFunc f = 
        ExecutionTasks.StartThreadForAction nameFunc (Action<unit>( f ))


        
/// <summary> 
/// Common portion of the customized threadpool, wait for handles. 
/// One wait thread will be spinned every 64 handles
/// </summary>
type internal ThreadPoolWait internal (id:int) as this = 
    do 
        CleanUp.Current.Register( 2000, this, ThreadPoolWait.TerminateAll, fun _ -> "ThreadPoolWait" ) |> ignore 
    // Each thread can wait at most 64 handles, 
    static member val private MAX_WAITHANDLES = 64 with get, set
    /// bTerminate: stop all waiting threads. 
    static member val private nTerminate = ref 0 with get, set
    /// Wait for clean up to be done before exist 
    static member val private evTerminate = new ManualResetEvent(false) with get
    static member val private ActiveThreadPools = ConcurrentDictionary<_,_>() with get
    static member RegisterThreadPool (name:string) = 
        Logger.LogF( LogLevel.MediumVerbose, ( fun _ -> sprintf "ThreadPoolWait, register wait pool %s ............" name ))
        while (!ThreadPoolWait.nTerminate)<>0 do
            // Old Waiting Threads are being terminated, need to wait for that procedure to end. 
            let spin = SpinWait()
            spin.SpinOnce()             
        ThreadPoolWait.ActiveThreadPools.Item( name ) <- (PerfADateTime.UtcNow())
    static member UnregisterThreadPool (name:string) = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ThreadPoolWait, unregister wait pool %s ............" name ))
        ThreadPoolWait.ActiveThreadPools.TryRemove( name ) |> ignore
        if ThreadPoolWait.ActiveThreadPools.IsEmpty then 
            if ( !(ThreadPoolWait.NumberOfWaitingThreads) > 0 ) then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ThreadPoolWait, threadpool %s is the last registered threadpools, try terminating all" name ))
            // Termniate all Wait thread. 
            ThreadPoolWait.TryTerminateAll()
    static member val WaitingThreads = ConcurrentQueue<ThreadPoolWait*Thread>() with get, set
    static member val NumberOfWaitingThreads = ref 0 with get, set
    static member private ClearAll() = 
        // Clear Up Structure. 
        let refValue = ref Unchecked.defaultof<_>
        while ThreadPoolWait.WaitingThreads.TryDequeue( refValue ) do 
            ()
        ThreadPoolWait.nTerminate := 0
    /// <summary>
    /// ThreadPoolWait.WaitForHandle schedule a continuation function to be executed when handle fires. 
    /// </summary>
    /// <param name="infoFunc"> Information delegate of the handle/continution to be waited for, used in diagnostics </param>
    /// <param name="handle"> The handle to be waited on. </param>
    /// <param name="continuation"> Continuation function to be executed after handle fires. Important information: there should not be any blocking operation 
    /// in the continuation function, as it will block the other handle to execute. If Prajna observes a long executing continuation function, a warning will be flagged. 
    /// </param> 
    /// <param name="unblockHandle"> handle to set if continuation function fired. </param>
    static member WaitForHandle (infoFunc:unit->string) (handle:WaitHandle) continuation (unblockHandle:ManualResetEvent) = 
        if (!ThreadPoolWait.nTerminate)<>0 then 
            let msg = sprintf "ThreadPoolWait.WaitForHandle should not be called when the corresponding Threadpool has been unregistered!" 
            Logger.Log( LogLevel.Error, msg )
            failwith msg 
        else
            let mutable bDone = false
            while not bDone do 
                for tuple in ThreadPoolWait.WaitingThreads do 
                    let pool, _ = tuple
                    if not bDone && not pool.IsFull && pool.EnqueueWaitHandle infoFunc handle continuation unblockHandle then 
                        bDone <- true
                if not bDone then 
                    // Add a new ThreadPoolWait object, we don't use Interlocked.Increment to avoid creating multiple waiting threads. 
                    lock ( ThreadPoolWait.WaitingThreads ) ( fun _ -> 
                            // For the thread that enters first, this gets executed. 
                            // Starting the waiting thread. 
                            let threadid = Interlocked.Increment( ThreadPoolWait.NumberOfWaitingThreads ) - 1
                            let curPool = new ThreadPoolWait(threadid)
                            let thread = ThreadTracking.StartThreadForFunction ( fun _ -> sprintf "ThreadPoolWait waiting thread %d" threadid) (curPool.Wait)
                            ThreadPoolWait.WaitingThreads.Enqueue( (curPool, thread) )  
                            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ThreadPoolWait, launching waiting thread %d by %s" threadid (infoFunc()) ))
                            if threadid > 2 then 
                                ThreadPoolWait.MonitorAllWaitHandles()   
                        )
        ()

    // First of the waiting hanle is a waiting handle used for informing the arriving of new handles. 
    static member private InitWaitingHandles() = 
        let arr = Array.zeroCreate ThreadPoolWait.MAX_WAITHANDLES
        arr.[0] <- new ManualResetEvent(false) :> WaitHandle
        arr 
    // Use List<_> to hold waiting threads, we don't want to create too many Waiting Threads. 
    member val ThreadID = id with get
    member val WaitingHandles = ThreadPoolWait.InitWaitingHandles() with get
    member val Continuations = Array.zeroCreate ThreadPoolWait.MAX_WAITHANDLES with get
    member val UnblockHandles = Array.zeroCreate ThreadPoolWait.MAX_WAITHANDLES with get
    member val InfoFunc = Array.zeroCreate ThreadPoolWait.MAX_WAITHANDLES with get
    member val LastWaitingHandles = ref 0 with get, set
    /// Information to detect deadlock in cont() 
    /// i.e., a blocking operation in cont()
    member val ContinueTicks = -1L with get, set
    member val ContinueInfo = Unchecked.defaultof<_> with get, set
    member val private Lock = SpinLockSlim(true) with get, set
    member x.IsFull with get() = (!x.LastWaitingHandles) >= ThreadPoolWait.MAX_WAITHANDLES - 1
    // Multiple thread on queueing. 
    member x.EnqueueWaitHandle (infoFunc:unit->string) handle continuation unblockHandle =
        if not (handle.WaitOne(0)) then 
            x.Lock.Enter()
            let idx = Interlocked.Increment( x.LastWaitingHandles ) 
            if idx >= ThreadPoolWait.MAX_WAITHANDLES then 
                Interlocked.Decrement( x.LastWaitingHandles ) |> ignore 
                // We have exhaussted the waiting slot
                x.Lock.Exit()
                false
            else
                // Use slot idx - 1 
                if idx >= 1 then 
                    if Utils.IsNotNull x.WaitingHandles.[idx] then 
                        Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "try to store %s to pos %d, threadID %d, but the position is not NULL." (infoFunc()) idx x.ThreadID )    )
                    x.WaitingHandles.[idx] <- handle
                    x.Continuations.[idx] <- continuation
                    x.UnblockHandles.[idx] <- unblockHandle
                    x.InfoFunc.[idx] <- infoFunc
                    let handle0 = x.WaitingHandles.[0] :?> ManualResetEvent // Handle 0 is always a manual reset event
                    handle0.Set() |> ignore
                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "add wait handle %s to pos %d, threadID %d " (infoFunc()) idx x.ThreadID ))
                else
                    let msg = sprintf "ThreadPoolWait.EnqueueWaitHandle, should never store the handle in slot 0"
                    Logger.Log( LogLevel.Error, msg )
                    failwith msg
                x.Lock.Exit()
                true
        else
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "wait handle %s already fired, don't put in the queue" (infoFunc()) ))
            continuation() 
            if Utils.IsNotNull unblockHandle then 
                // If there is an unblock handle, set it. 
                unblockHandle.Set() |> ignore     
            true
    /// Try to monitor all wait handles in the current ThreadPoolWait
    static member MonitorAllWaitHandles() = 
        Logger.Do( LogLevel.MildVerbose, ( fun _ -> 
           let ntotal = ref 0 
           let dic = Dictionary<string, int>(StringComparer.Ordinal)
           for tuple in ThreadPoolWait.WaitingThreads do
               let pool, _ = tuple
               let nHandles = !pool.LastWaitingHandles
               for i = 0 to nHandles - 1 do 
                   let infoFunc = pool.InfoFunc.[i]
                   if Utils.IsNotNull infoFunc then 
                       ntotal := !ntotal + 1
                       let key = infoFunc()
                       if dic.ContainsKey( key ) then 
                           dic.Item( key ) <- dic.Item( key ) + 1
                       else
                           dic.Item( key ) <- 1
           let contentInfo = dic |> Seq.map ( fun kv -> sprintf "%s(%d)" kv.Key kv.Value ) |> String.concat ","
           Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Snapshot of Wait Handle Collection (total=%d) : %s " !ntotal contentInfo ))
           ()
                       
               
       ))

    static member TryRemove handle =
        if (!ThreadPoolWait.nTerminate)<>0 then 
            let msg = sprintf "ThreadPoolWait.TryRemove should not be called when the corresponding Threadpool has been unregistered!" 
            Logger.Log( LogLevel.Error, msg )
            failwith msg 
        else
            let mutable bRemoved = false
            for tuple in ThreadPoolWait.WaitingThreads do
                let pool, _ = tuple
                bRemoved <- bRemoved || pool.TryRemove( handle )
            bRemoved
    /// Try remove an event
    member x.TryRemove( handle:WaitHandle ) = 
        x.Lock.Enter()
        let mutable bRemoved = false
        let mutable idx = Math.Min(!x.LastWaitingHandles, ThreadPoolWait.MAX_WAITHANDLES - 1)
        while idx >= 1 do 
            if Object.ReferenceEquals( handle, x.WaitingHandles.[idx] ) then 
                let info = x.InfoFunc.[idx]
                let mutable bSwapped = false
                while not bSwapped do
                    let nLastIdx = Interlocked.Decrement( x.LastWaitingHandles ) + 1
                    if nLastIdx > idx then 
                        x.WaitingHandles.[idx] <- x.WaitingHandles.[nLastIdx]
                        x.Continuations.[idx] <- x.Continuations.[nLastIdx]
                        x.UnblockHandles.[idx] <- x.UnblockHandles.[nLastIdx]
                        x.InfoFunc.[idx] <- x.InfoFunc.[nLastIdx]
                        // Did we get the last Index? 
                        if nLastIdx = (!x.LastWaitingHandles) + 1 then 
                            // We swapped the right one, x.LastWaitingHandle hasn't been changed (only one thread on wait())
                            bSwapped <- true
                        else
                            // x.LastWaitingHandle has been incremented, we will need to redo the swap
                            ()
                    x.Continuations.[nLastIdx] <- Unchecked.defaultof<_>
                    x.UnblockHandles.[nLastIdx] <- null
                    x.InfoFunc.[nLastIdx] <- Unchecked.defaultof<_>
                    x.WaitingHandles.[nLastIdx] <- null
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "wait handle %s has been forcefully removed without fired from thread %d" (info()) x.ThreadID ))
                bRemoved <- true
            idx <- idx - 1 
            if bRemoved then 
                let handle = x.WaitingHandles.[0] :?> ManualResetEvent
                handle.Set() |> ignore
        x.Lock.Exit()
        bRemoved
    /// Only One thread on wait
    member x.Wait() = 
        while (!ThreadPoolWait.nTerminate)=0 do 
                for func in ThreadPoolWaitCheck.Collection do 
                    func.Invoke()
                // Examine backwards. 
                let mutable bAnyFiring = true
                while bAnyFiring do 
                    bAnyFiring <- false
                    let mutable idx = Math.Min(!x.LastWaitingHandles, ThreadPoolWait.MAX_WAITHANDLES - 1)
                    while idx >= 1 do 
                        let handle = x.WaitingHandles.[idx]
                        if Utils.IsNotNull  handle then 
                            // The handle may have been removed. 
                            let bStatus = handle.WaitOne(0) // Get the status of the wait handle 
                            if bStatus then 
                                // fired, call continuation function, and remove the handle. 
                                let lockTaken = ref false
                                x.Lock.Enter()
                                let cont = x.Continuations.[idx]
                                let info = x.InfoFunc.[idx]
                                let unblockHandle = x.UnblockHandles.[idx]
                                let mutable bSwapped = false
                                let mutable nLastIdx = -1 
                                while not bSwapped do
                                    nLastIdx <- Interlocked.Decrement( x.LastWaitingHandles ) + 1
                                    if nLastIdx > idx then    
                                        if Utils.IsNull x.WaitingHandles.[nLastIdx] then 
                                            let curIdx = idx
                                            let showIdx = nLastIdx
                                            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Handle %A(pos:%d) fired, is replaced with handle %A(pos:%d), but the target handle is NULL, Queue length:%d for thread %d" handle curIdx (x.WaitingHandles.[curIdx]) showIdx (!x.LastWaitingHandles) x.ThreadID ))
                                        x.Continuations.[idx] <- x.Continuations.[nLastIdx]
                                        x.UnblockHandles.[idx] <- x.UnblockHandles.[nLastIdx]
                                        x.InfoFunc.[idx] <- x.InfoFunc.[nLastIdx]
                                        x.WaitingHandles.[idx] <- x.WaitingHandles.[nLastIdx]
                                    // Did we get the last Index? 
                                    if nLastIdx = (!x.LastWaitingHandles) + 1 then 
                                        // We swapped the right one, x.LastWaitingHandle hasn't been changed (only one thread on wait())
                                        bSwapped <- true
                                    else
                                        // x.LastWaitingHandle has been incremented, we will need to redo the swap
                                        ()
                                    x.Continuations.[nLastIdx] <- Unchecked.defaultof<_>
                                    x.UnblockHandles.[nLastIdx] <- null
                                    x.InfoFunc.[nLastIdx] <- Unchecked.defaultof<_>
                                    x.WaitingHandles.[nLastIdx] <- null
                                x.Lock.Exit()
                                if nLastIdx > idx then 
                                    let curIdx = idx
                                    let showIdx = nLastIdx
                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Handle %A(pos:%d) fired, is replaced with handle %A(pos:%d) Queue length:%d for thread %d" handle curIdx (x.WaitingHandles.[curIdx]) showIdx (!x.LastWaitingHandles) x.ThreadID ))
                                else
                                    let curIdx = idx
                                    Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Handle %A(pos:%d) fired and removed, queue length :%d for thread %d " handle curIdx (!x.LastWaitingHandles) x.ThreadID ))
                                // Execute continuation function. 
                                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "wait handle %s fired %d " (info()) x.ThreadID ))
                                x.ContinueTicks <- (PerfADateTime.UtcNowTicks())
                                x.ContinueInfo <- info
                                cont() 
                                x.ContinueTicks <- -1L
                                bAnyFiring <- true
                                if Utils.IsNotNull unblockHandle then 
                                    // If there is an unblock handle, set it. 
                                    unblockHandle.Set() |> ignore     
                        idx <- idx - 1
                // Wait for handles. 
                let handle0 = x.WaitingHandles.[0] :?> ManualResetEvent
                handle0.Reset() |> ignore 
                if (!ThreadPoolWait.nTerminate)=0 then 
                    let arr =   x.Lock.Enter()
                                let retArr = Array.sub x.WaitingHandles 0 (!x.LastWaitingHandles + 1) |> Array.filter ( fun handle -> Utils.IsNotNull handle )
                                x.Lock.Exit() 
                                retArr
                    WaitHandle.WaitAny( arr ) |> ignore
        if Interlocked.Decrement( ThreadPoolWait.NumberOfWaitingThreads ) = 0 then 
            // Last thread clear up the structure. 
            ThreadPoolWait.ClearAll()   
    static member TryTerminateAll() = 
        if Interlocked.CompareExchange( ThreadPoolWait.nTerminate, 1, 0 )=0 then 
            let arr = ThreadPoolWait.WaitingThreads.ToArray()
            if arr.Length > 0 then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ThreadPoolWait, try to terminate %d waiting threads" arr.Length ))
                for i = 0 to arr.Length - 1 do 
                    let pool, th = arr.[i]
                    let handle0 = pool.WaitingHandles.[0] :?> ManualResetEvent
                    // Unblocked
                    handle0.Set() |> ignore 
            else // arr.Length = 0 
                ThreadPoolWait.nTerminate := 0
            ThreadPoolWait.evTerminate.Set() |> ignore 
        ThreadPoolWait.evTerminate.WaitOne() |> ignore 
    /// Terminate all waiting tasks . 
    static member TerminateAll() = 
        // Wait for the thread to end. 
        ThreadPoolWait.TryTerminateAll()
// Is done through thread tracking
//        let arr = ThreadPoolWait.WaitingThreads.ToArray()
//        for i = 0 to arr.Length - 1 do 
//            let pool, th = arr.[i]
//            th.Join()    
    override x.Finalize() =
        /// Close All Active Connection, to be called when the program gets shutdown.
        CleanUp.Current.CleanUpAll()
    interface IDisposable with
        /// Close All Active Connection, to be called when the program gets shutdown.
        member x.Dispose() = 
            CleanUp.Current.CleanUpAll()
            GC.SuppressFinalize(x)
            

            

    
// We can't internalize the class as it is used as parameter in an abstract function, which is default to the public. 
// Wait Handle don't participate generic CleanUp, as it can be clean up per project. 
/// A collection of WaitHandles, each of which holds a continuation function to be executed if the handle fires. 
[<AllowNullLiteral>]
type internal WaitHandleCollection(collectionName:string, initialCapacity:int)  = 
    do 
        ThreadPoolWait.RegisterThreadPool( collectionName )    
    member val Collecton = ConcurrentDictionary<_,_>() with get
    member val PreDeposit = System.Collections.Generic.List<_>(initialCapacity) with get, set
    member val AllDone = new ManualResetEvent(false) with get
    member x.Reset() = 
        x.PreDeposit.Clear()
        x.Collecton.Clear()
        x.AllDone.Reset() |> ignore 
    member x.EnqueueWaitHandle infoFunc (handle:WaitHandle) continuation (unblockHandle) =       
        x.PreDeposit.Add( (infoFunc, handle, continuation, unblockHandle) )
    member x.Deposit() = 
        x.ExamineBeforeDeposit()
        if x.PreDeposit.Count > 0 then 
            x.AllDone.Reset() |> ignore
            for tuple in x.PreDeposit do 
                let infoFunc, handle, continuation, unblockHandle = tuple
                x.Collecton.TryAdd( (handle, continuation), (infoFunc, ref (PerfADateTime.UtcNow())) ) |> ignore
                let wrappedContinuation() = 
                    continuation()
                    // unblock Handle is automatically set in threadpool wait
                    x.Collecton.TryRemove( (handle, continuation) ) |> ignore
                    if x.Collecton.IsEmpty then 
                        x.AllDone.Set() |> ignore
                ThreadPoolWait.WaitForHandle infoFunc handle wrappedContinuation unblockHandle
            x.PreDeposit.Clear()
            false
        else
            x.AllDone.Set() |> ignore
            true
    member x.ClearWaitHandles() = 
        if not (x.Collecton.IsEmpty) then 
            for pair in x.Collecton do 
                let handle, _ = pair.Key
                let bRemoved = ThreadPoolWait.TryRemove handle
                ()
            let bEmptyCollection = x.Collecton.IsEmpty
            x.Collecton.Clear()
            bEmptyCollection
        else
            true
    member x.ExamineBeforeDeposit() = 
        // Before deposit into Thread pool, check if any of the condition is met. 
        let mutable bExamineDone = false
        while not bExamineDone do 
            let lstCount = x.PreDeposit.Count
            let newLst = List<_>(lstCount)
            for tuple in x.PreDeposit do 
                let infoFunc, handle, continuation, unblockHandle = tuple
                if handle.WaitOne(0) then 
                    // condition met 
                    continuation() 
                    if Utils.IsNotNull unblockHandle then 
                        unblockHandle.Set() |> ignore
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "WaitHandle %s fired before waiting" (infoFunc()) ))
                else
                    newLst.Add( tuple ) 
            x.PreDeposit <- newLst 
            bExamineDone <- x.PreDeposit.Count = lstCount
    /// Return: true, all waithandles have fired.
    ///         false, some waithandles have not fired. 
    member x.WaitAll nMaxWait (nOneWait:int) nMonitor traceLevel = 
        let startTime = (PerfADateTime.UtcNow())
        let mutable bDoneWaiting = x.Deposit()
        while not bDoneWaiting do 
            let curTime = (PerfADateTime.UtcNow())
            let elapse = int (curTime.Subtract( startTime).TotalMilliseconds)
            x.AllDone.Reset() |> ignore
            if x.Collecton.IsEmpty || elapse > nMaxWait then
                bDoneWaiting <- true
            else
                let bAllDone = x.AllDone.WaitOne( nOneWait )
                if not bAllDone then 
                    // Monitor 
                    Logger.Do(traceLevel, ( fun _ -> 
                        let bFirstMonitorLine = ref true
                        for pair in x.Collecton do 
                            let infoFunc, lastMonitor = pair.Value
                            let elapse = int (curTime.Subtract( !lastMonitor ).TotalMilliseconds)
                            if elapse >= nMonitor then 
                                lastMonitor := curTime
                                if ( !bFirstMonitorLine ) then 
                                    bFirstMonitorLine := false
                                    Logger.Log(traceLevel, ("========= still waiting for the following handles ============"))
                                Logger.Log(traceLevel, ( sprintf "Handles %s" (infoFunc()) ))
                        ))
                else
                    bDoneWaiting <- true
        x.ClearWaitHandles()
    member val private nAllClosed = ref 0 with get, set
    /// Forced to close all 
    member x.CloseAll() = 
        if Interlocked.CompareExchange( x.nAllClosed, 1, 0)=0 then 
            for pair in x.Collecton do
                let handle, cont = pair.Key
                let bRemoved = ThreadPoolWait.TryRemove handle 
                if bRemoved then 
                    let infoFunc, lastMonitor = pair.Value
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "!!! Removed !!! remove handle %s .............." (infoFunc()) ))
            // Unblock everything that is waiting 
            // Waiting all threads to unblock and shutdown. 
            ThreadPoolWait.UnregisterThreadPool( collectionName )
    override x.Finalize() =
        x.CloseAll()
    interface IDisposable with
        member x.Dispose() = 
            x.CloseAll()
            GC.SuppressFinalize(x)


        
type internal ThreadPoolStart<'K> = 
    struct
        val UsePool: ThreadPoolWithWaitHandles<'K>
        val ThreadID: int
        new( pool, id ) = { UsePool=pool; ThreadID=id; }
    end

/// <summary> 
/// Managing blocking event in thread pool. </summary>
and internal ThreadPoolWaitHandles() = 
    static member val Current = ThreadPoolWaitHandles() with get
    static member val MinActiveThreads = 1 with get, set
    member val ThreadPoolCollection = ConcurrentDictionary<_,_>() with get
    member x.RegisterThread( id, y: ThreadPoolWithWaitHandlesBase ) = 
        x.ThreadPoolCollection.GetOrAdd( id, (y, ref 0) ) |> ignore
    member x.UnRegisterThread( id ) = 
        x.ThreadPoolCollection.TryRemove( id ) |> ignore
    member x.EnterBlock() = 
        let id = Thread.CurrentThread.ManagedThreadId
        let bExist, tuple = x.ThreadPoolCollection.TryGetValue( id ) 
        if bExist then 
            let y, refcount = tuple
            Interlocked.Increment( refcount ) |> ignore
            if !y.NumThreads - !refcount < Math.Min( ThreadPoolWaitHandles.MinActiveThreads, !y.NumberOfWaitedTasks ) then 
                // Launch one additional thread, if blocked. 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Threadpool %s, %d of %d threads has been blocked, %d tasks waiting, a new thread is being launched"
                                                                           y.ThreadPoolName !refcount !y.NumThreads !y.NumberOfWaitedTasks ))
                y.TryExecuteN( !y.NumThreads + 1 ) 
        ()
    member x.LeaveBlock() = 
        let id = Thread.CurrentThread.ManagedThreadId
        let bExist, tuple = x.ThreadPoolCollection.TryGetValue( id ) 
        if bExist then 
            let _, refcount = tuple
            Interlocked.Decrement( refcount ) |> ignore
    member x.SafeWaitOne( ev: WaitHandle, millisecondsTimeout:int ) = 
        let bStatus = ev.WaitOne(0)
        if not bStatus then 
            x.EnterBlock() 
            let bWaitStatus = ev.WaitOne(millisecondsTimeout) 
            x.LeaveBlock() 
            bWaitStatus 
        else
            bStatus
    static member safeWaitOne ( ev ) = 
        ThreadPoolWaitHandles.Current.SafeWaitOne( ev, Timeout.Infinite )
    static member safeWaitOne ( ev,  millisecondsTimeout ) = 
        ThreadPoolWaitHandles.Current.SafeWaitOne( ev, millisecondsTimeout )


/// <summary> 
/// Managed a customzied thread pool that executes a set of (key, func() -> handle, bTerminated )
/// Key is used to identified the action, so that if the user desired, he/she can print some information of on the action. 
/// If the action is to block, it will return handle, false, so that the thread will wait on the handles. 
/// If the action can be executed again, it will return null, false, so that it will be queued for execution in the next cycle. 
/// If the action is terminated, it will return *, true, and it will be dequeued. </summary>
and [<AllowNullLiteral; AbstractClass>]
    internal ThreadPoolWithWaitHandlesBase() = 
    let mutable threadpoolName = ""
    member x.ThreadPoolName with get() = threadpoolName and set(v) = threadpoolName <- v
    member val NumThreads = ref 0 with get
    member val NumberOfTasks = ref 0 with get
    // Should reduce at the end of thawing. 
    member val NumberOfWaitedTasks = ref 0 with get
    abstract TryExecuteN: int -> unit
    /// TraceLevel for the life cycle of ThreadPoolWithWaitHandles
    static member val TraceLevelThreadPoolWithWaitHandles = LogLevel.WildVerbose with get, set
/// <summary> 
/// Managed a customzied thread pool of N Threads that executes a set of (key, func() -> handle, bTerminated ).
/// The threads are uniquenly allocated to execute the set of jobs enqueued to the thread pool. 
/// Key is used to identified the action, so that if the user desired, he/she can print some information of on the action. 
/// If the action is to block, it will return handle, false, so that the thread will wait on the handles. 
/// If the action can be executed again, it will return null, false, so that it will be queued for execution in the next cycle. 
/// If the action is terminated, it will return *, true, and it will be dequeued. </summary>
and [<AllowNullLiteral>]
    internal ThreadPoolWithWaitHandles<'K> private () =
    inherit ThreadPoolWithWaitHandlesBase()
    new (name : string) as x =
        new ThreadPoolWithWaitHandles<'K>()
        then
            x.ThreadPoolName <- name
            ThreadPoolWait.RegisterThreadPool(name)
            PoolTimer.AddTimer(x.ToMonitor, 100L, 100L)
            x.CleanUp <- CleanUp.Current.Register( 1500, x, (x.OperationsToCloseAllThreadPool), (fun _ -> sprintf "ThreadPoolWithWaitHandles for %s" name) )
            ()
            //x.MonitorTimer <- new Timer(x.ToMonitor, null, 5000, 10000)
            //x.MonitorTimer <- ThreadPoolTimer.TimerWait (fun _ -> "ThreadPool Monitor") (fun _ -> x.ToMonitor(null)) (5000) (10000)
    new (name : string, numThreads : int) as x =
        new ThreadPoolWithWaitHandles<'K>(name)
        then
            x.NumParallelExecution <- numThreads
    /// TraceLevel for Task tracking
    static member val TrackTaskTraceLevel = LogLevel.MildVerbose with get, set
    static member val DefaultNumParallelExecution = 4 with get, set
    member val CleanUp = null with get, set
    member val MonitorTimer : Timer = null with get, set
    //member val MonitorTimer : ThreadPoolTimer = null with get, set
    /// Cancel all jobs 
    member val nAllCancelled = ref 0 with get
    /// <summary> if bSyncExecution is true, the task will be executed on the same thread (in sync mode). </summary>
    member val bSyncExecution = false with get, set 
    member val NumParallelExecution = 0 with get, set
    member val CalculateNumParallelExecution = ( fun (numJobs:int) -> Math.Min( ThreadPoolWithWaitHandles<'K>.DefaultNumParallelExecution, numJobs) ) with get, set
    member x.GetNumParallelExecution numJobs =   
        if x.NumParallelExecution > 0 then Math.Min( x.NumParallelExecution, numJobs)  else x.CalculateNumParallelExecution numJobs 
    member val TaskList = ConcurrentDictionary<_,ConcurrentQueue<_>>() with get, set
    member val TaskStatus = ConcurrentDictionary<_,_>() with get, set
    member val HandleDoneExecution = new ManualResetEvent( true ) with get
    member val HandleWaitForMoreJob = new ManualResetEvent( true ) with get
    member val HandleBlockOnJob = null with get, set
    member val CompletedTasks =  ConcurrentDictionary<_,_>() with get, set
    member val TaskTracking = ConcurrentDictionary<_,int>() with get, set
    member val AllAffinityTasks = ConcurrentDictionary<_,_>() with get
    member val AffinityWaitingJobs = ConcurrentDictionary<_,_>() with get
    member x.Reset() = 
        // Clear TaskList & TaskStatus
        if not x.TaskTracking.IsEmpty || not x.CompletedTasks.IsEmpty || not x.TaskList.IsEmpty || not x.TaskStatus.IsEmpty then 
            x.OperationsToCloseAllThreadPool()
            ThreadPoolWait.RegisterThreadPool( x.ThreadPoolName )
        x.nAllCancelled := 0
        x.NumberOfTasks := 0
        x.NumberOfWaitedTasks := 0 
        x.CompletedTasks.Clear() 
        x.TaskTracking.Clear()
        x.AllAffinityTasks.Clear()
        x.AffinityWaitingJobs.Clear()
                
    /// <summary>
    /// Enqueue an action for repeated execution, until the action returns (*, false). The action is uniqueuely identified by a key, which can be used to get information
    /// of the action. 
    /// </summary>
    /// <param name="affinityMask"> Reserved for thread affinity mask (currently not supported by .Net). </param>
    /// <param name="action"> The function to be enqueued.  </param>
    /// <param name="key"> The key that uniquely identified the action.  </param>
    /// <param name="info"> a function that returns information of the action. </param>
    member x.EnqueueRepeatableFunctionWithAffinityMask (affinityMask:IntPtr) (func: unit -> ManualResetEvent * bool) (cts:CancellationToken) (key:'K) infoFunc =
        if (!x.nAllCancelled)<>0 then 
            let msg = sprintf "ThreadPoolWithWaitHandles.EnqueueActionWithAffinityMask, try to enqueue job after CloseAll() called"
            Logger.Log( LogLevel.Error, msg )
            failwith msg
        else
            let bSuccess = x.TaskStatus.TryAdd( key , (infoFunc, ref null, ref false, ref DateTime.MinValue ) )
            if bSuccess then 
                x.AllAffinityTasks.AddOrUpdate( affinityMask, 1, (fun _ v -> v+1) ) |> ignore
                x.TaskList.AddOrUpdate( affinityMask, ThreadPoolWithWaitHandles<'K>.CreateQueue (cts, key, func, affinityMask), 
                    ThreadPoolWithWaitHandles<'K>.AddToQueue (cts, key, func, affinityMask) ) |> ignore
                Interlocked.Increment( x.NumberOfTasks ) |> ignore
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "Enqueue job %s (key:%A) for execution" (infoFunc(key)) key ))
                x.HandleWaitForMoreJob.Set() |> ignore
    static member CreateQueue tuple mask = 
        ConcurrentQueue( Seq.singleton tuple )
    static member AddToQueue tuple mask queue = 
        queue.Enqueue( tuple )
        queue

    /// <summary>
    /// Enqueue a function for repeated execution, until the action returns (*, false). The action is uniqueuely identified by a key, which can be used to get information
    /// of the action. 
    /// </summary>
    /// <param name="action"> The action to be enqueued.  </param>
    /// <param name="key"> The key that uniquely identified the action.  </param>
    /// <param name="info"> a function that returns information of the action. </param>
    member x.EnqueueRepeatableFunction func cts key infoFunc =
        x.EnqueueRepeatableFunctionWithAffinityMask (IntPtr(-1)) func cts key infoFunc

    /// <summary>
    /// Enqueue a function for repeated execution, until the action returns (*, false). The action is uniqueuely identified by a key, which can be used to get information
    /// of the action. 
    /// </summary>
    /// <param name="action"> The action to be enqueued.  </param>
    /// <param name="key"> The key that uniquely identified the action.  </param>
    /// <param name="info"> a function that returns information of the action. </param>
    member x.EnqueueAction (action:Action<_>) (cts:CancellationToken) key infoFunc =
        let wrappedFunc() =
            if not (cts.IsCancellationRequested) then 
                action.Invoke()
            null, true
        x.EnqueueRepeatableFunctionWithAffinityMask (IntPtr(-1)) wrappedFunc cts key infoFunc

    member val Threads = ConcurrentDictionary<_,_>() with get, set
    member val private InLaunching = ref 0
    member val private InLaunchingEvent = new ManualResetEventSlim(true)
    member val CancelThis = new CancellationTokenSource() with get
    /// Try execute the task in the queue, until we reach the limit
    member x.TryExecute() = 
        x.TryExecuteN(0)
    override x.TryExecuteN( targetNumThreads ) = 
        if Volatile.Read(x.InLaunching)=0 then 
            if Interlocked.CompareExchange( x.InLaunching, 1, 0 ) = 0 then 
                x.InLaunchingEvent.Reset()
                let allAffinityMasks = x.AllAffinityTasks.ToArray() |> Array.map ( fun pair -> pair.Key )
                // We use x.NumberOfTasks instead of x.ExecutionQueue.Count as the later is much more complicated. 
                let orgParallels = x.GetNumParallelExecution (!x.NumberOfTasks)
                let numParallels = Math.Max( orgParallels, targetNumThreads )
                let useNumThreads = Math.Min( Math.Max( numParallels, 1), (!x.NumberOfTasks) )
                while (!x.NumThreads) < useNumThreads do
                    // launch new thread to execute the action
                    let numThreads = Interlocked.Increment( x.NumThreads )
                    if numThreads > useNumThreads then 
                        // The thread is launched already, no need to launch another
                        Interlocked.Decrement( x.NumThreads ) |> ignore     
                    else
                        x.HandleDoneExecution.Reset() |> ignore
                        let threadID = numThreads - 1
                        let useAffinityMask = allAffinityMasks.[ threadID % allAffinityMasks.Length ]
                        let cancelFunc() = 
                            x.CancelThis.Cancel()    
                        let nameFunc() = sprintf "ThreadPoolWithWaitHandles:%s, thread %d" x.ThreadPoolName threadID
                        let thread = ThreadTracking.StartThreadForActionWithCancelation useAffinityMask cancelFunc nameFunc (Action<_>(x.ExecuteOneJob x.CancelThis.Token useAffinityMask threadID))
                        ()
                x.InLaunchingEvent.Set()
                Volatile.Write(x.InLaunching, 0)

    /// Repeated execute partition jobs until all jobs have been executed. 
    // How many jobs that this thread can execute?
    member x.ExecuteOneJob ctsThread threadAffinityMask (threadID: int) ()= 
        let threadID = Thread.CurrentThread.ManagedThreadId
        ThreadPoolWaitHandles.Current.RegisterThread( threadID, x )
        let tuple = ref Unchecked.defaultof<_>
        let taskQueue = ref null
        let mutable bDoneAllJobs = not (x.TaskList.TryGetValue( threadAffinityMask, taskQueue ))
        while not bDoneAllJobs && not(ctsThread.IsCancellationRequested) do
            let nTasks = ref 0
            let ret = x.AllAffinityTasks.TryGetValue(threadAffinityMask, nTasks)
            if (!taskQueue).IsEmpty && !nTasks > 0 then
                x.HandleWaitForMoreJob.Reset() |> ignore
                if (!taskQueue).IsEmpty && !nTasks > 0 then
                    Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Waiting for more jobs WaitingJobs %s:%d: NumTasks:%d" x.ThreadPoolName threadID !nTasks))
                    x.HandleWaitForMoreJob.WaitOne() |> ignore
                    Logger.LogF( LogLevel.ExtremeVerbose, (fun _ -> sprintf "Start job execution again %s:%d" x.ThreadPoolName threadID))
                else
                    x.HandleWaitForMoreJob.Set() |> ignore
            if (!nTasks = 0) then
                Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Done all jobs %s:%d" x.ThreadPoolName threadID))
                bDoneAllJobs <- true
                x.HandleWaitForMoreJob.Set() |> ignore
            while (!taskQueue).TryDequeue( tuple ) do
                let cts, key, action, affinityMask = !tuple  
                // There are some jobs with the affinityMask
                let mutable bDoneExecution = false
                try
                    while not bDoneExecution do 
                        bDoneExecution <- true
                        let handle, bTerminate = action()
                        if not bTerminate then 
                            if Utils.IsNull handle then 
                                // Always put the action back on queue. 
                                if (not cts.IsCancellationRequested && not(ctsThread.IsCancellationRequested) && (!x.nAllCancelled)=0) then
                                    (!taskQueue).Enqueue( cts, key, action, affinityMask )
                                else
                                    x.Finished affinityMask cts key action threadID
                                x.HandleWaitForMoreJob.Set() |> ignore
                            else
                                let tuple = ref Unchecked.defaultof<_>
                                if x.TaskStatus.TryGetValue( key, tuple ) then 
                                    let infoFunc, handleHolder, waitStatus, waitTime = !tuple
                                    waitStatus := true
                                    waitTime := (PerfADateTime.UtcNow())
                                    handleHolder := handle
                                    if (not cts.IsCancellationRequested && not(ctsThread.IsCancellationRequested) && (!x.nAllCancelled)=0) then 
                                        if x.bSyncExecution then 
                                            x.HandleBlockOnJob <- handle
                                            // Wait on exeuction, and execute this job again. 
                                            handle.WaitOne() |> ignore
                                            waitStatus := false
                                            bDoneExecution <- false
                                        else
                                            // Put the job on wait queue. 
                                            let wrappedInfo() = 
                                                infoFunc( key )  
                                            Interlocked.Increment( x.NumberOfWaitedTasks ) |> ignore
                                            Logger.Do(ThreadPoolWithWaitHandles<'K>.TrackTaskTraceLevel, ( fun _ -> 
                                                let cnt = x.TaskTracking.AddOrUpdate( key, 1, (fun key v -> v + 1 ) ) 
                                                if cnt <> 1 then 
                                                    Logger.Log( LogLevel.Warning, ( sprintf "ThreadPoolWithWaitHandles.ExecuteOneJob enqueued %d jobs for task %s at thread %d" cnt (infoFunc(key)) threadID ))
                                            ))
                                            x.AffinityWaitingJobs.AddOrUpdate( affinityMask, 1, fun _ v -> v + 1 ) |> ignore
                                            ThreadPoolWait.WaitForHandle wrappedInfo handle (x.Continue cts key action affinityMask) x.HandleWaitForMoreJob
                                            //let rwh = ref Unchecked.defaultof<RegisteredWaitHandle>
                                            //rwh := ThreadPool.RegisterWaitForSingleObject(handle, x.continueDel, (rwh, handle, wrappedInfo, cts, key, action, affinityMask), -1, true)
                        else 
                            x.Finished affinityMask cts key action threadID
                            let nTasksRem = ref 0
                            x.AllAffinityTasks.TryGetValue(threadAffinityMask, nTasksRem) |> ignore
                            let nTasksWait = ref 0
                            x.AffinityWaitingJobs.TryGetValue(threadAffinityMask, nTasksWait) |> ignore
                            Logger.LogF( LogLevel.WildVerbose, (fun _ -> sprintf "Job %A on %s:%d terminates rem: %d waiting: %d" key x.ThreadPoolName threadID !nTasksRem  !nTasksWait))
                            x.HandleWaitForMoreJob.Set() |> ignore
                with 
                | e -> 
                    let tuple = ref Unchecked.defaultof<_>
                    let bEntryExist = x.TaskStatus.TryGetValue( key, tuple )
                    if bEntryExist then 
                        let infoFunc, _, _, _ = !tuple
                        let errMsg = sprintf "!!! Exception !!! ThreadPoolWithWaitHandles.ExecuteOneJob to execute task %s at thread %d with exception %A" (infoFunc(key)) threadID e
                        Logger.Log( LogLevel.Error, errMsg )
                    else
                        let errMsg = sprintf "!!! Exception !!! ThreadPoolWithWaitHandles.ExecuteOneJob failed at thread %d with exception %A" threadID e
                        Logger.Log( LogLevel.Error, errMsg )

        while Volatile.Read(x.InLaunching) <> 0 do 
            x.InLaunchingEvent.Wait()

        let curThreads = Interlocked.Decrement( x.NumThreads )
        ThreadPoolWaitHandles.Current.UnRegisterThread ( threadID )
        // Stuck below, don't understand
        Logger.LogF(ThreadPoolWithWaitHandlesBase.TraceLevelThreadPoolWithWaitHandles, ( fun _ -> sprintf "ThreadPoolWithWaitHandles:%s, terminating thread %d surviving threads %d" x.ThreadPoolName threadID curThreads ))
        if curThreads = 0 then 
            x.HandleDoneExecution.Set() |> ignore
    member x.continueDel (o : obj) (bTimeOut : bool) =
        let (rwh, handle, wrappedInfo, cts, key, action, affinityMask) = o :?> RegisteredWaitHandle ref*ManualResetEvent*(unit->string)*CancellationToken*'K*(unit->ManualResetEvent*bool)*IntPtr
        //Console.WriteLine("Release {0}", wrappedInfo())
        if (not bTimeOut && Utils.IsNotNull !rwh) then
            (!rwh).Unregister(handle) |> ignore
        x.Continue cts key action affinityMask ()
    /// The continuetion 
    member x.Continue cts key action affinityMask () = 
        Logger.Do(ThreadPoolWithWaitHandles<'K>.TrackTaskTraceLevel, ( fun _ -> 
            let cnt = x.TaskTracking.AddOrUpdate( key, -1, (fun key v -> v - 1 ) ) 
            if cnt <> 0 then 
                Logger.Log( LogLevel.Warning, ( sprintf "ThreadPoolWithWaitHandles.ExecuteOneJob at dequeue, had %d jobs for key %A" cnt key ))
            ))
        let tuple = ref Unchecked.defaultof<_>
        if x.TaskStatus.TryGetValue( key, tuple ) then 
            let infoFunc, _, waitStatus, _ = !tuple
            if not ( !waitStatus ) then 
                Logger.Log( LogLevel.Warning, ( sprintf "At dequeue, the waiting status of job %s of key %A is false " (infoFunc(key)) key ))
            waitStatus := false
        if cts.IsCancellationRequested || (!x.nAllCancelled)<>0 then 
            x.Finished affinityMask cts key action -1
            let nJobsAffinity = x.AffinityWaitingJobs.AddOrUpdate( affinityMask, 0, (fun _ v -> v-1) ) 
            Interlocked.Decrement( x.NumberOfWaitedTasks ) |> ignore
        else
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "ThreadPoolWithWaitHandles.Continue, requeing task %A for execution" key ))
            let taskQueue = ref null
            let mutable bFindQueue = x.TaskList.TryGetValue( affinityMask, taskQueue )
            if bFindQueue then 
                (!taskQueue).Enqueue( cts, key, action, affinityMask )
            else
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ThreadPoolWithWaitHandles.Continue, can't find the execution queue for affinity mask %A" affinityMask ))
            let nJobsAffinity = x.AffinityWaitingJobs.AddOrUpdate( affinityMask, 0, (fun _ v -> v-1) ) 
            Interlocked.Decrement( x.NumberOfWaitedTasks ) |> ignore
            x.HandleWaitForMoreJob.Set() |> ignore
    /// Finished the job
    member x.Finished affinityMask cts key action threadID = 
        let nJobs = x.AllAffinityTasks.AddOrUpdate( affinityMask, 0, (fun _ v -> v-1) ) 
        Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "ThreadPoolWithWaitHandles.Finished, %d jobs left in affinity group %A" nJobs affinityMask ))
        let tuple = ref Unchecked.defaultof<_>
        if x.TaskStatus.TryRemove( key, tuple ) then 
            let infoFunc, _, _, _ = !tuple       
            if cts.IsCancellationRequested || (!x.nAllCancelled)<>0 then 
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "ThreadPoolWithWaitHandles.Finished, cancelled task %s" (infoFunc(key)) ))
            else
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "ThreadPoolWithWaitHandles.Finished, done exeucting task %s on thread %d" (infoFunc(key)) threadID))
                x.CompletedTasks.Item( key ) <- (PerfADateTime.UtcNow())
        else
            Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "ThreadPoolWithWaitHandles.Finished, done exeucting task of key %A on thread %d, but can't find entry in TaskStatus" key (threadID) ))
    /// Execute all
    /// True: done execution, False: not complete execution during timeout. 
    member x.WaitForAll( timeOut:int ) = 
        x.TryExecute() 
        Logger.LogF(ThreadPoolWithWaitHandlesBase.TraceLevelThreadPoolWithWaitHandles, (fun _ -> sprintf "Starting wait for handle done execution"))
        let ret = x.HandleDoneExecution.WaitOne( timeOut )
        Logger.LogF(ThreadPoolWithWaitHandlesBase.TraceLevelThreadPoolWithWaitHandles, (fun _ -> sprintf "Done wait for handle done execution"))
        ret
    /// Execute all
    /// True: done execution, False: not complete execution during timeout. 
    member x.WaitForAllNonBlocking() = 
        x.TryExecute() 
        x.HandleDoneExecution
    /// Forced to close all 
    member x.OperationsToCloseAllThreadPool() = 
        if Interlocked.CompareExchange( x.nAllCancelled, 1, 0 )=0 then 
            Logger.LogF(ThreadPoolWithWaitHandlesBase.TraceLevelThreadPoolWithWaitHandles, (fun _ -> sprintf "Attempt to close threadpool %s" x.ThreadPoolName))
            let spin = SpinWait()
            x.CancelThis.Cancel()
            // Waiting all threads to unblock and shutdown. 
            while ( !x.NumThreads > 0 ) do
                for pair in x.TaskStatus do
                    let key = pair.Key
                    let _, handleHolder, waitStatus, _ = pair.Value
                    if !waitStatus then 
                        // Force firing
                        (!handleHolder).Set() |> ignore
                if Utils.IsNotNull  x.HandleBlockOnJob then 
                    // Unblock current job
                    x.HandleBlockOnJob.Set() |> ignore
                // Unblock everything that is waiting 
                x.HandleWaitForMoreJob.Set() |> ignore
                spin.SpinOnce()
            // Clear TaskList & TaskStatus 
            let tuple = ref Unchecked.defaultof<_>
            for pair in x.TaskList do 
                let queue = pair.Value
                while queue.TryDequeue(tuple) do
                    let cts, key, action, affinityMask = !tuple  
                    x.Finished affinityMask cts key action -1 
            x.TaskStatus.Clear()
            ThreadPoolWait.UnregisterThreadPool( x.ThreadPoolName )
            Logger.LogF(ThreadPoolWithWaitHandlesBase.TraceLevelThreadPoolWithWaitHandles, (fun _ -> sprintf "Threadpool %s closed" x.ThreadPoolName))
    member x.CloseAllThreadPool() = 
        if Utils.IsNull x.CleanUp then 
            x.OperationsToCloseAllThreadPool()
        else
            x.CleanUp.CleanUpThisOnly()
    static member val MaxContinuationDurationInMilliSeconds = 50L with get, set
    static member val MonitorDurationInMilliSeconds = 10000L with get, set
    member val DetectDeadLockTicksRef = ref (PerfADateTime.UtcNowTicks()) with get
    member val MonitorTicksRef = ref (PerfADateTime.UtcNowTicks()) with get
    member x.ToMonitor(o : obj) =
        Logger.Do( LogLevel.ExtremeVerbose, ( fun _ -> 
           let cur = (PerfADateTime.UtcNowTicks())
           let old = !x.MonitorTicksRef
           if cur - old > TimeSpan.TicksPerMillisecond * ThreadPoolWithWaitHandles<_>.MonitorDurationInMilliSeconds then 
               if Interlocked.CompareExchange( x.MonitorTicksRef, cur, old) = old then 
                   Logger.LogF( LogLevel.ExtremeVerbose, (fun _ ->
                      let mutable prtStr = System.Text.StringBuilder() 
                      prtStr.Append( sprintf "%s %s ==================\n" (UtcNowToString()) x.ThreadPoolName ) |> ignore
                      for pair in x.TaskList do
                          let affinityMask = pair.Key
                          let taskQ = pair.Value
                          let numTasks = taskQ.Count
                          let nTasksRem = ref 0
                          x.AllAffinityTasks.TryGetValue(affinityMask, nTasksRem) |> ignore
                          let nTasksWait = ref 0
                          x.AffinityWaitingJobs.TryGetValue(affinityMask, nTasksWait) |> ignore
                          prtStr.Append( sprintf "Rem: %d waiting: %d\n" !nTasksRem  !nTasksWait ) |> ignore 
                      for task in x.TaskStatus do
                          let key = task.Key
                          let job = task.Value
                          let (_, ev : ManualResetEvent ref, status, _) = job
                          let fired =
                              if (Utils.IsNotNull !ev) then
                                  (!ev).WaitOne(0)
                              else
                                  false
                          prtStr.Append ( sprintf "%A Fired: %b Status: %b\n" key fired !status ) |> ignore
                      for tuple in ThreadPoolWait.WaitingThreads do
                          let (pool, _) = tuple
                          if Utils.IsNotNull pool then
                              for j = 0 to !pool.LastWaitingHandles do
                                  let infoFunc = pool.InfoFunc.[j]
                                  if Utils.IsNotNull infoFunc then
                                      prtStr.Append( sprintf "Waiting handle: %s\n" (infoFunc()) ) |> ignore
                      prtStr.ToString()
                  ))
           ))
        let cur = (PerfADateTime.UtcNowTicks())
        let old = !x.DetectDeadLockTicksRef
        if cur - old > TimeSpan.TicksPerSecond then 
            if Interlocked.CompareExchange( x.DetectDeadLockTicksRef, cur, old) = old then 
                /// detect any deadlock 
                try 
                    for tuple in ThreadPoolWait.WaitingThreads do
                            let (pool, _) = tuple
                            let continueTicks = pool.ContinueTicks 
                            if continueTicks >=0L then 
                                let cur = (PerfADateTime.UtcNowTicks())
                                if cur - continueTicks > ThreadPoolWithWaitHandles<_>.MaxContinuationDurationInMilliSeconds * TimeSpan.TicksPerMillisecond then 
                                    let elapseMs = ( cur - continueTicks )/ TimeSpan.TicksPerMillisecond
                                    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Possible Deadlocks! cont() of %s has been in executionin ThreadPoolWait for more than %d ms. Possible deadlock or poor performance in implementing cont() (there should not be any blocking operation or long running operation in cont()) " 
                                                                                       (pool.ContinueInfo())
                                                                                       elapseMs ))
                with 
                | e ->
                    // no need to deal with inconsistency here.  
                    ()
    override x.Finalize() =
        x.CloseAllThreadPool()
    interface IDisposable with
        member x.Dispose() = 
            x.CloseAllThreadPool()
            x.InLaunchingEvent.Dispose()
            GC.SuppressFinalize( x ) 



/// <summary> 
/// A collection of thread pool timer. 
/// </summary>
and internal ThreadPoolTimerCollections() as this = 
    do 
        CleanUp.Current.Register( 300, this, this.CancelAll, fun _ -> "ThreadPoolTimerCollections" ) |> ignore 
    static member val Current = new ThreadPoolTimerCollections() with get
    /// Seconds when the timer is monitored
    static member val MonitorInterval = 30 with get
    static member val MonitorLevel = LogLevel.WildVerbose with get, set
    /// Global variable that control PeriodExamineFiring
    static member val PeriodExamineTicksInterval = TimeSpan.TicksPerMillisecond with get, set
    member val private CTS = false with get, set
    member val Collection = ConcurrentDictionary<_,_>() with get
    member val MonitorTicks = ref (PerfADateTime.UtcNowTicks()) with get
    member val ExaminedTicks = ref DateTime.MinValue.Ticks with get
    member val MinNextFiringTicks = DateTime.MaxValue.Ticks with get, set
    member val NextFiringTicks = ref DateTime.MaxValue.Ticks with get
    member val Timer = null with get, set
    member x.EnqueueTimer( timer: ThreadPoolTimer ) = 
        timer.CalculateNextFiring() 
        x.Collection.GetOrAdd( timer, true ) |> ignore
        x.CueForFiring()
    /// Compute the next firing ticks. 
    member inline x.CalculateNextFiring() = 
        // Heavy operation
        let minNextFiringTicks = // if x.Collection.IsEmpty then DateTime.MaxValue.Ticks else x.Collection |> Seq.map ( fun kv -> kv.Key.NextFiringTicks ) |> Seq.min
            let mutable minTicks = DateTime.MaxValue.Ticks
            for kv in x.Collection do 
                minTicks <- Math.Min( kv.Key.NextFiringTicks, minTicks )
            minTicks
        x.MinNextFiringTicks <- minNextFiringTicks
        minNextFiringTicks
    member val LastPeriodicExaminedTick = ref DateTime.MinValue.Ticks with get
    /// Entry point for examine firing by ThreadPoolWait, make sure we don't examine too frequent on the firing. 
    member x.PeriodExamineFiring() = 
        let curTicks = (PerfDateTime.UtcNowTicks())
        let oldValue = !x.LastPeriodicExaminedTick
        if curTicks > oldValue + ThreadPoolTimerCollections.PeriodExamineTicksInterval then 
            // Memory barrier below (heavy computation) 
            if Interlocked.CompareExchange( x.LastPeriodicExaminedTick, curTicks, oldValue ) = oldValue then 
                x.TryFiring()
    /// Try firing 
    member x.TryFiring() = 
        let curTicks = (PerfADateTime.UtcNowTicks())
        if curTicks >= x.MinNextFiringTicks then 
            let oldValue = !x.ExaminedTicks
            let newValue = x.MinNextFiringTicks
            if newValue > oldValue && 
                Interlocked.CompareExchange( x.ExaminedTicks, newValue, oldValue) = oldValue then 
                // We have the lock
                let mutable bAnyFiring = false
                for pair in x.Collection do 
                    let timer = pair.Key
                    if (PerfADateTime.UtcNowTicks()) >= timer.NextFiringTicks then 
                        if not x.CTS then 
                            timer.FireOnceBegin() // Roll timer forward, queue for execution. 
                            ThreadPoolCustomized.ExecuteShortFunctionOnce( timer.FireOnceDo )

                            bAnyFiring <- true
                //if bAnyFiring || not x.Collection.IsEmpty then 
                    // CueForFiring should advance the MinNextFiringTicks if something is fired. 
                //    x.CueForFiring()
        x.CueForFiring()
    member x.Monitor() = 
        let curTicks = (PerfADateTime.UtcNowTicks())
        let oldValue = !x.MonitorTicks
        let newValue = oldValue + TimeSpan.TicksPerSecond * (int64 ThreadPoolTimerCollections.MonitorInterval ) 
        if curTicks > newValue && 
            Interlocked.CompareExchange( x.MonitorTicks, newValue, oldValue ) = oldValue then 
                Logger.LogF(ThreadPoolTimerCollections.MonitorLevel, ( fun _ -> sprintf "%d timers are created, next firing timer is at %s, with a timer scheduled on %s"
                                                                                         x.Collection.Count
                                                                                         (VersionToString( DateTime( x.MinNextFiringTicks )))
                                                                                         (VersionToString( DateTime( !x.NextFiringTicks )))))
    /// Firing Event
    static member WaitTimerFired (o:obj) = 
        let x = o :?> ThreadPoolTimerCollections
        x.NextFiringTicks := DateTime.MaxValue.Ticks // Nothing scheduled to fire, new timer may be scheduled. 
        x.TryFiring() 
    /// Cue timers for firing 
    member x.CueForFiring() = 
        let minNextFiringTicks = x.CalculateNextFiring() 
        let curTicks = (PerfADateTime.UtcNowTicks())
        if minNextFiringTicks <= curTicks then 
            x.TryFiring() 
            // will automatically re-examine the timer. 
        elif minNextFiringTicks < DateTime.MaxValue.Ticks then 
            // calculate time to wait, this will always be larger than 1ms. 
            let waitMS = ( minNextFiringTicks - curTicks + TimeSpan.TicksPerMillisecond - 1L ) / TimeSpan.TicksPerMillisecond
            let scheduleTimerFiring = curTicks + waitMS * TimeSpan.TicksPerMillisecond // scheduled time that the timer will fire
            let oldValue = !x.NextFiringTicks
            if scheduleTimerFiring < oldValue - TimeSpan.TicksPerMillisecond // at least advance the timer 1 ms. 
                && Interlocked.CompareExchange( x.NextFiringTicks, scheduleTimerFiring, oldValue ) = oldValue then 
                    Logger.Do(ThreadPoolTimerCollections.MonitorLevel, x.Monitor)
                    // We have the lock, and may schedule/change the timer. 
                    if not (x.CTS) then 
                        if Utils.IsNull x.Timer then 
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "schedule a timer of ThreadPoolTimerCollections.TryFing in %d ms" waitMS ))
                            ThreadPoolWaitCheck.Collection.Enqueue( new UnitAction( ThreadPoolTimerCollections.Current.PeriodExamineFiring ) )
                            x.Timer <- new System.Threading.Timer( ThreadPoolTimerCollections.WaitTimerFired, x, int waitMS, Timeout.Infinite )
                        else
                            // Schedule to fire timer once 
                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "shorten the timer of ThreadPoolTimerCollections.TryFing to fire in %d ms" waitMS ))
                            x.Timer.Change( int waitMS, Timeout.Infinite ) |> ignore
    member x.CancelAll() = 
        x.CTS <- true 
        x.NextFiringTicks := DateTime.MinValue.Ticks // Will disable timer change routine, 
        let timer = x.Timer
        x.Timer <- null 
        if not(Utils.IsNull timer) then 
            timer.Change( Timeout.Infinite, Timeout.Infinite) |> ignore 
            timer.Dispose() 
    member x.RemoveTimer( timer: ThreadPoolTimer ) = 
        x.Collection.TryRemove( timer ) |> ignore
    override x.Finalize() =
        CleanUp.Current.CleanUpAll()
    interface IDisposable with
        member x.Dispose() = 
            CleanUp.Current.CleanUpAll()
            GC.SuppressFinalize( x ) 
/// <summary> 
/// Thread pool timer. The advantage of this class over the System.Threading.Timer is:
/// 1. The timer will be checked by any wakeup thread pool. So the firing will be more accurate. 
/// 2. We will queue only one timer per entire pool of timer, so it is lightweight on the system. 
/// </summary>
and [<AllowNullLiteral>]
    internal ThreadPoolTimer internal (infoFunc: unit-> string, callback: unit -> unit, dueTimeInMilliSeconds:int, periodInMilliSeconds: int) as timer = 
    let mutable dueTimeInternal = dueTimeInMilliSeconds
    let mutable periodInternal = periodInMilliSeconds
    let infiring = ref 0
    member val InfoFunc = infoFunc with get, set
    member val private Callback = callback with get, set
    member val LastFiredTicks = ref (PerfADateTime.UtcNowTicks()) with get
    member val NextFiringTicks = DateTime.MaxValue.Ticks with get, set
    /// <summary> 
    /// Initializes a new instance of the ThreadPoolTimer class.  
    /// </summary> 
    /// <param name="infoFunc"> A functional delegate that shows information of the timer if the timer later ill behaved (e.g., take a long time to execute in the callback function) </param>
    /// <param name="callback"> A callback function to be invoked when timer fires. The callback function should not block, otherwise, it may impact other 
    /// timers to fire. If the callback takes a long time to execute, warning may be issued. </param>
    /// <param name="dueTime"> Next firing interval in milliseconds. If dueTime is zero (0), callback is invoked immediately. If dueTime is Timeout.Infinite, callback is not invoked; 
    /// the timer is disabled, but can be re-enabled by calling the Change method. </param>
    /// <param name="period"> Periodic firing interval in milliseconds. If period is zero (0) or Timeout.Infinite, and dueTime is not Timeout.Infinite, the callback method is invoked once; 
    /// the periodic behavior of the timer is disabled, but can be re-enabled by calling Change and specifying a positive value for period. </param>
    static member TimerWait (infoFunc) (callback) dueTimeInMilliSeconds periodInMilliSeconds = 
        let timer = new ThreadPoolTimer( infoFunc, callback, dueTimeInMilliSeconds, periodInMilliSeconds ) 
        ThreadPoolTimerCollections.Current.EnqueueTimer( timer )     
        timer

    /// <summary>
    /// get, or set due time. Please note that set due time will reset the lastFired information
    /// From System.Threading.Timer
    /// If dueTime is zero (0), callback is invoked immediately. If dueTime is Timeout.Infinite, callback is not invoked; the timer is disabled, but can be re-enabled by calling the Change method.
    /// If period is zero (0) or Timeout.Infinite, and dueTime is not Timeout.Infinite, the callback method is invoked once; the periodic behavior of the timer is disabled, but can be re-enabled by calling Change and specifying a positive value for period.
    /// </summary>
    member x.DueTime with get() = dueTimeInternal
                      and set( t ) = x.LastFiredTicks := (PerfADateTime.UtcNowTicks())
                                     dueTimeInternal <- t
                                     x.CalculateNextFiring()
                                     ThreadPoolTimerCollections.Current.CueForFiring()
    /// <summary>
    /// get, or set firing period. Please note that setting firing period will reset the lastFired information
    /// From System.Threading.Timer
    /// If dueTime is zero (0), callback is invoked immediately. If dueTime is Timeout.Infinite, callback is not invoked; the timer is disabled, but can be re-enabled by calling the Change method.
    /// If period is zero (0) or Timeout.Infinite, and dueTime is not Timeout.Infinite, the callback method is invoked once; the periodic behavior of the timer is disabled, but can be re-enabled by calling Change and specifying a positive value for period.
    /// </summary>                                   
    member x.Period with get() = periodInternal
                     and set( t ) = x.LastFiredTicks := (PerfADateTime.UtcNowTicks())
                                    periodInternal <- t
                                    x.CalculateNextFiring()
                                    ThreadPoolTimerCollections.Current.CueForFiring()
    /// Change due time & period. Please note that set due time will reset the lastFired information
    /// <param name="dueTime"> Next firing interval in milliseconds. If dueTime is zero (0), callback is invoked immediately. If dueTime is Timeout.Infinite, callback is not invoked; 
    /// the timer is disabled, but can be re-enabled by calling the Change method. </param>
    /// <param name="period"> Periodic firing interval in milliseconds. If period is zero (0) or Timeout.Infinite, and dueTime is not Timeout.Infinite, the callback method is invoked once; 
    /// the periodic behavior of the timer is disabled, but can be re-enabled by calling Change and specifying a positive value for period. </param>
    member x.Change( dueTime, period ) = 
        x.LastFiredTicks := (PerfADateTime.UtcNowTicks())
        dueTimeInternal <- dueTime
        periodInternal <- period
        x.CalculateNextFiring()
        ThreadPoolTimerCollections.Current.CueForFiring()
    /// Remove and deallocated the timer. 
    member x.Cancel() = 
        ThreadPoolTimerCollections.Current.RemoveTimer( timer ) 
    member x.CalculateNextFiring() = 
        let nextFired = 
            if dueTimeInternal < 0 then                 
                // Timer disabled. 
                DateTime.MaxValue.Ticks
            else 
                let oldValue = !x.LastFiredTicks
                if dueTimeInternal > 0 then 
                    oldValue + (int64 dueTimeInternal ) * TimeSpan.TicksPerMillisecond
                else
                    // Advance the clock somewhat 
                    oldValue + 1L
        x.NextFiringTicks <- nextFired
    /// FireOnce
    member x.FireOnceBegin() = 
        let oldValue = !x.LastFiredTicks
        let newValue = x.NextFiringTicks
        if newValue>oldValue && Interlocked.CompareExchange( x.LastFiredTicks, newValue, oldValue ) = oldValue then 
            // We have the lock, and swap in the next fired information. 
            if periodInternal <= 0 then 
                dueTimeInternal <- Timeout.Infinite
            else
                dueTimeInternal <- periodInternal  
            x.CalculateNextFiring()
            // Fire Timer
            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "firing timer %s, %f ms different from target " 
                                                           (x.InfoFunc()) 
                                                           ((PerfADateTime.UtcNow()).Subtract( DateTime(newValue) ).TotalMilliseconds)
                                                           ))
    /// Fire timer
    member x.FireOnceDo() = 
        let mutable bCueAgain = false
        if Interlocked.CompareExchange( infiring, 1, 0 ) = 0 then 
            try
                x.Callback()
            with e -> 
                Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Timer %s got an exception during firing ... %A " 
                                                               (x.InfoFunc()) 
                                                               e
                                                               ))
                
            let ticksCur = (PerfADateTime.UtcNowTicks())
            if ticksCur > x.NextFiringTicks then 
                // Firing exceeds the required period
                x.LastFiredTicks := ticksCur
                x.CalculateNextFiring()
                bCueAgain <- true
            /// Allow firing again. 
            infiring := 0
        if bCueAgain then 
            ThreadPoolTimerCollections.Current.CueForFiring()

/// <summary>
/// delegate StreamMonitorAction provides call back for StreamMonitor
/// The call back function takes two parameter: 
/// 1st: string that are read recently. 
/// 2nd: bool: whether the monitored stream has reached the end (thus should be closed). 
/// </summary>
type internal StreamMonitorAction = Action<string * bool>

/// <summary>
/// Pipe the monitored content to a file.  
/// </summary>
type internal StreamMonitorToFile( filename: string ) = 
     let refCloseCalled = ref 0
     member val internal WriteStream = null with get, set
     member x.Write( info:string, bClose ) = 
        let bEmpty = StringTools.IsNullOrEmpty( info ) 
        if not bEmpty then 
            // Need to do something on the writestream. 
            if Utils.IsNull x.WriteStream then 
                lock ( x ) ( fun _ -> 
                    if Utils.IsNull x.WriteStream then 
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "create monitor file %s" filename ))
                        x.WriteStream <- CreateFileStreamForWrite( filename ) )
            let byt = System.Text.UTF8Encoding().GetBytes( info )
            if !refCloseCalled = 0 then 
                x.WriteStream.Write( byt, 0, byt.Length )
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "write %dB to monitor file %s" byt.Length filename ))
            else
                Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "discard %dB to monitor file %s" byt.Length filename ))
        if ( bClose && Utils.IsNotNull x.WriteStream ) then 
            if Interlocked.CompareExchange( refCloseCalled, 1, 0 ) = 0 then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "close monitor file %s" filename ))
                if (x.WriteStream.CanWrite) then
                    x.WriteStream.Flush() 
                x.WriteStream.Close()

/// <summary>
/// class StreamMonitor is usually used to monitor a output stream (such as stderr, stdout), and perform one or more callback operation on new output observed. 
/// </summary>
[<AllowNullLiteral>]
type internal StreamMonitor( ) =
    // member val MonitorStream: StreamReader = monitorStream with get
    member val Callback = List<StreamMonitorAction>() with get
    member val internal LastReceived = ref DateTime.MinValue.Ticks with get
    member val internal CheckInternvalInMS = 1000 with get, set
    /// <summary> 
    /// Add a file in which the output of the stream content will be written to
    /// </summary> 
    member x.AddMonitorFile( filename ) = 
        let mon = StreamMonitorToFile( filename ) 
        x.Callback.Add( new StreamMonitorAction( mon.Write ) )    
    member x.DataReceived (outLine:DataReceivedEventArgs ) = 
        try 
            let line = outLine.Data
            if ( not (StringTools.IsNullOrEmpty( line )) ) then 
                for callback in x.Callback do 
                    try
                        callback.Invoke( line + Environment.NewLine, false )
                    with 
                    | e -> 
                        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "StreamMonitor, exception in callback of %A" e ))
        with 
        | e -> 
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "StreamMonitor, exception in reading monitor stream of %A" e ))
            for callback in x.Callback do 
                callback.Invoke( null, true )
    member x.Close() = 
        for callback in x.Callback do 
                    try
                        callback.Invoke( null, true )
                    with 
                    | e -> 
                        Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "StreamMonitor.Close exception %A" e ))

/// <summary> 
/// ExecuteUponOnce holds a collection of delegate, each of the delegate will be garanteed to be called once when Trigger() is called. 
/// </summary>
type internal ExecuteUponOnce() =
    let nExecuted = ref 0
    member val private PendingWorks = ConcurrentQueue<UnitAction>() with get
    /// Register a Action delegate. All registered delegates will be garanteed to be called once when Trigger() is called. 
    member x.Add(del : UnitAction) =
        x.PendingWorks.Enqueue(del)
        if (!nExecuted)=1 then
            x.Trigger() 
    /// Execute the registered delegates (the registration can happen before or after trigger is called). 
    member x.Trigger() = 
        // 1000000 compares int, 6ms
        if (!nExecuted) = 0 then
            // 1000000 Compare and Exchange, 105ms.   
            if Interlocked.CompareExchange( nExecuted, 1, 0) = 0 then 
                x.DoWork()
    member private x.DoWork() = 
        let refWork = ref Unchecked.defaultof<_>
        while x.PendingWorks.TryDequeue( refWork ) do 
            (!refWork).Invoke()     
            
/// <summary> 
/// ExecuteEveryTrigger holds a collection of delegate, each of the delegate will be called once when Trigger() is called by one parameter 'U. 
/// We keep track of Trigger parameter 'U until a grace period (default 1sec). After 1sec after the class is constructed, we don't keep track of 'U that is beein called before, to release
/// reference point hold by 'U.  
/// </summary>
type internal ExecuteEveryTrigger<'U>(traceLevel) = 
    // After grace period (1s), all executed works are dequeued to release memory. 
    let gracePeriodTicks = (PerfADateTime.UtcNowTicks()) + TimeSpan.TicksPerSecond 
    let toExecuteWorks = ConcurrentDictionary<'U, _>()
    let pendingWorks = ConcurrentDictionary<_, _>()
    /// <summary> 
    /// Trigger the delegate collection once, with a certain trigger function. 
    /// </summary> 
    member x.Trigger( param: 'U, paramTrigger: unit->string ) = 
        let curTuple = paramTrigger, ConcurrentDictionary<_, unit->string>()
        toExecuteWorks.Item( param) <- curTuple
        let paramTrigger, dic = curTuple
        for pair in pendingWorks do 
            let del = pair.Key
            let infoFunc = pair.Value
            dic.TryAdd( del, infoFunc ) |> ignore
        x.TryTrigger()
    /// <summary> 
    /// Register a delegate with of Action &lt;'U>
    /// </summary> 
    /// <param name="del"> Action &lt;'U> to be called when Trigger() is called </param>
    /// <param name="infoFunc"> An informational functional delegate that provides trace information on the Action delegate registered </param>
    member x.Add( del: Action<'U>, infoFunc: unit->string ) = 
        pendingWorks.Item( del ) <- infoFunc
        for pair in toExecuteWorks do 
            let param = pair.Key
            let paramTrigger, dic = pair.Value
            dic.Item( del ) <- infoFunc
        x.TryTrigger() 
    /// <summary> 
    /// Register a delegate with of Action &lt;'U>
    /// </summary> 
    /// <param name="del"> Action &lt;'U> to be called when Trigger() is called </param>
    /// <param name="info"> A string that provides trace information on the Action delegate registered </param>
    member x.Add( del: Action<'U>, info: string ) = 
        x.Add( del, fun _ -> info ) 
    member internal x.TryTrigger() = 
        for pair in toExecuteWorks do 
            let param = pair.Key
            let pTrigger, dic = pair.Value
            for pair in dic do 
                let del = pair.Key
                let bRemove, iFunc = dic.TryRemove( del )
                if bRemove then 
                    del.Invoke( param ) 
                    Logger.LogF(traceLevel, ( fun _ -> sprintf "ExecuteEveryTrigger.Trigger, execute %s on %s once" (iFunc()) (pTrigger()) ))
            let ticksNow = (PerfADateTime.UtcNowTicks())
            if ticksNow >= gracePeriodTicks then 
                if dic.IsEmpty then 
                    toExecuteWorks.TryRemove( param ) |> ignore
                    Logger.LogF(traceLevel, ( fun _ -> sprintf "ExecuteEveryTrigger, done all actions on %s." (pTrigger()) ))
        
/// SingleCreation<'U> holds a single object 'U, and garantees that the creation function and destroy function is only called once. 
/// At time of init, the class garantees that initFunc will be called once to create the instance. At the time of 
/// destruction, the class also garantees that the destroyFunc is called once to destroy the instance. 
type internal SingleCreation<'U>() = 
    // After grace period (1s), all executed works are dequeued to release memory. 
    let obj = ref None
    let lock = SpinLockSlim(true)
    /// Create an object via the execution of an initFunc. SingleCreation garantees that the  initFunc is only called upon once in a multithread environment to construct an object. 
    member x.Create( initFunc: unit -> 'U ) = 
        let mutable bLockTaken = false
        try
            while (!obj).IsNone && not bLockTaken do 
                bLockTaken <- lock.TryEnter() 
            if bLockTaken then 
                // This thread is responsible to create the object
                obj := Some (initFunc())
                lock.Exit() 
        with 
        | e -> 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "!!! Exception !!! SingleCreation.Create( unit-> %s ) with exception %A"
                                                               typeof<'U>.FullName
                                                               e ) )
            if bLockTaken then 
               lock.Exit() 
        match !obj with 
        | None -> 
            Unchecked.defaultof<_>
        | Some o -> 
            o
    /// Access the object constructed by Create() function. 
    member x.Object() = 
        match !obj with 
        | None -> 
            Unchecked.defaultof<_>
        | Some o -> 
            o
    /// Destroy the object by calling a deallocation function destroyFunc. The destroyFunc is garanteed to be called only once.  
    member x.Destroy( destroyFunc: 'U -> unit ) = 
        let mutable bLockTaken = false
        try
            while (!obj).IsSome && not bLockTaken do 
                 bLockTaken <- lock.TryEnter() 
            if bLockTaken then 
                // This thread is responsible to create the object
                match !obj with 
                | None -> 
                    ()
                | Some o -> 
                    obj := None 
                    destroyFunc( o ) 
                    lock.Exit() 
        with 
        | e -> 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "!!! Exception !!! in SingleCreation.Destroy( %s -> unit )  with exception %A"
                                                               typeof<'U>.FullName
                                                               e ) )
            if bLockTaken then 
                lock.Exit() 
                
