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

	Author: Sanjeev Mehrotra
 ---------------------------------------------------------------------------*)

namespace Prajna.Tools.Queue

open System
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open System.Diagnostics
open Prajna.Tools
open Prajna.Tools.FSharp

module internal Ev =
    let inline ResetOnCond (cond : unit->bool, ev : ManualResetEvent, waitTime : int) : ManualResetEvent = 
        if (cond()) then
            ev.Reset() |> ignore
            // recheck condition - in case other thread set it prior to reset
            if (cond()) then
                if (ev.WaitOne(waitTime)) then
                    null
                else
                    ev
            else
                ev.Set() |> ignore
                null
        else
            null

    let inline ResetUntilCond (cond : unit->bool, ev : ManualResetEvent) =
        while (cond()) do
            ev.Reset() |> ignore            
            // recheck condition after reset in case another thread has set it
            if (cond()) then
                ev.WaitOne() |> ignore
                //if ev.WaitOne(0) then
                //    ev.Reset() |> ignore
                //else
                //    ev.WaitOne() |> ignore
                //()
            else
                ev.Set() |> ignore

    let inline SetOnNotCond (cond : unit->bool, ev : ManualResetEvent) =
        if not (cond()) then
            ev.Set() |> ignore
    let SetOnNotCondNoInline (cond : unit->bool, ev : ManualResetEvent) =
        SetOnNotCond(cond, ev)

/// The base class for various concurrent queue structures with and without flow control logic
/// all enqueue/dequeue methods return (success, waithandle) in all classes
/// 1. GrowQ: No limit on queue growth
/// 2. FixedLenQ: Growth limited by queue length
/// 3. FixedSizeQ: Growth limited by queue "size", where "size" is sum of all element sizes
[<AllowNullLiteral>]
[<AbstractClass>]
type BaseQ<'T>() =
    let mutable emptyEvent = new ManualResetEvent(true)
    let mutable fullEvent = new ManualResetEvent(false)

    member x.Empty with get() = emptyEvent
    member x.Full with get() = fullEvent
    member val WaitTimeEnqueueMs = 0 with get, set
    member val WaitTimeDequeueMs = 0 with get, set

    /// <summary> 
    /// Abstract member to enqueue synchronously - calling thread blocked until enqueue succeeds
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>Tuple of (success, event) where success is always true and event is null</returns>
    abstract member EnqueueSync : 'T -> bool*ManualResetEvent
    default x.EnqueueSync(item) =
        assert(false)
        (true, null)
    /// <summary>
    /// Abstract member to enqueue without blocking calling thread.
    /// If enqueue not sucessful returns an event which the caller can wait upon before trying again.
    /// Enqueue is not successful if queue is full.
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where event is null upon success,
    /// otherwise it is a ManualResetEvent which the caller can wait upon
    /// </returns>
    abstract member EnqueueWait : 'T -> bool*ManualResetEvent
    default x.EnqueueWait(item) = x.EnqueueSync(item)
    /// <summary>
    /// Abstract member to enqueue without blocking calling thread.
    /// Enqueue will wait for upto WaitTimeEnqueueMs milliseconds synchronously if needed.
    /// If enqueue not sucessful returns an event which the caller can wait upon before trying again.
    /// Enqueue is not successful if queue is full.
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where event is null upon success,
    /// otherwise it is a ManualResetEvent which the caller can wait upon
    /// </returns>
    abstract member EnqueueWaitTime : 'T -> bool*ManualResetEvent
    default x.EnqueueWaitTime(item) = x.EnqueueSync(item)

    /// <summary>
    /// Abstract member to dequeue synchronously, caller will be blocked while function waits for queue to fill.
    /// Even after waiting, dequeue may not return anything in case another thread retrieves item.
    /// </summary>
    /// <param name="item">A reference to location where retrieved item is stored</param>
    /// <returns>
    /// Tuple of (success, event)
    /// If success is true, event will be null.
    /// If success is false, event will either be null or will be a ManualResetEvent for which the caller
    /// can wait upon prior to calling again.
    /// Even after this wait, there is no guarantee object will be returned upon next call.
    /// </returns>
    abstract member DequeueSync : 'T ref -> bool*ManualResetEvent
    default x.DequeueSync(item) =
        assert(false)
        (true, null)

    /// <summary>
    /// Abstract member to dequeue without blocking caller.  If queue is empty automatically returns.
    /// </summary>
    /// <param name="item">A reference to location where retrieved item is stored</param>
    /// <returns>
    /// Tuple of (success, event)
    /// If success is true, event will be null.
    /// If success is false, event will either be null or will be a ManualResetEvent for which the caller
    /// can wait upon prior to calling again.
    /// Even after this wait, there is no guarantee object will be returned upon next call.
    /// </returns>
    abstract member DequeueWait : 'T ref -> bool*ManualResetEvent
    default x.DequeueWait(item) = x.DequeueSync(item)

    /// <summary>
    /// Abstract member to dequeue without blocking caller for long time.
    /// Function will block up to WaitTimeDequeueMs prior to returning.
    /// </summary>
    /// <param name="item">A reference to location where retrieved item is stored</param>
    /// <returns>
    /// Tuple of (success, event)
    /// If success is true, event will be null.
    /// If success is false, event will either be null or will be a ManualResetEvent for which the caller
    /// can wait upon prior to calling again.
    /// Even after this wait, there is no guarantee object will be returned upon next call.
    /// </returns>
    abstract member DequeueWaitTime : 'T ref -> bool*ManualResetEvent
    default x.DequeueWaitTime(item) = x.DequeueSync(item)
    /// Return if queue is empty or not
    abstract member IsEmpty : bool with get
    /// Function to clear out the queue
    abstract member Clear : unit->unit
    /// Returns count of number of elements in queue
    abstract member Count : int with get

    interface IDisposable with
        member x.Dispose() = 
            emptyEvent.Dispose()
            emptyEvent <- null
            fullEvent.Dispose()
            fullEvent <- null
            GC.SuppressFinalize(x)

    static member internal ClearQ(x : ConcurrentQueue<'T>) =
        let item : 'T ref = ref Unchecked.defaultof<'T>
        while (not x.IsEmpty) do
            x.TryDequeue(item) |> ignore

    static member internal OnWaitComplete (o : obj) (bTimedOut : bool) =
        let (rwh, callback, state) = o :?> (RegisteredWaitHandle ref)*(obj*bool->unit)*obj
        callback(state, bTimedOut)

    static member internal Wait(event : ManualResetEvent, callback, state : obj) =
        ThreadPoolWait.WaitForHandle (fun() -> "") event (fun() -> callback(state, false)) null

    static member inline internal Enqueue(enq: 'T->unit, item : 'T, cond : unit->bool, ev : ManualResetEvent, waitTime : int) =
        let ev = Ev.ResetOnCond(cond, ev, waitTime)
        if (Utils.IsNull ev) then
            enq(item)
            (true, null)
        else
            (false, ev)

    static member inline internal EnqueueSync(enq: 'T->unit, item : 'T, cond : unit->bool, ev : ManualResetEvent) =
        Ev.ResetUntilCond(cond, ev)
        enq(item)
        (true, null)


    static member internal EnqueueNoInline(enq: 'T->unit, item : 'T, cond : unit->bool, ev : ManualResetEvent, waitTime : int) =
        BaseQ.Enqueue(enq, item, cond, ev, waitTime)

    static member internal EnqueueSyncNoInline(enq: 'T->unit, item : 'T, cond : unit->bool, ev : ManualResetEvent) =
        BaseQ.EnqueueSync(enq, item, cond, ev)

    static member inline internal Dequeue(deq : 'T ref->bool, item : 'T ref, cond : unit->bool,
                                          ev : ManualResetEvent, waitTime : int) =
        let ev = Ev.ResetOnCond(cond, ev, waitTime)
        if (Utils.IsNull ev) then
            (deq(item), null)
        else
            (false, ev)

    static member internal DequeueNoInline(deq : 'T ref->bool, item : 'T ref, cond : unit->bool,
                                           ev : ManualResetEvent, waitTime : int) =
        BaseQ.Dequeue(deq, item, cond, ev, waitTime)

    static member inline internal DequeueSync(deq : 'T ref->bool, item : 'T ref, cond : unit->bool,
                                              ev : ManualResetEvent) =
        Ev.ResetUntilCond(cond, ev)
        (deq(item), null)
              
    static member inline internal Dequeue(deq : 'T ref->bool*ManualResetEvent, item : 'T ref, cond : unit->bool,
                                          ev : ManualResetEvent, waitTime : int) =
        let ev = Ev.ResetOnCond(cond, ev, waitTime)
        if (Utils.IsNull ev) then
            deq(item)
        else
            (false, ev)

    static member internal DequeueNoInline(deq : 'T ref->bool*ManualResetEvent, item : 'T ref, cond : unit->bool,
                                           ev : ManualResetEvent, waitTime : int) =
        BaseQ.Dequeue(deq, item, cond, ev, waitTime)

    static member inline internal DequeueSync(deq : 'T ref->bool*ManualResetEvent, item : 'T ref, cond : unit->bool,
                                              ev : ManualResetEvent) =
        Ev.ResetUntilCond(cond, ev)
        deq(item)

/// A queue derived from BaseQ with no limits on queue growth
[<AllowNullLiteral>]
type internal GrowQ<'T>() as x =
    inherit BaseQ<'T>()
    let q = ConcurrentQueue<'T>()
    let fullEvent = x.Full

    /// The internal concurrent queue used to hold data
    member x.Q with get() = q

    /// Returns if queue is empty or not
    override x.IsEmpty with get() = q.IsEmpty
    /// Clear the queue of all elements
    override x.Clear() =
        BaseQ<_>.ClearQ(q)
    /// Count of number of elements
    override x.Count with get() = q.Count

    /// <summary> 
    /// Enqueue an item, wait for space. However since queue can grow, no need to wait.
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where success is always true and event is null
    /// </returns>
    override x.EnqueueSync(item : 'T) =
        q.Enqueue(item)
        fullEvent.Set() |> ignore
        (true, null)
    /// <summary> 
    /// Enqueue an item, return (false, event) if no space.  However since queue can grow, no need to wait.
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where success is always true and event is null
    /// </returns>
    override x.EnqueueWait(item : 'T) =
        x.EnqueueSync(item)
    /// <summary> 
    /// Enqueue an item, return (false, event) if no space after waiting up to WaitTimeEnqueueMs
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where success is always true and event is null
    /// </returns>
    override x.EnqueueWaitTime(item : 'T) =
        x.EnqueueSync(item)

    /// <summary>
    /// Try dequeue, wait for full event (still no guarantee to succeed)
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueSync (result : 'T ref) =
        BaseQ<_>.Dequeue((fun r -> q.TryDequeue(r)), result, (fun () -> q.IsEmpty), fullEvent, -1)

    /// <summary>
    /// Try dequeue, return wait for full event.
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueWait (result : 'T ref) =
        BaseQ<_>.Dequeue((fun r -> q.TryDequeue(r)), result, (fun() -> q.IsEmpty), fullEvent, 0)         

    /// <summary>
    /// Try dequeue, return wait for full event after waiting for WaitTimeDequeueMs milliseconds.
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueWaitTime (result : 'T ref) =
        BaseQ<_>.Dequeue((fun r -> q.TryDequeue(r)), result, (fun() -> q.IsEmpty), fullEvent, x.WaitTimeDequeueMs)

/// A queue which limits queue growth up to a maximum number of elements.
/// The actual length of the queue may be slightly larger than max length if multiple threads are enqueuing, since queue.Count is used
[<AllowNullLiteral>]
type internal FixedLenQ<'T>() as x =
    inherit GrowQ<'T>()
    let q = x.Q
    let emptyEvent = x.Empty
    let fullEvent = x.Full
    // no limits unless specified
    let mutable maxLen : int = Int32.MaxValue
    let mutable desiredLen : int = Int32.MaxValue

    // Actual length may become larger if multiple threads may enqueue
    // Can be fixed by utilizing our own counter, and using Iterlocked.Increment prior to enqueue check
    // then, using Interlocked.Decrement upon no enqueue
    let isFullMax() = q.Count + 1 > maxLen
    let isFullDesired() = q.Count > desiredLen
    let isEmpty() = q.IsEmpty

    let eq (t) =
        q.Enqueue(t)
        fullEvent.Set() |> ignore
        
    let dq (r) =
        let ret = q.TryDequeue(r)
        Ev.SetOnNotCond(isFullDesired, emptyEvent)
        ret

    new(_desiredLen : int, _maxLen : int) as x =
        new FixedLenQ<'T>()
        then
            x.DesiredLen <- _desiredLen
            x.MaxLen <- _maxLen
    new(_desiredLen : int, _maxLen : int, _waitTimeEnqueueMs : int, _waitTimeDequeueMs : int) as x =
        new FixedLenQ<'T>()
        then
            x.DesiredLen <- _desiredLen
            x.MaxLen <- _maxLen
            x.WaitTimeEnqueueMs <- _waitTimeEnqueueMs
            x.WaitTimeDequeueMs <- _waitTimeDequeueMs

    /// The maximum length of the queue as a count
    member x.MaxLen with get() = maxLen and set(v) = maxLen <- v
    /// The desired length of the quueu (once queue is full, it must go below this for more items to enter)
    member x.DesiredLen with get() = desiredLen and set(v) = desiredLen <- v

    /// <summary> 
    /// Enqueue an item, wait for space. Queue grows to max len, if it exceeds then it synchronously waits on same thread
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where success is always true and event is null
    /// </returns>
    override x.EnqueueSync (item : 'T) =
        BaseQ.EnqueueSync(eq, item, isFullMax, emptyEvent)

    /// <summary> 
    /// Enqueue an item. Queue grows to max len, if it exceeds then it returns a wait handle
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where if success is false, an event is returned which the caller can wait upon.
    /// </returns>
    override x.EnqueueWait (item : 'T) =
        BaseQ.Enqueue(eq, item, isFullMax, emptyEvent, 0)           

    /// <summary> 
    /// Enqueue an item. 
    /// Queue grows to max len, if it exceeds then it waits for some duration of time and then returns wait handle if wait does not succeed.
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <returns>
    /// Tuple of (success, event) where if success is false, an event is returned which the caller can wait upon.
    /// </returns>
    override x.EnqueueWaitTime (item : 'T) =
        BaseQ.Enqueue(eq, item, isFullMax, emptyEvent, x.WaitTimeEnqueueMs)

    /// <summary>
    /// Try dequeue, wait for full event (still no guarantee to succeed)
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueSync (result : 'T ref) =
        BaseQ.DequeueSync(dq, result, isEmpty, fullEvent)

    /// <summary>
    /// Try dequeue, return wait for full event.
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueWait (result : 'T ref) =
        BaseQ.Dequeue(dq, result, isEmpty, fullEvent, 0)        

    /// <summary>
    /// Try dequeue, return wait for full event after waiting for WaitTimeDequeueMs milliseconds.
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueWaitTime (result : 'T ref) =
        BaseQ.Dequeue(dq, result, isEmpty, fullEvent, x.WaitTimeDequeueMs)

/// A queue which limits queue growth up to a maximum size
/// Actual size may be larger, and condition violated if multiple threads enqueue.
/// This can be fixed if desired by utilizing Interlocked.Add prior to and after conditional check
[<AllowNullLiteral>]
type internal FixedSizeQ<'T>() as x =
    inherit FixedLenQ<'T>()
    let q = x.Q
    let emptyEvent = x.Empty
    let fullEvent = x.Full
    // no limits unless specified
    let mutable maxSize : int64 = Int64.MaxValue
    let mutable desiredSize : int64 = Int64.MaxValue

    let currentSize = ref 0L

    // at least something must be allowed - so use !currentsize > 0L
    //
    // Actual size may be larger due to checking.  To fix utilize:
    // Interlocked.Add(currentSize, size)
    // if (!currentSize > maxSize) && (!currentSize > size)
    //    ... other stuff here ...
    //    enqueue()
    // else
    //    Interlocked.Add(currentSize, -size)
    // or --
    // Interlocked.Add(currentSize, size)
    // BaseQ<_>.Enqueue(eq(size), neq(size), item, isFullMax(size), emptyEvent, 0)
    // modify: isFullMax(size)() = (!currentSize > maxSize) && (!currentSize > size)
    // modify: BaseQ<_>.Enqueue(eq(size), neq(size), item, isFullMax(size), emptyEvent, 0)
    // where neq size is exercised if eq not exercised
    // let eq (size) (t) =
    //     q.Enqueue(t)
    //     fullEvent.Set() |> ignore
    // let neq(size) =
    //     Interlocked.Add(currentSize, -size) |> ignore
    let isFullMax(size)() = (!currentSize + size) > maxSize && (!currentSize > 0L)
    let isFullDesired() = !currentSize > desiredSize
    let isEmpty() = q.IsEmpty

    let eq (size) (t) =
        q.Enqueue(t)
        Interlocked.Add(currentSize, size) |> ignore
        fullEvent.Set() |> ignore
        
    let dq (size) (r) =
        let ret = q.TryDequeue(r)
        if (ret) then
            Interlocked.Add(currentSize, -size) |> ignore
            Ev.SetOnNotCond(isFullDesired, emptyEvent)
        ret

    let dqCond (cond) (size) (r) =
        let ret = q.TryDequeue(r)
        if (ret) then
            Interlocked.Add(currentSize, -size) |> ignore
            Ev.SetOnNotCond(cond, emptyEvent)
        ret

    new(_desiredSize : int64, _maxSize : int64) as x =
        new FixedSizeQ<'T>()
        then
            x.DesiredSize <- _desiredSize
            x.MaxSize <- _maxSize
    new(_desiredSize : int64, _maxSize : int64, waitTimeEnqueueMs : int, waitTimeDequeueMs : int) as x =
        new FixedSizeQ<'T>()
        then
            x.DesiredSize <- _desiredSize
            x.MaxSize <- _maxSize
            x.WaitTimeDequeueMs <- waitTimeEnqueueMs
            x.WaitTimeDequeueMs <- waitTimeDequeueMs

    /// The maximum size of the queue - threshold for declaring queue is "full"
    member x.MaxSize with get() = maxSize and set(v) = maxSize <- v
    /// The desired size of the queue - threshold for declaring queue is "empty"
    member x.DesiredSize with get() = desiredSize and set(v) = desiredSize <- v
    /// The current size of the queue
    member x.CurrentSize with get() = !currentSize
    /// The current size ref
    member internal x.CurrentSizeRef with get() = currentSize

    /// An arbitrary function for testing fullness for declaring queue is "full"
    member val IsFullMaxCond : int64->unit->bool = (fun size -> isFullMax(size)) with get, set
    /// An arbitrary function for testing fullness for declaring queue is "empty"
    member val IsFullDesiredCond : unit->bool = (fun _ -> isFullDesired()) with get, set

    /// <summary> 
    /// Enqueue an item, wait for space. 
    /// Queue grows to max size (using size as count), if it exceeds then it synchronously waits on same thread
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <param name="size">Size of item being queued</param>
    /// <returns>
    /// Tuple of (success, event) where success is always true and event is null
    /// </returns>
    member x.EnqueueSyncSize (item : 'T) (size : int64) =
        BaseQ<_>.EnqueueSync(eq(size), item, isFullMax(size), emptyEvent)

    /// <summary> 
    /// Enqueue an item.
    /// Queue grows to max size (using size as count), if it exceeds then it returns a wait handle
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <param name="size">Size of item being queued</param>
    /// <returns>
    /// Tuple of (success, event) where if success is false, an event is returned which the caller can wait upon.
    /// </returns>
    member x.EnqueueWaitSize (item : 'T) (size : int64) =
        BaseQ<_>.Enqueue(eq(size), item, isFullMax(size), emptyEvent, 0)          

    /// <summary> 
    /// Enqueue an item. 
    /// Queue grows to max size (using size as count), if it exceeds then it waits for some duration of time and returns wait handle
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <param name="size">Size of item being queued</param>
    /// <returns>
    /// Tuple of (success, event) where if success is false, an event is returned which the caller can wait upon.
    /// </returns>
    member x.EnqueueWaitTimeSize (item : 'T) (size : int64) =
        BaseQ<_>.Enqueue(eq(size), item, isFullMax(size), emptyEvent, x.WaitTimeEnqueueMs)

    /// <summary> 
    /// Enqueue an item. 
    /// Queue grows to max size (using size as count), if it exceeds then it waits for some duration of time and returns wait handle
    /// Generic condition for testing of "full"
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <param name="size">Size of item being queued</param>
    /// <param name="cond">Condition for testing if full</param>
    /// <returns>
    /// Tuple of (success, event) where if success is false, an event is returned which the caller can wait upon.
    /// </returns>
    member x.EnqueuSyncCond (item : 'T) (size : int64) (cond : (FixedSizeQ<'T>*int64)->unit->bool) =
        BaseQ<_>.EnqueueSync(eq(size), item, cond(x, size), emptyEvent)

    /// <summary> 
    /// Enqueue an item. 
    /// Queue grows to max size (using size as count), if it exceeds then it waits for some duration of time and returns wait handle
    /// Generic condition for testing of "full"
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <param name="size">Size of item being queued</param>
    /// <param name="cond">Condition for testing if full</param>
    /// <returns>
    /// Tuple of (success, event) where if success is false, an event is returned which the caller can wait upon.
    /// </returns>
    member x.EnqueueWaitTimeCond (item : 'T) (size : int64) (cond : (FixedSizeQ<'T>*int64)->unit->bool) =
        BaseQ<_>.Enqueue(eq(size), item, cond(x, size), emptyEvent, x.WaitTimeEnqueueMs)

    /// <summary> 
    /// Enqueue an item. 
    /// Queue grows to max size (using size as count), if it exceeds then it waits for some duration of time and returns wait handle
    /// Generic condition for testing of "full" utilizing member IsFullMaxCond which caller can set to arbitrary function
    /// </summary>
    /// <param name="item">Item to be inserted into queue</param> 
    /// <param name="size">Size of item being queued</param>
    /// <returns>
    /// Tuple of (success, event) where if success is false, an event is returned which the caller can wait upon.
    /// </returns>
    member x.EnqueueWaitTimeFullMaxCond (item : 'T) (size : int64) =
        BaseQ<_>.Enqueue(eq(size), item, x.IsFullMaxCond(size), emptyEvent, x.WaitTimeEnqueueMs)

    /// <summary>
    /// Try dequeue, wait for full event (still no guarantee to succeed)
    /// A separate call to dequeue the size is needed
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueSync (result : 'T ref) =
        BaseQ.Dequeue((fun r -> q.TryDequeue(r)), result, isEmpty, fullEvent, -1)

    /// <summary>
    /// Try dequeue, return wait for full event.
    /// A separate call to dequeue the size is needed
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueWait (result : 'T ref) =
        BaseQ.Dequeue((fun r -> q.TryDequeue(r)), result, isEmpty, fullEvent, 0)        

    /// <summary>
    /// Try dequeue, return wait for full event after waiting for WaitTimeDequeueMs milliseconds.
    /// A separate call to dequeue the size is needed
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, caller can wait on event prior to calling again.
    /// </returns>
    override x.DequeueWaitTime (result : 'T ref) =
        BaseQ.Dequeue((fun r -> q.TryDequeue(r)), result, isEmpty, fullEvent, x.WaitTimeDequeueMs)

    /// <summary>
    /// Try dequeue, wait for full event (still no guarantee to succeed)
    /// Not so useful since "size" often unknown until dequeue succeeds, but add for completeness
    /// Better to simply use DequeueSync followed by call to DequeueSize which will reduce queue size
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <param name="size">Size of item being retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    member x.DequeueSyncSize (result : 'T ref) (size : int64) =
        BaseQ<_>.Dequeue(dq(size), result, isEmpty, fullEvent, -1)

    /// <summary>
    /// Try dequeue, return wait for full event.
    /// Not so useful since "size" often unknown until dequeue succeeds, but add for completeness
    /// Better to simply use DequeueSync followed by call to DequeueSize which will reduce queue size
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <param name="size">Size of item being retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, an event may be returned.
    /// If so, caller can wait on event prior to calling again.
    /// </returns>
    member x.DequeueWaitSize (result : 'T ref) (size : int64) =
        BaseQ<_>.Dequeue(dq(size), result, isEmpty, fullEvent, 0)           

    /// <summary>
    /// Try dequeue, return wait for full event after waiting for WaitTimeDequeueMs milliseconds.
    /// Not so useful since "size" often unknown until dequeue succeeds, but add for completeness
    /// Better to simply use DequeueSync followed by call to DequeueSize which will reduce queue size
    /// </summary>
    /// <param name="result">Item retrieved</param>
    /// <param name="size">Size of item being retrieved</param>
    /// <returns>
    /// Tuple of (success, event), where if success is false, caller can wait on event prior to calling again.
    /// </returns>
    member x.DequeueWaitTimeSize (result : 'T ref) (size : int64) =
        BaseQ<_>.Dequeue(dq(size), result, isEmpty, fullEvent, x.WaitTimeDequeueMs)

    //member x.DequeueWaitTimeFullDesiredCond (result : 'T ref) (size : int64) =
    //    BaseQ<_>.Dequeue((dqCond x.IsFullDesiredCond size), result, isEmpty, fullEvent, x.WaitTimeDequeueMs)

    /// <summary>
    /// Decrement queue size after item has been retrieved and set empty condition on queue if needed.
    /// </summary>
    /// <param name="size">Size of item which was retrieved</param>
    member x.DequeueSize(size : int64) =
        Interlocked.Add(currentSize, -size) |> ignore
        Ev.SetOnNotCond(isFullDesired, emptyEvent)

    /// <summary>
    /// Decrement queue size after item has been retrieved and set empty condition on queue if needed.
    /// Allow generic fullness condition check IsFullDesiredCond function to be used (which caller can set).
    /// </summary>
    /// <param name="size">Size of item which was retrieved</param>
    member x.DequeueSizeFullDesiredCond(size : int64) =
        Interlocked.Add(currentSize, -size) |> ignore
        Ev.SetOnNotCond(x.IsFullDesiredCond, emptyEvent)

/// A queue which limit queue growth by size, but also allows for minimum size before it is declared to have items to process
/// A timer is also provided for preventing an indefinite wait through the variable maxQWaitTime
[<AllowNullLiteral>]
type internal FixedSizeMinSizeQ<'T>() as x =
    inherit BaseQ<'T>()
    let q = ConcurrentQueue<'T*int64*int64>()
    let emptyEvent = x.Empty
    let fullEvent = x.Full
    // no limits unless specified
    let mutable maxSize : int64 = Int64.MaxValue
    let mutable desiredMaxSize : int64 = Int64.MaxValue
    // queue doesn't say it's full unless minimum size is reached
    let mutable minSize : int64 = 1L
    let mutable desiredMinSize : int64 = 1L

    let currentSize = ref 0L
    let mutable remProcess = 0L
    let elemCount = ref 0

    let stopwatch = Stopwatch()
    do stopwatch.Start()
    let mutable timer : ThreadPoolTimer = null
    let mutable maxQWaitTime = 0L
    let setToFull() =
        fullEvent.Set() |> ignore

    new(desiredMaxSize : int64, maxSize : int64, desiredMinSize : int64, minSize : int64) as x =
        new FixedSizeMinSizeQ<'T>()
        then
            x.DesiredMaxSize <- desiredMaxSize
            x.MaxSize <- maxSize
            x.DesiredMinSize <- desiredMinSize
            x.MinSize <- minSize
    new(desiredMaxSize : int64, maxSize : int64, desiredMinSize : int64, minSize : int64,
        waitTimeEnqueueMs : int, waitTimeDequeueMs : int) as x =
        new FixedSizeMinSizeQ<'T>(desiredMaxSize, maxSize, desiredMinSize, minSize)
        then
            x.WaitTimeEnqueueMs <- waitTimeEnqueueMs
            x.WaitTimeDequeueMs <- waitTimeDequeueMs

    override x.IsEmpty with get() = q.IsEmpty
    override x.Clear() =
        BaseQ<_>.ClearQ(q)
    override x.Count with get() = q.Count

    member x.MaxSize with get() = maxSize and set(v) = maxSize <- v
    member x.DesiredMaxSize with get() = desiredMaxSize and set(v) = desiredMaxSize <- v
    member x.MinSize with get() = minSize and set(v) = minSize <- v
    member x.DesiredMinSize with get() = desiredMinSize and set(v) = desiredMinSize <- v
    member x.CurrentSize with get() = !currentSize
    member x.StopWatch with get() = stopwatch

    member x.InitTimer (infoFuncTimer : unit -> string) (maxWaitTime : int64) =
        timer <- ThreadPoolTimer.TimerWait infoFuncTimer setToFull Timeout.Infinite Timeout.Infinite
        maxQWaitTime <- maxWaitTime

    member private x.UpdateTopTime(topTime : int64) =
        let dueTime = topTime + maxQWaitTime - stopwatch.ElapsedMilliseconds
        if (dueTime > 3L) then
            timer.DueTime <- int dueTime
        else
            setToFull()

    member x.FullDesiredCond() : bool = (!currentSize > desiredMaxSize)
    member x.FullMaxCond(size)() : bool = (!currentSize + size > maxSize)
    member x.EmptyDesiredCond() : bool = (!currentSize < desiredMinSize)
    member x.EmptyMinCond() : bool = (!currentSize < minSize)

    member private x.EnqueueElem (size : int64) (item : 'T) =
        let time = stopwatch.ElapsedMilliseconds
        q.Enqueue(item, size, time)
        if (Interlocked.Increment(elemCount) = 1) then
            x.UpdateTopTime(time)
        Interlocked.Add(currentSize, size) |> ignore
        Ev.SetOnNotCond(x.EmptyDesiredCond, fullEvent)

    // queue grows to max len (using size as count), if it exceeds then it synchronously waits on same thread
    member x.EnqueueSyncSize (item : 'T) (size : int64) =
        BaseQ<_>.Enqueue((x.EnqueueElem size), item, (x.FullMaxCond size), emptyEvent, Timeout.Infinite)

    // queue grows to max len (using size as count), if it exceeds then it returns a wait handle
    member x.EnqueueWaitSize (item : 'T) (size : int64) =
        BaseQ<_>.Enqueue((x.EnqueueElem size), item, (x.FullMaxCond size), emptyEvent, 0)

    // queue grows to max len (using size as count), if it exceeds then it waits for some duration of time and returns wait handle
    member x.EnqueueWaitTimeSize (item : 'T) (size : int64) =
        BaseQ<_>.Enqueue((x.EnqueueElem size), item, (x.FullMaxCond size), emptyEvent, x.WaitTimeEnqueueMs)

    member x.DequeueSize(size : int64) =
        Interlocked.Add(currentSize, -size) |> ignore
        Ev.SetOnNotCond(x.FullDesiredCond, emptyEvent)

    member x.DequeueSize(result : ('T*int64*int64) ref) =
        let (item, size, time) = !result
        x.DequeueSize(size)
        if (Interlocked.Decrement(elemCount) > 0) then
            let next = ref Unchecked.defaultof<'T*int64*int64>
            let ret = q.TryPeek(next)
            if (ret) then
                let (item, size, time) = !next
                x.UpdateTopTime(time)
        else
            x.UpdateTopTime(0L)

    member private x.DequeueElem(result : ('T*int64*int64) ref) =
        let ret = q.TryDequeue(result)
        if (ret) then
            x.DequeueSize(result)
        ret

    // try dequeue, wait for full event (still no guarantee to succeed)
    member x.DequeueSyncSize (result : ('T*int64*int64) ref) =
        BaseQ<_>.Dequeue(x.DequeueElem, result, x.EmptyMinCond, fullEvent, Timeout.Infinite)

    // try dequeue, return wait for full event
    member x.DequeueWaitSize (result : ('T*int64*int64) ref) =
        BaseQ<_>.Dequeue(x.DequeueElem, result, x.EmptyMinCond, fullEvent, 0)

    // try dequeue, wait for some amount of time, then return wait for full event
    member x.DequeueWaitTimeSize (result : ('T*int64*int64) ref) =
        BaseQ<_>.Dequeue(x.DequeueElem, result, x.EmptyMinCond, fullEvent, x.WaitTimeDequeueMs)

    member private x.DequeueWaitSizeRemProcessDQ (item : 'T ref) =
        let output = ref Unchecked.defaultof<'T*int64*int64>
        let (success, event) = x.DequeueWaitSize(output)
        if (success) then
            let (itemOut, size, time) = !output
            item := itemOut
            if (0L = remProcess) then
                remProcess <- !currentSize
            else
                remProcess <- remProcess - size
        else if Utils.IsNotNull event then
            remProcess <- 0L
        (success, event)
   
    member x.DequeueWaitSizeRemProcess (item : 'T ref) =
        if (0L = remProcess) then
            BaseQ<_>.Dequeue(x.DequeueWaitSizeRemProcessDQ, item, x.EmptyDesiredCond, fullEvent, 0)
        else
            x.DequeueWaitSizeRemProcessDQ(item)

    member x.Num with get() = q.Count

/// Shared stack of memory (for example to share across network connections)
/// Use stack so same buffer gets used with higher frequency (e.g. to improve caching)
[<AllowNullLiteral>]
type internal SharedStack<'T when 'T : (new : unit -> 'T)>(initSize : int, allocObj : 'T -> unit, infoStr : string) =
    let stack = ConcurrentStack<'T>()
    let infoStr = infoStr
    let mutable size = 0
    let expandStack(newSize : int)() =
        if (newSize > size) then
            Logger.LogF( LogLevel.MildVerbose, (fun _ -> sprintf "Expand %s stack from %d to %d" infoStr size newSize))
            for i = size to (newSize-1) do
                let elem = new 'T()
                allocObj(elem)
                stack.Push(elem)
            size <- newSize
    do expandStack(initSize)()
    let st = SingleThreadExec()
    let notEmpty = new ManualResetEvent(true)

    let isEmpty = (fun _ -> stack.IsEmpty)
    member x.Count() = stack.Count
    member x.Size with get() = size
    member internal x.Stack with get() = stack
    /// The maximum stack size as count of number of elements
    member val MaxStackSize = Int32.MaxValue with get, set

    /// Expand stack to 150% of current size
    member private x.ExpandStack(prevSize : int) =
        let newSize = Math.Min(prevSize + ((prevSize+1)>>>1),  x.MaxStackSize) // increase by 50%, up to max
        if (newSize = prevSize) then
            // cannot grow further
            Ev.ResetOnCond(isEmpty, notEmpty, 0)
        else
            st.ExecQ(expandStack(newSize))
            null

//    /// Get element from stack
//    /// <param name="elem">The location of where to store retrieved element</param>
//    /// <returns>An event which caller can wait upon in case stack has no more elements and cannot grow</returns>
//    member x.GetElem(elem : 'T ref) =
//        let mutable event = null
//        while (Utils.IsNull event) && (not (stack.TryPop(elem))) do
//            let prevSize = size
//            if (stack.IsEmpty) then
//                event <- x.ExpandStack(prevSize)
//        event

    /// Get element from stack
    /// <param name="elem">The location of where to store retrieved element</param>
    /// <returns>An event which caller can wait upon in case stack has no more elements and cannot grow</returns>
    member x.GetElem(elem : 'T byref) =
        let mutable event = null
        while (Utils.IsNull event) && (not (stack.TryPop(&elem))) do
            let prevSize = size
            if (stack.IsEmpty) then
                event <- x.ExpandStack(prevSize)
        event
    /// Get element from stack
    /// <returns>
    /// A tuple of (event, elem)
    /// event: An event which caller can wait upon in case stack has no more elements and cannot grow
    /// elem: An element from the stack
    /// </returns>
    member x.GetElem() =
        let mutable elem : 'T = Unchecked.defaultof<'T>
        let event = x.GetElem(&elem)
        (event, elem)

    /// Release element back to stack
    /// <param name="elem">The element to return to the stack</param>
    member x.ReleaseElem(elem : 'T) =
        stack.Push(elem)
        notEmpty.Set() |> ignore

    interface IDisposable with
        member x.Dispose() = 
            notEmpty.Dispose()
            GC.SuppressFinalize(x)
