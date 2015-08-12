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
        memory.fs
  
    Description: 
        Memory & Garbage Collection Policy for Prajna

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        May. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core
open System
open System.Collections.Generic

open Prajna.Tools

/// Class to implement Garbage Collection Policy for Prajna 
type internal GCPolicy() = 
    /// For GC statistics
    static member val GCCount = 0 with get, set 
    static member val SumGCTime = 0. with get, set
    static member val MaxGCTime = 0. with get, set
    static member val MaxBlockingGCTime = 0. with get, set
    /// GC Statistics 
    /// Return avg & maximum time GC executes. 
    static member GetGCStatisticsInfo() = 
        let avg = if GCPolicy.GCCount>0 then GCPolicy.SumGCTime / float GCPolicy.GCCount else 0.
        let max = GCPolicy.MaxGCTime
        let maxb = GCPolicy.MaxBlockingGCTime
        GCPolicy.GCCount <- 0 
        GCPolicy.SumGCTime <- 0.
        GCPolicy.MaxGCTime <- 0.
        GCPolicy.MaxBlockingGCTime <- 0.
        avg, max, maxb
    /// GC Statistics 
    static member GetGCStatistics() = 
        let avg, max, maxb = GCPolicy.GetGCStatisticsInfo()
        sprintf "GC avg %0.3fms, max %0.3fms, maxb %0.3f" avg max maxb
    /// Last time GC.Collect() is called with no activity with full GC
    static member val LastGCAtNoActivity = (PerfDateTime.UtcNow()) with get, set 
    /// Last time GC.Collect() is called. 
    static member val LastGC = (PerfDateTime.UtcNow()) with get, set
    /// Call a GC collection 
    /// Activity level: 0, no activity, garbage collection performed on all levels. 
    ///                 1, always activity, short internal, perform a shallow garbage collection
    ///                 2, always activity, medium interval, perform a medium garbage collection
    ///                 3, always activity, long interval, a full garbage collection is executed
    static member internal CallGCCollect( activity_level ) = 
        GCPolicy.LastGC <- (PerfDateTime.UtcNow())
        let mutable bBlocking = false
        match activity_level with 
        | x when x <= 0 ->
            GCPolicy.LastGCAtNoActivity <- (PerfDateTime.UtcNow())
            GC.Collect( 2, System.GCCollectionMode.Default, true ) 
            bBlocking <- true
        | x when x = 1 -> 
            GC.Collect( 0, System.GCCollectionMode.Optimized, false ) 
//            bStatistics <- false // Don't count on non blocking call
        | x when x = 2 -> 
            GC.Collect( 1, System.GCCollectionMode.Optimized, false ) 
        | _ -> 
            // Garbage Collection
            GC.Collect( 2, System.GCCollectionMode.Optimized, false ) 
        if true then 
            let elapseMS = (PerfDateTime.UtcNow()).Subtract( GCPolicy.LastGC ).TotalMilliseconds
            GCPolicy.GCCount <- GCPolicy.GCCount + 1
            GCPolicy.SumGCTime <- GCPolicy.SumGCTime + elapseMS
            if bBlocking then 
                GCPolicy.MaxBlockingGCTime <- Math.Max( GCPolicy.MaxBlockingGCTime, elapseMS )  
            else
                GCPolicy.MaxGCTime <- Math.Max( GCPolicy.MaxGCTime, elapseMS )
        
    /// Try to schedule for a GC collection. 
    /// bActivity: whether any IO or operation at the moment
    /// Return: GC executed or not. 
    static member TryGCCollect1( bActivity ) = 
        if bActivity then 
            let timeNow = (PerfDateTime.UtcNow())
            let e1 = timeNow.Subtract( GCPolicy.LastGC ).TotalMilliseconds
            if e1 > DeploymentSettings.ShortGCInterval then 
                // We will need to do a GC, but at what level? 
                let e2 = timeNow.Subtract( GCPolicy.LastGCAtNoActivity ).TotalMilliseconds
                if e2 >= DeploymentSettings.LongGCInterval then 
                    GCPolicy.CallGCCollect( 2 )
                    true
                else
                    GCPolicy.CallGCCollect( 1 )
                    true
            else
                // Short time since last GC, no GC operation is performed
                false
        else
            // No activity, execute a full GC
            // Both GC time will be reset
            GCPolicy.CallGCCollect( 0 )
            true     
    /// A more lazy GC policy, only initiated GC during no IO activity, and no more than 1 non blocking GC at a LongGCInterval. 
    static member TryGCCollect( bActivity ) = 
        match DeploymentSettings.GCMode with
        | NoGarbageCollection -> 
            false
        | GarbageCollectionWhenNoIO -> 
            if not bActivity then 
                let timeNow = (PerfDateTime.UtcNow())
                let e1 = timeNow.Subtract( GCPolicy.LastGC ).TotalMilliseconds
                if e1 > DeploymentSettings.LongGCInterval then 
                    // No activity, execute a full GC
                    // Both GC time will be reset
                    GCPolicy.CallGCCollect( 1 )
                    true     
                else
                    false
            else
                false
