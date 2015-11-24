(*---------------------------------------------------------------------------
	Copyright 2014 Microsoft

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
		process.fs
  
	Description: 
		Test for process.fs

	Author:																	
 		Jin Li, Partner Researcher Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com

    Date:
        Nov. 2015
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools.Tests

open System
open System.Threading
open System.Threading.Tasks
open System.Collections.Generic
open System.Runtime.Serialization
open System.IO

open NUnit.Framework

open Prajna.Tools
open Prajna.Tools.FSharp

[<TestFixture(Description = "Tests for process.fs")>]
type ProcessTests () =
    // The lines below trigger cluster setup
    [<Test(Description = "Test for class ExecuteEveryTrigger")>]
    member x.ProcessExecuteEveryTrigger() =
        let holder = ExecuteEveryTrigger<int>(LogLevel.WildVerbose)
        let numUnit = 100
        let numOperation = 100
        let arr = Array.init numUnit ( fun _ -> ref 0 )
        let actionUnit unitValue = Action<int>( fun addValue -> let refV = arr.[unitValue]
                                                                Interlocked.Add( refV, addValue ) |> ignore
                                              )
        let addActions() = 
            for i = 0 to numUnit-1 do
                holder.Add( actionUnit i, fun _ -> sprintf "Add on unit %d" i)
        let addOps() = 
            for i = 1 to numOperation do
                holder.Trigger( i, fun _ -> sprintf "param on unit %d" i)
        [| async{ addActions() }; async{ addOps() }; |] |> Async.Parallel |> Async.RunSynchronously |> ignore 
        let expValue = ( 1 + numOperation ) * numOperation / 2
        for i = 0 to numUnit-1 do
            let refV = arr.[i]
            let value = Volatile.Read( refV )
            Assert.AreEqual(value, expValue)

    member internal x.TestSafeCTSWrapperOnce(num, cancelAfterMS:int) =
        let cts = SafeCTSWrapper(cancelAfterMS)
        Parallel.For( 0, num, fun (i:int) state ->   use token = cts.Token 
                                                     if Utils.IsNotNull token then 
                                                        token.WaitHandle.WaitOne( i ) |> ignore 
        ) |> ignore 
        cts

    // Test for SafeCTSWrapper
    [<Test(Description = "Test for class SafeCTSWrapper")>]
    member x.ProcessSafeCTSWrapper() =
        let num = 10
        let tries = 5
        let cancelAfterMs = 2
        let maxWait = 10000L
        let ctsArray = Array.zeroCreate<_> tries
        let res = Parallel.For( 0, tries, fun i state -> ctsArray.[i] <- x.TestSafeCTSWrapperOnce( num,cancelAfterMs)
                              )
        let mutable confirmDisposed = false
        let ticksStart = DateTime.UtcNow.Ticks
        while not confirmDisposed && ( DateTime.UtcNow.Ticks - ticksStart )/TimeSpan.TicksPerMillisecond < maxWait do
            confirmDisposed <- true
            for i = 0 to tries - 1 do 
                let cts = ctsArray.[i]
                if Utils.IsNull cts || not cts.IsDisposed then 
                    confirmDisposed <- false
            if not confirmDisposed then 
                // Wait for job to finish and the disposing to kick in
                Thread.Sleep( 10 )
        if confirmDisposed then 
            Logger.LogF( LogLevel.MildVerbose, fun _ -> sprintf "SafeCTSWrapper resource disposed after %d ms" (( DateTime.UtcNow.Ticks - ticksStart )/TimeSpan.TicksPerMillisecond) )
        else
            Assert.Fail( sprintf "SafeCTSWrapper resource failes to be disposed after %d ms" (( DateTime.UtcNow.Ticks - ticksStart )/TimeSpan.TicksPerMillisecond) )
