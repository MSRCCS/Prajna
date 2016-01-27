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
		distributedfunction.fs
  
	Description: 
		Test for distributed function

	Author:											
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com

    Date:
        Aug. 2015
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Core.Tests

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Threading
open System.Threading.Tasks

open NUnit.Framework

open Prajna.Core
open Prajna.Tools
open Prajna.Tools.Network

open Prajna.Tools.FSharp
open Prajna.Service
open Prajna.Service.FSharp

open Prajna.Api.FSharp
open Prajna.Core.Tests

[<TestFixture(Description = "Tests for Distributed functions")>]
type DistributedFunctionTest() =
    inherit Prajna.Test.Common.Tester()
    let cluster = TestSetup.SharedCluster
    let clusterSize = TestSetup.SharedClusterSize   
    // The lines below trigger cluster setup
//    let cluster = TestSetup.SharedCluster.GetSingleNodeCluster(0)
//    let clusterSize = 1 
//    let storeName = "TestKV"
//    let serverInfo = ContractServersInfo( Cluster = cluster )
    do 
//        let provider = DistributedFunctionProvider(
//                            PublicID = Guid("{630C8E4D-69A4-4493-BFCD-03A548581631}"), 
//                            PrivateID = Guid( "{049EE851-3190-42BB-BBCF-589ED56E048E}") 
//                       )
//        DistributedFunctionStore.Current.RegisterProvider( provider )
        ()

    static member IncrementAction() = 
        let countRef = ref 0
        let incrementAction() = 
            countRef := !countRef + 1
        let finalValueFunc() = 
            !countRef
        incrementAction, finalValueFunc

    static member Add2Function(addValue: int ) = 
        let countRef = ref 0
        let addFunc( i ) = 
            countRef := !countRef + 1
            i + addValue
        let numberAddedFunc() = 
            !countRef
        addFunc, numberAddedFunc


    [<Test(Description = "Test: Increment without lock (as a perf benchmark)")>]
    member x.DistributedActionBenchmarkTest() = 
        let ticks = DateTime.UtcNow.Ticks 
        let incrementAction, finalValueFunc = DistributedFunctionTest.IncrementAction()
        Parallel.For( 0, 1000, Action<int>(fun _ -> incrementAction() ) ) |> ignore 
        let result = finalValueFunc()
        let elapse = ( DateTime.UtcNow.Ticks - ticks ) / TimeSpan.TicksPerMillisecond
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Local Parallel For = %d (%dms)" result elapse )

    member x.DistributedActionLocal( concurrencyLevel, niterations, bCheck ) = 
        DistributedFunctionStore.Current.ConcurrentCapacity <- concurrencyLevel
        let name = sprintf "Increment with concurrency level %d and %d iterations" concurrencyLevel niterations
        let incrementAction, finalValueFunc = DistributedFunctionTest.IncrementAction()
        // The distributed action will be deregistered at the end of the call (use) 
        use disposer = DistributedFunctionStore.Current.RegisterUnitAction( name, incrementAction )
        let schemas = disposer.GetSchemas()
        Assert.GreaterOrEqual( Seq.length schemas, 1 )
        for schema in schemas do 
            let provider, domainID, schemaIn, schemaOut = schema
            let infoIn = JobDependencies.GetSchemaInformation( schemaIn )
            let infoOut = JobDependencies.GetSchemaInformation( schemaOut )
            Logger.LogF( LogLevel.Info, fun _ -> sprintf "Register Action of %s, schemaIn = %s, schemaOut = %s" name infoIn infoOut )
            Assert.IsNullOrEmpty( infoIn )
            Assert.IsNullOrEmpty( infoOut )
        let executeUnitAction = DistributedFunctionStore.Current.TryImportUnitActionLocal( name )
        let ticks = DateTime.UtcNow.Ticks 
        Parallel.For( 0, niterations, Action<int>(fun _ -> executeUnitAction() ) ) |> ignore 
        let result = finalValueFunc()
        let elapse = ( DateTime.UtcNow.Ticks - ticks ) / TimeSpan.TicksPerMillisecond
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Parallel Distributed function of %d increment of 1 with concurrency level %d, result = %d (%dms)" niterations concurrencyLevel result elapse )
        if bCheck then 
            // We purposefully designed a inner function which is not multi-thread safe, 
            // Only when the concurrency level is 1, the function will return 1 result as if the function is running in single thread. 
            // At all other concurrency level, the return result is uncertain 
            Assert.AreEqual( result, niterations )

        let executeUnitActionAsync = DistributedFunctionStoreAsync.Current.TryImportUnitActionLocal( name )
        let input = Array.init niterations Operators.id
        let ticks = DateTime.UtcNow.Ticks
        let tasks = input |> Array.map( fun i -> executeUnitActionAsync() )
        tasks |> Task.WaitAll
        let result = finalValueFunc()
        let elapse = ( DateTime.UtcNow.Ticks - ticks ) / TimeSpan.TicksPerMillisecond
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Parallel Distributed function of %d increment( async mode) of 1 with concurrency level %d, result = %d (%dms)" niterations concurrencyLevel result elapse )
        if bCheck then 
            // We purposefully designed a inner function which is not multi-thread safe, 
            // Only when the concurrency level is 1, the function will return 1 result as if the function is running in single thread. 
            // At all other concurrency level, the return result is uncertain 
            Assert.AreEqual( result, niterations * 2 )

    [<Test(Description = "Test: Distributed Action local")>]
    member x.DistributedUnitActionLocalTest() = 
        let cnt0 = DistributedFunctionStore.Current.NumberOfRegistered()
        x.DistributedActionLocal( 1, 1, true )
        // no capacity check, no concurrency garantee
        x.DistributedActionLocal( 0, 1000, false )
        // capacity check, no concurrency garantee
        x.DistributedActionLocal( Int32.MaxValue, 1000, false )
        // capacity check, no concurrency garantee
        x.DistributedActionLocal( 1, 1000, true )
        let cnt1 = DistributedFunctionStore.Current.NumberOfRegistered()
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Number of distributed functions registered is %d(before) and %d (after)." cnt0 cnt1 )
        Assert.AreEqual( cnt0, cnt1 )

    member x.DistributedFunctionLocal( concurrencyLevel, niterations, addValue, bCheck ) = 
        DistributedFunctionStore.Current.ConcurrentCapacity <- concurrencyLevel
        let name = sprintf "Add with concurrency level %d and %d iterations" concurrencyLevel niterations
        let addFunc, numberAddedFunc = DistributedFunctionTest.Add2Function(addValue)
        // The function should be installed always (let, dispose is not called). 
        let disposer = DistributedFunctionStore.Current.RegisterFunction<_,_>( name, addFunc )
        let schemas = disposer.GetSchemas()
        Assert.GreaterOrEqual( Seq.length schemas, 3 )
        for schema in schemas do 
            let provider, domainID, schemaIn, schemaOut = schema
            let infoIn = JobDependencies.GetSchemaInformation( schemaIn )
            let infoOut = JobDependencies.GetSchemaInformation( schemaOut )
            Logger.LogF( LogLevel.Info, fun _ -> sprintf "Register Distributed Function of %s, schemaIn = %s, schemaOut = %s" name infoIn infoOut )
            Assert.IsNotNullOrEmpty( infoIn )
            Assert.IsNotNullOrEmpty( infoOut )

        let executeFunction = DistributedFunctionStore.Current.TryImportFunctionLocal<int,int>( name )
        let ticks = DateTime.UtcNow.Ticks 
        Parallel.For( 0, niterations, Action<int>(fun i -> Assert.AreEqual( executeFunction(i), i+addValue) ) ) |> ignore 
        let cnt = numberAddedFunc()
        let elapse = ( DateTime.UtcNow.Ticks - ticks ) / TimeSpan.TicksPerMillisecond
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Parallel Distributed function of %d addition of %d with concurrency level %d, result = %d (%dms)" niterations addValue concurrencyLevel cnt elapse )
        if bCheck then 
            // We purposefully designed a inner function which is not multi-thread safe, 
            // Only when the concurrency level is 1, the function will return 1 result as if the function is running in single thread. 
            // At all other concurrency level, the return result is uncertain 
            Assert.AreEqual( cnt, niterations )

        let executeFunctionAsync = DistributedFunctionStoreAsync.Current.TryImportFunctionLocal<int,int>( name )
        let ticks = DateTime.UtcNow.Ticks
        let input = seq { 0 .. niterations - 1 }
        input 
        |> Seq.map( fun i -> async {   let! res = Async.AwaitTask(executeFunctionAsync(i)) 
                                    Assert.AreEqual( res, i + addValue )
                                    })
        |> Async.Parallel |> Async.RunSynchronously |> ignore
        let cnt = numberAddedFunc()
        let elapse = ( DateTime.UtcNow.Ticks - ticks ) / TimeSpan.TicksPerMillisecond
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Parallel Distributed function of %d addition of %d with concurrency level %d, result = %d (%dms)" niterations addValue concurrencyLevel cnt elapse )
        if bCheck then 
            // We purposefully designed a inner function which is not multi-thread safe, 
            // Only when the concurrency level is 1, the function will return 1 result as if the function is running in single thread. 
            // At all other concurrency level, the return result is uncertain 
            Assert.AreEqual( cnt, niterations * 2 )


    [<Test(Description = "Test: Distributed Function Local")>]
    member x.DistributedFunctionLocalTest() = 
        let cnt0 = DistributedFunctionStore.Current.NumberOfRegistered()
        x.DistributedFunctionLocal( 1, 1, 2, true )
        // capacity check, no concurrency garantee
        x.DistributedFunctionLocal( 1, 1000, 2, true )
        // no capacity check, no concurrency garantee
        x.DistributedFunctionLocal( 0, 1000, 2, false )
        // capacity check, no concurrency garantee
        x.DistributedFunctionLocal( Int32.MaxValue, 1000, 2, false )
        let cnt1 = DistributedFunctionStore.Current.NumberOfRegistered()
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Number of distributed functions registered is %d(before) and %d (after)." cnt0 cnt1 )
        Assert.Greater( cnt1, cnt0 )

    static member val WaitForConnectedContainersInMilliseconds = 2000 with get

    static member WaitForContainerLaunch() = 
        let startTime = Prajna.Test.Common.TestEnvironment.Environment.Value.StartTime
        let curTime = DateTime.UtcNow
        let elapse = curTime.Subtract( startTime ).TotalMilliseconds
        let waitTime = Math.Max( 0, DistributedFunctionTest.WaitForConnectedContainersInMilliseconds - int elapse )
        if waitTime > 0 then 
            Logger.LogF( LogLevel.Info, fun _ -> sprintf "Wait for %d ms for the containers to get started" waitTime )
            Thread.Sleep( waitTime )

    [<Test(Description = "Test: Builtin Function: GetContainers")>]
    member x.DistributedFunctionGetConnectedContainers() = 
        DistributedFunctionTest.WaitForContainerLaunch()
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Start to execute GetConnectedContainers()"  )
        let numTries = 5 
        let mutable bValidMeasure = false 
        for i = 0 to numTries do 
            let containers = DistributedFunctionBuiltIn.GetConnectedContainers() |> Seq.toArray
            Assert.AreEqual( TestSetup.SharedCluster.NumNodes, containers.Length )
            Logger.LogF( LogLevel.Info, fun _ -> sprintf "Number of connected containers is %d: %A" containers.Length containers )
            let perfArray = DistributedFunctionBuiltIn.GetBuiltInFunctionPerformance()
            Assert.AreEqual( TestSetup.SharedCluster.NumNodes, perfArray.Length )
            for entry in perfArray do 
                let signature, perf = entry
                Logger.LogF( LogLevel.Info, fun _ -> sprintf "Performance of containers %s: %s, latency = %s" 
                                                        (LocalDNS.GetHostInfoInt64(signature))
                                                        (perf.QueueInfo()) 
                                                        (perf.ExpectedLatencyInfo()) )
                if not bValidMeasure then 
                    /// We have got some measurement 
                    bValidMeasure <- perf.ExpectedLatencyInMS<>ServiceEndpointPerformance.DefaultExpectedLatencyInMS
        Assert.IsTrue( bValidMeasure )                

    [<Test(Description = "Test: Builtin Function: TriggerRemoteException")>]
    member x.DistributedFunctionTriggerRemoteException() = 
        DistributedFunctionTest.WaitForContainerLaunch()
        let exRemote = ref null
        try 
            let string = DistributedFunctionBuiltIn.TriggerRemoteException() 
            // The function should not peacecully exit, a remote exception should be triggered and thrown 
            ()
        with 
        | ex -> 
            exRemote := ex
        Assert.IsNotNull( exRemote ) 
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Expected remote exception of %A" !exRemote)
        ()