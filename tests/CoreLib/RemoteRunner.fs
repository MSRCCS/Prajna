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
open Prajna.Service

[<TestFixture(Description = "Tests for Remote Runner")>]
[<Ignore("We are still working on this part")>]
type RemoteRunnerTest() =
    inherit Prajna.Test.Common.Tester()
    let cluster = TestSetup.SharedCluster
    let clusterSize = TestSetup.SharedClusterSize  

    [<Test(Description = "Test: Execute a function remotely")>]
    member x.RemoteFunction() = 
        let ticks = DateTime.UtcNow.Ticks 
        let addValue = 2
        let cnt = 5
        let runner = RemoteRunner( cluster )
        for i = 0 to cnt do 
            let j = runner.RunFunctionOnOne<int,int>( (fun x -> x + addValue), i ) 
            Assert.AreEqual( j, i + addValue )
        let elapse = ( DateTime.UtcNow.Ticks - ticks) / TimeSpan.TicksPerMillisecond
        Logger.LogF( LogLevel.Info, fun _ -> sprintf "Successfully completes %d remote add operation in %dms" cnt elapse )

