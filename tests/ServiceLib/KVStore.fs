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
		setup.fs
  
	Description: 
		Test for Key-Value Store

	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com

    Date:
        Aug. 2015
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.KVStoreService.Tests

open System
open System.Collections.Generic
open System.Diagnostics
open System.IO
open System.Threading

open NUnit.Framework

open Prajna.Core
open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Service.FSharp
open Prajna.Service.KVStoreService

open Prajna.ServiceLib.Tests
open Prajna.Api.FSharp

[<TestFixture(Description = "Tests for KV Store service")>]
[<Ignore("This part is undergoing a re-write, ignores for now")>]
type KVStoreServiceTests () =
    inherit Prajna.Test.Common.Tester()

    // The lines below trigger cluster setup
    let cluster = TestSetup.SharedCluster.GetSingleNodeCluster(0)
    let clusterSize = 1  
    let storeName = "TestKV"
    let serverInfo = ContractServersInfo( Cluster = cluster )

    [<Test(Description = "Start a service remotely")>]
    member x.KVStoreServiceStart() =
        KVStore<string,int>.StartService( storeName, 
                                            serverInfo, 
                                            StringComparer.OrdinalIgnoreCase, 
                                            (fun _ -> 0), (fun (k,v,dv) -> v + dv), false )
        let store = KVStore<string,int>.GetKVStore(storeName, serverInfo )
        let store1 = KVStore<string,int>.GetKVStore(storeName, serverInfo )

        Assert.AreSame( store, store1)
        store.Clear()
        Assert.AreEqual( store.Get( "A" ), 0 )
        store.Update( "a", 1 )
        Assert.AreEqual( store.Get( "A" ), 1 )
        store.Put( "a", 2)
        Assert.AreEqual( store.Get( "A" ), 2 )
        KVStore<string,int>.StopService( storeName, 
                                            serverInfo ) 
        KVStore<string,int>.StartService( storeName, 
                                            serverInfo, 
                                            StringComparer.OrdinalIgnoreCase, 
                                            (fun _ -> 0), (fun (k,v,dv) -> v + dv), false )
        Assert.AreEqual( store.Get( "A" ), 2 )
        store.Clear()
        KVStore<string,int>.StopService( storeName, 
                                            serverInfo ) 
                        

        ()
