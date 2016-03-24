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
		network.fs
  
	Description: 
		Test for network

	Author:											
        Sanjeev Mehrotra

    Date:
        March 2016
---------------------------------------------------------------------------*)
namespace Prajna.Core.Tests

open System
open System.IO
open System.Net.Sockets
open NUnit.Framework

open Prajna.Core
open Prajna.Tools
open Prajna.Tools.Network

[<TestFixture(Description = "Tests for Network")>]
type NetworkTest() =
    inherit Prajna.Test.Common.Tester()
    let cluster = TestSetup.SharedCluster
    let clusterSize = TestSetup.SharedClusterSize

    [<Test(Description = "Test: Illegal Network Connection")>]
    member x.IllegalNetworkConnectionTest() =
        let remoteName = cluster.Nodes.[0].MachineName
        let remotePort = cluster.Nodes.[1].MachinePort
        let socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp)
        let addr = LocalDNS.GetAnyIPAddress(remoteName, true)
        let arrSend = Array.init<byte> 1024 (fun _ -> 0xffuy)

        socket.Connect(addr, remotePort)
        let mutable offset = 0
        let mutable rem = arrSend.Length
        while (rem > 0) do
            let sent = socket.Send(arrSend, offset, rem, SocketFlags.None)
            offset <- offset + sent
            rem <- rem - sent

        Assert.AreEqual(1,1)
