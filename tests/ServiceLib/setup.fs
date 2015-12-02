namespace Prajna.ServiceLib.Tests

open Prajna.Core
open Prajna.Test.Common

type TestSetup private ()=
   static member val SharedCluster : Cluster = TestEnvironment.Environment.Value.Cluster
   static member val SharedClusterSize = TestSetup.SharedCluster.NumNodes
   
