namespace Prajna.Core.Tests

open Prajna.Core
open Prajna.Test.Common

type TestSetup private ()=
   static member val SharedCluster : Cluster = TestEnvironment.Environment.Value.LocalCluster
   static member val SharedClusterSize = TestSetup.SharedCluster.NumNodes

   
