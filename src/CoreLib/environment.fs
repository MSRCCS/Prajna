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

    Fle: 
        environment.fs
  
    Description: 
        Get parameters of the Prajna remote running environment. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jan. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Core

open System
open System.Threading
open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Service

/// Represent Prajna Environment for running Prajna program
type Environment() =
    static let init = lazy(
        Cluster.SetCreateLocalCluster(LocalCluster.Create)
        DistributedFunctionEnvironment.Init()
    )

    /// Initialize Prajna Environment for running Prajna program
    /// Currently, under the following scenario, Environment.Init() should be called explicitly by users:
    /// 1）Local cluster is used
    /// 2) Distributed function is used. 
    static member Init () = 
        init.Force()

    /// Cleanup Prajna Environment for running Prajna program
    static member Cleanup() =
        Logger.Log( LogLevel.Info, "Cleanup the environment" )
        CleanUp.Current.CleanUpAll()
