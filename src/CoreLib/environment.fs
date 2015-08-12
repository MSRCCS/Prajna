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
open Prajna.Tools
open Prajna.Tools.FSharp

/// Represent Prajna Environment for running Prajna program
type Environment() =

    /// Initialize Prajna Environment for running Prajna program
    /// Currently, it's required to be called only if one needs to create a local cluster via Cluster
    static member Init () = 
        Cluster.SetCreateLocalCluster(LocalCluster.Create)

    /// Cleanup Prajna Environment for running Prajna program
    static member Cleanup() =
        Logger.Log( LogLevel.Info, "Cleanup the environment" )
        CleanUp.Current.CleanUpAll()
