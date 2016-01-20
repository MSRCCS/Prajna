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

type internal RemoteExecutionEnvironmentKindOf = 
    | ContainerEnvironment
    | DaemonEnvironment
    | ClientEnvironment

/// <summary> Parameters related to Prajna remote running environment </summary>
type RemoteExecutionEnvironment() = 
    static let remoteEnvironmentKindOf = lazy(
        if RemoteExecutionEnvironment.ContainerName.StartsWith("AppDomain:") || 
            RemoteExecutionEnvironment.ContainerName.StartsWith("Exe:") then 
            RemoteExecutionEnvironmentKindOf.ContainerEnvironment
        elif RemoteExecutionEnvironment.ContainerName.StartsWith("Daemon") then 
            RemoteExecutionEnvironmentKindOf.DaemonEnvironment
        else
            RemoteExecutionEnvironmentKindOf.ClientEnvironment
    
    )
    /// <summary> Obtain the MachineName that the Prajna Program is executed upon. </summary>
    static member MachineName with get() = Environment.MachineName
    /// Obtain the remote container name that the Prajna program is executed upon. 
    static member val ContainerName:string ="" with get, set
    /// Obtain the log folder that is used by Prajna
    static member GetLogFolder() = 
        DeploymentSettings.LogFolder
    /// Obtain the folder of the service store
    static member GetServiceFolder() = 
        DeploymentSettings.ServiceFolder
    static member internal GetExecutionEnvironment() = 
        remoteEnvironmentKindOf.Value
    /// Is the current execution environment a remote container
    static member IsContainer() = 
        remoteEnvironmentKindOf.Value = RemoteExecutionEnvironmentKindOf.ContainerEnvironment
    /// Is the current execution environment a client?
    static member IsClient() = 
        remoteEnvironmentKindOf.Value = RemoteExecutionEnvironmentKindOf.ClientEnvironment
    /// Is the current execution environment a daemon?
    /// We don't expect user code in daemon outside of the Core
    static member internal IsDaemon() = 
        remoteEnvironmentKindOf.Value = RemoteExecutionEnvironmentKindOf.DaemonEnvironment

