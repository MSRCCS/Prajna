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

/// <summary> Parameters related to Prajna remote running environment </summary>
type RemoteExecutionEnvironment() = 
    /// <summary> Obtain the MachineName that the Prajna Program is executed upon. </summary>
    static member MachineName with get() = Environment.MachineName
    /// Obtain the remote container name that the Prajna program is executed upon. 
    static member val ContainerName="" with get, set
    /// Obtain the log folder that is used by Prajna
    static member GetLogFolder() = 
        DeploymentSettings.LogFolder
    /// Obtain the folder of the service store
    static member GetServiceFolder() = 
        DeploymentSettings.ServiceFolder
