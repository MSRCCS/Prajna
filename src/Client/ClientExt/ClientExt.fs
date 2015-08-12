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

	File: 
		PrajnaClient.fs
  
	Description: 
		Prajna Client Extension module. Host Application that needs unmanaged code component that is not safe to place in AppDomain

	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Apr. 2014

    Argument:
        Should be called by PrajnaClient.exe only
        "-job %s -ver %d -mem %d -verbose %d" 
	
 ---------------------------------------------------------------------------*)
open System
open System.IO
open System.Diagnostics
open Prajna.Core

[<EntryPoint>]
let main orgargv = 
    ContainerLauncher.Main orgargv
