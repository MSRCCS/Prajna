(*---------------------------------------------------------------------------
    Copyright 2015 Microsoft

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
        DistributedFunctionFSharp.fs
        FSharp API for Distributed Function

    Description: 

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Oct. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.CSharp

open Prajna.Core
open Prajna.Service

/// Infromation of distributed Function Provider 
/// To use distributed function, 
type DistributedFunctionProvider() = 
    inherit Prajna.Service.DistributedFunctionProvider()

///// Govern the behavior of the default serialization to be used 
//type DefaultSerializerForDistributedFunction = 
//    inherit Prajna.Service.DefaultSerializerForDistributedFunction

/// Will move the external F# function once the API stabelize