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
        IExample.fs
  
    Description: 
        Interface for examples 
---------------------------------------------------------------------------*)
namespace Prajna.Examples.Common

open System

/// The interface that every example should implement
type IExample =
    abstract member Description : string
    abstract member Run : Prajna.Core.Cluster -> bool

/// The attribute that identify an example that should not be ran as part of unit test suite
[<AttributeUsage(AttributeTargets.Class)>]
type SkipUnitTest ()=
    inherit Attribute()
