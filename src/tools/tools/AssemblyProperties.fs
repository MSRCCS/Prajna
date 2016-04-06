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
        AssemblyProperties.fs
  
    Description: 
        Assembly Properties

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Apr. 2015
    
 ---------------------------------------------------------------------------*)

namespace Prajna.Tools

open System.Reflection;
open System.Runtime.CompilerServices

module internal AssemblyProperties =

#if DEBUG
    [<assembly: AssemblyConfiguration("Debug")>]
#else
    [<assembly: AssemblyConfiguration("Release")>]
#endif 
    [<assembly: InternalsVisibleTo("Prajna")>]

    [<assembly: InternalsVisibleTo("PrajnaClient")>]

    [<assembly: InternalsVisibleTo("Prajna.WCFTools" )>]

    [<assembly: InternalsVisibleTo("Prajna.WPFTools" )>]

    [<assembly: InternalsVisibleTo("Prajna.Service.ServiceEndpoint" )>]
    
    [<assembly: InternalsVisibleTo("Prajna.Service.Gateway" )>]

    [<assembly: InternalsVisibleTo("Prajna.BasicService" )>]
    
    [<assembly: InternalsVisibleTo("Prajna.Tools.Tests" )>]
    
    // Temporary! PrajnaCopy is currently using internal stuff
    [<assembly: InternalsVisibleTo("PrajnaCopy" )>]
    // Temporary! SortBenchmark is currently using internal stuff
    [<assembly: InternalsVisibleTo("SortBenchmark")>]
    // Temporary! BinSort is currently using internal stuff
    [<assembly: InternalsVisibleTo("BinSort")>]

    do()
