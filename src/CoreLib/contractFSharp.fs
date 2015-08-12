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
        contractFSharp.fs
  
        FSharp API for Contract
    Description: 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.FSharp

open Prajna.Core
open Prajna.Service
open System
open System.Threading.Tasks
open System.Collections.Generic

/// Specify servers to be used to launch services, import and export contract
type ContractServersInfo = Prajna.Service.ContractServersInfo

/// Manifested list of servers to be used to launch services, import and export contract
/// This class is a local manifestation of ContractServersInfo that is to be used. 
type ContractServerInfoLocal = Prajna.Service.ContractServerInfoLocal

/// <summary>
/// ContractStore provides a central location for export/import contract
/// </summary>
type ContractStore internal () = 
    /// <summary>
    /// Export an action Action&lt;'T>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="act"> An action of type 'T -> unit to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    static member ExportAction<'T>( name, act: 'T->unit, bReload) = 
        ContractStore.Current.ExportAction( name, Action<_>(act), bReload ) 
    /// <summary>
    /// Export as a function Func&lt;'T,'TResult>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type 'T -> 'TResult to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    static member ExportFunction<'T,'TResult>( name, func:'T -> 'TResult, bReload ) = 
        ContractStore.Current.ExportFunction<_,_>( name, Func<_,_>(func), bReload  )
    /// <summary>
    /// Export as a function Func&lt;'T,'TResult>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type unit -> 'TResult to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    static member ExportFunction<'TResult>( name, func:unit -> 'TResult, bReload ) = 
        ContractStore.Current.ExportFunction<_>( name, Func<'TResult>(func), bReload  )
    /// <summary>
    /// Export as a function unit -> seq&lt;'TResult>, the result can be imported and executed by Prajna data analytical pipeline. 
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type unit -> seq&lt;'TResult> to be exported </param>
    /// <param name="serializationLimit"> Parameter that controls granularity of serialization. If serializationLimit&lt;=0, the size is equivalent to Int32.MaxValue.
    ///      The export function will collect an array of size serializationLimit of 'TResult[] worth of data, and then send the result to the calling function. 
    /// </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportSeqFunction<'TResult>( name, func:unit -> seq<'TResult>, serializationLimit, bReload) = 
        ContractStore.Current.ExportSeqFunction<'TResult>( name, Func<IEnumerable<'TResult>>(func), serializationLimit, bReload )
    /// <summary>
    /// Export as a function 'T -> seq&lt;'TResult>, the result can be imported and executed by Prajna data analytical pipeline. 
    /// F# cannot overload this function with the form unit -> seq&lt;'TResult>, we hence use name to specify the difference. 
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type 'T -> seq&lt;'TResult> to be exported </param>
    /// <param name="serializationLimit"> Parameter that controls granularity of serialization. If serializationLimit&lt;=0, the size is equivalent to Int32.MaxValue.
    ///      The export function will collect an array of size serializationLimit of 'TResult[] worth of data, and then send the result to the calling function. 
    /// </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportSeqFunctionWithParam<'T,'TResult>( name, func:'T->seq<'TResult>, serializationLimit, bReload) = 
        ContractStore.Current.ExportSeqFunction<'T,'TResult>( name, Func<'T,IEnumerable<'TResult>>(func), serializationLimit, bReload )
    /// <summary>
    /// Export as a function that return a Task: 'T-> Task&lt;'TResult>
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportFunctionTaskWithParam<'T,'TResult>( name, func:'T->Task<'TResult>, bReload ) = 
        ContractStore.Current.ExportFunctionTask( name, func, bReload  )
    /// <summary>
    /// Export as a function that return a Task: unit -> Task&lt;'TResult>
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportFunctionTask<'TResult>( name, func:unit -> Task<'TResult>, bReload ) = 
        ContractStore.Current.ExportFunctionTask<'TResult>( name, Func<Task<'TResult>>(func), bReload  )
// Async Interface hasn't been implemented yet. 
//    /// <summary>
//    /// Export as a function that return a Task Func&lt;'T,Task&lt;'TResult>>
//    /// </summary>
//    /// <param name="name"> Name of the action to be exported </param>
//    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
//    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
//    static member ExportFunctionTask<'T,'TResult>( name, func:'T->Async<'TResult>, bReload ) = 
//        ContractStore.Current.ExportFunctionTask<_,_>( name, Func<_,_>(fun inp -> Async.StartAsTask(func inp)), bReload  )
//    /// <summary>
//    /// Export as a function that return a Task Func&lt;Task&lt;'TResult>>
//    /// </summary>
//    /// <param name="name"> Name of the action to be exported </param>
//    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
//    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
//    static member ExportFunctionTask<'TResult>( name, func:unit->Async<'TResult>, bReload ) = 
//        ContractStore.Current.ExportFunctionTask( name, Func<_>(fun () -> Async.StartAsTask(func())), bReload )


    // -------------------------------------------------------------------------------------------------------------------------------
    //      The following import combines both local contract and contract at servers. We use C# interface for usage by language other than F#. 
    // For contract at servers, it will install the contract locally, so that next import will be a simple concurrent dictionary lookup
    // -------------------------------------------------------------------------------------------------------------------------------

    /// <summary> 
    /// Import a Action from both local &amp; server, with name, input parameter, if successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportAction<'T>( serverInfo, name ) = 
        let act = ContractStore.Current.ImportAction<'T>( serverInfo, name )
        act.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionWithTimeout<'T,'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportFunctionWithTimeout<'T,'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionWithTimeout<'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportFunctionWithTimeout<'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTaskWithTimeout<'T,'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportFunctionTaskWithTimeout<'T,'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTaskWithTimeout<'TResult>( serverInfo, name ) = 
        let func = ContractStore.Current.ImportFunctionTaskWithTimeout<'TResult>( serverInfo, name ) 
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunctionWithTimeout<'T,'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportSeqFunctionWithTimeout<'T,'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunctionWithTimeout<'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportSeqFunctionWithTimeout<'TResult>( serverInfo, name )
        func.Invoke
    // -------------------------------------------------------------------------------------------------------------------------------
    //      The following import combines both local contract and contract at servers. Default timeout is used. 
    // -------------------------------------------------------------------------------------------------------------------------------
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunction<'T,'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportFunction<'T,'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunction<'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportFunction<'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTask<'T,'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportFunctionTask<'T,'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTask<'TResult>( serverInfo, name ) = 
        let func = ContractStore.Current.ImportFunctionTask<'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunction<'T,'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportSeqFunction<'T,'TResult>( serverInfo, name )
        func.Invoke
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunction<'TResult>( serverInfo, name )= 
        let func = ContractStore.Current.ImportSeqFunction<'TResult>( serverInfo, name )
        func.Invoke
