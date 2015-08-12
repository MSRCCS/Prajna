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
        serviceCSharp.fs
  
    Description: 
        CSharp API for Contract

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2015
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Service.CSharp

open System
open System.Collections.Generic
open System.Threading.Tasks
open Prajna.Core
open Prajna.Service


/// Specify servers to be used to launch services, import and export contract
[<AllowNullLiteral; Serializable>]
type ContractServersInfo() = 
    inherit Prajna.Service.ContractServersInfo()

/// Manifested list of servers to be used to launch services, import and export contract
/// This class is a local manifestation of ContractServersInfo that is to be used. 
[<AllowNullLiteral>]
type ContractServerInfoLocal() = 
    inherit Prajna.Service.ContractServerInfoLocal()


/// <summary>
/// ContractStore provides a central location for export/import contract
/// </summary>
type ContractStore() = 

    /// <summary>
    /// Export an action Action&lt;'T>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="act"> An action of type Action&lt;'T> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    static member ExportAction<'T>( name, act: Action<'T>, bReload) = 
        ContractStore.Current.ExportAction( name, act, bReload ) 
    /// <summary>
    /// Export as a function Func&lt;'T,'TResult>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    static member ExportFunction<'T,'TResult>( name, func:Func<'T,'TResult>, bReload ) = 
        ContractStore.Current.ExportFunction<_,_>( name, func, bReload  )
    /// <summary>
    /// Export as a function Func&lt;'T,'TResult>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'TResult> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    /// </summary>
    static member ExportFunction<'TResult>( name, func:Func<'TResult>, bReload ) = 
        ContractStore.Current.ExportFunction<_>( name, func, bReload  )
    /// <summary>
    /// Export as a function Func&lt;seq&lt;'TResult>>, the result can be imported and executed by Prajna data analytical pipeline. 
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be exported </param>
    /// <param name="serializationLimit"> Parameter that controls granularity of serialization. If serializationLimit&lt;=0, the size is equivalent to Int32.MaxValue.
    ///      The export function will collect an array of size serializationLimit of 'TResult[] worth of data, and then send the result to the calling function. 
    /// </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportSeqFunction<'TResult>( name, func:Func<IEnumerable<'TResult>>, serializationLimit, bReload) = 
        ContractStore.Current.ExportSeqFunction<'TResult>( name, func, serializationLimit, bReload )
    /// <summary>
    /// Export as a function Func&lt;'T,seq&lt;'TResult>>, the result can be imported and executed by Prajna data analytical pipeline. 
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,'TResult> to be exported </param>
    /// <param name="serializationLimit"> Parameter that controls granularity of serialization. If serializationLimit&lt;=0, the size is equivalent to Int32.MaxValue.
    ///      The export function will collect an array of size serializationLimit of 'TResult[] worth of data, and then send the result to the calling function. 
    /// </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportSeqFunction<'T,'TResult>( name, func:Func<'T, IEnumerable<'TResult>>, serializationLimit, bReload) = 
        ContractStore.Current.ExportSeqFunction( name, func, serializationLimit, bReload )
    /// <summary>
    /// Export as a function that return a Task Func&lt;'T,Task&lt;'TResult>>
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportFunctionTask<'T,'TResult>( name, func:Func<'T,Task<'TResult>>, bReload ) = 
        ContractStore.Current.ExportFunctionTask( name, func, bReload  )
    /// <summary>
    /// Export as a function that return a Task Func&lt;Task&lt;'TResult>>
    /// </summary>
    /// <param name="name"> Name of the action to be exported </param>
    /// <param name="func"> A function of type Func&lt;'T,Task&lt;'TResult>> to be exported </param>
    /// <param name="bReload"> Whether allows Action reloading. If bReload is false, Prajna will throw an exception if an item of same name already exists in contract store. </param>
    static member ExportFunctionTask<'TResult>( name, func:Func<Task<'TResult>>, bReload ) = 
        ContractStore.Current.ExportFunctionTask( name, func, bReload  )
    // -------------------------------------------------------------------------------------------------------------------------------
    //      The following import combines both local contract and contract at servers. We use C# interface for usage by language other than F#. 
    // For contract at servers, it will install the contract locally, so that next import will be a simple concurrent dictionary lookup
    // -------------------------------------------------------------------------------------------------------------------------------

    /// <summary> 
    /// Import a Action from both local &amp; server, with name, input parameter, if successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportAction<'T>( serverInfo, name ) = 
        ContractStore.Current.ImportAction<'T>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionWithTimeout<'T,'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportFunctionWithTimeout<'T,'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionWithTimeout<'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportFunctionWithTimeout<'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTaskWithTimeout<'T,'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportFunctionTaskWithTimeout<'T,'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTaskWithTimeout<'TResult>( serverInfo, name ) = 
        ContractStore.Current.ImportFunctionTaskWithTimeout<'TResult>( serverInfo, name ) 
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunctionWithTimeout<'T,'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportSeqFunctionWithTimeout<'T,'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunctionWithTimeout<'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportSeqFunctionWithTimeout<'TResult>( serverInfo, name )
    // -------------------------------------------------------------------------------------------------------------------------------
    //      The following import combines both local contract and contract at servers. Default timeout is used. 
    // -------------------------------------------------------------------------------------------------------------------------------
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunction<'T,'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportFunction<'T,'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunction<'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportFunction<'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTask<'T,'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportFunctionTask<'T,'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportFunctionTask<'TResult>( serverInfo, name ) = 
        ContractStore.Current.ImportFunctionTask<'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunction<'T,'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportSeqFunction<'T,'TResult>( serverInfo, name )
    /// <summary> 
    /// Import a Function from both local &amp; server, with name, input parameter. The function has a timeout to control 
    /// remote execution behavior. If successfully imported, the function will be cached for faster reimporting. 
    /// </summary> 
    static member ImportSeqFunction<'TResult>( serverInfo, name )= 
        ContractStore.Current.ImportSeqFunction<'TResult>( serverInfo, name )
