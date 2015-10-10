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
		Utils.fs
  
	Description: 
		Utilities

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Jul. 2013
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.Runtime.CompilerServices
open System.Collections.Generic
open System.Security.Principal

/// <summary>
/// Construct a comparer that uses Object.ReferenceEquals to compare object. 
/// </summary>
type ReferenceComparer<'T>()=
    interface IEqualityComparer<'T> with
        override this.Equals (x, y) = 
            ( Object.ReferenceEquals( x, y )  )
        override this.GetHashCode (x) = 
            RuntimeHelpers.GetHashCode( x )

/// Utilities
module Utils =
    // Null checks
    // See: http://latkin.org/blog/2015/05/18/null-checking-considerations-in-f-its-harder-than-you-think/

    /// Returns true if 'value' is null
    let inline IsNull value = obj.ReferenceEquals(value, null)

    /// Returns true if 'value' is not null
    let inline IsNotNull value = not (obj.ReferenceEquals(value, null))

    /// Returns hash code for 'v'
    let internal GetHashCode<'T> (v : 'T) = let h =
                                                if Operators.typedefof<'T>.IsValueType then 
                                                    box(v).GetHashCode() 
                                                else 
                                                    Runtime.CompilerServices.RuntimeHelpers.GetHashCode(v)
                                            h &&& Int32.MaxValue



/// Runtime related information
module Runtime =
    /// Boolean variable to test if the code is running on mono. 
    let RunningOnMono = try Utils.IsNotNull (System.Type.GetType("Mono.Runtime")) with e -> false

/// Convert System.Func to FSharpFunc
[<Extension>]
type internal FuncUtil = 

    [<Extension>] 
    static member ToFSharpFunc (action : Action) = fun () -> action.Invoke()

    [<Extension>] 
    static member ToFSharpFunc<'a> (action : Action<'a>) = FuncConvert.ToFSharpFunc(action)

    [<Extension>] 
    static member ToFSharpFunc<'a, 'b> (action : Action<'a, 'b>) = fun a b -> action.Invoke(a, b)

    [<Extension>] 
    static member ToFSharpFunc<'a, 'b, 'c> (action : Action<'a, 'b, 'c>) = fun a b c -> action.Invoke(a, b, c)

    [<Extension>] 
    static member ToFSharpFunc<'a, 'b, 'c, 'd> (action : Action<'a, 'b, 'c, 'd>) = fun a b c d -> action.Invoke(a, b, c, d)

    [<Extension>] 
    static member ToFSharpFunc<'a, 'b, 'c, 'd, 'e> (action : Action<'a, 'b, 'c, 'd, 'e>) = fun a b c d e-> action.Invoke(a, b, c, d, e)

    [<Extension>] 
    static member ToFSharpFunc<'a> (func : Func<'a>) = fun () -> func.Invoke()

    [<Extension>] 
    static member ToFSharpFunc<'a,'b> (func : Func<'a,'b>) = fun a -> func.Invoke(a)

    [<Extension>] 
    static member ToFSharpFunc<'a,'b,'c> (func : Func<'a,'b,'c>) = fun a b -> func.Invoke(a, b)

    [<Extension>] 
    static member ToFSharpFunc<'a,'b,'c,'d> (func : Func<'a,'b,'c,'d>) = fun a b c -> func.Invoke(a, b, c)

    [<Extension>] 
    static member ToFSharpFunc<'a,'b,'c,'d, 'e> (func : Func<'a,'b,'c,'d, 'e>) = fun a b c d  -> func.Invoke(a, b, c, d)

    [<Extension>] 
    static member ToFSharpFunc<'a,'b,'c,'d, 'e, 'f> (func : Func<'a,'b,'c,'d, 'e, 'f>) = fun a b c d e -> func.Invoke(a, b, c, d, e)

// Utilities for directory
// Note: may move to a different file
module internal DirUtils =
    open System.IO
    open System.Security.AccessControl

    /// Return DirectoryInfo, create the directory if it doesn't exist. 
    /// The directory created this way will allow access control by everyone to ease use in cluster scenario. 
    let DirectoryInfoCreateIfNotExists dir =
        if String.IsNullOrEmpty dir then 
            null 
        else
            let mutable dirInfo = DirectoryInfo(dir)
            if not dirInfo.Exists then 
                let everyoneSid = SecurityIdentifier( WellKnownSidType.WorldSid, null )
                try 
                    Directory.CreateDirectory(dir) |> ignore
                    if not Runtime.RunningOnMono then
                        let fSecurity = File.GetAccessControl( dir ) 
                        fSecurity.AddAccessRule( new FileSystemAccessRule( everyoneSid, FileSystemRights.FullControl, AccessControlType.Allow ) )
                        File.SetAccessControl( dir, fSecurity )
                    dirInfo <- new DirectoryInfo(dir)
                    if not Runtime.RunningOnMono then
                        let dSecurity = dirInfo.GetAccessControl()
                        dSecurity.AddAccessRule( new FileSystemAccessRule( everyoneSid, FileSystemRights.FullControl, AccessControlType.Allow ) )
                        dirInfo.SetAccessControl( dSecurity )
                with 
                | e -> 
                    Threading.Thread.Sleep( 10 )
                    dirInfo <- new DirectoryInfo(dir)
                    if not dirInfo.Exists then 
                        reraise()
            dirInfo
