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
