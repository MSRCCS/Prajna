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
        StringTools.fs
  
    Description: 
        Customized String Tools F#

    Author:																	
        Jin Li, Partner Research Manager
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Jul. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open System.Security.Cryptography
open System.IO
open System.Security.AccessControl
open System.Collections.Generic
open System.Text.RegularExpressions
open System.Runtime.Serialization

open Prajna.Tools

/// <summary>
/// Construct a comparer to quick compare String*'T. 
/// The class is constructured to take a comparer of string, and a type T that supports equality, and construct a new comparer
/// </summary>
type StringTComparer<'T when 'T:equality>=
    ///  comparer of string used. 
    val private _comp : StringComparer
    /// Constract a comparer that compares a tuple of String*'T, in which 'T supports equality, and comp is a comparer of string
    new  ( comp ) = { _comp = comp }
    interface IEqualityComparer<string*'T> with
        override this.Equals (x, y) = 
            let xstring, xval = x
            let ystring, yval = y
            ( this._comp.Equals( xstring, ystring ) && (xval = yval) )
        override this.GetHashCode (x) = 
            let xstring, xval = x
            if Utils.IsNull xstring then 
                xval.GetHashCode()
            else
                (17 * 23 + this._comp.GetHashCode( xstring )) * 23 + xval.GetHashCode()

type internal StringPlusClassComparer<'T> ( comp: StringComparer, cmpT, hashT ) =
     interface IEqualityComparer<string*'T> with
         override this.Equals (x, y) = 
            let xstring, xval = x
            let ystring, yval = y
            ( comp.Equals( xstring, ystring ) && (cmpT xval yval) )
         override this.GetHashCode (x) = 
            let xstring, xval = x
            if Utils.IsNull xstring then 
                (hashT xval)
            else
                (17 * 23 + comp.GetHashCode( xstring )) * 23 + (hashT xval)

type internal StringDateTimeTupleComparer( comp )  = 
    inherit StringPlusClassComparer<DateTime>( comp, (fun d1 d2 -> d1.Ticks = d2.Ticks), (fun d1 -> int d1.Ticks) )

/// <summary>
/// A set of helper routine for string operations
/// </summary>
module  StringTools =
    /// <summary> 
    /// Equivalent to String.IsNullOrEmpty, but our implementation is more efficient for the comparison to null. 
    ///     code in http://referencesource.microsoft.com/#mscorlib/system/string.cs,23a8597f842071f4 is follows. 
    ///     public static bool IsNullOrEmpty(String value) {
    ///         return (value = null || value.Length == 0);
    ///     }
    /// </summary> 
    let inline IsNullOrEmpty (p:string ) = 
        Utils.IsNull p || p.Length = 0

    /// String.Replace, but take a comparison 
    let ReplaceString (str:string) (oldValue:string) (newValue:string) (comparison:StringComparison )=
        let sb = System.Text.StringBuilder()
        let mutable bDoneReplacing = false
        let mutable previousIndex = 0
        while not bDoneReplacing do
            let index = str.IndexOf(oldValue, previousIndex, comparison)
            if index < 0 then 
                bDoneReplacing <- true    
            else
                sb.Append(str.Substring(previousIndex, index - previousIndex)) |> ignore
                sb.Append(newValue) |> ignore
                previousIndex <- index + oldValue.Length
        sb.Append(str.Substring(previousIndex)) |> ignore

        sb.ToString()

    /// If s:string starts with p:string, return Some ( rest of s:string ), otherwise, return None
    let internal Prefix (p: string) (s:string) =
        if s.StartsWith( p ) then
            Some (s.Substring(p.Length) )
        else
            None

    /// Case insensitive prefix match, if s:string starts with p:string, return Some ( rest of s:string ), otherwise, return None
    let internal Prefixi (p: string) (s:string) =
        if s.StartsWith( p, StringComparison.CurrentCultureIgnoreCase ) then
            Some (s.Substring(p.Length) )
        else
            None

    /// If s:string starts with p:string, return Some ( rest of s:string ), otherwise, return None
    let internal (|Prefix|_|) (p: string) (s:string) =
        if s.StartsWith( p ) then
            Some (s.Substring(p.Length) )
        else
            None

    /// Case insensitive prefix match, if s:string starts with p:string, return Some ( rest of s:string ), otherwise, return None
    let internal (|Prefixi|_|) (p: string) (s:string) =
        if s.StartsWith( p, StringComparison.CurrentCultureIgnoreCase ) then
            Some (s.Substring(p.Length) )
        else
            None

    let internal (|Exacti|_|) (p: string) (s:string) =
        if String.Compare( p, s, StringComparison.CurrentCultureIgnoreCase ) = 0 then
            Some ( p )
        else
            None

    /// Similar to Path.Combine, concatenate directory and filename, add \ if necessary. 
    let BuildFName (dir:string) (filename:string) = 
        Path.Combine(dir, filename)

    /// concatenate web URL and subsequent subdirectory, add / if necessary. 
    let BuildWebName (dir:string) (filename:string) = 
        if dir.Length=0 || dir.EndsWith("/") then
            dir + filename 
        else
            dir + "/" + filename

    /// Concatenate string[] to string with deliminator in between. 
    /// ToDo: use String.concat
    let ConcatWDelim (delim:char) (s:string[]) =
        Array.fold ( fun (res:string) x -> if res.Length = 0 then x else res+delim.ToString()+x ) "" s

    /// Find all directories of a sub directories 
    let internal SubDirectories dir =
        Directory.GetDirectories(dir)    
    
    /// Formulate a default set of file extension for pattern matching.
    /// Special Prefix F#, FSharp, C#, csharp, C++, 
    let internal BuildExts (exts:string[]) = 
        [|
            for ext in exts do 
                match ext with 
                | Prefixi "f#" v
                | Prefixi "fsharp" v
                    -> yield "fs" 
                       yield "fsi" 
                       yield "fsx" 
                       yield "fsy" 
                       yield "fsl" 
                       yield "fstemplate" 
                | Prefixi "c#" v
                | Prefixi "csharp" v
                    -> yield "cs" 
                       yield "h"
                | Prefixi "c++" v
                | Prefixi "cpp" v
                    -> yield "cpp"
                       yield "h"
                       yield "hpp"
                | v -> yield v
        |]

    let private GetHashCodeUInt64( s:string ) = 
        let sha = new SHA1CryptoServiceProvider()
        let data = System.Text.UTF8Encoding().GetBytes( s )
        let result = sha.ComputeHash( data )
        BitConverter.ToUInt64( result, 0 )

    let internal GetHashCodeQuickUInt64( s:string ) = 
        uint64 (StringComparer.Ordinal.GetHashCode(s))


    let inline internal HashString( s:string ) = 
        let sha256 = new SHA256Managed()
        let result = sha256.ComputeHash( System.Text.UTF8Encoding().GetBytes( s ) )
        UInt128( result )

    /// <summary>
    /// Hash a string via SHA256 hash, and then use the first 16B of hash to form a GUID 
    /// </summary>
    let inline HashStringToGuid( s:string ) = 
        let sha256 = new SHA256Managed()
        let result = sha256.ComputeHash( System.Text.UTF8Encoding().GetBytes( s ) )
        System.Guid( Array.sub result 0 16 )        

    /// <summary>
    /// Standard datetime format in Prajna
    /// </summary>
    let inline VersionToString( d: DateTime) = 
        d.ToString("yyMMdd_HHmmss.ffffff")

    /// <summary>
    /// Current timestamp in standard Prajna format. 
    /// </summary>
    let UtcNowToString() = 
        VersionToString( PerfADateTime.UtcNow() )

    let internal Version64ToString( d: int64) = 
        DateTime(d).ToString("yyMMdd_HHmmss.ffffff")

    /// <summary>
    /// Parse a string of standard datetime format in Prajna
    /// </summary>  
    let inline VersionFromString( s: string ) = 
        DateTime.ParseExact( s, "yyMMdd_HHmmss.ffffff", Globalization.CultureInfo.InvariantCulture )


    let internal SeqToString ( sep, d:seq<'T> ) =
        d |> Seq.map ( fun x -> x.ToString() ) |> String.concat sep 

    let internal check f x = if f x then x
                                else failwithf "format failure \"%s\"" x

    let internal parsers = dict [
                                 'd', int >> box
                                 'b', Boolean.Parse >> box
                                 's', box
                                 'i', int >> box
                                 'u', uint32 >> int >> box
                                 'X', check (String.forall Char.IsUpper) >> ((+) "0x") >> int >> box
                                 'x', check (String.forall Char.IsLower) >> ((+) "0x") >> int >> box
                                 'e', float >> box // no check for correct format for floats
                                 'o', ((+) "0o") >> int >> box
                                 'M', ( fun x -> Decimal.Parse(x, System.Globalization.CultureInfo.InvariantCulture) ) >> box
                                 'c', char >> box
                                 'f', float >> box
                                 'F', float >> box
                                 'E', float >> box
                                 'g', float >> box
                                 'G', float >> box
                                ]

    let internal seps =
       parsers.Keys
       // |> Seq.map (fun c -> sprintf "%%%c" c) // there's a bug in F# 3.1's sprintf/printf, see http://stackoverflow.com/questions/27820355/how-to-escape-in-printfn-sprintf
                                                 // use string concatenation as below, so it works in any version of F#
       |> Seq.map (fun c -> "%" + (sprintf "%c" c))
       |> Seq.toArray 
    
    let rec internal getFormat xs =
       match xs with
       | '%'::'%'::xr -> getFormat xr
       | '%'::x::xr -> if parsers.ContainsKey x then x::getFormat xr
                       else failwithf "Unknown formatter %%%c" x
       | x::xr -> getFormat xr
       | [] -> []
    
    
    let internal sscanf (pf:PrintfFormat<_,_,_,_,'t>) s : 't =
      let formatStr = pf.Value.Replace("%%", "%")
      let constants = formatStr.Split(seps, StringSplitOptions.None)
      let regex = Regex("^" + String.Join("(.*?)", constants |> Array.map Regex.Escape) + "$")
      let format = pf.Value.ToCharArray() 
                       |> Array.toList |> getFormat 
      let groups = 
        regex.Match(s).Groups 
        |> Seq.cast<Group> 
        |> Seq.skip 1
      let matches =
        (groups, format)
        ||> Seq.map2 (fun g f -> g.Value |> parsers.[f])
        |> Seq.toArray
    
      if matches.Length = 1 then matches.[0] :?> 't
      else Microsoft.FSharp.Reflection.FSharpValue.MakeTuple(matches, typeof<'t>) :?> 't

        
