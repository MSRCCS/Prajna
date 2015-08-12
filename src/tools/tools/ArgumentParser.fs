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
		ArgumentParser.fs
  
	Description: 
		Helpful function to parse arguments

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Apr. 2013
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools

open System
open StringTools
open Microsoft.Win32

/// Register key usage control  
type private RegKeyOps =
    /// Do not use register key 
    | Ignore = 0
    /// Read register key if exist 
    | Read = 1
    /// Read register key & write back the content as default argument after parsing
    | ReadAndWrite = 2

/// <summary>
/// Utility class to parse commandline argument. Each argument is in the form of -pattern param, and the programmer can supplied a default value for each parameter. Please note that the pattern is case insensitive prefix matched, so pattern
/// -dir will match "-directory" "-DIRECT". Please design your commandline argument pattern with different prefix. 
/// </summary>
type ArgumentParser internal (arguments : string[], shouldParseLoggerArgs : bool) =  
    
    /// Command line arguments to be used. 
    let args = arguments

    do
        if shouldParseLoggerArgs then
            Logger.ParseArgs(args)

    /// Register path under HKEY_CURRENT_USER used when read/write register key 
    static let [<Literal>] CurrentUserRegistryPath = "Software"
    
    /// Additional registry path under HKEY_CURRENT_USER\\CurrentUserRegistryPath
    static let [<Literal>] ComponentRegistryPath = "PrajnaTools"

    /// attemp to open registry path    
    static member private OpenRegistryPath (writable:bool) =      
        use keyuser = Registry.CurrentUser.OpenSubKey(CurrentUserRegistryPath, writable)           
        if Utils.IsNull keyuser then
            Logger.Fail( "Access failed to registry: CurrentUser\\" + CurrentUserRegistryPath )
        
        let path2 = keyuser.OpenSubKey(ComponentRegistryPath, writable)
        if Utils.IsNull path2 
            then 
                use writekeyuser = Registry.CurrentUser.OpenSubKey(CurrentUserRegistryPath,true)
                writekeyuser.CreateSubKey(ComponentRegistryPath)
            else
                path2             
           
    /// Store a key-value pair to windows register 
    static member private SetRegistryKeyString key (value:string) =
        use path = ArgumentParser.OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.String )

    /// Store a String[] of value to register key, concated with delimiter
    static member private SetRegistryKeyStrings key value delim =
        use path = ArgumentParser.OpenRegistryPath true
        path.SetValue( key, (ConcatWDelim delim value ), RegistryValueKind.String  )

    /// Store a value of int to register key
    static member private SetRegistryKeyInt key (value:int) =
        use path = ArgumentParser.OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.Binary )

    /// Store a value of bool to register key
    static member private SetRegistryKeyBoolean key (value:bool) =
        ArgumentParser.SetRegistryKeyInt key (Convert.ToInt32 value)

    /// Store a value of double to register key
    static member private SetRegistryKeyFloat key (value:float) =
        use path = ArgumentParser.OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.QWord )

    /// Store a value of int64 to register key
    static member internal SetRegistryKeyInt64 key (value:int64) = 
        use path = ArgumentParser.OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.QWord )

    /// Get Registry Key
    static member private GetRegistryKey<'T> (key:string) (defVal:'T) expType =
        if Utils.IsNull key || key.Length=0 then 
            defVal
        else
            use path = ArgumentParser.OpenRegistryPath false
            try
                let kind = path.GetValueKind(key)
                if kind = expType then
                    match path.GetValue(key, defVal ) with
                    | :? 'T as v -> v
                    | _ -> defVal
                else
                    Logger.Fail(sprintf "CurrentUser\\%s\\%s\\%s expects a %A key but holds %A" CurrentUserRegistryPath ComponentRegistryPath key expType kind )
                    defVal
            with
                | _ -> defVal


    /// Read a register key of type string, with a default value if register does not exist. 
    static member private GetRegistryKeyString key defVal = ArgumentParser.GetRegistryKey<string> key defVal RegistryValueKind.String 

    /// Read a register key of type String[] with a delimiter, if register does not exist, used the default value enclosed. 
    static member private GetRegistryKeyStrings key defVal delim =
        let regVals = ArgumentParser.GetRegistryKey<string> key "" RegistryValueKind.String 
        if regVals.Length > 0 then 
            regVals.Split(delim)
        else
            defVal

    /// Read a register key of type int, if register does not exist, used the default value. 
    static member private GetRegistryKeyInt key defVal = ArgumentParser.GetRegistryKey<int> key defVal RegistryValueKind.DWord
    
    /// Read a register key of type bool, if register does not exist, used the default value.
    static member private GetRegistryKeyBoolean key (defVal:bool) = 
        let v = ArgumentParser.GetRegistryKeyInt key (Convert.ToInt32 defVal)
        Convert.ToBoolean v

    /// Read a register key of type double, if register does not exist, used the default value.
    static member private GetRegistryKeyFloat key defVal = ArgumentParser.GetRegistryKey<float> key defVal RegistryValueKind.QWord

    /// Read a register key of type int64, if register does not exist, used the default value.
    static member internal GetRegistryKeyInt64 key defVal = ArgumentParser.GetRegistryKey<int64> key defVal RegistryValueKind.QWord

    /// To parse the commandline parameter related to traces for one parameters
    /// ParseArgs args lookfor registername default_val :
    ///      if lookfor exists in args, then use the next param in args, mark both as "", and 
    static member private ParseArgs<'T> (args:string[]) lookfor (defVal:'T) (parseFunc:string->'T) =
        let mutable i = 0
        let mutable ret = defVal
        let mutable bOptionExist = false
        while i < args.Length && not bOptionExist do
            match args.[i] with 
                | Prefixi lookfor v 
                    -> args.[i] <- ""
                       if i+1 < args.Length then 
                           ret <- parseFunc args.[i+1]
                           bOptionExist <- true
                           args.[i+1] <- ""
                | _ 
                    -> ()
            i<-i+1
        (ret, bOptionExist)
            
    /// Parse a command line argument of int32, if register does not exist, used the default value.
    static member private ParseArgsInt args lookfor defVal = ArgumentParser.ParseArgs<int> args lookfor defVal Convert.ToInt32

    /// 
    static member private ParseArgsInt64 args lookfor defVal = ArgumentParser.ParseArgs<int64> args lookfor defVal Convert.ToInt64

    ///
    static member private ParseArgsFloat args lookfor defVal = ArgumentParser.ParseArgs<float> args lookfor defVal Convert.ToDouble

    ///
    static member private ParseArgsString args lookfor defVal = ArgumentParser.ParseArgs<string> args lookfor defVal ( fun x -> x )

    /// Parse Boolean argument
    /// use -option on|true|off|false
    static member private ParseArgsBoolean (args:string[]) lookfor (defVal:bool) = 
        let mutable i = 0
        let mutable ret = defVal
        let mutable bOptionExist = false
        while i < args.Length do
            match args.[i] with 
                | Prefixi lookfor v 
                    -> args.[i] <- ""
                       bOptionExist <- true
                       if i+1 < args.Length then 
                           match args.[i+1] with
                           | Exacti "on" _
                           | Exacti "true" _
                                -> ret <- true
                                   args.[i+1] <- ""
                           | Exacti "off" _
                           | Exacti "false" _
                                -> ret <- false
                                   args.[i+1] <- ""
                           | _ 
                                -> ret <- true
                        else
                            ret <- true

                | _ 
                    -> ()
            i<-i+1
        ( ret, bOptionExist )

    /// Constructor that takes an array of string that represents the command line arguments
    new (arguments : string[]) =
        ArgumentParser(arguments, true)
    
    /// Return remaining args that is unparsed. 
    member this.RemainingArgs =
        Seq.ofArray(args) |> Seq.filter (fun e -> e <> "") |> Seq.toArray

    /// Does a certain argument exists
    member this.ArgExists arg = 
        let mutable i = 0
        let mutable bOptionExist = false
        while i < args.Length && not bOptionExist do
            match args.[i] with 
            | Prefixi arg v 
                -> bOptionExist <- true
            | _ 
                -> ()
            i<-i+1
        bOptionExist

    /// <summary>
    /// Parse a Boolean argument and optionally set the argument in Win32 register
    /// </summary>
    /// <param name="arg"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
    /// <param name="regKey"> optional registration key to arg to get/set default parameter </param>
    /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
    /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
    /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
    member private this.ParseArgWinRegBoolean arg regKey (defVal:bool) (setKey:RegKeyOps) =
        let mutable retVal = defVal
        match setKey with 
        | RegKeyOps.Read
        | RegKeyOps.ReadAndWrite 
            -> retVal <- ArgumentParser.GetRegistryKeyBoolean regKey retVal
        | _
            -> ()
        let retVal, bExistOption = ArgumentParser.ParseArgsBoolean args arg retVal
        if bExistOption && setKey = RegKeyOps.ReadAndWrite then
            ArgumentParser.SetRegistryKeyBoolean regKey retVal                
        retVal 

    /// <summary>
    /// Parse a int32 argument and optionally set the argument in Win32 register
    /// </summary>
    /// <param name="arg"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
    /// <param name="regKey"> optional registration key to arg to get/set default parameter </param>
    /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
    /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
    /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
    member private this.ParseArgWinRegInt arg regKey (defVal:int) (setKey:RegKeyOps) =
        let mutable retVal = defVal
        match setKey with 
        | RegKeyOps.Read
        | RegKeyOps.ReadAndWrite 
            -> retVal <- ArgumentParser.GetRegistryKeyInt regKey retVal
        | _
            -> ()
        let retVal, bExistOption = ArgumentParser.ParseArgsInt args arg retVal
        if bExistOption && setKey = RegKeyOps.ReadAndWrite then
            ArgumentParser.SetRegistryKeyInt regKey retVal                
        retVal 

    /// <summary>
    /// Parse a int64 argument and optionally set the argument in Win32 register
    /// </summary>
    /// <param name="arg"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
    /// <param name="regKey"> optional registration key to arg to get/set default parameter </param>
    /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
    /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
    /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
    member private this.ParseArgWinRegInt64 arg regKey (defVal:int64) (setKey:RegKeyOps) =
        let mutable retVal = defVal
        match setKey with 
        | RegKeyOps.Read
        | RegKeyOps.ReadAndWrite 
            -> retVal <- ArgumentParser.GetRegistryKeyInt64 regKey retVal
        | _
            -> ()
        let retVal, bExistOption = ArgumentParser.ParseArgsInt64 args arg retVal
        if bExistOption && setKey = RegKeyOps.ReadAndWrite then
            ArgumentParser.SetRegistryKeyInt64 regKey retVal                
        retVal 

    /// <summary>
    /// Parse a double argument and optionally set the argument in Win32 register
    /// </summary>
    /// <param name="arg"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
    /// <param name="regKey"> optional registration key to arg to get/set default parameter </param>
    /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
    /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
    /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
    member private this.ParseArgWinRegFloat arg regKey (defVal:float) (setKey:RegKeyOps) =
        let mutable retVal = defVal
        match setKey with 
        | RegKeyOps.Read
        | RegKeyOps.ReadAndWrite 
            -> retVal <- ArgumentParser.GetRegistryKeyFloat regKey retVal
        | _
            -> ()
        let retVal, bExistOption = ArgumentParser.ParseArgsFloat args arg retVal
        if bExistOption && setKey = RegKeyOps.ReadAndWrite then
            ArgumentParser.SetRegistryKeyFloat regKey retVal                
        retVal 

    /// <summary>
    /// Parse a string argument and optionally set the argument in Win32 register
    /// </summary>
    /// <param name="arg"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
    /// <param name="regKey"> optional registration key to arg to get/set default parameter </param>
    /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
    /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
    /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
    member private this.ParseArgWinRegString arg regKey (defVal:string) (setKey:RegKeyOps) =
        let mutable retVal = defVal
        match setKey with 
        | RegKeyOps.Read
        | RegKeyOps.ReadAndWrite 
            -> retVal <- ArgumentParser.GetRegistryKeyString regKey retVal
        | _
            -> ()
        let retVal, bExistOption = ArgumentParser.ParseArgsString args arg retVal
        if bExistOption && setKey = RegKeyOps.ReadAndWrite then
            ArgumentParser.SetRegistryKeyString regKey retVal                
        retVal 
    /// <summary>
    /// Parse a string[] argument and optionally set the argument in Win32 register. The parameter can be set using -arg param1,param2,param3 where , is the delimiter, 
    /// or using -arg param1 -arg param2 -arg param3
    /// </summary>
    /// <param name="arg"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
    /// <param name="regKey"> optional registration key to arg to get/set default parameter </param>
    /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
    /// <param name="delim"> delimiter to separate one argument into a string[] </param>
    /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
    /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
    member private this.ParseArgWinRegStrings arg regKey (defVal:string[]) delim (setKey:RegKeyOps) =
        let mutable retVal = defVal
        match setKey with 
        | RegKeyOps.Read
        | RegKeyOps.ReadAndWrite 
            -> retVal <- ArgumentParser.GetRegistryKeyStrings regKey retVal delim
        | _
            -> ()
        let mutable bExistOption = false
        let mutable bMoreOptions = true
        while bMoreOptions do
            let retSingleString, bExistOptionI = ArgumentParser.ParseArgsString args arg "" 
            bMoreOptions <- bExistOptionI
            let retStrings = retSingleString.Split( delim, StringSplitOptions.RemoveEmptyEntries )
            if bExistOptionI then
                if bExistOption then 
                    retVal <- (Array.append retVal retStrings )
                else
                    retVal <- retStrings
                bExistOption <- true
        if bExistOption && setKey = RegKeyOps.ReadAndWrite then
            ArgumentParser.SetRegistryKeyStrings regKey retVal delim.[0]
        retVal 

    /// <summary>
    /// Parse a Boolean argument, with a supplied default value
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseBoolean( arg, defVal) =
        this.ParseArgWinRegBoolean arg "" defVal RegKeyOps.Ignore

    /// <summary>
    /// Parse a int32 argument, with a supplied default value
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseInt( arg, defVal ) =
        this.ParseArgWinRegInt arg "" defVal RegKeyOps.Ignore

    /// <summary>
    /// Parse a double argument, with a supplied default value
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseFloat(arg, defVal) =
        this.ParseArgWinRegFloat arg "" defVal RegKeyOps.Ignore

    /// <summary>
    /// Parse a Int64 argument, with a supplied default value
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseInt64( arg, defVal) =
        this.ParseArgWinRegInt64 arg "" defVal RegKeyOps.Ignore

    /// <summary>
    /// Parse a float argument (double), with a supplied default value
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseDouble( arg, defVal ) =
        this.ParseArgWinRegFloat arg "" defVal RegKeyOps.Ignore

    /// <summary>
    /// Parse a string argument, with a supplied default value
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseString( arg, defVal ) =
        this.ParseArgWinRegString arg "" defVal RegKeyOps.Ignore

    /// <summary>
    /// Parse a string[] argument, where multiple values The parameter can be set using -arg param1,param2,param3 where , is the delimiter, 
    /// or using -arg param1 -arg param2 -arg param3
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseStrings( arg, defVal ) =
        this.ParseArgWinRegStrings arg "" defVal [|','|] RegKeyOps.Ignore

    /// <summary>
    /// Parse a int[] argument, where multiple values can appear by using  -arg param1 -arg param2 -arg param3. 
    /// <param name="arg"> pattern to arg </param>
    /// <param name="defValue"> default value when pattern does not exist </param>
    /// </summary> 
    member this.ParseInts( arg, defVal ) =
        let mutable bExistOption = true
        let mutable retVal = defVal
        while bExistOption do
            let (retValI, bExistOptionI) = ArgumentParser.ParseArgsInt args arg 0
            if bExistOptionI then
                retVal <- Array.append retVal [|retValI|]
            bExistOption <- bExistOptionI
        retVal

    /// print a usage statement, combined with default trace usage information. 
    member this.PrintUsage usage =
        printf "%s" usage
        Logger.PrintArgsUsage()
    
    /// Return true if all argument has been parsed, otherwise, return false, with usage stagement (including trace usage) printed. 
    member this.AllParsed usage =
        let remaining_args = args |> String.concat " "

        if args.Length>0 && remaining_args.Length >= args.Length then
            printfn "Unparsed arguments: %s" remaining_args
            this.PrintUsage usage
            false
        else
            true
