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
		ParseArgs.fs
  
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

/// A set of helper functions for command line parsing. 
module ParseArgs = 
    open System
    open StringTools
    open Microsoft.Win32

    /// Register Software used when read/write register key 
    let mutable REGISTRYSOFTWARE = "Software"
    /// Additional path used when read/write register key
    let mutable REGISTRYMYPATH = "JinLFSharp"

    /// Register key usage control  
    type RegKeyOps =
        /// Do not use register key 
        | Ignore = 0
        /// Read register key if exist 
        | Read = 1
        /// Read register key & write back the content as default argument after parsing
        | ReadAndWrite = 2

    /// attemp to open registry path    
    let internal OpenRegistryPath (writable:bool) =      
        use keyuser = Registry.CurrentUser.OpenSubKey(REGISTRYSOFTWARE, writable)           
        if Utils.IsNull keyuser then
            Logger.Fail( "Access failed to registry: CurrentUser\\" + REGISTRYSOFTWARE )
        
        let path2 = keyuser.OpenSubKey(REGISTRYMYPATH, writable)
        if Utils.IsNull path2 
            then 
                use writekeyuser = Registry.CurrentUser.OpenSubKey(REGISTRYSOFTWARE,true)
                writekeyuser.CreateSubKey(REGISTRYMYPATH)
            else
                path2             
           
    /// Store a key-value pair to windows register 
    let SetRegistryKeyString key (value:string) =
        use path = OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.String )

    /// Store a String[] of value to register key, concated with delimiter
    let SetRegistryKeyStrings key value delim =
        use path = OpenRegistryPath true
        path.SetValue( key, (ConcatWDelim delim value ), RegistryValueKind.String  )

    /// Store a value of int to register key
    let SetRegistryKeyInt key (value:int) =
        use path = OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.Binary )

    /// Store a value of bool to register key
    let SetRegistryKeyBoolean key (value:bool) =
        SetRegistryKeyInt key (Convert.ToInt32 value)

    /// Store a value of double to register key
    let SetRegistryKeyFloat key (value:float) =
        use path = OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.QWord )

    /// Store a value of int64 to register key
    let SetRegistryKeyInt64 key (value:int64) = 
        use path = OpenRegistryPath true
        path.SetValue( key, value, RegistryValueKind.QWord )

    /// Get Registry Key
    let internal GetRegistryKey<'T> (key:string) (defVal:'T) expType =
        if Utils.IsNull key || key.Length=0 then 
            defVal
        else
            use path = OpenRegistryPath false
            try
                let kind = path.GetValueKind(key)
                if kind = expType then
                    match path.GetValue(key, defVal ) with
                    | :? 'T as v -> v
                    | _ -> defVal
                else
                    Logger.Fail(sprintf "CurrentUser\\%s\\%s\\%s expects a %A key but holds %A" REGISTRYSOFTWARE REGISTRYMYPATH key expType kind )
                    defVal
            with
                | _ -> defVal

    /// Read a register key of type string, with a default value if register does not exist. 
    let GetRegistryKeyString key defVal = GetRegistryKey<string> key defVal RegistryValueKind.String 

    /// Read a register key of type String[] with a delimiter, if register does not exist, used the default value enclosed. 
    let GetRegistryKeyStrings key defVal delim =
        let regVals = GetRegistryKey<string> key "" RegistryValueKind.String 
        if regVals.Length > 0 then 
            regVals.Split(delim)
        else
            defVal

    /// Read a register key of type int, if register does not exist, used the default value. 
    let GetRegistryKeyInt key defVal = GetRegistryKey<int> key defVal RegistryValueKind.DWord
    
    /// Read a register key of type bool, if register does not exist, used the default value.
    let GetRegistryKeyBoolean key (defVal:bool) = 
        let v = GetRegistryKeyInt key (Convert.ToInt32 defVal)
        Convert.ToBoolean v

    /// Read a register key of type double, if register does not exist, used the default value.
    let GetRegistryKeyFloat key defVal = GetRegistryKey<float> key defVal RegistryValueKind.QWord

    /// Read a register key of type int64, if register does not exist, used the default value.
    let GetRegistryKeyInt64 key defVal = GetRegistryKey<int64> key defVal RegistryValueKind.QWord

    /// To parse the commandline parameter related to traces for one parameters
    /// ParseArgs args lookfor registername default_val :
    ///      if lookfor exists in args, then use the next param in args, mark both as "", and 
    let internal ParseArgs<'T> (args:string[]) lookfor (defVal:'T) (parseFunc:string->'T) =
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

    let internal ArgExist (args:string[]) lookfor = 
        let mutable i = 0
        let mutable bOptionExist = false
        while i < args.Length && not bOptionExist do
            match args.[i] with 
            | Prefixi lookfor v 
                -> bOptionExist <- true
            | _ 
                -> ()
            i<-i+1
        bOptionExist
            


    /// Parse a command line argument of int32, if register does not exist, used the default value.
    let internal ParseArgsInt args lookfor defVal = ParseArgs<int> args lookfor defVal Convert.ToInt32

    /// 
    let internal ParseArgsInt64 args lookfor defVal = ParseArgs<int64> args lookfor defVal Convert.ToInt64

    ///
    let internal ParseArgsFloat args lookfor defVal = ParseArgs<float> args lookfor defVal Convert.ToDouble

    ///
    let internal ParseArgsString args lookfor defVal = ParseArgs<string> args lookfor defVal ( fun x -> x )

    /// Parse Boolean argument
    /// use -option on|true|off|false
    let internal ParseArgsBoolean (args:string[]) lookfor (defVal:bool) = 
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

    /// <summary>
    /// Helper class to parse commandline argument. Each argument is in the form of -pattern param, and the programmer can supplied a default value for each parameter. Please note that the pattern is case insensitive prefix matched, so pattern
    /// -dir will match "-directory" "-DIRECT". Please design your commandline argument pattern with different prefix. 
    /// </summary>
    type FParseArgs = 
        /// Command line arguments to be used. 
        val private m_args : string[]
        /// Construct FParseArgs (args: string[] ), where args contains command line option to be parsed. 
        new ( args, shouldParseLogArgs : bool) = 
            if shouldParseLogArgs then
                Logger.ParseArgs(args)
            { m_args = args }
        /// Construct FParseArgs (args: string[] ), where args contains command line option to be parsed. 
        new ( args ) =
            FParseArgs(args, true)

        /// Return remaining args that is unparsed. 
        member this.RemainingArgs =
            Seq.ofArray(this.m_args) |> Seq.filter (fun e -> e <> "") |> Seq.toArray

        /// Is a certain argument exist
        member this.ArgExist lookfor = 
            ArgExist this.m_args lookfor    
        /// <summary>
        /// Parse a Boolean argument and optionally set the argument in Win32 register
        /// </summary>
        /// <param name="lookfor"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
        /// <param name="regKey"> optional registration key to lookfor to get/set default parameter </param>
        /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
        /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
        /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
        member this.ParseArg_WinReg_Boolean lookfor regKey (defVal:bool) (setKey:RegKeyOps) =
            let mutable retVal = defVal
            match setKey with 
            | RegKeyOps.Read
            | RegKeyOps.ReadAndWrite 
                -> retVal <- GetRegistryKeyBoolean regKey retVal
            | _
                -> ()
            let retVal, bExistOption = ParseArgsBoolean this.m_args lookfor retVal
            if bExistOption && setKey = RegKeyOps.ReadAndWrite then
                SetRegistryKeyBoolean regKey retVal                
            retVal 

        /// <summary>
        /// Parse a int32 argument and optionally set the argument in Win32 register
        /// </summary>
        /// <param name="lookfor"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
        /// <param name="regKey"> optional registration key to lookfor to get/set default parameter </param>
        /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
        /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
        /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
        member this.ParseArg_WinReg_Int lookfor regKey (defVal:int) (setKey:RegKeyOps) =
            let mutable retVal = defVal
            match setKey with 
            | RegKeyOps.Read
            | RegKeyOps.ReadAndWrite 
                -> retVal <- GetRegistryKeyInt regKey retVal
            | _
                -> ()
            let retVal, bExistOption = ParseArgsInt this.m_args lookfor retVal
            if bExistOption && setKey = RegKeyOps.ReadAndWrite then
                SetRegistryKeyInt regKey retVal                
            retVal 

        /// <summary>
        /// Parse a int64 argument and optionally set the argument in Win32 register
        /// </summary>
        /// <param name="lookfor"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
        /// <param name="regKey"> optional registration key to lookfor to get/set default parameter </param>
        /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
        /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
        /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
        member this.ParseArg_WinReg_Int64 lookfor regKey (defVal:int64) (setKey:RegKeyOps) =
            let mutable retVal = defVal
            match setKey with 
            | RegKeyOps.Read
            | RegKeyOps.ReadAndWrite 
                -> retVal <- GetRegistryKeyInt64 regKey retVal
            | _
                -> ()
            let retVal, bExistOption = ParseArgsInt64 this.m_args lookfor retVal
            if bExistOption && setKey = RegKeyOps.ReadAndWrite then
                SetRegistryKeyInt64 regKey retVal                
            retVal 

        /// <summary>
        /// Parse a double argument and optionally set the argument in Win32 register
        /// </summary>
        /// <param name="lookfor"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
        /// <param name="regKey"> optional registration key to lookfor to get/set default parameter </param>
        /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
        /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
        /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
        member this.ParseArg_WinReg_Float lookfor regKey (defVal:float) (setKey:RegKeyOps) =
            let mutable retVal = defVal
            match setKey with 
            | RegKeyOps.Read
            | RegKeyOps.ReadAndWrite 
                -> retVal <- GetRegistryKeyFloat regKey retVal
            | _
                -> ()
            let retVal, bExistOption = ParseArgsFloat this.m_args lookfor retVal
            if bExistOption && setKey = RegKeyOps.ReadAndWrite then
                SetRegistryKeyFloat regKey retVal                
            retVal 

        /// <summary>
        /// Parse a string argument and optionally set the argument in Win32 register
        /// </summary>
        /// <param name="lookfor"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
        /// <param name="regKey"> optional registration key to lookfor to get/set default parameter </param>
        /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
        /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
        /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
        member this.ParseArg_WinReg_String lookfor regKey (defVal:string) (setKey:RegKeyOps) =
            let mutable retVal = defVal
            match setKey with 
            | RegKeyOps.Read
            | RegKeyOps.ReadAndWrite 
                -> retVal <- GetRegistryKeyString regKey retVal
            | _
                -> ()
            let retVal, bExistOption = ParseArgsString this.m_args lookfor retVal
            if bExistOption && setKey = RegKeyOps.ReadAndWrite then
                SetRegistryKeyString regKey retVal                
            retVal 
        /// <summary>
        /// Parse a string[] argument and optionally set the argument in Win32 register. The parameter can be set using -lookfor param1,param2,param3 where , is the delimiter, 
        /// or using -lookfor param1 -lookfor param2 -lookfor param3
        /// </summary>
        /// <param name="lookfor"> pattern to be looked for in the command line argument, pattern matching is prefixed based and is case-insensitive </param>
        /// <param name="regKey"> optional registration key to lookfor to get/set default parameter </param>
        /// <param name="defVal"> default value if the pattern does not exist in the commandline argument </param>
        /// <param name="delim"> delimiter to separate one argument into a string[] </param>
        /// <param name="setKey"> If the value is Ignore, the register key is ignored. If the value is Read, an attempt is made to read the register key to retrieve the default value. 
        /// If the value is ReadAndWrite, the register key is written if the commandline argument is presents.  </param>
        member this.ParseArg_WinReg_Strings lookfor regKey (defVal:string[]) delim (setKey:RegKeyOps) =
            let mutable retVal = defVal
            match setKey with 
            | RegKeyOps.Read
            | RegKeyOps.ReadAndWrite 
                -> retVal <- GetRegistryKeyStrings regKey retVal delim
            | _
                -> ()
            let mutable bExistOption = false
            let mutable bMoreOptions = true
            while bMoreOptions do
                let retSingleString, bExistOptionI = ParseArgsString this.m_args lookfor "" 
                bMoreOptions <- bExistOptionI
                let retStrings = retSingleString.Split( delim, StringSplitOptions.RemoveEmptyEntries )
                if bExistOptionI then
                    if bExistOption then 
                        retVal <- (Array.append retVal retStrings )
                    else
                        retVal <- retStrings
                    bExistOption <- true
            if bExistOption && setKey = RegKeyOps.ReadAndWrite then
                SetRegistryKeyStrings regKey retVal delim.[0]
            retVal 

        /// <summary>
        /// Parse a Boolean argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseArg_Boolean lookfor defVal =
            this.ParseArg_WinReg_Boolean lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a int32 argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseArg_Int lookfor defVal =
            this.ParseArg_WinReg_Int lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a int64 argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseArg_Int64 lookfor defVal =
            this.ParseArg_WinReg_Int64 lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a double argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseArg_Float lookfor defVal =
            this.ParseArg_WinReg_Float lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a string argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseArg_String lookfor defVal =
            this.ParseArg_WinReg_String lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a string[] argument, The parameter can be set using -lookfor param1,param2,param3 where , is the delimiter, 
        /// or using -lookfor param1 -lookfor param2 -lookfor param3
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseArg_Strings lookfor defVal =
            this.ParseArg_WinReg_Strings lookfor "" defVal [|','|] RegKeyOps.Ignore

        /// <summary>
        /// Parse a int[] argument, where multiple values can be set using multiple -lookfor param1 -lookfor param2 -lookfor param3
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseArg_Ints lookfor defVal =
            let mutable bExistOption = true
            let mutable retVal = defVal
            while bExistOption do
                let (retValI, bExistOptionI) = ParseArgsInt this.m_args lookfor 0
                if bExistOptionI then
                    retVal <- Array.append retVal [|retValI|]
                bExistOption <- bExistOptionI
            retVal

        /// <summary>
        /// Parse a Boolean argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseBoolean( lookfor, defVal) =
            this.ParseArg_WinReg_Boolean lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a int32 argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseInt( lookfor, defVal ) =
            this.ParseArg_WinReg_Int lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a Int64 argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseInt64( lookfor, defVal) =
            this.ParseArg_WinReg_Int64 lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a float argument (double), with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseDouble( lookfor, defVal ) =
            this.ParseArg_WinReg_Float lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a string argument, with a supplied default value
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseString( lookfor, defVal ) =
            this.ParseArg_WinReg_String lookfor "" defVal RegKeyOps.Ignore

        /// <summary>
        /// Parse a string[] argument, where multiple values The parameter can be set using -lookfor param1,param2,param3 where , is the delimiter, 
        /// or using -lookfor param1 -lookfor param2 -lookfor param3
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseStrings( lookfor, defVal ) =
            this.ParseArg_WinReg_Strings lookfor "" defVal [|','|] RegKeyOps.Ignore

        /// <summary>
        /// Parse a int[] argument, where multiple values can appear by using  -lookfor param1 -lookfor param2 -lookfor param3. 
        /// <param name="lookfor"> pattern to lookfor </param>
        /// <param name="defValue"> default value when pattern does not exist </param>
        /// </summary> 
        member this.ParseInts( lookfor, defVal ) =
            this.ParseArg_Ints lookfor defVal

        /// print a usage statement, combined with default trace usage information. 
        member this.PrintUsage usage =
            printf "%s" usage
            Logger.PrintArgsUsage()
        
        /// Return true if all argument has been parsed, otherwise, return false, with usage stagement (including trace usage) printed. 
        member this.AllParsed usage =
            let remaining_args = this.m_args |> String.concat " "

            if this.m_args.Length>0 && remaining_args.Length >= this.m_args.Length then
                printfn "Unparsed arguments: %s" remaining_args
                this.PrintUsage usage
                false
            else
                true
