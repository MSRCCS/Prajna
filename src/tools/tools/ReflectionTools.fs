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
		ReflectionTools.fs
  
	Description: 
		Helper math function 

	Author:																	
 		Sanjeev Mehrotra, Principal Architect
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Sept. 2013
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools



module internal ReflectionTools =

    open System
    open System.IO
    open System.Reflection
    open System.Collections.Generic
    open System.Text.RegularExpressions
    open System.Net.Sockets
    open System.Diagnostics
    open System.Threading
    open Prajna.Tools.FSharp

    exception MapError of string

    let GetMethod (t : string) (m : string) : MethodInfo =
        let mutable retVal : MethodInfo = null
        let assems = AppDomain.CurrentDomain.GetAssemblies()
        for i in 0 .. (Array.length assems - 1) do
            let mods = assems.[i].GetModules()
            for j in 0 .. (Array.length mods - 1) do
                let tt = mods.[j].GetType(t)
                if (Utils.IsNotNull tt) then
                    let (mi : MethodInfo) = tt.GetMethod(m)
                    if (Utils.IsNotNull mi) then
                        retVal <- mi
        retVal

    let GetMethodGen (t : string) (m : string) (tp : System.Type[]) =
        let mi = GetMethod t m
        if (mi.IsGenericMethod) then
            mi.MakeGenericMethod(tp)
        else
            mi

    let InvokeMethod<'T> (t : string) (m : string) (o : obj) (oarg : obj[]) : bool*Option<'T> =
        let mutable mi = GetMethod t m
        if (Utils.IsNull mi) then
            (false, None)
        else
            try
                let outArg = mi.Invoke(o, oarg) :?> 'T
                (true, Some(outArg))
            with
                ex -> (false, None)

    let InvokeMethodGen (t : string) (m : string) (tp : System.Type[]) (o : obj) (oarg : obj[]) : bool*obj =
        let mutable mi = GetMethodGen t m tp
        if (Utils.IsNull mi) then
            (false, null)
        else
            try
                let outArg = mi.Invoke(o, oarg)
                (true, outArg)
            with
                ex -> (false, null)

    let InvokeMethodGenNoGen (t : string) (m : string) (o : obj) (oarg : obj[]) : bool*obj =
        let mi = GetMethod t m
        if (Utils.IsNull mi) then
            (false, null)
        else
            try
                let outArg =
                    if (mi.IsGenericMethod) then
                        let mi = mi.MakeGenericMethod(oarg.[0] :?> System.Type[])
                        let oarg2 = Array.sub oarg 1 (oarg.Length-1)
                        mi.Invoke(o, oarg2)
                    else
                        mi.Invoke(o, oarg)
                (true, outArg)
            with
                ex -> (false, null)
    
    type Split =
        | Split
        | NoSplit
        | Offset

    type ArgType =
        | Ser
        | SerArr
        | IntOffset

    type InOut =
        | In
        | Out
        | InOut
    
    type Arg =
        | Ser of obj
        | SerArr of obj * int * Split * int * InOut * MethodInfo
        | IntOffset of int * int * Split * int
        | SerByte of byte[] * int * Split * int * InOut * ArgType 

    type ArrayOps =
        | Split
        | Unsplit
        | Len

    let ArrayFn<'T> (o : ArrayOps) (x : obj) (xFull : obj) (start : int) (count : int) : obj =
        let xT = x :?> 'T[]
        match o with
            | ArrayOps.Split ->
                let yT = Array.sub xT start count
                box yT
            | ArrayOps.Unsplit ->
                let xTFull = xFull :?> 'T[]
                Array.Copy(xT, 0, xTFull, start, count)
                null
            | ArrayOps.Len ->
                box (xT.Length)

    let ArgInOut arg =
        match arg with
            | Ser(x) -> InOut.In
            | SerArr(x, d, s, a, i, m) -> i
            | SerByte(x, d, s, a, i, at) -> i
            | IntOffset(x, d, s, a) -> InOut.In

    let ArgSplit arg =
        match arg with
            | SerArr(x, d, s, a, i, m) -> s
            | SerByte(x, d, s, a, i, at) -> s
            | IntOffset(x, d, s, a) -> s
            | Ser(x) -> Split.NoSplit

    let ArgAlign arg =
        match arg with
            | SerArr(x, d, s, a, i, m) -> a
            | SerByte(x, d, s, a, i, at) -> a
            | IntOffset(x, d, s, a) -> a
            | Ser (x) -> 1

    let ArgLen arg =
        match arg with
            | SerArr(x, d, s, a, i, m) -> d
            | SerByte(x, d, s, a, i, at) -> assert(false); -1 // not deserialized, don't know length (dimension)
            | Ser (x) -> 1
            | IntOffset(x, d, s, a) -> 1

    let OffsetLen (arg : Arg) (index : int) (num : int) =
        if (ArgSplit arg = Split.NoSplit) then
            (0, 1)
        else
            if (ArgInOut arg = InOut.Out) then
                let align = ArgAlign arg
                let offset = index*align
                (offset, align)
            else
                let dim64 = int64(ArgLen arg)
                let align64 = int64(ArgAlign arg)
                let offset = int (int64(index)*dim64/(int64(num)*align64) * align64)
                let nextOffset = int (int64(index+1)*dim64/(int64(num)*align64) * align64)
                let len = nextOffset - offset
                (offset, len)

    let SplitArg (arg : Arg) (index : int) (num : int) : Arg =
        if (ArgSplit arg = Split.NoSplit) then
            arg
        elif (ArgSplit arg = Split.Split) then
            let (offset, len) = OffsetLen arg index num
            match arg with
                | SerArr (x, d, s, a, i, m) ->
                  let spObj = m.Invoke(null, [|ArrayOps.Split; x; null; offset; len|])
                  let splitD = m.Invoke(null, [|ArrayOps.Len; spObj; null; 0; 0|]) :?> int
                  Arg.SerArr(spObj, splitD, s, a, i, m)
                | IntOffset (x, d, s, a) -> Arg.IntOffset(len, len, s, a)
                | SerByte (x, d, s, a, i, at) -> assert(false); arg // cannot split prior to binding
                | Ser (x) -> arg
        elif (ArgSplit arg = Split.Offset) then
            let (offset, len) = OffsetLen arg index num    
            match arg with
                | IntOffset (x, d, s, a) -> Arg.IntOffset(x+offset, len, s, a)
                | _ -> assert(false); arg
        else
            assert(false);
            arg

    let UnsplitArg (argFull : Arg) (arg : Arg) (index : int) (num : int) : unit =
        if (ArgInOut arg <> InOut.In) then
            let (offset, len) = OffsetLen arg index num
            match arg with
                | SerArr (x, d, s, a, i, m) ->
                    match argFull with
                        | SerArr (xFull, dFull, sFull, aFull, iFull, mFull) ->
                            m.Invoke(null, [|ArrayOps.Unsplit; x; xFull; offset; len|]) |> ignore
                        | _ -> ()
                | _ -> ()

    let Serialize x =
        let ms = new MemStream()
        do ms.Serialize(x)
        ms.GetBuffer()

    let Deserialize (buffer : byte[]) =
        let ms = new MemStream(buffer)
        ms.Deserialize()

    let BindArg arg =
        match arg with
            | Arg.SerByte (x, d, s, a, i, at) ->
                match at with
                    | ArgType.Ser -> Arg.Ser(Deserialize x)
                    | ArgType.SerArr ->
                        let o = Deserialize(x)
                        let arrType = o.GetType().GetElementType()
                        let mi = GetMethodGen "Tools.ReflectionTools" "ArrayFn" [|arrType|]
                        let d = mi.Invoke(null, [|ArrayOps.Len; x; null; 0; 0|]) :?> int
                        Arg.SerArr(o, d, s, a, i, mi)
                    | ArgType.IntOffset -> Arg.IntOffset(BitConverter.ToInt32(x, 0), d, s, a)
            | _ -> arg

    let UnbindArg arg =
        match arg with
            | Arg.SerByte(x, d, s, a, i, at) -> arg
            | Arg.Ser (x) -> Arg.SerByte(Serialize(x), 1, Split.NoSplit, 1, InOut.In, ArgType.Ser)
            | Arg.SerArr (x, d, s, a, i, m) -> Arg.SerByte(Serialize(x), d, s, a, i, ArgType.SerArr)
            | Arg.IntOffset(x, d, s, a) -> Arg.SerByte(BitConverter.GetBytes(x), d, s, a, InOut.In, ArgType.IntOffset)

    let ArgToObj arg =
        match arg with
            | Arg.SerArr(x, d, s, a, i, m) -> box x
            | Arg.SerByte (x, d, s, a, i, at) -> assert(false); null
            | Arg.Ser(x) -> box x
            | Arg.IntOffset (x, d, s, a) -> box x

    let ObjToArg (origArg : Arg) (o : obj) =
        match origArg with
            | Arg.SerArr(x, d, s, a, i, m) -> SerArr(o, d, s, a, i, m)
            | Arg.Ser (x) -> Ser(o)
            | Arg.SerByte (x, d, s, a, i, at) -> assert(false); Arg.Ser(null)
            | Arg.IntOffset(x, d, s, a) -> IntOffset(unbox<int> o, d, s, a)

    let SerByteArrFromArg<'T> (arg : Arg) : byte[] =
        match arg with
            | Arg.SerByte(x, d, s, a, i, at) -> x
            | _ -> assert(false); [||]

    let TFromArg<'T> (arg : Arg) : 'T =
        match arg with
            | Arg.Ser(x) -> unbox<'T> x
            | _ -> assert(false); unbox<'T> null

    let TArrFromArg<'T> (arg : Arg) : 'T[] =
        match arg with
            | Arg.SerArr(x, d, s, a, i, m) -> unbox<'T[]> x
            | _ -> assert(false); [||]

    // already serialized byte array
    let SerByteArrAsArg (x : byte[], i : InOut) : Arg =
        Arg.SerByte(x, 1, Split.NoSplit, 1, i, ArgType.Ser)

    let AsArg<'T> (o : 'T) : Arg =
        let tp =  o.GetType();
        if (tp = typeof<Arg> || tp.BaseType = typeof<Arg>) then
            unbox<Arg> o
        else
            Arg.Ser(box o)   

    let AsArrArg<'T> ((x, s, a, i) : ('T[]*Split*int*InOut)) : Arg =
        let baseType = x.GetType().GetElementType()
        let mi = GetMethodGen "Tools+ReflectionTools" "ArrayFn" [|baseType|]
        let d = mi.Invoke(null, [|ArrayOps.Len; x; null; 0; 0|]) :?> int
        Arg.SerArr(x, d, s, a, i, mi)

    let SerializeArg (arg : Arg) =
        let argUnbind = UnbindArg arg // produce a byte array arg
        Serialize argUnbind

    let DeserializeArg (buffer : byte[]) (bind : bool) =
        let argUnbind = Deserialize buffer :?> Arg
        if bind then
            BindArg argUnbind
        else
            argUnbind

    let SerializeArgArr (arg : Arg[]) =
        let ms = new MemStream()
        ms.WriteInt32(arg.Length)
        for i in 0 .. arg.Length - 1 do
            do ms.Serialize(UnbindArg arg.[i])
        ms.GetBuffer()

    let DeserializeArgArr (buffer : byte[]) (bind : bool) =
        let ms = new MemStream(buffer)
        let len = ms.ReadInt32()
        let arg = Array.zeroCreate<Arg> len
        for i in 0 .. len - 1 do
            if (bind) then
                arg.[i] <- BindArg (ms.Deserialize() :?> Arg)
            else
                arg.[i] <- ms.Deserialize() :?> Arg
        arg

    let SerializeArgOut (arg : Arg[]) =
        let ms = new MemStream()
        for i in 0 .. arg.Length - 1 do
            if (ArgInOut arg.[i] <> InOut.In) then
                do ms.Serialize(UnbindArg arg.[i])
        ms.GetBuffer()

    let DeserializeArgOut (arg : Arg[]) (buffer : byte[]) (bind : bool) =
        let ms  = new MemStream(buffer)
        for i in 0 .. arg.Length - 1 do
            if (ArgInOut arg.[i] <> InOut.In) then
                if (bind) then
                    arg.[i] <- BindArg (ms.Deserialize() :?> Arg)
                else
                    arg.[i] <- ms.Deserialize() :?> Arg 

    type Del2<'T1,'T2> = delegate of 'T1->'T2
    type Del3<'T1,'T2,'T3> = delegate of 'T1*'T2->'T3
    type Del4<'T1,'T2,'T3,'T4> = delegate of 'T1*'T2*'T3->'T4

    let GetDictionaryKeys<'K,'U> (x  : Dictionary<'K,'U>) =
        let keys = ref [||]
        lock x (fun() ->
            keys := Array.zeroCreate x.Keys.Count
            x.Keys.CopyTo(!keys, 0)
        )
        !keys

    let GetDel2<'U,'V> (methodType : string, methodName : string) : Del2<'U,'V> =
        let mapping = GetMethodGen methodType methodName [|typeof<'U>;typeof<'V>|]
        // create delegate
        Delegate.CreateDelegate(typeof<Del2<'U,'V>>, mapping) :?> Del2<'U,'V>

    let GetDel3<'K,'U,'V> (methodType : string, methodName : string) : Del3<'K,'U,'V> =
        let mapping = GetMethodGen methodType methodName [|typeof<'K>;typeof<'U>;typeof<'V>|]
        // create delegate
        Delegate.CreateDelegate(typeof<Del3<'K,'U,'V>>, mapping) :?> Del3<'K,'U,'V>

    let Map<'U,'V> (methodType : string, methodName : string, x : 'U[]) : 'V[] =
        let del = GetDel2 (methodType, methodName)
        let y = Array.zeroCreate<'V> x.Length
        for i in 0 .. x.Length - 1 do
            y.[i] <- del.Invoke(x.[i])
        y

    let HashMap<'K,'U,'V  when 'K:equality> (methodType : string, methodName : string, x : Dictionary<'K,'U>) : Dictionary<'K,'V> =
        let del = GetDel2 (methodType, methodName)
        let y = new Dictionary<'K,'V>()
        let keys = GetDictionaryKeys(x)
        for key in keys do
            y.Add(key, del.Invoke(x.[key]))
        y

    let HashMapSerV<'K,'U,'V when 'K:equality> (methodType : string, methodName : string, x : Dictionary<'K,byte[]>) : Dictionary<'K,byte[]> =
        let del = GetDel2(methodType, methodName)
        let y = new Dictionary<'K,byte[]>()
        let keys = GetDictionaryKeys(x)
        for key in keys do
            let ms = new MemStream(x.[key])
            let msOut = new MemStream()
            let (value : 'V) = ms.Deserialize() :?> 'V
            let output = del.Invoke(value)
            msOut.Serialize(output)
            y.Add(key, msOut.GetBuffer())
        y

    let HashMapSerKV<'K,'U,'V when 'K:equality> (methodType : string, methodName : string, xK : byte[], xU : byte[]) : byte[] =
        let del = GetDel2(methodType, methodName)
        let msK = new MemStream(xK)
        let msU = new MemStream(xU)
        let msV = new MemStream()
        let keys = msK.Deserialize() :?> 'K[]
        let values = msU.Deserialize() :?> 'U[]
        if (keys.Length <> values.Length) then
            raise (MapError("Lengths don't match"))
            [||]
        else
            let output = Array.zeroCreate<'V> keys.Length
            for i in 0 .. keys.Length - 1 do
                output.[i] <- del.Invoke(values.[i])
            msV.Serialize(output)
            msV.GetBuffer()

    // hash map directly from memory stream
    let HashMapMS<'K, 'U, 'V> (methodType : string, methodName : string, ms : MemStream) : byte[] =
        let del = GetDel2<'U,'V>(methodType, methodName) // get delegate for mapping function
        let keys = ms.Deserialize() :?> 'K[]
        let values = ms.Deserialize() :?> 'U[]
        let msV = new MemStream()
        if (keys.Length <> values.Length) then
            raise (MapError("Lengths don't match"))
            [||]
        else
            let output = Array.zeroCreate<'V> keys.Length
            for i in 0 .. keys.Length - 1 do
                output.[i] <- del.Invoke(values.[i])
            msV.Serialize(output)
            msV.GetBuffer()

    let HashMapTo<'K,'U,'V  when 'K:equality> (methodType : string, methodName : string, x : Dictionary<'K,'U>, y : Dictionary<'K,'V>) =
        let del = GetDel2 (methodType, methodName)
        lock y (fun() ->
            for key in x.Keys do
                y.[key] <- del.Invoke(x.[key])
        )

    let HashMapTo3<'K,'U,'V  when 'K:equality> (methodType : string, methodName : string, x : Dictionary<'K,'U>, y : Dictionary<'K,'V>) =
        let del = GetDel3 (methodType, methodName)
        lock y (fun() ->
            for key in x.Keys do
                y.[key] <- del.Invoke(key, x.[key])
        )

    [<AllowNullLiteral>]
    type AssemblyLoader() =
        class

        inherit MarshalByRefObject()

        let assems = new Dictionary<string, Assembly>()

        member this.AddAssem(filename : string) =
            lock assems (fun() ->
                assems.Add(filename, null)
            )

        member this.PopulateLoadedAssems() =
            let assemLoaded = AppDomain.CurrentDomain.GetAssemblies()
            for a in assemLoaded do
                assems.[a.FullName] <- a

        member this.LoadAssembly (assemName : string) =
            let assembly = Assembly.ReflectionOnlyLoadFrom(assemName)
            lock assems (fun() ->
                if (not (assems.ContainsKey(assembly.FullName)) ||
                    Utils.IsNull (assems.[assembly.FullName])) then
                    assems.[assembly.FullName] <- Assembly.LoadFrom(assemName)
            )

        // returns (success, inArgs modified, outArg - as option)
        member this.ArgInvoke (methodType : string, methodName : string, inArgs : Arg[]) : (bool*Arg[]*Option<Arg>) =
            let boundArgs = inArgs |> Array.map (fun x -> BindArg x)
            let objArgs = boundArgs |> Array.map (fun x -> ArgToObj x)
            let (ret, outArg) =
                try
                    InvokeMethodGenNoGen methodType methodName null objArgs
                with
                    ex -> (false, null)
            // convert object to arbitrary serialized array
            let inArgsNew = Array.map2 (fun x y -> UnbindArg (ObjToArg x y)) boundArgs objArgs
            let out =
                if (Utils.IsNull outArg) then
                    None
                else
                    Some (UnbindArg (AsArg outArg))
            (ret, inArgsNew, out)

        member this.ObjInvoke (methodType : string, methodName : string, inArgs : obj[]) : (bool*obj[]*obj) =
            let (ret, outArg) =
                try
                    InvokeMethod<obj> methodType methodName null inArgs
                with
                    ex -> (false, None)
            let outArg2 =
                match outArg with
                | Some(x) -> x
                | None -> null
            (ret, inArgs, outArg2)

        member this.ObjInvokeGen (methodType : string, methodName : string, tp : System.Type[], inArgs : obj[]) : (bool*obj[]*obj) =
            let (ret, outArg) =                    
                try
                    InvokeMethodGen methodType methodName tp null inArgs
                with
                    ex -> (false, null)
            (ret, inArgs, outArg)

        member this.ObjInvokeMS (methodType : string, methodName : string, ms : MemStream, inArgs : obj[]) =
            let (ret, outArg) =
                try
                    let tp = ms.Deserialize() :?> System.Type[]
                    InvokeMethodGen methodType methodName tp null inArgs
                with
                    ex -> (false, null)
            (ret, inArgs, outArg)

        end

    let CanonicalAssemDir dir =
        let (x : Regex) = new Regex("\\\\")
        let dirFS = x.Replace(dir, "/")
        //if (Regex.IsMatch(dirFS, "^file:///"))
        let (x : Regex) = new Regex("^file:///(.*)")
        let m = x.Match(dirFS)
        let mutable dir =
            if (m.Success) then
                m.Groups.[1].Value
            else
                dirFS
        // now remove double dots
        let (x : Regex) = new Regex("(.*)\/(.*?)\/\.\.(.*)")
        let mutable terminate = false
        while (not terminate) do
            let m = x.Match(dir)
            if (m.Success) then
                dir <- m.Groups.[1].Value+m.Groups.[3].Value
            else
                terminate <- true
        // now remove single dots
        let (x : Regex) = new Regex("(.*)\.\/(.*)")
        terminate <- false
        while (not terminate) do
            let m = x.Match(dir)
            if (m.Success) then
                dir <- m.Groups.[1].Value+m.Groups.[2].Value
            else
                terminate <- true
        dir                

    let AssemNameNoPath (fullname : string) =
        let fullname = CanonicalAssemDir fullname
        let i = fullname.LastIndexOf(Path.PathSeparator)
        if (i < 0) then
            fullname
        else
            fullname.Substring(i+1)

    let DirContainsBase bd dir =
        let (bdc : string) = CanonicalAssemDir bd
        let (dirc : string) = CanonicalAssemDir dir
        (dirc.IndexOf(bdc) = 0)

    let ObtainJobAssemblyList () : Assembly[] =
        let (domain : AppDomain) = AppDomain.CurrentDomain
        let (assems : Assembly[]) = domain.GetAssemblies()
        let assemShip = new List<Assembly>()
        let added = new Dictionary<string, bool>()
        
        for assem in assems do
            if (DirContainsBase domain.BaseDirectory assem.CodeBase) then
                if (not (added.ContainsKey(assem.CodeBase.ToLower()))) then
                    assemShip.Add(assem)
                    added.[assem.CodeBase.ToLower()] <- true
            let refAssems = assem.GetReferencedAssemblies()
            for refAssem in refAssems do
                let assemLoad = Assembly.ReflectionOnlyLoad(refAssem.FullName)
                if (DirContainsBase domain.BaseDirectory assemLoad.CodeBase) then
                    if (not (added.ContainsKey(assemLoad.CodeBase.ToLower()))) then
                        assemShip.Add(assemLoad)
                        added.[assemLoad.CodeBase.ToLower()] <- true
        assemShip.ToArray()

    let RecvT (sock : System.Net.Sockets.Socket) (buffer : byte[]) (len : int) =
        let mutable bytesToGo = len
        let mutable offset = 0
        while (bytesToGo > 0) do
            let bytesRecv = sock.Receive(buffer, offset, bytesToGo, SocketFlags.None)
            bytesToGo <- bytesToGo - bytesRecv
            offset <- offset + bytesRecv

    let RecvInt (sock : System.Net.Sockets.Socket) =
        let buffer = Array.zeroCreate<byte> sizeof<int>
        RecvT sock buffer sizeof<int> |> ignore
        BitConverter.ToInt32(buffer, 0)

    let RecvByteArr (sock : System.Net.Sockets.Socket) =
        let xlen = RecvInt sock
        let x = Array.zeroCreate<byte> xlen
        RecvT sock x xlen |> ignore
        x

    let RecvStr (sock : System.Net.Sockets.Socket) =
        let buffer = Array.zeroCreate<byte> 256
        RecvT sock buffer sizeof<int> |> ignore
        let strLen = BitConverter.ToInt32(buffer, 0)
        let buffer = Array.zeroCreate<byte> strLen
        RecvT sock buffer strLen |> ignore
        System.Text.Encoding.ASCII.GetString(buffer)

    let SendT (sock : System.Net.Sockets.Socket) (buffer : byte[]) (len : int) =
        sock.Send(buffer, 0, len, SocketFlags.None)

    let SendInt (sock : System.Net.Sockets.Socket) (x : int) =
        let (buffer : byte[]) = BitConverter.GetBytes(x)
        SendT sock buffer sizeof<int>

    let SendByteArr (sock : Socket) (x : byte[]) =
        SendInt sock (Array.length x) |> ignore
        SendT sock x (Array.length x)

    let SendStr (sock : System.Net.Sockets.Socket) (str : string) =
        let (buffer : byte[]) = System.Text.Encoding.ASCII.GetBytes(str)
        let (buffer2 : byte[]) = BitConverter.GetBytes(Array.length<byte> buffer)
        SendT sock buffer2 sizeof<int> |> ignore
        SendT sock buffer (Array.length<byte> buffer) |> ignore
        ()

    type LaunchType =
        | SameDomain
        | DiffDomain
        | DiffProc

    type FnInfo = {
        args : Arg[];
        mutable success : bool;
        mutable argOut : Option<Arg>;
        finish : AutoResetEvent;
    }

    [<AllowNullLiteral>]
    type Assem() =
        class

        let mutable (appDomain : AppDomain) = null
        let mutable (assemblyLoader : AssemblyLoader) = null
        static let assemblyLoaderThisDomain = new AssemblyLoader()
        static let randGen = new System.Random(1234)
        let mutable (proxyProcess : Process) = null
        let mutable (proxySocket : Socket) = null
        let proxyConnMade = new ManualResetEvent(false)
        let mutable assemLoadedThisDomain = false
        let mutable assemLoadedDiffDomain = false
        let fns = new Dictionary<int, FnInfo>()
        let mutable fnindex = 0
        static let mutable proxyAssem : Assem = null
        let assemList = new List<string>()

        member this.ProxyAssem with get() = proxyAssem

        member this.AssemList with get() = assemList

        member this.LoadAssems (loader : AssemblyLoader) =
            lock loader (fun() ->
                let assems = assemList.ToArray()
                for assem in assems do
                    try
                        loader.LoadAssembly(assem)
                    with
                        ex -> Logger.Log(LogLevel.Warning, (sprintf "Assembly assem not loaded"))
            )

        member this.StartSameDomain() =
            lock assemblyLoaderThisDomain (fun() ->
                if (not assemLoadedThisDomain) then
                    assemblyLoaderThisDomain.PopulateLoadedAssems()
                    this.LoadAssems assemblyLoaderThisDomain
                    assemLoadedThisDomain <- true
            )

        member this.StartDiffDomain() =
            lock this (fun() ->
                if (not assemLoadedDiffDomain) then
                    assert(Utils.IsNull appDomain);
                    let setup = new AppDomainSetup()
                    setup.ApplicationBase <- AppDomain.CurrentDomain.BaseDirectory
                    let evidence = new Security.Policy.Evidence(AppDomain.CurrentDomain.Evidence)
                    let appDomain = AppDomain.CreateDomain("", evidence, setup)
                    assemblyLoader <- appDomain.CreateInstanceFromAndUnwrap(Assembly.GetExecutingAssembly().CodeBase,
                                                                            "Tools.ReflectionTools+AssemblyLoader")
                                      :?> AssemblyLoader
                    assemblyLoader.PopulateLoadedAssems()
                    this.LoadAssems assemblyLoader
                    assemLoadedDiffDomain <- true
            )

        member this.ProcListen(listen : Net.Sockets.TcpListener) = async {
            proxySocket <- listen.AcceptSocket()
            proxyConnMade.Set() |> ignore
        }

        member this.FnLaunch(launchType, remoteIndex, assemName, methodType, methodName, argArr : Arg[]) = async {
            let (ret, out) = this.LaunchFn(launchType, assemName, methodType, methodName, argArr)
            lock proxySocket (fun() ->
                try
                    SendInt proxySocket remoteIndex |> ignore
                    SendInt proxySocket (if ret then 1 else 0) |> ignore
                    if (ret) then
                        let argMem = SerializeArgOut argArr
                        SendByteArr proxySocket argMem |> ignore
                        SendInt proxySocket (match out with | None -> 0 | Some(x) -> 1) |> ignore
                        match out with
                            | None -> ()
                            | Some(x) -> (SendByteArr proxySocket (SerializeArg x) |> ignore)
                with
                    ex -> ()                
            )
        }

        member this.RecvMsgLaunchFn(sock : Socket) = async {
            let terminate = ref false
            while (not !terminate) do
                try
                    let remoteIndex = RecvInt sock
                    let assemName = RecvStr sock
                    let methodType = RecvStr sock
                    let methodName = RecvStr sock
                    let argArr = DeserializeArgArr (RecvByteArr sock) true
                    Async.Start(this.FnLaunch(SameDomain, remoteIndex, assemName, methodType, methodName, argArr))
                with
                    ex -> terminate := true
        }

        member this.RecvMsgFinishFn() = async {
            let terminate = ref false
            while (not !terminate) do
                try
                    let index = RecvInt proxySocket
                    let fn = fns.[index]
                    fn.success <- (
                        let x = RecvInt proxySocket;
                        if (x=1) then
                            true
                        else
                            false
                    )
                    if (fn.success) then
                        DeserializeArgOut fn.args (RecvByteArr proxySocket) true
                        fn.argOut <- (
                            let x = RecvInt proxySocket;
                            if (x=1) then
                                Some (DeserializeArg (RecvByteArr proxySocket) false)
                            else
                                None
                        )
                    fn.finish.Set() |> ignore
                with
                    ex -> terminate := true
            // proxy process has terminated connection
            if (Utils.IsNotNull proxyProcess) then
                proxyProcess.Kill()
                proxyProcess <- null
            proxySocket <- null
            proxyConnMade.Reset() |> ignore
            lock fns (fun() ->
                for key in fns.Keys do
                    fns.[key].finish.Set() |> ignore
            )
        }

        member this.StartDiffProcess(assemName) =
            lock this (fun() ->
                if (Utils.IsNull proxyProcess) then
                    let mutable randomPort = 30000
                    let mutable success = false
                    let mutable listen = null
                    while (not success) do
                        let (ipAddrs : Net.IPAddress[]) = System.Net.Dns.GetHostAddresses("localhost")
                        let ipAddr = ipAddrs.[0]
                        try
                            listen <- Net.Sockets.TcpListener(ipAddr, randomPort)
                            listen.Start()
                            success <- true
                        with
                            ex -> randomPort <- randGen.Next(30000, 65535)
                    Async.Start(this.ProcListen listen)
                    let repl = new Regex("/")
                    let exec = repl.Replace(assemName, "\\")
                    proxyProcess <- Process.Start(exec, "-proxyproc "+randomPort.ToString())
                    let connMade = proxyConnMade.WaitOne(5000)
                    if (not connMade) then
                        if (Utils.IsNotNull proxyProcess) then
                            proxyProcess.Kill()
                            proxyProcess <- null
                        false
                    else
                        Async.Start(this.RecvMsgFinishFn())
                        true
                else
                    true
            )

        member this.StartProxyProc (port : int) =
            proxySocket <- new Socket(SocketType.Stream, ProtocolType.Tcp)
            proxySocket.Blocking <- true
            proxySocket.Connect("localhost", port) |> ignore
            proxyAssem <- this
            Async.RunSynchronously(this.RecvMsgLaunchFn(proxySocket))

        member this.ConnInvoke(assemName, methodType, methodName, arg) =
            let index = ref 0
            let success = ref false
            let fn = {FnInfo.args = arg; FnInfo.success = false; FnInfo.finish = new AutoResetEvent(false); FnInfo.argOut = None}
            lock fns (fun() ->
                index := fnindex
                fns.Add(fnindex, fn)
                fnindex <- fnindex + 1
            )
            lock proxySocket (fun() ->
                try
                    SendInt proxySocket !index |> ignore
                    SendStr proxySocket assemName |> ignore
                    SendStr proxySocket methodType |> ignore
                    SendStr proxySocket methodName |> ignore
                    SendByteArr proxySocket (SerializeArgArr arg) |> ignore
                    success := true
                with
                    ex -> success := false; ()
            )
            if (!success) then
                fn.finish.WaitOne() |> ignore
                (fn.success, fn.args, fn.argOut)
            else
                (false, arg, None)

        member this.LaunchFnSameDomain (assemName, methodType, methodName, arg : Arg[]) =
            Logger.Log( LogLevel.Info, ( sprintf "Launch %s.%s.%s in same domain" assemName methodType methodName ))
            try
                this.StartSameDomain()
                let (ret, argo, out) = assemblyLoaderThisDomain.ArgInvoke(methodType, methodName, arg)
                (ret, out)
            with
                ex -> (false, None)

        member this.LaunchFnDiffDomain (assemName, methodType, methodName, arg  : Arg[]) =
            Logger.Log( LogLevel.Info, ( sprintf "Launch %s.%s.%s in diff domain" assemName methodType methodName ))
            try
                this.StartDiffDomain()
                let (ret, argo, out) = assemblyLoader.ArgInvoke(methodType, methodName, arg)
                if (ret) then
                    for i in 0 .. arg.Length - 1 do
                        arg.[i] <- argo.[i]
                (ret, out)
            with
                ex -> (false, None)

        member this.LaunchFnDiffProc (assemName, methodType, methodName, arg) =
            Logger.Log( LogLevel.Info, ( sprintf "Launch %s.%s.%s in diff process" assemName methodType methodName ))
            try
                let success = this.StartDiffProcess(assemName)
                if (not success) then
                    (false, None)
                else
                    let (ret, argo, out) = this.ConnInvoke(assemName, methodType, methodName, arg)
                    if (ret) then
                        for i in 0 .. arg.Length - 1 do
                            arg.[i] <- argo.[i]
                        (ret, out)
                    else
                        (false, None)
            with
                ex -> (false, None)

        member this.LaunchFn (launchType, assemName, methodType, methodName, arg) =
            match launchType with
                | SameDomain -> this.LaunchFnSameDomain(assemName, methodType, methodName, arg)
                | DiffDomain -> this.LaunchFnDiffDomain(assemName, methodType, methodName, arg)
                | DiffProc -> this.LaunchFnDiffProc(assemName, methodType, methodName, arg)

        member this.MapFnSameDomain<'K,'U,'V when 'K:equality> (assemName, methodType, methodName, x) =
            Logger.Log( LogLevel.Info, ( sprintf "Map %s.%s.%s in same domain" assemName methodType methodName ))
            this.StartSameDomain()
            let (success, newX, y) =
                assemblyLoaderThisDomain.ObjInvokeGen("Tools.ReflectionTools", "HashMap", [|typeof<'K>;typeof<'U>;typeof<'V>|],
                                                      [|box methodType; box methodName; box x|])
            y :?> Dictionary<'K,'V>

        member this.MapFnDiffDomain<'K,'U,'V when 'K:equality> (assemName, methodType, methodName, x) =
            Logger.Log( LogLevel.Info, ( sprintf "Map %s.%s.%s in diff domain" assemName methodType methodName ))
            this.StartDiffDomain()
            let (success, newX, y) =
                assemblyLoader.ObjInvokeGen("Tools.ReflectionTools", "HashMap", [|typeof<'K>;typeof<'U>;typeof<'V>|],
                                            [|box methodType; box methodName; box x|])
            y :?> Dictionary<'K,'V>

        member this.MapFnDiffProc<'K,'U,'V when 'K:equality> (assemName, methodType, methodName, x : Dictionary<'K,'U>) =
            Logger.Log( LogLevel.Info, ( sprintf "Map %s.%s.%s in diff process" assemName methodType methodName ))
            let success = this.StartDiffProcess(assemName)
            if not success then
                new Dictionary<'K,'V>()
            else
                let y = new Dictionary<'K,'V>()
                let msK = new MemStream()
                let msU = new MemStream()
                lock x (fun() ->
                    let keys = Array.zeroCreate<'K> x.Count
                    let values = Array.zeroCreate<'U> x.Count
                    msK.Serialize(x.Keys.CopyTo(keys, 0))
                    msU.Serialize(x.Values.CopyTo(values, 0))
                )
                let (success, newX, y) =
                    this.ConnInvoke(assemName, "Tools.ReflectionTools", "HashMapSerKV", [|AsArg([|typeof<'K>;typeof<'U>;typeof<'V>|]);
                                                                                    AsArg(methodType); AsArg(methodName);
                                                                                    SerByteArrAsArg(msK.GetBuffer(), InOut.In);
                                                                                    SerByteArrAsArg(msU.GetBuffer(), InOut.In)|])
                TFromArg<Dictionary<'K,'V>>(y.Value)

        member this.MapFnSameDomainMS(assemName : string, methodType : string, methodName : string,
                                      ms : MemStream, msFile : MemStream, out : byte[] ref) : byte[] =
            this.StartSameDomain()
            let (success, newX, y) =
                assemblyLoaderThisDomain.ObjInvokeMS("Tools.ReflectionTools", "HashMapMS", ms, [|box methodType; box methodName; box msFile|])
            let z = Array.copy(y :?> byte[])
            out := Array.copy(z)
            z

        member this.MapFnDiffDomainMS(assemName, methodType, methodName, ms, msFile, out : byte[] ref) : byte[] =
            this.StartDiffDomain()
            let (success, newX, y) =
                assemblyLoader.ObjInvokeMS("Tools.ReflectionTools", "HashMapMS", ms, [|box methodType; box methodName; box msFile|])
            if success then
                out := Array.copy(y :?> byte[])
                !out
            else
                [||]

        member this.MapFnDiffProcMS(assemName, methodType, methodName, ms : MemStream, name, out : byte[] ref) =
            let success = this.StartDiffProcess(assemName)
            if not success then
                [||]
            else
                let (sucess, newX, y) = this.ConnInvoke(assemName, "Prajna.DKVPeerStatic", "DKVFileMapNoAssem",
                                                        [|AsArg(LaunchType.SameDomain);
                                                          AsArg(name);
                                                          AsArg(assemName);
                                                          AsArg(methodType);
                                                          AsArg(methodName);
                                                          AsArg(ms)|])
                if (success) then
                    let out2 = SerByteArrFromArg(y.Value)
                    let ms = new MemStream(out2)
                    out := ms.Deserialize() :?> byte[]
                    !out
                else
                    [||]

        member this.MapFnMS(launchType : LaunchType, assemName : string, methodType : string,
                            methodName : string, ms : MemStream, msFile : MemStream, msFileName : string, out : byte[] ref) =
            match launchType with
                | SameDomain -> this.MapFnSameDomainMS(assemName, methodType, methodName, ms, msFile, out)
                | DiffDomain -> this.MapFnDiffDomainMS(assemName, methodType, methodName, ms, msFile, out)
                | DiffProc -> this.MapFnDiffProcMS(assemName, methodType, methodName, ms, msFileName, out)

        end
