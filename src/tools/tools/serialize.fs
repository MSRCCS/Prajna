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

	Author: Sanjeev Mehrotra
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools
open System
open System.IO
open System.Runtime.Serialization.Formatters.Binary

module internal Serialize =

    // generic convert of primitive native types ============
    // can easily add support for other types

    let SupportedConvert<'T>() =
        (typeof<'T> = typeof<System.SByte> ||
         typeof<'T> = typeof<System.Int16> ||
         typeof<'T> = typeof<System.Int32> ||
         typeof<'T> = typeof<System.Int64> ||
         typeof<'T> = typeof<System.Byte> ||
         typeof<'T> = typeof<System.UInt16> ||
         typeof<'T> = typeof<System.UInt32> ||
         typeof<'T> = typeof<System.UInt64> ||
         typeof<'T> = typeof<System.Single> ||
         typeof<'T> = typeof<System.Double> ||
         typeof<'T> = typeof<System.Boolean>
         )

    // byte[] to 'T
    let ConvertTo<'T> (buffer : byte[]) =
        if (typeof<'T> = typeof<System.SByte>) then
            box(sbyte ((int buffer.[0])-128)) :?> 'T
        else if (typeof<'T> = typeof<System.Int16>) then
            box(BitConverter.ToInt16(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Int32>) then
            box(BitConverter.ToInt32(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Int64>) then
            box(BitConverter.ToInt64(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Byte>) then
            box(buffer.[0]) :?> 'T
        else if (typeof<'T> = typeof<System.UInt16>) then
            box(BitConverter.ToUInt16(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt32>) then
            box(BitConverter.ToUInt32(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt64>) then
            box(BitConverter.ToUInt64(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Single>) then
            box(BitConverter.ToSingle(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Double>) then
            box(BitConverter.ToDouble(buffer, 0)) :?> 'T
        else if (typeof<'T> = typeof<System.Boolean>) then
            box(BitConverter.ToBoolean(buffer, 0)) :?> 'T
        else
            // deserialize
            let ms = new MemoryStream(buffer)
            let fmt = BinaryFormatter()
            fmt.Deserialize(ms) :?> 'T

    // 'T -> byte[]
    let ConvertFrom<'T> (x : 'T) =
        if (typeof<'T> = typeof<System.SByte>) then
            [|byte ((int (unbox<sbyte>(x)))+128)|]
        else if (typeof<'T> = typeof<System.Int16>) then
            BitConverter.GetBytes(unbox<int16>(x))
        else if (typeof<'T> = typeof<System.Int32>) then
            BitConverter.GetBytes(unbox<int32>(x))
        else if (typeof<'T> = typeof<System.Int64>) then
            BitConverter.GetBytes(unbox<int64>(x))
        else if (typeof<'T> = typeof<System.Byte>) then
            [|unbox<byte>(x)|]
        else if (typeof<'T> = typeof<System.UInt16>) then
            BitConverter.GetBytes(unbox<uint16>(x))
        else if (typeof<'T> = typeof<System.UInt32>) then
            BitConverter.GetBytes(unbox<uint32>(x))
        else if (typeof<'T> = typeof<System.UInt64>) then
            BitConverter.GetBytes(unbox<uint64>(x))
        else if (typeof<'T> = typeof<System.Boolean>) then
            BitConverter.GetBytes(unbox<bool>(x))
        else if (typeof<'T> = typeof<System.Single>) then
            BitConverter.GetBytes(unbox<System.Single>(x))
        else if (typeof<'T> = typeof<System.Double>) then
            BitConverter.GetBytes(unbox<System.Double>(x))
        else
            // serialize
            let ms = new MemoryStream()
            let fmt = BinaryFormatter()
            fmt.Serialize(ms, x)
            ms.GetBuffer()

    // ===================================================

    // compiler should hopefully optimize since typeof<'V> resolves at compile-time
    let Deserialize<'V> (s : Stream) =
        if (SupportedConvert<'V>()) then
            let buf = Array.zeroCreate<byte> sizeof<'V>
            s.Read(buf, 0, sizeof<'V>) |> ignore
            ConvertTo<'V> buf
        else
            let fmt = BinaryFormatter()
            fmt.Deserialize(s) :?> 'V

    // could make non-generic (without 'V) by using x.GetType() instead of typeof<'V>
    // but typeof<'V> resolves at compile time and is probably more performant
    let Serialize<'V> (s : Stream) (x : 'V) =
        if (SupportedConvert<'V>()) then
            s.Write(ConvertFrom x, 0, sizeof<'V>)
        else
            let fmt = BinaryFormatter()
            fmt.Serialize(s, x)

    // =======================================================

    // string to 'T
    let ConvertStringTo<'T> (str : string) =
        if (typeof<'T> = typeof<System.String>) then
            box(str) :?> 'T
        else if (typeof<'T> = typeof<System.SByte>) then
            box(Convert.ToSByte(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Int16>) then
            box(Convert.ToInt16(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Int32>) then
            box(Convert.ToInt32(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Int64>) then
            box(Convert.ToInt64(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Byte>) then
            box(Convert.ToByte(str)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt16>) then
            box(Convert.ToUInt16(str)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt32>) then
            box(Convert.ToUInt32(str)) :?> 'T
        else if (typeof<'T> = typeof<System.UInt64>) then
            box(Convert.ToUInt64(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Single>) then
            box(Convert.ToSingle(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Double>) then
            box(Convert.ToDouble(str)) :?> 'T
        else if (typeof<'T> = typeof<System.Boolean>) then
            box(Convert.ToBoolean(str)) :?> 'T
        else
            assert(false)
            Unchecked.defaultof<'T>

