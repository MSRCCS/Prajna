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
		Math.fs
  
	Description: 
		Helper math function 

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        May. 2013
	
 ---------------------------------------------------------------------------*)
namespace Prajna.Tools
open System
[<StructuralEquality; StructuralComparison>]
type internal UInt128 = 
    struct 
        val High : uint64
        val Low : uint64
        new ( high: uint64, low: uint64 ) = { High = high; Low = low }
        new ( value:byte[] ) = { High = BitConverter.ToUInt64( value, 0 ); Low = BitConverter.ToUInt64( value, 8) }
        override x.ToString() = x.High.ToString("X16")+x.Low.ToString("X16")
    end

type internal MurmurHash() = 
    [<Literal>] 
    static let Murmur64_m = 0xc6a4a7935bd1e995UL
    [<Literal>] 
    static let Murmur64_r = 47
    static member MurmurHash64OneStep( h1, d1 ) =
        let d2 = Murmur64_m * d1
        let d3 = d2 ^^^ ( d2 >>> Murmur64_r )
        let d4 = d3 * Murmur64_m

        let h2 = h1 ^^^ d4
        let h3 = h2 * Murmur64_m
        h3
    static member MurmurHash64OneStep( h1:int64, d1:int64 ) =
        int64 (MurmurHash.MurmurHash64OneStep( uint64 h1, uint64 d1 ))

/// This class is created for initialization of Random(), in which the 
/// Random generator can be correlated as they have the same time signature. A salt is added to make sure that the 
/// resultant random number is different based on different salt value. 
type RandomWithSalt( salt:int64 ) = 
    inherit Random( int (MurmurHash.MurmurHash64OneStep( salt, (PerfADateTime.UtcNowTicks()) )) )
    /// Construct Random() class with a salt, so that different random generator seed is used in a multithread environment. 
    new ( salt:int ) = 
        RandomWithSalt( int64 salt )    

type internal Algorithm() =
    /// Water filling algorithm implementation
    /// First fulfilling the requirement of small value bins, and then move to bigger bins
    static member WaterFilling (valArray:int64[] ) (total:int64) = 
        let retArray = Array.zeroCreate<_> valArray.Length
        let sortedValArray = 
            valArray 
            |> Array.mapi ( fun i v -> (i,v) )
            |> Array.sortBy( fun (i,v) -> v )
        let mutable nAllocated = 0L
        for i = 0 to sortedValArray.Length-1 do 
            let idx, minVal = sortedValArray.[i]
            let v = Math.Min( ( total - nAllocated ) / int64 ( sortedValArray.Length - i ), minVal )
            retArray.[idx] <- v
            nAllocated <- nAllocated + v
        retArray

