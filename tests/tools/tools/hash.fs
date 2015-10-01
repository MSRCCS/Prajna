namespace Prajna.Tools.Tests

open System
open System.Security.Cryptography
open System.Collections.Generic

open NUnit.Framework

open Prajna.Tools
open Prajna.Tools.Hash

[<TestFixture(Description = "Tests for hashing")>]
module HashingTests =

    let hashBlocks (data: byte[]) (blockSizes: int[]) =
        System.Diagnostics.Debug.Assert(Array.sum blockSizes = data.Length)
        let f64 : HashAlgorithm = Hash.CreateChecksum()
        for start,len in Seq.zip (blockSizes |> Seq.scan (+) 0 |> Seq.take blockSizes.Length) blockSizes do
            f64.TransformBlock(data, start, len, null, 0) |> ignore
        f64.TransformFinalBlock([||], 0, 0) |> ignore
        f64.Hash

    let getBytes (data: int[]) = data |> Array.map BitConverter.GetBytes |> Array.concat
    
    let hashInts (data: int[]) =
        let bytes = getBytes data
        hashBlocks bytes [|bytes.Length|]

    let genBlocks size =
        let r = new Random(0)
        seq {
            while true do
                let sizes = List<int>()
                while sizes |> Seq.sum < size do
                    sizes.Add (r.Next(size + 1))
                if sizes |> Seq.sum > size then
                    sizes.[sizes.Count - 1] <- size - (sizes |> Seq.take (sizes.Count - 1) |> Seq.sum)
                yield (sizes |> Seq.toArray)                
        } 

    [<Test>]
    let HashBlockSizes() = 
        let dataBlocks = 
            let r = new System.Random(0)
            [for _ in 1..10 do
                let data : byte[] = Array.init (r.Next(100) + 1000) (fun _ -> byte <| r.Next(10) + 246)
                let blockSizes = genBlocks data.Length |> Seq.take 10 |> Seq.toArray
                yield data, blockSizes]
        let results = 
            [for data,blocks in dataBlocks do
                if blocks.Length > 1 then
                    let b0 = blocks.[0]
                    for b in blocks.[1..] do
                        let h = hashBlocks data b0
                        yield (h = hashBlocks data b), h]
        Assert.IsTrue(results |> Seq.map fst |> Seq.forall id)

    [<Test>]
    let HashKnownValue() = 
        let data = getBytes [|1; 2|]
        for blocks in genBlocks data.Length |> Seq.take 100 do
            let hash =  BitConverter.ToUInt64(hashBlocks data blocks, 0)
            let lo = hash &&& 0xFFFFFFFFUL
            let hi = hash >>> 32
            // BitConverter.GetBytes(uint64 data.Length) is added to the array
            // so we must sum that up as well
            Assert.AreEqual(11UL, lo) // 1 + 2 + 8 + 0 = 11
            Assert.AreEqual(26UL, hi) // 1 + (1 + 2) + (1 + 2 + 8) + (1 + 2 + 8 + 0) = 26

    [<Test>]
    let HashSamePaddedDataOnlyDifferentLength() = 
        let data = getBytes [|1; 2; 0; 0; 0|]
        use f64 = Hash.CreateChecksum()
        let hashes = Array.init (data.Length + 1) (fun i -> f64.ComputeHash(data, 0, i))
        for i = 0 to hashes.Length - 1 do
            for j = i + 1 to hashes.Length - 1 do
                Assert.AreNotEqual( hashes.[i], hashes.[j] )

    [<Test>]
    let HashMultipleMods() = 
        let data =
            let r = new Random(0)
            Array.init 200000 (fun _ -> byte <| r.Next(256))
        let bs = genBlocks data.Length |> Seq.take 2 |> Seq.toArray
        let b0Hash = hashBlocks data bs.[0]
        for b in bs.[1..] do
            Assert.AreEqual(b0Hash, hashBlocks data b)



