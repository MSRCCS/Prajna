namespace Prajna.Tools.Tests

open System
open System.IO

open NUnit.Framework

open Prajna.Tools

[<TestFixture(Description = "Tests for type MemStream")>]
module MemStreamTests =

    [<Test(Description = "Test constructors")>]
    let testConstructors() =
        BufferListStream<byte>.InitSharedPool()
        
        use s1 = new MemStream()
        Assert.AreEqual(0, s1.Length)        
        // GetValidBuffer is internalized and is no longer assessable
        // Assert.IsEmpty(s1.GetValidBuffer())

        use s2 = new MemStream(100)
        Assert.AreEqual(0, s2.Length)
        Assert.IsTrue(s2.Capacity >= 100)
//        Assert.AreEqual(100, s2.Capacity)        
//        Assert.IsEmpty(s2.GetValidBuffer())

        let byteArray = [| 1uy; 2uy; 3uy; 4uy |]

        use s3 = new MemStream(byteArray)
        Assert.AreEqual(4, s3.Length)        
//        Assert.AreEqual(4, s3.GetValidBuffer().Length)
//        Assert.AreEqual(1uy, s3.GetValidBuffer().[0])
//        Assert.AreEqual(2uy, s3.GetValidBuffer().[1])

        use s4 = new MemStream(byteArray, false)
        Assert.AreEqual(4, s4.Length)        
        Assert.IsFalse(s4.CanWrite)

        use s5 = new MemStream(byteArray, 1, 2)
        Assert.AreEqual(2, s5.Length)        

        use s6 = new MemStream(byteArray, 0, 3, true)
        Assert.AreEqual(3, s6.Length)        
        Assert.IsTrue(s5.CanWrite)

        use s7 = new MemStream(byteArray, 3, 1, true, true)
        Assert.AreEqual(1, s7.Length)        
        Assert.IsTrue(s7.CanWrite)

        // use s8 = new MemStream(s7)
        // Assert.AreEqual(1, s8.Length)        

        // use s9 = new MemStream(s3, 1L)
        // Assert.AreEqual(3, s9.Length)        

    [<Test(Description = "Test read write bytes")>]
    let testReadWriteBytes() =
        let b1 : byte array = Array.zeroCreate 15
        use s1 = new MemStream(b1, 0, 15, true, true)

        let b2 = [| for i in 1uy..5uy do yield i |] 
        s1.WriteBytes(b2)

        let b3 : byte array = Array.zeroCreate 10
        s1.Seek(0L, SeekOrigin.Begin) |> ignore
        s1.ReadBytes(b3) |> ignore 
        b3 |> Array.iteri ( fun i x -> if i < 5 then Assert.AreEqual((byte)i + 1uy, x) else Assert.AreEqual(0uy, x))
               
        let b4 = [| for i in 1uy..10uy do yield i |] 
        s1.WriteBytesWithOffset(b4, 5, 5)
        s1.Seek(0L, SeekOrigin.Begin) |> ignore

        // s1:  1, 2, 3, 4, 5, 0, 0, 0, 0, 0, 6, 7, 8, 9, 10
        s1.ReadBytes(b3) |> ignore 
        b3 |> Array.iteri ( fun i x -> if i < 5 then Assert.AreEqual((byte)i + 1uy, x) else Assert.AreEqual(0uy, x))
        s1.ReadBytes(b3) |> ignore 
        b3 |> Array.iteri ( fun i x -> if i < 5 then Assert.AreEqual((byte)(i + 5 + 1), x) else Assert.AreEqual(0uy, x))
        
        s1.Seek(0L, SeekOrigin.Begin) |> ignore
        let b5 = s1.ReadBytes(12) 
        Assert.AreEqual(12, b5.Length)
        b5 |> Array.iteri ( fun i x -> if i < 5 then Assert.AreEqual((byte)i + 1uy, x) 
                                       else if i < 10 then Assert.AreEqual(0uy, x)
                                       else Assert.AreEqual((byte)(i - 10 + 6), x))

        let b6 = s1.ReadBytesToEnd()
        Assert.AreEqual(3, b6.Length)
        b6 |> Array.iteri ( fun i x -> Assert.AreEqual((byte)i + 8uy, x))
