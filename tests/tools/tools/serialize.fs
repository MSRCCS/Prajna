#nowarn "9" // using [<StructLayout>] to test serialization
#nowarn "0346" // implementing Equals() without GetHashCode() for Assert.AreEqual(,)
namespace Prajna.Tools.Tests

open System
open System.Runtime.Serialization
open System.Diagnostics
open System.Collections.Generic
open System.Runtime.InteropServices
open System.IO

open NUnit.Framework

open Prajna.Tools

[<TestFixture(Description = "Tests for serialization")>]
module SerializationTests =

    type MemoryStreamConstructors = (unit -> MemoryStream) * ((byte[] * int * int * bool * bool) -> MemoryStream)

    do BufferListStream<byte>.InitSharedPool()
    let memStreamBConstructors : MemoryStreamConstructors = (fun () -> upcast new MemoryStreamB()), (fun (a,b,c,d,e) -> upcast new MemoryStreamB(a,b,c,d,e))
    let memStreamDotNetConstructors = (fun () -> new MemoryStream()), (fun (a,b,c,d,e) -> new MemoryStream(a,b,c,d,e))

    let MemoryStreamConstructors =  [| memStreamBConstructors; memStreamDotNetConstructors|]

    let roundTrip (streamConstructors: MemoryStreamConstructors) (obj: obj) = 
        let emptyConstructor = fst streamConstructors
        let stream = emptyConstructor()
        let formatter = GenericSerialization.GetDefaultFormatter( CustomizedSerializationSurrogateSelector(streamConstructors) )
        formatter.Serialize(stream, obj)
        stream.Position <- 0L
        formatter.Deserialize(stream)
        
    let testObject (obj: obj) = 
        for ms in MemoryStreamConstructors do
            Assert.AreEqual(obj, roundTrip ms obj)

    [<Test>]
    let testPrimitive() = testObject 5

    [<Test>]
    let testPrimitiveArray() = testObject [|1..10|]
        
    [<Test>]
    let testBoolArray() = testObject <| Array.concat (Array.init 5 (fun _ -> [|true; false|]))
        
    [<StructLayout(LayoutKind.Explicit)>]
    type Point = struct
        [<FieldOffset(0)>][<DefaultValue>] val mutable Which : bool
        [<FieldOffset(1)>][<DefaultValue>] val mutable X : float32 
        [<FieldOffset(5)>][<DefaultValue>] val mutable Y : float32
        [<FieldOffset(1)>][<DefaultValue>] val mutable Rho : float32
        [<FieldOffset(5)>][<DefaultValue>] val mutable Theta : float32
    end

    [<Test>]
    let testExplicitLayoutArray() = 
        let mutable p1 = new Point()
        p1.Rho <- 1.0f
        let p1 = p1
        testObject <| Array.concat (Array.init 5 (fun _ -> [|p1; new Point()|]))
        
    [<Test>]
    let testHigherRankArray() = testObject (Array2D.init 3 3 (fun i j -> 3 * i + j))
        
    [<Test>]
    let testPrimitiveList() = testObject [1..10]

    [<Test>]
    let testPrimitiveSeq() = testObject <| seq{1..10}

    type Complex =
        struct
            val Real: float
            val Imaginary: float
            new (r: float, i: float) = {Real = r; Imaginary = i; }
            override this.ToString() = sprintf "(%f, %f)" this.Real this.Imaginary
        end

    [<Test>]
    let testStruct() = testObject (Complex(1.0, 2.0))

    [<Test>]
    let testStructArray() = testObject [|Complex(1.0, 2.0); Complex(3.0, 4.0)|]

    [<Test>]
    let testHigherRankStructArray() = testObject <| Array2D.init 2 2 (fun i j -> Complex(float i, float j))
        
    [<Test>]
    let testHigherRankObjectArray() = testObject <| Array2D.init 2 2 (fun i j -> sprintf "%A" (Complex(float i, float j)))
        
    type PersonStruct =
        struct
            val Name: string
            val Building: int
            new (name: string, building: int) = {Name = name; Building = building}
        end

    [<Test>]
    let testStructWithRefField() = 
        let name = "foo"
        let ps = [|PersonStruct(name, 1); PersonStruct(name, 2)|]
        testObject ps
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms ps :?> PersonStruct[]
            Assert.AreSame(ps.[0].Name, ps.[1].Name) // of course
            Assert.AreEqual(ps.[0].Name, other.[0].Name) // equals survives serialization ...
            Assert.AreNotSame(ps.[0].Name, other.[0].Name) // ReferenceEquals does not
            Assert.AreSame(other.[0].Name, other.[1].Name) // ...but ReferenceEquals in the same graph still holds.

    type Person() =
        member val Name = "foo" with get,set
        member val Building = 99 with get, set
        override this.Equals(other: obj) = 
            match other with
            | :? Person as person -> person.Name = this.Name && person.Building = this.Building
            | _ -> false

    [<Test>]
    let testSimpleClass() = testObject <| Person()

    [<AllowNullLiteral>]
    type Cyclic() = 
        member val Value: int = 0 with get, set
        member val Next: Cyclic = null with get, set

    [<Test>]
    let testCyclicSelf() = 
        let cyclicValue = Cyclic(Value = 0, Next = null)
        cyclicValue.Next <- cyclicValue
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms cyclicValue :?> Cyclic
            Assert.AreEqual(cyclicValue.Value, other.Value)
            Assert.AreEqual(other.Next, other)

    [<Test>]
    let testCyclicDirect() = 
        let cyclicValue = Cyclic(Value = 0, Next = Cyclic(Value = 1, Next = null))
        cyclicValue.Next.Next <- cyclicValue
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms cyclicValue :?> Cyclic
            Assert.AreEqual(cyclicValue.Value, other.Value)
            Assert.AreEqual(cyclicValue.Next.Value, other.Next.Value)
            Assert.AreEqual(other.Next.Next, other)

    [<Test>]
    let testCyclicIndirect() = 
        let cyclicValue = Cyclic(Value = 0, Next = Cyclic(Value = 1, Next = Cyclic(Value = 2, Next = null)))
        cyclicValue.Next.Next.Next <- cyclicValue
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms cyclicValue :?> Cyclic
            Assert.AreEqual(cyclicValue.Value, other.Value)
            Assert.AreEqual(cyclicValue.Next.Value, other.Next.Value)
            Assert.AreEqual(cyclicValue.Next.Next.Value, other.Next.Next.Value)
            Assert.AreEqual(other.Next.Next.Next, other)

    [<Test>]
    let testType() = 
        testObject typeof<Cyclic>

    let ignoreArg f = fun _ -> f()
    
    [<Test>]
    let testClosure() =
        let cur = ref 0
        let next() = let ret = !cur in cur := ret + 1; ret
        do next() |> ignore; next()|> ignore; next()|> ignore
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms next :?> (unit -> int)
            Assert.AreEqual(Array.init 3 (ignoreArg next), Array.init 3 (ignoreArg other))

    [<Test>]
    let testClosureNegative() =
        let cur = ref 0
        let next() = let ret = !cur in cur := ret + 1; ret
        do next() |> ignore; next()|> ignore; next()|> ignore
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms next :?> (unit -> int)
            cur := !cur + 1
            Assert.AreNotEqual(Array.init 3 (ignoreArg next), Array.init 3 (ignoreArg other))

    [<Test>]
    let testDictionaryCustomSer() =
        let dict = new Dictionary<string, int>()
        dict.Add("a", 1)
        dict.Add("b", 2)
        dict.Add("d", 3)
        testObject dict

    [<Test>]
    let testDictionaryCustomSerSelfCycle() =
        let dict = new Dictionary<string, obj>()
        dict.Add("a", 1)
        dict.Add("0", dict)
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms dict :?> Dictionary<string, obj>
            Assert.AreEqual(dict.["a"], other.["a"])
            Assert.IsTrue( Object.ReferenceEquals( other, other.["0"] ) )

    [<Test>]
    let testDictionaryCustomSerCyclic() =
        let dict1 = new Dictionary<string, obj>()
        let dict2 = new Dictionary<string, obj>()
        dict1.Add("2", dict2)
        dict2.Add("1", dict1)
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms dict1 :?> Dictionary<string, obj>
            Assert.IsTrue( Object.ReferenceEquals( other, (other.["2"] :?> Dictionary<string, obj>).["1"]  ) )

    type [<AllowNullLiteral>] KitchenSink() = 
        member val Name: string = "Bruno" with get, set
        member val Building: int = 99 with get, set
        member val Nums : int[] = [|1..3|] with get, set
        member val OneComplex = Complex(1.0, 1.0) with get, set
        member val Complexes : Complex[] = Array.init 3 (fun j -> Complex(float j, float -j))
        member val Parents : KitchenSink[] = null with get, set
        member val Other: KitchenSink option = None with get, set
        member val AddOne: (int -> int) = (fun x -> x + 1)
        override this.ToString() = sprintf "SS(%s, %A, %A, %A, %A)" this.Name this.Nums this.OneComplex this.Complexes "(other)"
        member this.NonRecursiveEquals(other: KitchenSink) =
            this.Name = other.Name
            && this.Building = other.Building
            && this.Nums = other.Nums
            && this.OneComplex = other.OneComplex
            && this.Complexes = other.Complexes

        member private this.Traverse (visited: HashSet<KitchenSink>) =
            seq {
                if not <| visited.Contains(this) then
                    visited.Add(this) |> ignore
                    yield this
                    if this.Other.IsSome then
                        yield! this.Other.Value.Traverse(visited)
                    if this.Parents <> null then
                        yield! (this.Parents |> Seq.map (fun p -> p.Traverse(visited)) |> Seq.concat)
            }
        member this.Traverse() = this.Traverse(new HashSet<_>())

    type OtherStuff =
        struct
            val mutable Stuff: string 
            val mutable SomeInt: int
            val mutable Complex : Complex
            val mutable Complexes : Complex[]
            val mutable SS0: KitchenSink
            val mutable SS: KitchenSink[]
            override this.ToString() = sprintf "OtherStuff(%A, %A, %d, %A)" this.Complex this.Stuff this.SomeInt this.SS
        end

    let createKitchenSinkArray() =
        let kitchenSink = new KitchenSink()
        kitchenSink.Name <- "Foo"
        let kitchenSinkArr : KitchenSink[] = 
            let newSink = new KitchenSink(Other = Some kitchenSink) in kitchenSink.Other <- Some newSink
            [|kitchenSink; newSink|]
        kitchenSinkArr

    [<Test>]
    let testKitchenSink() = 
        let ks = createKitchenSinkArray().[0]
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms ks :?> KitchenSink
            Assert.IsTrue <|
                Seq.forall2 (fun (ks1: KitchenSink) ks2 -> ks1.NonRecursiveEquals(ks2)) (ks.Traverse()) (other.Traverse()) 

    [<Test>]
    let testOtherStuff() =
        let mutable os = OtherStuff()
        os.Complex <- Complex(5.0, 5.0)
        os.Complexes <- Array.init 3 (fun j -> Complex(float j, float -j))
        os.SomeInt <- 5
        os.Stuff <- "test"
        let kitchenSinkArr = createKitchenSinkArray()
        os.SS0 <- new KitchenSink(Name = "Eric", Parents = kitchenSinkArr)
        os.SS <- kitchenSinkArr
        for ms in MemoryStreamConstructors do
            let other = roundTrip ms os :?> OtherStuff
            Assert.AreEqual(os.Complex, other.Complex)
            Assert.AreEqual(os.Complexes, other.Complexes)
            Assert.AreEqual(os.SomeInt, other.SomeInt)
            Assert.AreEqual(os.Stuff, other.Stuff)
            Assert.IsTrue(os.SS0.NonRecursiveEquals(other.SS0))
            for (k, o) in Seq.zip os.SS other.SS do
                Assert.IsTrue <| k.NonRecursiveEquals(o)

    [<AutoSerializable(false)>]
    type MyType() =
        member val Data1 = Array.init 10 byte
        member val Data2 = Array.init 10 byte |> Array.rev
        override this.Equals(other: obj) = 
            match other with
            | :? MyType as mt -> this.Data1 = mt.Data1 && this.Data2 = mt.Data2
            | _ -> false

    [<Test>]
    let testNonSerializable() = 
        testObject <| MyType()
        
    let timeRoundTrip (name: string) (formatter: IFormatter) (stream: MemoryStream) obj =
        GC.Collect()
        let sw = Stopwatch.StartNew()
        do formatter.Serialize(stream, obj)
        let serTime = sw.Elapsed
        stream.Position <- 0L
        sw.Restart()
        let ret = formatter.Deserialize(stream) 
        let deserTime = sw.Elapsed
        serTime, deserTime

    let memStreamConstructors = memStreamBConstructors
    let noArgConstructor = fst memStreamBConstructors

    let compareRoundTripTimes : int -> obj -> TimeSpan * TimeSpan = 
        let selector = CustomizedSerializationSurrogateSelector(memStreamConstructors)
        let prajnaFormatter = GenericSerialization.GetFormatter(GenericSerialization.PrajnaFormatterGuid, selector)
        let binaryFormatter = GenericSerialization.GetFormatter(GenericSerialization.BinaryFormatterGuid, selector)
        fun numRepeats obj ->
            let accPair (f,s) (f2,s2) = f + f2, s + s2
            let sumTimePairs = Seq.fold accPair (TimeSpan.Zero, TimeSpan.Zero)
            let binarySer,binaryDeser = [for i in 1..numRepeats -> timeRoundTrip "Binary" binaryFormatter (noArgConstructor()) obj] |> sumTimePairs
            let prajnaSer,prajnaDeser = [for i in 1..numRepeats -> timeRoundTrip "Prajna" prajnaFormatter (noArgConstructor()) obj] |> sumTimePairs
            Console.WriteLine(sprintf "Prajna: %A, %A, %A" prajnaSer prajnaDeser (prajnaSer + prajnaDeser))
            Console.WriteLine(sprintf "Binary: %A, %A, %A" binarySer binaryDeser (binarySer + binaryDeser))
            (prajnaSer + prajnaDeser), (binarySer + binaryDeser)
    
    [<Test>]
    [<Category("Performance")>]
    let SerPerfMatrix() =
        printfn "Square Matrix"
        let r = new Random()
        let A : float[,] = Array2D.init 5000 5000 (fun _ _ -> r.NextDouble() |> float)
        let prajnaTime, binaryTime = compareRoundTripTimes 1 A
        Assert.IsTrue(prajnaTime.TotalMilliseconds < binaryTime.TotalMilliseconds)
        
    [<Test>]
    [<Category("Performance")>]
    let SerPerfRank3Tensor() =
        printfn "Cube Tensor"
        let r = new Random()
        let A : float[,,] = Array3D.init 300 300 300 (fun _ _ _ -> r.NextDouble() |> float)
        let prajnaTime, binaryTime = compareRoundTripTimes 1 A
        Assert.IsTrue(prajnaTime.TotalMilliseconds < binaryTime.TotalMilliseconds)
        
    let createRandomJaggedMatrix rows cols = 
        let r = new Random()
        Array.init rows (fun _ -> Array.init cols (fun _ -> r.NextDouble() |> float))

    let baseSize = 5000
    
    [<Test>]
    [<Category("Performance")>]
    let SerPerfJaggedArrayA_VeryWide() =
        printfn "Very Wide Jagged"
        let A = createRandomJaggedMatrix (baseSize / 100) (baseSize * 100) 
        let prajnaTime, binaryTime = compareRoundTripTimes 60 A
        Assert.IsTrue(prajnaTime.TotalMilliseconds < binaryTime.TotalMilliseconds)
        
    [<Test>]
    [<Category("Performance")>]
    let SerPerfJaggedArrayB_Wide() =
        printfn "Wide Jagged"
        let A = createRandomJaggedMatrix (baseSize / 10) (baseSize * 10) 
        let prajnaTime, binaryTime = compareRoundTripTimes 60 A
        Assert.IsTrue(prajnaTime.TotalMilliseconds < binaryTime.TotalMilliseconds)
        
    [<Test>]
    [<Category("Performance")>]
    let SerPerfJaggedArrayC_Square() =
        printfn "Square Jagged"
        let A = createRandomJaggedMatrix baseSize baseSize
        let prajnaTime, binaryTime = compareRoundTripTimes 20 A
        Assert.IsTrue(prajnaTime.TotalMilliseconds < binaryTime.TotalMilliseconds)
        
    [<Test>]
    [<Category("Performance")>]
    let SerPerfJaggedArrayD_Tall() =
        printfn "Tall Jagged"
        let A = createRandomJaggedMatrix (baseSize * 10) (baseSize / 10)
        let prajnaTime, binaryTime = compareRoundTripTimes 20 A
        Assert.IsTrue(prajnaTime.TotalMilliseconds < binaryTime.TotalMilliseconds)
        
    [<Test>]
    [<Category("Performance")>]
    let SerPerfJaggedArrayE_VeryTall() =
        printfn "Very Tall Jagged"
        let A = createRandomJaggedMatrix (baseSize * 100) (baseSize / 100)
        let prajnaTime, binaryTime = compareRoundTripTimes 1 A
        Assert.IsTrue(prajnaTime.TotalMilliseconds < binaryTime.TotalMilliseconds)


