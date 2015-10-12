namespace Prajna.Tools

#nowarn "9"
    
open System
open System.Runtime.InteropServices
open System.Security.Cryptography

open Microsoft.FSharp.NativeInterop

module Hash =

    open System.Collections.Generic

//    let inline safeItersWithoutOverflow maxShortAsLong = 
//        let sums = 
//            seq {
//                let mutable sum1 = maxShortAsLong
//                let mutable sum2 = maxShortAsLong
//                while true do
//                    sum1 <- sum1 + maxShortAsLong
//                    sum2 <- sum2 + sum1
//                    sum1 <- sum1 + maxShortAsLong
//                    sum2 <- sum2 + sum1
//                    yield sum2
//            }
//        sums 
//        |> Seq.pairwise
//        |> Seq.takeWhile (fun (a,b) -> a < b)
//        |> Seq.length
    [<Literal>] 
    let safeIntItersWithoutOverflow = 46337 // safeItersWithoutOverflow 0xFFFFFFFFUL - 2

    let inline ( ~% ) (ptr: nativeptr<'T>) = NativePtr.read ptr 
    let inline ( &+ ) (ptr: nativeptr<'T>) (index: int) = NativePtr.add ptr index

    type nativeptr<'T when 'T : unmanaged> with
        member inline this.Item(i: int) = NativePtr.get this i


    // Hashes a byte stream by interpreting it as a sequence of 32-bit uints,
    // computing its sum and cummulative sum modulo 2^32-1, 
    // and returning cumSum <<< 32 || sum
    type internal Fletcher64() =
        inherit HashAlgorithm()

#if VERIFY_HASH
        static let sha256s = HashSet<Guid>()
        static let f64s = HashSet<Guid>()
        let inner = SHA256Managed()
#endif

        // We use uint64s storage for speed and convenience, but before we return,
        // each is taken modulo 2^32-1 and stacked together in a single 64-bit result
        // By convention, sum1 is the regular sum and sum2 the cummulative sum
        let mutable sum1 = 0xFFFFFFFFUL
        let mutable sum2 = 0xFFFFFFFFUL
        let mutable edgeBytesDone = 0
        let mutable edgeBytes = 0UL
        let mutable totalSize = 0UL

        let reset() = 
            sum1 <- 0xFFFFFFFFUL
            sum2 <- 0xFFFFFFFFUL
            edgeBytesDone <- 0
            edgeBytes <- 0UL

        let doMod() = 
            // Faster way to do sum % (2^32-1)
            sum1 <- (sum1 &&& 0xFFFFFFFFUL) + (sum1 >>> 32) 
            sum2 <- (sum2 &&& 0xFFFFFFFFUL) + (sum2 >>> 32)

        let addEdgeBytes() = 
            // since we use a long pointer to fetch ints from the byte array,
            // the first int (in byte array index order) is in the low part
            // so we sum that first
            sum1 <- sum1 + (edgeBytes &&& 0xFFFFFFFFUL)
            sum2 <- sum2 + sum1
            sum1 <- sum1 + (edgeBytes >>> 32)
            sum2 <- sum2 + sum1
            doMod() 

        let addEdge (bytePtr: nativeptr<byte>) (edgeLen: int)= 
            assert(edgeBytesDone < 8)
            assert(0 < edgeLen && edgeLen < 8)
            let mutable i = 0
            // load Big-endian uint32s, byte by byte, into the edgeBytes uint64
            // the first int, in byte array order, will end up in the low part of edgeBytes
            while i < edgeLen do
                edgeBytes <- (edgeBytes >>> 8) ||| (uint64 bytePtr.[i] <<< 56)
                edgeBytesDone <- edgeBytesDone + 1
                i <- i + 1
            assert (edgeBytesDone <= 8)
            if edgeBytesDone = 8 then 
                do addEdgeBytes()
                do doMod()
                edgeBytesDone <- 0
                edgeBytes <- 0UL

        let fletcher64 (voidPtr: nativeint) (voidPtrLen: int) : unit =

            let bytePtr, bytesLen = 
                if edgeBytesDone > 0 then
                    let bytePtr = NativePtr.ofNativeInt<byte> voidPtr
                    let edgeSize = min voidPtrLen (8 - edgeBytesDone)
                    addEdge bytePtr edgeSize
                    bytePtr &+ edgeSize, voidPtrLen - edgeSize
                else
                    NativePtr.ofNativeInt<byte> voidPtr, voidPtrLen

            if bytesLen > 0 then
                let bytePtr, bytesLen =
                    let longPtr = NativePtr.toNativeInt bytePtr |> NativePtr.ofNativeInt<uint64>
                    let numLongs = bytesLen / 8
                    for blockStart in 0 .. safeIntItersWithoutOverflow .. (numLongs - 1) do
                        let timesInner = min safeIntItersWithoutOverflow (numLongs - blockStart)
                        // Copy to locals to ensure enregistration. Yields major perf improvement (> 60%).
                        let mutable localSum1 = sum1
                        let mutable localSum2 = sum2
                        for pos = blockStart to (blockStart + timesInner - 1) do
                            let long = longPtr.[pos]
                            // Add low part first (first int), then high part
                            localSum1 <- localSum1 + (long &&& 0xFFFFFFFFUL)
                            localSum2 <- localSum2 + localSum1
                            localSum1 <- localSum1 + (long >>> 32)
                            localSum2 <- localSum2 + localSum1
                        sum1 <- localSum1
                        sum2 <- localSum2
                        do doMod()
                    let bytesDone = 8 * numLongs
                    bytePtr &+ bytesDone, bytesLen - bytesDone

                assert (bytesLen < 8)
                if bytesLen > 0 then
                    addEdge bytePtr bytesLen

        override this.Initialize() = reset()

        override this.HashCore(arr: byte[], offset: int, count: int) = 
            assert (offset >= 0)
            assert (count >= 0)
            assert (offset + count <= if arr = null then 0 else arr.Length)
#if VERIFY_HASH
            inner.TransformBlock(arr, offset, count, null, 0) |> ignore
#endif
            if edgeBytesDone = -1 then
                raise <| InvalidOperationException("TransformBlock(...) not supported after Finish()")
            if count > 0 then
                totalSize <- totalSize + uint64 count
                let handle = GCHandle.Alloc(arr, GCHandleType.Pinned)
                try
                    let ptr = NativePtr.ofNativeInt<byte> <| handle.AddrOfPinnedObject()
                    fletcher64 (NativePtr.toNativeInt (ptr &+ offset)) count
                finally
                    handle.Free() 


        override this.HashFinal() : byte[] = 
#if VERIFY_HASH
            inner.TransformFinalBlock([||], 0, 0) |> ignore
            let innerHash = inner.Hash 
#endif
            this.HashCore(BitConverter.GetBytes(totalSize), 0, sizeof<uint64>)
            if edgeBytesDone > 0 then
                edgeBytes <- edgeBytes >>> (8 * (8 - edgeBytesDone))
                // Even though we read 64 bits at a time, the stream is still interpreted
                // as 32-bit uints. Therefore, we only zero-pad to the next *uint32*.
                // Padding to full uint64 would not affect the regular sum (sum1)
                // but would affect the cummulative sum2.
                sum1 <- sum1 + (edgeBytes &&& 0xFFFFFFFFUL)
                sum2 <- sum2 + sum1
                if edgeBytesDone > 4 then
                    sum1 <- sum1 + (edgeBytes >>> 32)
                    sum2 <- sum2 + sum1
                do doMod()
            edgeBytesDone <- -1
            let finalHash = BitConverter.GetBytes(sum2 <<< 32 ||| sum1)
            //BitConverter.GetBytes(sum2 <<< 32 ||| sum1)
            // Array.concat [|bytes; bytes; bytes; bytes|]
#if VERIFY_HASH
            lock f64s (fun _ ->
                f64s.Add(Guid(Array.concat [|finalHash; finalHash|])) |> ignore
                sha256s.Add(Guid(innerHash.[0..15])) |> ignore
                if f64s.Count <> sha256s.Count then
                    failwith "Found collision!"
            )
#endif
            finalHash


        override this.Dispose(disposing: bool) = 
#if VERIFY_HASH
            inner.Dispose()
#endif
            base.Dispose(disposing)

    let CreateChecksum() : HashAlgorithm = upcast new Fletcher64()
