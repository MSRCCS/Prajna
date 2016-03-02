namespace Prajna.Nano

open System

open Prajna.Tools

module BaseADTs =

    let mutable private cur = 0L

    type Numbered<'T> = Numbered of Number: int64 * Value: 'T with
        member this.N = let (Numbered(n,_)) = this in n
        member this.V = let (Numbered(_,v)) = this in v

    let newNumbered x =
        let newNumber = System.Threading.Interlocked.Increment &cur
        Numbered(newNumber,x)

    type Request = 
        | RunDelegate of int * Delegate
        | RunDelegateAndGetValue of int * Delegate
        | RunDelegateAndAsyncGetValue of int * Delegate
        | RunDelegateSerialized of int * MemoryStreamB
        | GetValue of int

    type Response =
        | RunDelegateResponse of int
        | GetValueResponse of obj



