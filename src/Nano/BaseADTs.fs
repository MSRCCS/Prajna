namespace Prajna.Nano

open System
open System.Threading

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
        | GetValue of int
        | GetServerId

    type Response =
        | RunDelegateResponse of int
        | GetValueResponse of obj
        | GetServerIdResponse of Guid

