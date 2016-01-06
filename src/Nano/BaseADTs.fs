namespace Prajna.Nano

open System

type Request = 
    | RunDelegate of int * Delegate
    | GetValue of int

type Response =
    | RunDelegateResponse of int
    | GetValueResponse of obj

