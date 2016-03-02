// Learn more about F# at http://fsharp.org
// See the 'F# Tutorial' project for more help.

open System
open Prajna.Tools
open Prajna.Nano

let printUsage() = printfn "Usage: NanoServer.exe <portToLisen>"

[<EntryPoint>]
let main argv = 

    do Prajna.Tools.Logger.ParseArgs([|"-verbose"; "err" (*; "-con"*)|])

    if argv.Length <> 1 then
        printUsage()
        -1
    else
        match UInt16.TryParse argv.[0] with
        | true, port ->
            BufferListStream<byte>.BufferSizeDefault <- 1 <<< 20
            BufferListStream<byte>.InitSharedPool()
            let thisAsm = System.Reflection.Assembly.GetExecutingAssembly()
            printfn "Starting NanoServer on port %d" port
            printfn "CodeBase is: %s" thisAsm.CodeBase
            use server = new ServerNode(int port)
            lock argv (fun _ -> System.Threading.Monitor.Wait argv |> ignore; 0)
        | _ -> 
            printfn "Error: Invalid port number"
            -2
