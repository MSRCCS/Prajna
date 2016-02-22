#I __SOURCE_DIRECTORY__

#r @"..\..\Tools\Tools\bin\Debugx64\Prajna.Tools.dll"

#load "load-project-debug.fsx"

open Prajna.Nano

let remoteNextClients : Remote<ClientNode>[] = null



do Prajna.Tools.Logger.ParseArgs([|"-con"|])

let serverNode = new ServerNode(1500)

let cn = new ClientNode(ServerNode.GetDefaultIP(), 1500)
let cn2 = new ClientNode(ServerNode.GetDefaultIP(), 1500)

async {
    let acc = ref 0
    for i = 1 to 1 do
        let! r1 = cn.AsyncNewRemote(fun _ -> "Test2")
        let! r2 = r1.AsyncApply(fun str -> str.Length)

        let! r3 = cn2.AsyncNewRemote(fun _ -> "Test33")
        let! r4 = r3.AsyncApply(fun str -> str.Length)

        acc := !acc + r2.GetValue() + r4.GetValue()

    printfn "%d" !acc
}
|> Async.RunSynchronously
printfn ""

