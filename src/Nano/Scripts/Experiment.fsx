#I __SOURCE_DIRECTORY__

#r @"..\..\Tools\Tools\bin\Debugx64\Prajna.Tools.dll"

#load "load-project-debug.fsx"

open Prajna.Nano

let serverNode = new ServerNode(1500)

let cn = new ClientNode("127.0.0.1", 1500)
let cn2 = new ClientNode("127.0.0.1", 1500)

let r1 = cn.NewRemote(fun _ -> "Test2")
let r2 = r1.Run(fun str -> str.Length)

let r3 = cn2.NewRemote(fun _ -> "Test33")
let r4 = r3.Run(fun str -> str.Length)

r1.GetValue()
r2.GetValue()
r3.GetValue()
r4.GetValue()
