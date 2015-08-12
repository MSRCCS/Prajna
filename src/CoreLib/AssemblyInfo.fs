namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("Prajna.CoreLib")>]
[<assembly: AssemblyProductAttribute("Prajna")>]
[<assembly: AssemblyDescriptionAttribute("Prajna: A Distributed Functional Programming Platform for Interactive Big Data Analytics and Cloud Service Building")>]
[<assembly: AssemblyVersionAttribute("0.0.1.0")>]
[<assembly: AssemblyFileVersionAttribute("0.0.1.0")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.1.0"
