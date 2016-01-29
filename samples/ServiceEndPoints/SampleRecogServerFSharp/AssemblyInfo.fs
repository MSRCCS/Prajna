namespace System
open System.Reflection

[<assembly: AssemblyTitleAttribute("VMHub.Services.SampleRecogServerFSharp")>]
[<assembly: AssemblyProductAttribute("VMHub.Services")>]
[<assembly: AssemblyDescriptionAttribute("VMHub: Visual Media Hub")>]
[<assembly: AssemblyVersionAttribute("0.0.1.2")>]
[<assembly: AssemblyFileVersionAttribute("0.0.1.2")>]
do ()

module internal AssemblyVersionInformation =
    let [<Literal>] Version = "0.0.1.2"
