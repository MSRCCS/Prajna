namespace Prajna.Core.Tests

open System.Reflection;
open System.Runtime.CompilerServices

module internal AssemblyProperties =

#if DEBUG
    [<assembly: AssemblyConfiguration("Debug")>]
#else
    [<assembly: AssemblyConfiguration("Release")>]
#endif 

    do()
