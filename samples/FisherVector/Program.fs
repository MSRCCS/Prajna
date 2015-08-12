open System
open System.IO
open System.Drawing
open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp
open ShoNS.Array
open FisherVector.PDesc2FV
open FisherVector.PExtractFV

//System.Environment.SetEnvironmentVariable("shodir", """.""")
//System.Environment.SetEnvironmentVariable("shodir", """C:\Program Files (x86)\Sho 2.1-A""")

let mutable gridSpacing = 0
let mutable patchSizes = [||]
let mutable spr = Unchecked.defaultof<IntArray>
let mutable pqDim = 0
let gmmFileRemote = "gmm.bin"
let pcaFileRemote = "pca.bin"
let pqFileRemote = "pq.bin"

let lockObj = new Object()

let mutable PFVExt : Option<PFVExtractor> = None

let GetRemoteFile file =
    if File.Exists (Path.Combine(Directory.GetCurrentDirectory(), file)) then
        Path.Combine(Directory.GetCurrentDirectory(), file)
    else
        Path.Combine(AppDomain.CurrentDomain.BaseDirectory, file)

let SetShoDir() =
    //Logger.Log(LogLevel.Info, ( sprintf "current %s" (Directory.GetCurrentDirectory())))
    //if File.Exists (Path.Combine([|Directory.GetCurrentDirectory();"bin";"ShoViz.dll"|])) then
    //    Logger.Log(LogLevel.Info, ( sprintf "set sho directory to %s" (Directory.GetCurrentDirectory()) ))
    //    System.Environment.SetEnvironmentVariable("shodir", Directory.GetCurrentDirectory())
    //else
    //    Logger.Log(LogLevel.Info, ( sprintf "set sho directory to %s" AppDomain.CurrentDomain.BaseDirectory ))
    //    System.Environment.SetEnvironmentVariable("shodir", AppDomain.CurrentDomain.BaseDirectory)
    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "shodir set to %s" (System.Environment.GetEnvironmentVariable("shodir")) ))
    Initialize(&gridSpacing, &patchSizes, &spr, &pqDim)

let FisherVectorExtS(buf : byte[]) =
    let gmmFileLocal = GetRemoteFile(gmmFileRemote)
    (AppDomain.CurrentDomain.BaseDirectory, Directory.GetCurrentDirectory(), File.Exists gmmFileLocal)

let FisherVectorExt(filename : string, buf : byte[]) =
    lock lockObj (fun() ->
        if (PFVExt = None) then
            let gmmFileLocal = GetRemoteFile(gmmFileRemote)
            let pcaFileLocal = GetRemoteFile(pcaFileRemote)
            let pqFileLocal = GetRemoteFile(pqFileRemote)
            SetShoDir()
            PFVExt <- Some(new PFVExtractor(gmmFileLocal, pcaFileLocal, pqFileLocal, spr, gridSpacing, patchSizes, pqDim))
        let t1 = (DateTime.UtcNow)
        Logger.Log( LogLevel.Info, ( sprintf "Start processing file %s at time %A" filename t1))
        let fv = 
            try
                let ms = new MemStream(buf)
                use bmpStream = new Bitmap(ms)
                // simply copy uncompressed for testing
                if (false) then
                    let bmpData = bmpStream.LockBits(new Rectangle(0, 0, bmpStream.Width, bmpStream.Height),
                                                     Imaging.ImageLockMode.ReadWrite, bmpStream.PixelFormat);
                    let fv : byte[] = Array.zeroCreate(bmpData.Stride*bmpData.Height)
                    Runtime.InteropServices.Marshal.Copy(bmpData.Scan0, fv, 0, fv.Length)
                    fv
                else
                    PFVExt.Value.ExtractCompressed(bmpStream)
            with e->
                Logger.Log( LogLevel.Error, (sprintf "Error processing file %s with\n%A" filename e))
                [||]
        let t2 = (DateTime.UtcNow)
        Logger.Log( LogLevel.Info, ( sprintf "Finish processing file %s at time %A - elapse: %f" filename t2 (t2.Subtract(t1).TotalSeconds)))
        let filenameBase = filename.Substring(0, filename.LastIndexOf('.'))
        (filenameBase+".bin", Array.append (BitConverter.GetBytes(fv.Length)) fv)
    )

[<EntryPoint>]
let main argv = 
    let parse = ArgumentParser(argv)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let localdir = parse.ParseString( "-local", "" )
    let remoteDKVname = parse.ParseString( "-remote", "" )
    let versionInfo = parse.ParseString( "-ver", "" )
    let ver = if versionInfo.Length=0 then (DateTime.UtcNow) else StringTools.VersionFromString(versionInfo) 
    let dir = parse.ParseString( "-depdir", "..\..\dependencies" )
    let fvdir = Path.Combine( dir, "fishervector" )
    let shodir = Path.Combine( dir, "sho" )
//    let gmmFile = parse.ParseString( "-gmm", (dir+"\\fishervector\\gmm.bin") )
//    let pcaFile = parse.ParseString( "-pca", (dir+"\\fishervector\\pca.bin") )
//    let pqFile = parse.ParseString( "-pq", (dir+"\\fishervector\\pq.bin") )
    let bRunLocal = parse.ParseBoolean( "-run", false )
    DeploymentSettings.RemoteContainerEstablishmentTimeoutLimit <- parse.ParseInt64( "-timeout", 18000L )

    if (bRunLocal) then
        let localdir = Path.GetFullPath(localdir)
        let remoteDKVname = Path.GetFullPath(remoteDKVname)
        //Directory.SetCurrentDirectory(dir)
        let allFiles =
            if (File.Exists(localdir)) then
                [|localdir|]
            else
                Directory.GetFiles(localdir, "*", SearchOption.AllDirectories)
        if not (Directory.Exists remoteDKVname) then
            Directory.CreateDirectory(remoteDKVname) |> ignore
        for file in allFiles do
            Console.WriteLine("File: {0}", file)
            let outname = Path.Combine(remoteDKVname, Path.GetFileNameWithoutExtension(file)+".bin")
            if (File.Exists outname) then
                Logger.Log( LogLevel.Info, (sprintf "File %s exists - skipping" outname))
            else
                let (outname, output) = FisherVectorExt(Path.GetFileName(file), File.ReadAllBytes(file))
                File.WriteAllBytes(Path.Combine(remoteDKVname, outname), output)
    else
        JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask
        let curJob = JobDependencies.setCurrentJob "DistributedFisherVector"
        curJob.AddDataDirectoryWithPrefix( null, fvdir, null, "*", SearchOption.AllDirectories ) |> ignore
        curJob.AddDataDirectoryWithPrefix( null, shodir, "bin", "*", SearchOption.TopDirectoryOnly ) |> ignore
        /// Can't use SearchOption.AllDirectories, as we need to exclude bin32, otherwise, the .DLL in bin32 will be loaded, which crashes the application. 
        curJob.AddDataDirectoryWithPrefix( null, shodir + "\\bin64", "bin\\bin64", "*", SearchOption.TopDirectoryOnly ) |> ignore

        curJob.EnvVars.Add("shodir", DeploymentSettings.EnvStringGetJobDirectory )

        Cluster.Start( "", PrajnaClusterFile )

        let cluster = Cluster.GetCurrent()

        let t1 = (DateTime.UtcNow)
        // Order in constructor matters, why?
        // IsSource MUST come after Version = ver, otherwise version is wrong
        // since IsSource loads metadata and updates Version
        let mutable curDKV = DSet<string*byte[]>( Name = remoteDKVname,
                                                  Version = ver ) |> DSet.loadSource

        curDKV.StorageType <- StorageKind.HDD
        curDKV.NumParallelExecution <- 1
        let (numFiles, total) =
            let nFiles = ref 0
            let nTotal = ref 0UL
            //let procDKVSeq = curDKV.ToSeq()
            let fltDKV = curDKV.FilterByKey( fun fname -> fname.IndexOf(".jpg",StringComparison.OrdinalIgnoreCase)>=0 )
            let procDKV = fltDKV.Map(FisherVectorExt)
            procDKV.NumParallelExecution <- 1
            let procDKVSeq = procDKV.ToSeq()
            DSet.RetrieveFolderRecursive(localdir, procDKVSeq)
            //let procDKVSeq = curDKV.MapByValue(FisherVectorExtS).ToSeq()
            //Seq.iter (fun(a,b) -> 
            //    Console.WriteLine("{0} {1}", a, b)
            //    nFiles := !nFiles + 1
            //) procDKVSeq
            //(!nFiles, !nTotal)
        let t2 = (DateTime.UtcNow)
        let elapse = t2.Subtract(t1)
        Logger.Log( LogLevel.Info, ( sprintf "Processed %d Files with total %dB in %f sec, throughput = %f MB/s" numFiles total elapse.TotalSeconds ((float total)/elapse.TotalSeconds/1000000.) ))

        Cluster.Stop()

    0
