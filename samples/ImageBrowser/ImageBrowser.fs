(*---------------------------------------------------------------------------
    Copyright 2014 Microsoft

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.                                                     

    File: 
        ImageBrowser.fs
  
    Description: 
        Image Browser interface of Prajna. Present an interface in which the the user may browse the image in the set via an interface.

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com

    Date:
        Oct. 2014
    
 ---------------------------------------------------------------------------*)
open System
open System.Collections.Generic
open System.Threading
open System.Diagnostics
open System.IO
open System.Net
open System.Runtime.Serialization

open System.Threading
open System.Threading.Tasks

open Prajna.Tools

open Prajna.Tools.FSharp
open Prajna.Tools.StringTools
open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp

open System.Windows
open System.Windows.Media
open System.Windows.Controls
open System.Windows.Threading

open Prajna.WPFTools

let Usage = "
    Usage: Image Browser interface of Prajna. Present an interface in which the the user may browse the image stored in Prajna via a GUI interface. \n\
    Command line arguments:\n\
    -remote     Name of the distributed Prajna image set (both .download and .meta) \n\
    -countTag   Column for the tag. \n\
    -nowin      Do not launch the GUI, just show user interface & statistics. \n\
    "

module AssemblyProperties =
// Signs the assembly in F#
    open System
    open System.Reflection;
    open System.Runtime.InteropServices;

#if DEBUG
    [<assembly: AssemblyConfiguration("Debug")>]
#else
    [<assembly: AssemblyConfiguration("Release")>]
#endif 
    do()


// type MainWindow = XAML<"MainWindow.xaml">
type ImageBrowserWindow( remoteDKVname:string, countTag, sizex, sizey ) as x = 
    inherit Canvas( ) 
    let tabs = new GeneralTab( Width=sizex, Height=sizey - 10. )
    let scrollTrace = new ScrollViewer( Width=sizex-5., Height=sizey-50., VerticalScrollBarVisibility = ScrollBarVisibility.Visible )
    let mutable lenTrace = 0
    let tagGrid = new GridWithStringContent( "TagGrid", Width=sizex-5., Height=sizey-50., HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top ) 
    let metadataGrid = new GridWithStringContent( "Metadata", Width=sizex-5., Height=sizey-50., HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top ) 
    let imageGrid = new GridWithStringContent( "Images", Width=sizex-5., Height=sizey-50., HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top ) 
    // timer for updating frame, 1 frame every 25ms
    let timerUpdate = new Threading.DispatcherTimer(Interval = new TimeSpan(0,0,0,0,1000))
    let mutable timerLogUpdate = null 
    do 
        x.Width <- sizex
        x.Height <- sizey
        tabs.AddTab( "Tag", tagGrid )
        tabs.AddTab( "Metadata", metadataGrid )
        tabs.AddTab( "Images", imageGrid )
        tabs.AddTab( "Log", scrollTrace )
        scrollTrace.Content <- ""       
        tagGrid.Show()
        x.Children.Add( tabs ) |> ignore
        timerUpdate.Tick.Add(fun _ -> x.InvalidateVisual() )       
        tabs.SelectionChanged.Add( x.TabSelectionChanged )
    member val Metadata = null with get, set
    member val Tags = null with get, set
    member val MetadataReady = new ManualResetEvent(false) with get
    member val private ContentGrid = None with get, set
    member x.ScrollTrace with get() = scrollTrace
    /// Initialize Browser, retrieve data on another thread
    member x.Initialize() = 
        Async.Start ( async {  Logger.Log(LogLevel.Info, ( "Thread to retrieve metadata" ))
                               x.InitializeAll() })
    member x.InitializeAll() = 
        x.InitializeTag( )
        x.InitializeMetadata( )
    member x.InitializeMetadata( ) = 
        let imageName = remoteDKVname + ".download.async"
        let metaName = remoteDKVname + ".meta.async"
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "start to retrieve DKV %s ..." metaName ))
        let imageDKV = DSet<string*byte[]> ( Name = imageName ) |> DSet.loadSource
        let metaDKV = DSet<string*string[]> ( Name = metaName ) |> DSet.loadSource
        let metaArray = metaDKV.ToSeq() |> Seq.map ( fun (url, cols) -> let ncol = cols |> Array.mapi ( fun i st -> if st=url then i else -1 ) |> Array.max
                                                                        if ncol < 0 then 
                                                                            Array.concat [| [| url |]; cols |]
                                                                        else
                                                                            // Swap URL to col 0
                                                                            let target = Array.copy cols
                                                                            target.[ ncol ] <- target.[0]
                                                                            target.[ 0 ] <- url 
                                                                            target ) 
                                        |> Seq.toArray
        x.Metadata <- metaArray
        x.MetadataReady.Set() |> ignore
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DKV %s is retrieved ... " metaName ))
        let tagsArray = metaArray |> Seq.map( fun cols -> cols.[countTag] )
        let foldFunc (dic:Dictionary<_,_>) ( tags:string ) = 
            let useDic = if Utils.IsNull dic then Dictionary<_,_>() else dic
            let tag = tags.Split( ";,\t".ToCharArray(), StringSplitOptions.RemoveEmptyEntries ) 
            let taglower = tag |> Array.map( fun str -> str.ToLower() )
            let tagall = Array.append taglower [| "All Images" |]
            for t in tagall do 
                if not (useDic.ContainsKey( t )) then 
                    useDic.Item( t ) <- 1
                else
                    let c = useDic.Item( t )
                    useDic.Item( t ) <- c+1
            useDic
        let retDic = tagsArray |> Seq.fold foldFunc null
        x.Tags <- retDic
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "DKV %s, tag counting is completed ... " metaName ))
        x.MetadataReady.Set() |> ignore
        Async.Start ( async {  Logger.Log(LogLevel.Info, ( "Thread to update metadata" ))
                               x.ShowMetadata() } )
    member x.ShowMetadata( ) =
//        x.MetadataReady.WaitOne() |> ignore
        metadataGrid.SetContent( x.Metadata, null )

    member x.InitializeTag( ) = 
                let t1 = (DateTime.UtcNow)
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "CountTag = %d" countTag ))
                let imageName = remoteDKVname + ".download.async"
                let metaName = remoteDKVname + ".meta.async"
                let imageDKV = DSet<string*byte[]> ( Name = imageName ) |> DSet.loadSource
                let metaDKV = DSet<string*string[]> ( Name = metaName ) |> DSet.loadSource
                /// !!! System.Bug !!! Serialization error for closure (?STA thread )
                /// if we use countTag instead of ctag, the result will be wrong!!!
                let ctag = countTag
                let colDKV = metaDKV |> DSet.map( fun tuple -> let url, cols = tuple
                                                               Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "!!! Remote Debugging ... !!! Meta Columns %d use %d " cols.Length ctag )                    )
                                                               url, cols.[ctag] )
                let mixedFunc (url0:string, tags:string) ( url1:string, bytes:byte[] ) =
                    tags, bytes.Length
                let mixedDKV = DSet.map2 mixedFunc colDKV imageDKV
                let foldFunc (dic:Dictionary<_,_>) ( tags:string, imgLen:int) = 
                    let useDic = if Utils.IsNull dic then Dictionary<_,_>() else dic
                    let tag = tags.Split( ";,\t".ToCharArray(), StringSplitOptions.RemoveEmptyEntries ) 
                    let taglower = tag |> Array.map( fun str -> str.ToLower() )
                    let tagall = Array.append taglower [| "All Images" |]
                    for t in tagall do 
                        if not (useDic.ContainsKey( t )) then 
                            useDic.Item( t ) <- (1, int64 imgLen)
                        else
                            let c, totalLen = useDic.Item( t )
                            useDic.Item( t ) <- (c+1, totalLen + int64 imgLen)
                    useDic
                let aggrFunc (dic1:Dictionary<string,(int*int64)>) (dic2:Dictionary<string,(int*int64)>) =
                    for kvpair in dic1 do 
                        let tag = kvpair.Key
                        if dic2.ContainsKey( tag ) then 
                            let c1, totalLen1 = kvpair.Value
                            let c2, totalLen2 = dic2.Item( tag )
                            dic2.Item( tag ) <- (c2+c1, totalLen1 + totalLen2)
                        else
                            dic2.Item( tag ) <- kvpair.Value
                    dic2
                let retDic = mixedDKV.Fold(foldFunc, aggrFunc, null)
                let retArray = retDic :> seq<_> |> Seq.toArray 
                let sortedArray = retArray |> Array.sortBy ( fun kvpair -> let count, totalLen = kvpair.Value 
                                                                           (int64 count <<< 20) + totalLen / (int64 count ) ) |> Array.rev
                for kvpair in sortedArray do 
                    let tag = kvpair.Key
                    let count, totalLen = kvpair.Value
                    if tag <> "All Images" then 
                        Logger.Log( LogLevel.ExtremeVerbose, ( sprintf "Tag %s ........ %d images (avg length=%dB)" tag count (totalLen / int64 count) ))
                let countAll, totalAll = retDic.Item( "All Images" )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.Log( LogLevel.Info, ( sprintf "The dataset has %d Tags with %d images of %dMB total (avg length = %dB) processed in %f sec, throughput = %f MB/s" 
                                                       (sortedArray.Length-1) countAll (totalAll/1000000L) (totalAll/int64 countAll) (elapse.TotalSeconds) ((float totalAll)/elapse.TotalSeconds/1000000.) ))
                let tagInfo = sortedArray |> Array.map ( fun kvpair -> let n, len = kvpair.Value
                                                                       [| kvpair.Key.ToString(); n.ToString(); len.ToString() |] )
                tagGrid.SetContent( tagInfo, [| "Tags"; "Images"; "Sizes" |] )
        // x.Dispatcher.BeginInvoke( DispatcherPriority.Background, Action<_>( scrollMetadata.InvalidateVisual ) ) |> ignore 
//        Dispatcher.Run() |> ignore
//        
//        
//        let obj = Application.Current.Dispatcher.Invoke( Action<_>( fun _ -> scrollMetadata.Content <- contentGrid 
//                                                                             contentGrid.Show( ) |> ignore ) )
    member x.GridMetadata with get() = Option.get x.ContentGrid
    member x.ChangeSize( width, height ) = 
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Window size changes to %f X %f ... " width height ))
        x.Width <- width
        x.Height <- height
        tabs.Width <- width
        tabs.Height <- height - 10.
        tagGrid.DoSetSizeWithDelayUpdate( width - 5., height - 50. )
        metadataGrid.DoSetSizeWithDelayUpdate( width - 5., height - 50.)
        scrollTrace.Width <- width - 50.
        scrollTrace.Height <- height - 50.
        ()
    member x.TabSelectionChanged( e ) = 
        let tabs = e.Source :?> GeneralTab
        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Tab %d is selected" tabs.SelectedIndex ))
        // let cv = tabs.Parent :?> ImageBrowserWindow
        e.Handled <- true
        x.ChangeSelection(tabs.SelectedIndex)
        ()     
    member x.ChangeSelection( idx ) = 
        match idx with
        | 0 ->
            tagGrid.InvalidateVisual()
            tagGrid.Show()
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start showing Tags." ))
        | 1 ->
            metadataGrid.InvalidateVisual()
            metadataGrid.Show()
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start showing Metadata." ))
        | 2 -> 
        // Column 0 is selection tag. 
            let tagsSelected = tagGrid.SelectedContent() |> Seq.map ( fun lines -> lines.[1] ) |> Seq.toArray
            let imagesSelected = metadataGrid.SelectedContent() |> Seq.map ( fun lines -> lines.[1] ) |> Seq.toArray
            let tagsInfo = sprintf "Tags Selected : %s" (tagsSelected |> String.concat ",")
            let lastSurfix (st:string) = 
                let idx = st.LastIndexOfAny( @"/\".ToCharArray()) 
                if idx >= 0 then 
                    st.Substring( idx + 1 ) 
                else
                    st
            let imagesInfo = sprintf "Images Selected : %s" (imagesSelected |> Array.map( lastSurfix ) |> String.concat ",")
            imageGrid.SetContent( [| [| tagsInfo |]; [| imagesInfo |] |], null );
            imageGrid.InvalidateVisual()
            imageGrid.Show()
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start showing Image Grid." ))
        | _ -> 
            timerLogUpdate <- new System.Threading.Timer( ImageBrowserWindow.UpdateLog, x, 0, Timeout.Infinite )
            scrollTrace.InvalidateVisual()
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start showing log." ))
        tabs.InvalidateVisual()
        ()
    static member UpdateLog o = 
        Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Trace Update called " ))
        let x = o :?> ImageBrowserWindow
        let logFile = Logger.GetLogFile()
        if Utils.IsNotNull logFile then 
            try 
                let logF = new FileStream( logFile, FileMode.Open, FileAccess.Read, FileShare.ReadWrite )
                let readStream = new StreamReader( logF ) 
                let recentTraceLog = readStream.ReadToEnd()
                x.UpdateLogContent recentTraceLog
            with 
            | e -> 
                let msg = sprintf "Error in reading trace log: %A" e
                x.UpdateLogContent msg
        else
            x.UpdateLogContent "No Log File"
    member x.UpdateLogContent logContent = 
        if logContent.Length > lenTrace then 
            lenTrace <- logContent.Length
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Update Log with %d chars" logContent.Length ))
            x.Dispatcher.BeginInvoke( DispatcherPriority.Background, Action( fun _ -> scrollTrace.Content <- logContent
                                                                                      scrollTrace.InvalidateVisual()
                                                                                      ) ) |> ignore
        let timerLogUpdate = new System.Threading.Timer( ImageBrowserWindow.UpdateLog, x, 0, Timeout.Infinite )
        ()
// Define your library scripting code here
module main = 
    let evargs = Environment.GetCommandLineArgs()
    let orgargs = Array.sub evargs 1 ( evargs.Length - 1 ) // The first argument is program name, needs to be filtered out. 
    let args = Array.copy orgargs
    let parse = ArgumentParser(args)
    let PrajnaClusterFile = parse.ParseString( "-cluster", "" )
    let remoteDKVname = parse.ParseString( "-remote", "" )
    let password = parse.ParseString( "-password", "" )
    let nump = parse.ParseInt( "-nump", 0 )
    let countTag = parse.ParseInt( "-countTag", 0 )
    let bExe = parse.ParseBoolean( "-exe", false )
    let nMaxWait = parse.ParseInt( "-maxwait", 30000 )
    let bNoGUI = parse.ParseBoolean( "-nowin", false )
    let mutable bExecute = false

    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) ))
    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Execution param %A" orgargs ))

    let bAllParsed = parse.AllParsed Usage
    [<STAThread>]
    Cluster.StartCluster( PrajnaClusterFile )
    let cluster = Cluster.GetCurrent()
    let t1 = (DateTime.UtcNow)
    if bNoGUI && Utils.IsNotNull remoteDKVname && remoteDKVname.Length>0 then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "CountTag = %d" countTag ))
                let imageName = remoteDKVname + ".download.async"
                let metaName = remoteDKVname + ".meta.async"
                let imageDKV = DSet<string*byte[]> ( Name = imageName ) |> DSet.loadSource
                let metaDKV = DSet<string*string[]> ( Name = metaName ) |> DSet.loadSource
                /// !!! System.Bug !!! Serialization error for closure (?STA thread )
                /// if we use countTag instead of ctag, the result will be wrong!!!
                let ctag = countTag
                let colDKV = metaDKV |> DSet.map( fun tuple -> let url, cols = tuple
                                                               Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "!!! Remote Debugging ... !!! Meta Columns %d use %d " cols.Length ctag )                    )
                                                               url, cols.[ctag] )
                let mixedFunc (url0:string, tags:string) ( url1:string, bytes:byte[] ) =
                    tags, bytes.Length
                let mixedDKV = DSet.map2 mixedFunc colDKV imageDKV
                let foldFunc (dic:Dictionary<_,_>) ( tags:string, imgLen:int) = 
                    let useDic = if Utils.IsNull dic then Dictionary<_,_>() else dic
                    let tag = tags.Split( ";,\t".ToCharArray(), StringSplitOptions.RemoveEmptyEntries ) 
                    let taglower = tag |> Array.map( fun str -> str.ToLower() )
                    let tagall = Array.append taglower [| "zzzzzzzz" |]
                    for t in tagall do 
                        if not (useDic.ContainsKey( t )) then 
                            useDic.Item( t ) <- (1, int64 imgLen)
                        else
                            let c, totalLen = useDic.Item( t )
                            useDic.Item( t ) <- (c+1, totalLen + int64 imgLen)
                    useDic
                let aggrFunc (dic1:Dictionary<string,(int*int64)>) (dic2:Dictionary<string,(int*int64)>) =
                    for kvpair in dic1 do 
                        let tag = kvpair.Key
                        if dic2.ContainsKey( tag ) then 
                            let c1, totalLen1 = kvpair.Value
                            let c2, totalLen2 = dic2.Item( tag )
                            dic2.Item( tag ) <- (c2+c1, totalLen1 + totalLen2)
                        else
                            dic2.Item( tag ) <- kvpair.Value
                    dic2
                let retDic = mixedDKV.Fold(foldFunc, aggrFunc, null)
                let retArray = retDic :> seq<_> |> Seq.toArray 
                let sortedArray = retArray |> Array.sortBy ( fun kvpair -> let count, totalLen = kvpair.Value 
                                                                           (int64 count <<< 20) + totalLen / (int64 count ) )
                for kvpair in sortedArray do 
                    let tag = kvpair.Key
                    let count, totalLen = kvpair.Value
                    if tag <> "zzzzzzzz" then 
                        Logger.Log( LogLevel.Info, ( sprintf "Tag %s ........ %d images (avg length=%dB)" tag count (totalLen / int64 count) ))
                let countAll, totalAll = retDic.Item( "zzzzzzzz" )
                let t2 = (DateTime.UtcNow)
                let elapse = t2.Subtract(t1)
                Logger.Log( LogLevel.Info, ( sprintf "The dataset has %d Tags with %d images of %dMB total (avg length = %dB) processed in %f sec, throughput = %f MB/s" 
                                                       (sortedArray.Length-1) countAll (totalAll/1000000L) (totalAll/int64 countAll) (elapse.TotalSeconds) ((float totalAll)/elapse.TotalSeconds/1000000.) ))
                bExecute <- true
    elif Utils.IsNotNull remoteDKVname && remoteDKVname.Length>0 then 
        let ev = new ManualResetEvent( false )
        if true then 
            let app = new Application()
            if bExe then 
                JobDependencies.DefaultTypeOfJobMask <- JobTaskKind.ApplicationMask
            SynchronizationContext.SetSynchronizationContext( new DispatcherSynchronizationContext( Dispatcher.CurrentDispatcher));
            let cv = ImageBrowserWindow( remoteDKVname, countTag, 1400., 1000. ) 
            // Note that the initialiazation will complete in another thread
            cv.Initialize() 
            let win = new Window( Title = sprintf "Prajna Image Browser for DKV %s" remoteDKVname, 
                                  Width = 1425., 
                                  Height = 1030. )
//            Dispatcher.CurrentDispatcher.BeginInvoke( Action<_>( fun _ -> win.Content <- cv), DispatcherPriority.Render, [| win :> Object; cv :> Object |] ) |> ignore           
            win.Content <- cv
            win.SizeChanged.Add( fun arg -> 
                                  Dispatcher.CurrentDispatcher.BeginInvoke( Action<_>(fun _ -> cv.ChangeSize( arg.NewSize.Width - 25., arg.NewSize.Height - 30. )), [| cv :> Object |] ) |> ignore )
            win.Closed.Add( fun _ -> ev.Set() |> ignore 
                                     Dispatcher.CurrentDispatcher.BeginInvokeShutdown(DispatcherPriority.Background)
                                     Application.Current.Shutdown() )
            app.MainWindow <- win
            win.Show()   
            app.Run() |> ignore
            bExecute <- true
        ev.WaitOne() |> ignore
    else
        ()
    Cluster.Stop()
//    ThreadTracking.CloseAllActiveThreads(-1)


