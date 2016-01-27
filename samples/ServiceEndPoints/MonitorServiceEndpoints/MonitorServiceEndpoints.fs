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
        vHubMonitorBackEnd.fs
  
    Description: 
        Monitor a cluster of backend, show statistics information on both network performance and real-time query statistics. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Nov. 2014
    
 ---------------------------------------------------------------------------*)
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp


open System
open System.IO
open System.Diagnostics
open System.Threading
open System.Collections.Generic
open System.Collections.Concurrent
open System.ComponentModel
open System.IO
open System.Windows
open System.Windows.Data
open System.Windows.Media
open System.Windows.Media.Imaging
open System.Windows.Controls
open System.Windows.Threading
open Prajna.WPFTools

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp
open Prajna.Service.ServiceEndpoint
open VMHub.Data
open VMHub.ServiceEndpoint

let Usage = "
    Usage:  Monitor a cluster of backend, show statistics information on both network performance and real-time query statistics.\n\
    -cluster    Cluster to be monitored. \n\
    -progname   Program container of the backend. \n\
    "

type NetworkPerformanceCollection() = 
    member val BackEndCollection = ConcurrentDictionary<_,_>(StringComparer.OrdinalIgnoreCase) with get
    member val FrontEndCollection = ConcurrentDictionary<_,_>(StringComparer.OrdinalIgnoreCase) with get
    member val RTTCollection = ConcurrentDictionary<_,_>(StringComparer.OrdinalIgnoreCase) with get
    member val TimeToGetRttInMS = 0L with get, set

[<AllowNullLiteral; Serializable>] 
type QueryPerformance() = 
    member val AvgInProc = 0 with get, set
    member val MediumInProc = 0 with get, set        
    member val Pct90InProc = 0 with get, set        
    member val Pct999InProc = 0 with get, set
    member val AvgInQueue = 0 with get, set
    member val MediumInQueue = 0 with get, set        
    member val Pct90InQueue = 0 with get, set        
    member val Pct999InQueue = 0 with get, set
    static member Average (x:QueryPerformance) (y:QueryPerformance) = 
        let avg x y = 
            ( x + y ) / 2
        // Medium, Pct90 and Pct99 should not be averaged. The operation will actually never executes 
        QueryPerformance( AvgInProc = avg x.AvgInProc y.AvgInProc, 
                          MediumInProc = avg x.MediumInProc y.MediumInProc, 
                          Pct90InProc = avg x.Pct90InProc y.Pct90InProc, 
                          Pct999InProc = avg x.Pct999InProc y.Pct999InProc, 
                          AvgInQueue = avg x.AvgInQueue y.AvgInQueue, 
                          MediumInQueue = avg x.MediumInQueue y.MediumInQueue, 
                          Pct90InQueue = avg x.Pct90InQueue y.Pct90InQueue, 
                          Pct999InQueue = avg x.Pct999InQueue y.Pct999InQueue ) 

[<AllowNullLiteral; Serializable>]
type QueryPerformanceCollection() = 
    member val PerformanceCollection = ConcurrentDictionary<_,_>( StringTComparer<Guid>(StringComparer.Ordinal) ) with get 
    static member CalculateQueryPerformance ( arr: (string*string*Guid*string*int*int)[] ) = 
        if Utils.IsNull arr then 
            null
        else
            let t1 = (DateTime.UtcNow.Ticks)
            let hostmachine = RemoteExecutionEnvironment.MachineName
            let dic = ConcurrentDictionary<_,List<_>>( StringTComparer<Guid>(StringComparer.Ordinal) )
            let pct length percentile = 
                let idx = int (round( float length * percentile ))
                if idx >= length then length - 1 else idx
            for item in arr do 
                let _, containerName, serviceID, proc, inQueue, inProc = item
                let containerDigits =   
                    if StringTools.IsNullOrEmpty containerName then 
                        null
                    else
                        let mutable lastNonDigitPos = containerName.Length - 1
                        let mutable bFindNonDigit = false
                        while not bFindNonDigit && lastNonDigitPos >= 0 do 
                            if Char.IsDigit (containerName.[lastNonDigitPos]) then 
                                lastNonDigitPos <- lastNonDigitPos - 1   
                            else
                                bFindNonDigit <- true
                        containerName.Substring( lastNonDigitPos + 1 )
                let useMachineName = if StringTools.IsNullOrEmpty containerDigits then hostmachine else hostmachine + "-" + containerDigits
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Use machine name %s. " useMachineName ))
                let lst = dic.GetOrAdd( (useMachineName+":"+proc, serviceID), fun _ -> List<_>() )   
                lst.Add( (inQueue, inProc) )
            let x = QueryPerformanceCollection()
            for pair in dic do 
                let lst = pair.Value
                let sortArrProc =  lst.ToArray() |> Array.sortBy( fun (inQueue, inProc) -> inProc )
                let sortArrQueue = lst.ToArray() |> Array.sortBy( fun (inQueue, inProc) -> inQueue )
                let inQueueTotal = sortArrQueue |> Array.sumBy( fun (inQueue, inProc) -> int64 inQueue )
                let inProcTotal = sortArrProc |> Array.sumBy( fun (inQueue, inProc) -> int64 inProc )
                // sortArr.Length is at least 1
                let avgQueue = int (inQueueTotal / int64 sortArrQueue.Length )
                let avgProc = int ( inProcTotal / int64 sortArrProc.Length )
                let mediumProc = (snd sortArrProc.[sortArrProc.Length / 2 ])
                let pct90Proc = (snd sortArrProc.[pct sortArrProc.Length 0.9 ]) 
                let pct999Proc = ( snd sortArrProc.[pct sortArrProc.Length 0.999 ]) 
                let mediumQueue = (snd sortArrQueue.[sortArrQueue.Length / 2 ])
                let pct90Queue = (snd sortArrQueue.[pct sortArrQueue.Length 0.9 ]) 
                let pct999Queue = ( snd sortArrQueue.[pct sortArrQueue.Length 0.999 ]) 

                x.PerformanceCollection.Item( pair.Key ) <- 
                    QueryPerformance( AvgInProc = avgProc, 
                                      MediumInProc = mediumProc, 
                                      Pct90InProc = pct90Proc, 
                                      Pct999InProc = pct999Proc, 
                                      AvgInQueue = avgQueue, 
                                      MediumInQueue = mediumQueue, 
                                      Pct90InQueue = pct90Queue, 
                                      Pct999InQueue = pct999Queue
                                       )
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> let t2 = (DateTime.UtcNow.Ticks)
                                                          sprintf "calculate QueryPerformance from %d data points using %dms" 
                                                                   arr.Length 
                                                                   ((t2-t1)/TimeSpan.TicksPerMillisecond) ))
            Array.create 1 x
    /// Fold may give a null variable. 
    static member Aggregate (x:QueryPerformanceCollection) (y:QueryPerformanceCollection) = 
        if Utils.IsNull y then 
            x 
        elif Utils.IsNull x then 
            y
        else
            for pair in y.PerformanceCollection do 
                x.PerformanceCollection.AddOrUpdate( pair.Key, pair.Value, fun k vx -> QueryPerformance.Average vx pair.Value ) |> ignore 
            x

[<AllowNullLiteral; Serializable>]
type ServiceCollection() = 
    member val ServiceIDToDomain = ConcurrentDictionary<_,_>( ) with get 
    member val ServiceCollection = ConcurrentDictionary<_,_>( StringTComparer<Guid>(StringComparer.Ordinal) ) with get 
    static member FoldServiceCollection (inSeq:seq<Guid*string*string*string*string*int64*int*int>) (y:ServiceCollection) = 
        let x = if Utils.IsNull y then ServiceCollection() else y
        for tuple in inSeq do 
            // Aggregate by container:
            let serviceID, machineName, containerName, engineName, domainName, totalItems, curItems, capacity = tuple
            let name = engineName + ":" + domainName + ":" + machineName 
            x.ServiceCollection.AddOrUpdate( (name, serviceID), ( 1, totalItems, curItems, capacity ),
                fun _ tuple -> let cnt, t1, c1, c2 = tuple
                               cnt+1, t1 + totalItems, c1 + curItems, c2 + capacity ) |> ignore 
            x.ServiceIDToDomain.Item( serviceID ) <- domainName
        x

[<AllowNullLiteral>]
type NetworkPerfWindow( ) = 
    inherit GridWithStringContent("Network(Back-Front)" ) 
    let nertworkPerf = ref (NetworkPerformanceCollection())
    let columns = ref (Array.empty)
    let rows = ref (Array.empty)
    member x.UpdateNetworkPerformance( newPerf: NetworkPerformanceCollection ) = 
        nertworkPerf := newPerf 
        if Utils.IsNull newPerf then 
            x.SetContent( [| [| "No backends" |] |], [| "Rtt(ms)" |] )
        else
            // Only place where we formatting data. 
            rows := newPerf.BackEndCollection.Keys |> Seq.sort |> Seq.toArray 
            columns := newPerf.FrontEndCollection.Keys |> Seq.sort |> Seq.toArray 
            let contentarr = 
                !rows |> Array.map ( fun backend -> Array.append [| backend |] 
                                                        ( (!columns) |> Array.map ( fun frontend -> let bExist, tuple = newPerf.RTTCollection.TryGetValue( backend+"-"+frontend ) 
                                                                                                    if bExist then 
                                                                                                        let sumRtt, cnt = tuple
                                                                                                        sprintf "%.2f" ( sumRtt / float cnt )
                                                                                                    else
                                                                                                        "<null>" ) ) )
            if contentarr.Length > 1 then 
                x.SetContent( contentarr, Array.append [| "Rtt(ms)" |] (!columns) )
            else
                x.SetContent( [| [| "No backends" |] |], [| "Rtt(ms)" |] )
        ()

[<AllowNullLiteral>]
type ServiceCollectionWindow( ) = 
    inherit GridWithStringContent("Service Collection" ) 
    member x.UpdateServiceCollection( svc: ServiceCollection ) = 
        if Utils.IsNotNull svc then 
            // Only place where we formatting data. 
            let columnsPair = svc.ServiceIDToDomain |> Seq.sortBy( fun pair -> pair.Value ) |> Seq.map( fun pair -> pair.Key, pair.Value ) |> Seq.toArray
            let guidToIdx = Dictionary<_,_>( )
            for idx = 0 to columnsPair.Length - 1 do     
                guidToIdx.Item( fst columnsPair.[idx] ) <- idx
            let header = columnsPair |> Array.map ( fun tuple -> snd tuple )
            let rowDic = ConcurrentDictionary<_,(_)[]>(StringComparer.Ordinal)
            for pair in svc.ServiceCollection do 
                let info, serviceID = pair.Key
                let cnt, total, curitem, curcapacity = pair.Value
                let infoarr = info.Split([|':'|])
                let engineName = infoarr.[0]
                let machineName = infoarr.[2]
                let name = machineName + ":" + engineName
                let rowArr = rowDic.GetOrAdd( name, fun _ -> Array.create columnsPair.Length "" )
                let idx = guidToIdx.Item( serviceID ) 
                rowArr.[idx] <- sprintf "(%d) %d %d/%d" cnt total curitem curcapacity
            let contentarr = rowDic |> Seq.map ( fun pair -> let infoarr = pair.Key.Split([|':'|])
                                                             Array.append infoarr pair.Value ) 
                                    |> Seq.toArray
            if contentarr.Length > 0 then 
                x.SetContent( contentarr, Array.append [| "Machine"; "Engine" |] header )
            else
                x.SetContent( Array.create 1 [| "None"; "" |], [| "Machine"; "Engine" |] )
        else
            x.SetContent( Array.create 1 [| "None"; "" |], [| "Machine"; "Engine" |] )


[<AllowNullLiteral>]
type QueryPerformanceWindow( name, perfFunc: QueryPerformance -> int, filterFunc: string -> bool ) = 
    inherit GridWithStringContent(name ) 
    member x.UpdateQueryPerformance( svc: ServiceCollection, qPerf: QueryPerformanceCollection ) = 
        if Utils.IsNotNull svc && Utils.IsNotNull qPerf then 
            // Only place where we formatting data. 
            let columnsPair = svc.ServiceIDToDomain |> Seq.sortBy( fun pair -> pair.Value ) |> Seq.map( fun pair -> pair.Key, pair.Value ) |> Seq.toArray
            let guidToIdx = Dictionary<_,_>( )
            for idx = 0 to columnsPair.Length - 1 do     
                guidToIdx.Item( fst columnsPair.[idx] ) <- idx
            let header = columnsPair |> Array.map ( fun tuple -> snd tuple )
            let rowDic = ConcurrentDictionary<_,(_)[]>(StringComparer.Ordinal)
            for pair in qPerf.PerformanceCollection do 
                let info, serviceID = pair.Key
                let onePerformance = pair.Value
                let name = info
                let infoarr = info.Split([|':'|])
                let filterContent = if infoarr.Length >= 2 then infoarr.[1] else ""
                if filterFunc filterContent then 
                    let rowArr = rowDic.GetOrAdd( name, fun _ -> Array.create columnsPair.Length "" )
                    let idx = guidToIdx.Item( serviceID ) 
                    let showPerf = perfFunc onePerformance
                    rowArr.[idx] <- sprintf "%d" showPerf
            let contentarr = rowDic |> Seq.map ( fun pair -> let infoarr = pair.Key.Split([|':'|])
                                                             Array.append infoarr pair.Value ) 
                                    |> Seq.toArray
            if contentarr.Length > 0 then 
                x.SetContent( contentarr, Array.append [| "Machine"; "Proc" |] header )
            else
                x.SetContent( Array.create 1 [| "None"; "" |] , [| "Machine"; "Proc" |]  )
        else
            x.SetContent( Array.create 1 [| "None"; "" |], [| "Machine"; "Proc" |]  )

/// launchWindow needs to be started in ApartmentState.STA, hence always should be started in a function
type LaunchWindow(ev:ManualResetEvent) = 
    member val ClusterCollection = ConcurrentDictionary<_,_>(StringComparer.Ordinal) with get
    member val CurrentServiceCollection = null with get, set
    member val CurrentQueryPerformance = null with get, set
    member val NetworkPerfWin : NetworkPerfWindow = null with get, set
    member val ServiceCollectionWin : ServiceCollectionWindow = null with get, set
    member val AvgProcWin : QueryPerformanceWindow = null with get, set
    member val MidProcWin : QueryPerformanceWindow = null with get, set
    member val Pct90ProcWin : QueryPerformanceWindow = null with get, set
    member val Pct999ProcWin : QueryPerformanceWindow = null with get, set
    member val AvgQueueWin : QueryPerformanceWindow = null with get, set
    member val MidQueueWin : QueryPerformanceWindow = null with get, set
    member val Pct90QueueWin : QueryPerformanceWindow = null with get, set
    member val Pct999QueueWin : QueryPerformanceWindow = null with get, set
    member val MonitorIntervalInMS = 1000 with get, set
    member x.AddCluster( clusterFileName:string ) = 
        let cl = Prajna.Core.Cluster( clusterFileName )
        if not (Utils.IsNull cl) then 
            x.ClusterCollection.GetOrAdd( clusterFileName, cl )  |> ignore
    member x.GetCluster() = 
        Seq.singleton (Cluster.GetCombinedCluster( x.ClusterCollection.Values ))
    member x.MonitorAll() = 
        x.MonitorClusterNetwork() 
        x.MonitorClusterServiceCollection()
        x.MonitorClusterQuery()
    member x.MonitorClusterNetwork() = 
        let newNetworkPerf = NetworkPerformanceCollection() 
        for cl in x.GetCluster() do 
            // Monitor
            let t1 = (DateTime.UtcNow.Ticks)
            let perfDKV0 = DSet<string*float>(Name="NetworkPerf", Cluster = cl )
            let perfDKV1 = perfDKV0.Import(null, (BackEndInstance<_>.ContractNameActiveFrontEnds))
            let foldFunc (lst:List<_>) (kv) = 
                let retLst = 
                    if Utils.IsNull lst then 
                        List<_>()
                    else
                        lst
                retLst.Add( kv ) 
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Machine %s %d records now" RemoteExecutionEnvironment.MachineName retLst.Count ))
                retLst
            let aggrFunc (lst1:List<_>) (lst2:List<_>) = 
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Machine %s aggregate %d + %d records" RemoteExecutionEnvironment.MachineName lst1.Count lst2.Count ))
                lst1.AddRange( lst2 ) 
                lst1
            let aggrNetworkPerf = perfDKV1 |> DSet.fold foldFunc aggrFunc null
            if not (Utils.IsNull aggrNetworkPerf ) then 
                aggrNetworkPerf |> Seq.iter ( fun tuple ->  let path, msRtt = tuple
                                                            newNetworkPerf.RTTCollection.AddOrUpdate( path, (fun _ -> (msRtt, 1)), 
                                                                                                            fun _ tuple -> let sumRtt, cnt = tuple 
                                                                                                                           ( sumRtt + msRtt, cnt + 1) ) |> ignore
                                                            //let arr = path.Split([|'-'|])
                                                            let index = path.IndexOf('-')
                                                            let arr =
                                                                if (index >= 0) then
                                                                    let arr1 = Array.zeroCreate<string>(2)                                                                    
                                                                    arr1.[0] <- path.Substring(0, index)
                                                                    arr1.[1] <- path.Substring(index+1)
                                                                    arr1
                                                                else
                                                                    let arr1 = Array.zeroCreate<string>(1)
                                                                    arr1.[0] <- path
                                                                    arr1
                                                            if arr.Length > 0 then 
                                                                newNetworkPerf.BackEndCollection.GetOrAdd( arr.[0], true ) |> ignore
                                                                if arr.Length > 1 then 
                                                                    newNetworkPerf.FrontEndCollection.GetOrAdd( arr.[1], true )  |> ignore
                                                            Logger.LogF( LogLevel.WildVerbose, ( fun _ -> sprintf "%s: RTT %.2f(ms)" path msRtt ) ))
            else
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "no active front ends ... " ))
            let t2 = (DateTime.UtcNow.Ticks)
            newNetworkPerf.TimeToGetRttInMS <- ( t2 - t1 ) / TimeSpan.TicksPerMillisecond
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "get network performance statistics for cluster %s in %.2f ms" 
                                                               cl.Name (TimeSpan(t2-t1).TotalMilliseconds) ))
        x.NetworkPerfWin.UpdateNetworkPerformance( newNetworkPerf )
    member x.MonitorClusterServiceCollection() = 
        let newNetworkPerf = NetworkPerformanceCollection() 
        for cl in x.GetCluster() do 
            // Monitor
            let t1 = (DateTime.UtcNow.Ticks)
            let serviceDSet0 = DSet<Guid*string*string*string*string*int64*int*int>(Name="ServiceCollection", Cluster = cl )
            let serviceDSet1 = serviceDSet0.Import(null, ( VHubBackEndInstance<_>.ContractNameGetServiceCollection ))
            let serviceCollection = ServiceCollection.FoldServiceCollection (serviceDSet1.ToSeq()) null
            x.CurrentServiceCollection <- serviceCollection
            x.ServiceCollectionWin.UpdateServiceCollection( serviceCollection )
    member x.MonitorClusterQuery() = 
        for cl in x.GetCluster() do 
            let perfDSet0 = DSet<string*string*Guid*string*int*int>(Name="QueryPerf", Cluster = cl )
            let perfDSet1 = perfDSet0.Import(null, (BackEndInstance<_>.ContractNameRequestStatistics))
            let perfDSet2 = perfDSet1.RowsReorg Int32.MaxValue
            let perfDSet3 = perfDSet2.MapByCollection QueryPerformanceCollection.CalculateQueryPerformance
            let aggrQueryPerf = perfDSet3 |> DSet.fold QueryPerformanceCollection.Aggregate QueryPerformanceCollection.Aggregate null
            x.CurrentQueryPerformance <- aggrQueryPerf
            x.AvgProcWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )
            x.MidProcWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )
            x.Pct90ProcWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )
            x.Pct999ProcWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )
            x.AvgQueueWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )
            x.MidQueueWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )
            x.Pct90QueueWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )
            x.Pct999QueueWin.UpdateQueryPerformance( x.CurrentServiceCollection, aggrQueryPerf )

    member x.Launch() = 
        // need to initialize in STA thread
        x.NetworkPerfWin <- NetworkPerfWindow()
        x.ServiceCollectionWin <- ServiceCollectionWindow() 
        x.AvgProcWin <- QueryPerformanceWindow( "Avg Query", (fun x -> x.AvgInProc), (fun info -> String.Compare( info, "Reply", StringComparison.OrdinalIgnoreCase)=0) )
        x.MidProcWin <- QueryPerformanceWindow( "Medium Query", (fun x -> x.MediumInProc), (fun info -> String.Compare( info, "Reply", StringComparison.OrdinalIgnoreCase)=0) )
        x.Pct90ProcWin <- QueryPerformanceWindow( "90-percentile Query", (fun x -> x.Pct90InProc), (fun info -> String.Compare( info, "Reply", StringComparison.OrdinalIgnoreCase)=0) )
        x.Pct999ProcWin <- QueryPerformanceWindow( "999-percentile Query", (fun x -> x.Pct999InProc), (fun info -> String.Compare( info, "Reply", StringComparison.OrdinalIgnoreCase)=0) )
        x.AvgQueueWin <- QueryPerformanceWindow( "Avg Queue", (fun x -> x.AvgInQueue), fun _ -> true )
        x.MidQueueWin <- QueryPerformanceWindow( "Medium Queue", (fun x -> x.MediumInQueue), fun _ -> true )
        x.Pct90QueueWin <- QueryPerformanceWindow( "90-percentile Queue", (fun x -> x.Pct90InQueue), fun _ -> true )
        x.Pct999QueueWin <- QueryPerformanceWindow( "999-percentile Queue", (fun x -> x.Pct999InQueue), fun _ -> true )

        let curfname = Process.GetCurrentProcess().MainModule.FileName
        let defaultRootdir = 
            try
                let curdir = Directory.GetParent( curfname ).FullName
                let upperdir = Directory.GetParent( curdir ).FullName
                let upper2dir = Directory.GetParent( upperdir ).FullName                        
                Path.Combine( upper2dir, "image" )
            with
            | e -> 
                null
        let app = new Application()
        SynchronizationContext.SetSynchronizationContext( new DispatcherSynchronizationContext( Dispatcher.CurrentDispatcher));
        let cv = TabWindowWithLog( 1400., 1000. ) 
        use monitorTimer = new Threading.Timer((fun _ -> x.MonitorAll()), null, x.MonitorIntervalInMS, x.MonitorIntervalInMS)
        cv.AddTab( "Network", x.NetworkPerfWin )
        cv.AddTab( "Service Collection", x.ServiceCollectionWin )
        cv.AddTab( "Avg Query", x.AvgProcWin ) 
        cv.AddTab( "Medium Query", x.MidProcWin ) 
        cv.AddTab( "90 Query", x.Pct90ProcWin ) 
        cv.AddTab( "999 Query", x.Pct999ProcWin ) 
        cv.AddTab( "Avg Queue", x.AvgQueueWin ) 
        cv.AddTab( "Medium Queue", x.MidQueueWin ) 
        cv.AddTab( "90 Queue", x.Pct90QueueWin ) 
        cv.AddTab( "999 Queue", x.Pct999QueueWin ) 

        let mainMenu = Menu( Width = 1425., Height = 100., Margin = Thickness(0.,0.,0.,0.),
                                HorizontalAlignment=HorizontalAlignment.Left,
                                VerticalAlignment=VerticalAlignment.Top, IsMainMenu = true )
        let imagefilename1 = Path.Combine(defaultRootdir, "data-center.png")
        let uri1 = System.Uri(imagefilename1, UriKind.Absolute )
        Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "data-center Icon = %s, Uri = %A " imagefilename1 uri1))
        let imagefilename2 = Path.Combine(defaultRootdir, "computer.png")
        let uri2 = System.Uri(imagefilename2, UriKind.Absolute )

        let img1 = Image( Width=100., Height=100., Source = new BitmapImage( uri1 ) )
        let addClusterMenu = MenuItem( Header = img1 )
        mainMenu.Items.Add( addClusterMenu ) |> ignore
        let img2 = Image( Width=100., Height=100., Source = new BitmapImage( uri2 ) )
        let changeNodeMenu = MenuItem( Header = img2 )
        mainMenu.Items.Add( changeNodeMenu ) |> ignore

        let stackpanel = WindowWithMenu( mainMenu, cv, 1425., 1130. )
        // Note that the initialiazation will complete in another thread
        let win = new Window( Title = sprintf "Monitoring vHub Backend", 
                                Width = 1425., 
                                Height = 1130. )
    //            Dispatcher.CurrentDispatcher.BeginInvoke( Action<_>( fun _ -> win.Content <- cv), DispatcherPriority.Render, [| win :> Object; cv :> Object |] ) |> ignore           
        win.Content <- stackpanel
        win.SizeChanged.Add( fun arg -> 
                                Dispatcher.CurrentDispatcher.BeginInvoke( Action<_>(fun _ -> cv.ChangeSize( arg.NewSize.Width - 25., arg.NewSize.Height - 130. )), [| cv :> Object |] ) |> ignore )
        win.Closed.Add( fun _ -> ev.Set() |> ignore 
                                 Dispatcher.CurrentDispatcher.BeginInvokeShutdown(DispatcherPriority.Background)
                                 Application.Current.Shutdown() )
        app.MainWindow <- win
        win.Show()   
        app.Run() |> ignore


[<EntryPoint>]
let main argv = 
    let logFile = sprintf @"c:\Log\vHub\vHub.MonitorBackEnd_%s.log" (VersionToString( (DateTime.UtcNow) ))
    let inputargs =  Array.append [| "-log"; logFile |] argv 
    let orgargs = Array.copy inputargs
    let parse = ArgumentParser(orgargs)
    
    let PrajnaClusterFiles = parse.ParseStrings( "-cluster", [||] )
    let progName = parse.ParseString( "-progname", "PrajnaRecognitionService" )

    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Program %s started  ... " (Process.GetCurrentProcess().MainModule.FileName) ) )
    Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Execution param %A" inputargs ))

    let bAllParsed = parse.AllParsed Usage
    if bAllParsed then 
        let clusterLst = ConcurrentQueue<_>()
        for file in PrajnaClusterFiles do 
            let cl = Prajna.Core.Cluster( file )
            if cl.NumNodes > 0 then 
                clusterLst.Enqueue( cl ) 
        let ev = new ManualResetEvent( false )
        let x = LaunchWindow( ev )
        for file in PrajnaClusterFiles do 
            x.AddCluster( file )
        Async.Start ( async {  Logger.Log(LogLevel.Info, ( "Main Thread for vHub Backend monitoring" ))
                               x.Launch() })
        ev.WaitOne() |> ignore
    0
