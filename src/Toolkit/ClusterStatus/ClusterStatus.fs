(*---------------------------------------------------------------------------
    Copyright 2015 Microsoft

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
        PrajnaClusterStatus.fs
  
    Description: 
        Monitor the status of the cluster

    Author:																	
        Weirong Zhu
        Microsoft Research, One Microsoft Way
        Email: wezhu@microsoft.com, Tel. (425) 703-7787
    Date:
        March. 2015
    
 ---------------------------------------------------------------------------*)
open System
open System.Collections.Generic
open System.Configuration
open System.Diagnostics
open System.IO
open System.Windows
open System.Windows.Controls
open System.Windows.Forms.Integration
open System.Windows.Markup
open System.Windows.Threading
open System.Xml

open FSharp.Charting
open FSharp.Charting.ChartTypes

open Xceed.Wpf.Toolkit

open Prajna.Core
open Prajna.Api.FSharp
open Prajna.Api.FSharp

open Prajna.Tools
open Prajna.Tools.FSharp
open Prajna.Tools.StringTools

// Measures
[<Measure>] type Byte
[<Measure>] type MByte

[<Literal>]
let BytesPerMByte = 1048576L<MByte^-1>

let convertBytesToMBytes ( x : int64 ) = x / BytesPerMByte

[<Literal>]
let TopNFolders = 5

[<Literal>]
let UsersFolder = "Users"

let excludedSystemFolders = [| "Windows"; "Program Files"; "Program Files (x86)"; "ProgramData"; "Documents and Settings"; "PerfLogs"; "System Volume Information"; "Users\\All Users"; |]

// Utiltiy
let FInfo f = 
  Logger.LogF(LogLevel.Info, f)

type LogTimer(desc : string) =
    let description = desc
    let start = DateTime.UtcNow   
    do FInfo (fun _ -> sprintf "%s: start" description)

    interface IDisposable with
     member x.Dispose(): unit = 
        let stop = DateTime.UtcNow
        let elapsed = stop.Subtract(start)
        FInfo (fun _ -> sprintf "%s: %.2f (ms)" description (float elapsed.Milliseconds))

type FolderStatus = 
    {
        Path : string
        Size : int64<MByte>        
        SubFolders : FolderStatus [] option      
    }

// Status records
type DriveStatus = 
    { 
        Name : string
        VolumeLabel: string
        TotalSize : int64<MByte>
        FreeSpaceSize : int64<MByte>
        TopSizedFolders : FolderStatus[] option 
    }

type DataFoldersStatus =
    {
        TotalSize : int64<MByte>
        Folders : FolderStatus []
    }

type MachineDrivesStatus =
    {
        Name : string
        Drives : DriveStatus []
        PrajnaLocalFolderStatus : FolderStatus
        PrajnaDataFoldersStatus : DataFoldersStatus option
    }

let getDirectorySize dir =
    Directory.EnumerateFiles(dir, "*", SearchOption.AllDirectories)
    |> Seq.sumBy ( 
        fun f -> 
            try
                FileInfo(f).Length 
            with
            | :? PathTooLongException -> 0L  // Does not count such files for now (thus result may not be accurate)
       )
    |> convertBytesToMBytes

// return (Users, excluded system folders)
let systemFolders() =
    let sysDrivePath = Path.GetPathRoot(Environment.SystemDirectory)
    sysDrivePath + UsersFolder, excludedSystemFolders |> Array.map ((+) sysDrivePath)

let arePathsEqual p1 p2 =
    String.Compare(Path.GetFullPath(p1).TrimEnd('\\'), Path.GetFullPath(p2).TrimEnd('\\'), StringComparison.InvariantCultureIgnoreCase) = 0

let getTopNSizedSubfolders dir n =
    let dirs = Directory.GetDirectories(dir)
    let usersFolder,excludedFolders = systemFolders()
    match dirs with
    | [||] -> None
    | ds -> let r = ds 
                    |> Array.collect (fun d -> if arePathsEqual d usersFolder then Directory.GetDirectories(d) else [|d|] ) // expand with sub-dirs under Users
                    |> Array.filter (fun d -> not (excludedFolders |> Array.exists (fun e -> arePathsEqual d e)) )  // filter out excluded system dirs
                    |> Array.Parallel.map (fun d -> { Path = d; Size = getDirectorySize d; SubFolders = None } )
                    |> Array.sortBy (fun d -> -d.Size)
            let arr = if r.Length <= n then r else Array.sub r 0 n
            arr |> Some

let getDrivesStatus topNFolders =
    let drives = DriveInfo.GetDrives()
    drives 
    |> Array.filter (fun d -> d.IsReady)
    |> Array.Parallel.map ( fun d -> 
                                let topSizedFolders = 
                                    if topNFolders = 0 
                                    then None 
                                    else getTopNSizedSubfolders d.RootDirectory.FullName topNFolders
                                { Name = d.Name
                                  VolumeLabel = d.VolumeLabel
                                  TotalSize = convertBytesToMBytes d.TotalSize
                                  FreeSpaceSize = convertBytesToMBytes d.TotalFreeSpace
                                  TopSizedFolders = topSizedFolders })

let statusSeqFunc topNSizedFolders () =
    let localFolder = DeploymentSettings.LocalFolder
    let subFolders = Directory.GetDirectories(localFolder)

    let drives = getDrivesStatus topNSizedFolders

    // Get Prajna Local Folder status
    let localFolderStatus = 
        {
            Path = localFolder
            Size = getDirectorySize localFolder
            SubFolders = 
                match subFolders with
                | [||] -> None
                | folders -> folders |> Array.Parallel.map ( fun d-> { Path = d; Size = getDirectorySize d; SubFolders = None } ) |> Some
        }   

    let dataFolders = 
        drives 
        |> Array.Parallel.choose (fun d -> 
                                    let dataFolder = d.Name + DeploymentSettings.DataFolder
                                    if (Directory.Exists(dataFolder))
                                    then { Path = dataFolder; Size = getDirectorySize dataFolder; SubFolders = None } |> Some
                                    else None)

    let dataFoldersStatus = 
        match dataFolders with
        | [||] -> None
        | folders -> { TotalSize = folders |> Array.sumBy (fun f -> f.Size); Folders = folders } |> Some

    seq {
        yield { Name = Environment.MachineName; 
                Drives = drives
                PrajnaLocalFolderStatus =  localFolderStatus
                PrajnaDataFoldersStatus = dataFoldersStatus }
    }

let fetchClusterStatus cluster =
    use t = new LogTimer("fetchClusterStatus")
    let statusDSet = DSet<_> ( Name = "ClusterStatus", Cluster = cluster)
    ((statusDSet |> DSet.source (statusSeqFunc 0) ).ToSeq()) |> Array.ofSeq
   
//================================================================================
// Functions for processing returned status and generate charts for cluster view
//================================================================================

let formatXAxis chart =
    chart |> Chart.WithXAxis(MajorGrid = ChartTypes.Grid(Enabled = false), LabelStyle = ChartTypes.LabelStyle(Angle = -45, Interval = 1.0))

// Get the disk usage as a dictionary of 
//   Disk Name ->  List < MachineName * DriveStatus>    
let getDiskUsage (status : MachineDrivesStatus []) =
    let dict = new Dictionary<_,List<_>>()
    for m in status do
        for d in m.Drives do
            let item = m.Name,d
            let exists,value = dict.TryGetValue(d.Name)
            if exists then
                value.Add(item)
            else
                let list = new List<_>()
                list.Add(item) 
                dict.Add(d.Name, list)
    dict               

// Take the output from getDiskUsage, draw the chart for disk usage summary
let getDiskUsageSummaryChart (dict : Dictionary<string, List<string * DriveStatus>>) =
    use t = new LogTimer("getDiskUsageSummaryChart")
    let charts =
        dict 
        |> List.ofSeq 
        |> List.map ( fun u ->
                let name = u.Key
                let values = u.Value 
                            |> List.ofSeq 
                            |> List.sortBy ( fun v -> fst v)
                            |> List.map ( fun v -> 
                                              let name, d = v
                                              let usagePercentage = ((float (d.TotalSize - d.FreeSpaceSize)) / (float d.TotalSize)) * 100.0
                                              name, usagePercentage)
                Chart.Line(values, name) )
    Chart.Combine(charts).WithTitle(Text="Disk Usage (Percentage)", InsideArea=false) |> Chart.WithLegend(Enabled = true) |> formatXAxis

// Take the output from getDiskUsage, draw the chart for each drive's usage
let getDiskUsageChart (dict : Dictionary<string, List<string * DriveStatus>>, drive) =
    use t = new LogTimer("getDiskUsageChart")
    let list = (dict.[drive]) |> List.ofSeq |> List.sortBy ( fun v -> fst v)
    let total = list 
                |> List.map ( fun v -> 
                                  let name, d = v
                                  name, d.TotalSize)
    let totalChart = Chart.Line(total, "Total Size (MB)")
    let free = list
               |> List.map ( fun v -> 
                                 let name,d = v
                                 name, d.FreeSpaceSize)
    let freeChart = Chart.Line(free, "Free Space Size (MB)")
    let used = list
               |> List.map ( fun v -> 
                                 let name,d = v
                                 name, d.TotalSize - d.FreeSpaceSize)
    let usedChart = Chart.Line(used, "Used Space Size (MB)")
    Chart.Combine([totalChart; freeChart; usedChart]).WithTitle(Text=drive + " Size (MB)", InsideArea=false) 
    |> Chart.WithLegend(Enabled = true, Docking=ChartTypes.Docking.Bottom) 
    |> formatXAxis

// Get the Prajna local folder usage, returns 
// * MachineName * Size list
// * a dictionary of SubFolderName ->  List < MachineName * FolderStatus > 
let getPrajnaLocalFolderUsage (status : MachineDrivesStatus []) =
    let sizes = status |> Seq.map ( fun s -> (s.Name, s.PrajnaLocalFolderStatus.Size)) |> List.ofSeq
    let dict = new Dictionary<_,List<_>>()
    for m in status do
        if Option.isSome(m.PrajnaLocalFolderStatus.SubFolders) then
            for d in m.PrajnaLocalFolderStatus.SubFolders.Value do
                let folderName = (new DirectoryInfo(d.Path)).Name
                let item = m.Name,d
                let exists,value = dict.TryGetValue(folderName)
                if exists then
                    value.Add(item)
                else
                    let list = new List<_>()
                    list.Add(item) 
                    dict.Add(folderName, list)
    sizes, dict

// Take the output from getPrajnaLocalFolderUsage, draw the chart for the summary
let getPrajnaLocalFolderSummaryChart (sizes : (string * int64<MByte>) list) =
    use t = new LogTimer("getPrajnaLocalFolderSummaryChart")
    let title = "Prajna Local Folder Total Size (MB)"
    Chart.Line((sizes |> List.sortBy ( fun v -> fst v)), Name = title, Title = title) |> formatXAxis   

// Take the output from getPrajnaLocalFolderUsage, draw the chart for the specified folder
let getPrajnaLocalFolderSubChart (dict : Dictionary<string, List<string * FolderStatus>>, folder) =
    use t = new LogTimer("getPrajnaLocalFolderSubChart")
    let list = (dict.[folder]) |> List.ofSeq |> List.sortBy ( fun v -> fst v)
    let sizes = list 
                |> List.map ( fun v -> 
                                  let name, d = v
                                  name, d.Size)
    let title = folder + " Size (MB)"
    Chart.Line(sizes, Name = folder, Title = title).WithTitle(InsideArea=false) |> formatXAxis

// Get the Prajna local folder usage, returns 
// * MachineName * Size list
// * a dictionary of Drive Name ->  List < MachineName * FolderStatus > 
let getPrajnaDataFolderUsage (status : MachineDrivesStatus []) =
    let sizes = status |> Seq.choose ( fun s -> 
                                        match s.PrajnaDataFoldersStatus with
                                        | None -> None
                                        | Some ds -> (s.Name, ds.TotalSize) |> Some ) 
                       |> List.ofSeq
    let dict = new Dictionary<_,List<_>>()
    for m in status do
        if Option.isSome(m.PrajnaDataFoldersStatus) then
            for d in m.PrajnaDataFoldersStatus.Value.Folders do
                let root = d.Path
                let item = m.Name,d
                let exists,value = dict.TryGetValue(root)
                if exists then
                    value.Add(item)
                else
                    let list = new List<_>()
                    list.Add(item) 
                    dict.Add(root, list)
    sizes, dict


// Take the output from getPrajnaDataFolderUsage, draw the chart for the summary
let getPrajnaDataFolderSummaryChart (sizes : (string * int64<MByte>) list) =
    use t = new LogTimer("getPrajnaDataFolderSummaryChart")
    let title = "Prajna Data Folders Total Size (MB)"
    Chart.Line((sizes |> List.sortBy ( fun v -> fst v)), Name = title, Title = title).WithTitle(InsideArea=false) |> formatXAxis  

// Take the output from getPrajnaLocalFolderUsage, draw the chart for the specified drive
let getPrajnaDataFolderSubChart (dict : Dictionary<string, List<string * FolderStatus>>, drive) =
    use t = new LogTimer("getPrajnaDataFolderSubChart")
    let list = (dict.[drive]) |> List.ofSeq |> List.sortBy ( fun v -> fst v)
    let sizes = list 
                |> List.map ( fun v -> 
                                  let name, d = v
                                  name, d.Size)
    let title = "Data Folder Size (MB) for '" + drive + "'"
    Chart.Line(sizes, Name = drive, Title = title).WithTitle(InsideArea=false) |> formatXAxis

//================================================================================
// Functions for processing returned status and generate charts for node view
//================================================================================

let getSizeMBLabel desc (size : int64<MByte>) =
    let s = String.Format("{0:n0}", (int64 size))
    sprintf "%s: %s(MB)" desc s

let getSizeGBLabel desc (size : int64<MByte>) =
    sprintf "%s: %.2f(GB)" desc ((float size) / 1024.0)

// Draw the disk usage charts for the drives
let getDrivesCharts (status : DriveStatus []) =
    use t = new LogTimer("getDrivesCharts")
    status
    |> Array.map (fun d ->
                    let used = d.TotalSize - d.FreeSpaceSize
                    let data = [ (getSizeGBLabel "Free Space" d.FreeSpaceSize), d.FreeSpaceSize
                                 (getSizeGBLabel "Used Space" used), used ]
                    Chart.Pie(data, Title = (getSizeGBLabel (d.Name + " Size") d.TotalSize))
                 )
        
// Draw the Prajna Local folder usage chart
let getPrajnaLocalFoldersUsageChart (folder : FolderStatus) =
    use t = new LogTimer("getPrajnaLocalFoldersUsageChart")
    let data = 
        folder.SubFolders.Value
        |> List.ofArray
        |> List.map ( fun f ->  (new DirectoryInfo(f.Path)).Name, f.Size )
    Chart.Column(data, Name = "Size (MB)", Title = (getSizeMBLabel folder.Path folder.Size)).WithTitle(InsideArea=false) |> formatXAxis

// Draw the Prajna Data folder usage chart
let getPrajnaDataFoldersUsageChart (dataFolder : DataFoldersStatus) =
    use t = new LogTimer("getPrajnaDataFoldersUsageChart")
    let data = 
        dataFolder.Folders
        |> List.ofArray
        |> List.map ( fun f -> f.Path, f.Size )
    Chart.Column(data, Name = "Size (MB)", Title = (getSizeMBLabel "Prajna Data" dataFolder.TotalSize)).WithTitle(InsideArea=false) |> formatXAxis

//================================================================================
// Main module
//================================================================================
module Main =
    
    // The UI window type
    type ClusterStatusWindow(xamlFile : string) =
        let loadXamlWindow (xamlFile : string) =
            use t = new LogTimer("loadXamlWindow")
            if (not (File.Exists xamlFile)) then failwith(xamlFile + " does not exist!")
            let reader = XmlReader.Create(xamlFile)
            XamlReader.Load(reader) :?> Window

        // =========================================================================
        // UI Controls
        // =========================================================================

        let window = loadXamlWindow xamlFile
        let exitMenuItem = window.FindName("ExitApp") :?> MenuItem
        let openMenuItem = window.FindName("OpenFile") :?> MenuItem
        let viewMenuItem = window.FindName("View") :?> MenuItem
        let selectClusterViewMenuItem = window.FindName("SelectClusterView") :?> MenuItem
        let selectNodesViewMenuItem = window.FindName("SelectNodesView") :?> MenuItem
        let grid = window.FindName("Grid") :?> Controls.Grid
        let leftTree = window.FindName("LeftTree") :?> TreeView
        let refreshButton = window.FindName("RefreshButton") :?> Button
        
        let busyIndicator = new BusyIndicator()
        do 
            busyIndicator.SetValue(BusyIndicator.BusyContentProperty, "Retrieve Cluster Status ...")
            busyIndicator.IsBusy <- true

        let rightChartHost = new WindowsFormsHost()
        let clusterDiskUsage = new TreeViewItem()
        do clusterDiskUsage.Header <- "Disk Usages"
        let clusterPrajnaLocalFolder = new TreeViewItem()
        do clusterPrajnaLocalFolder.Header <- "Prajna Local Folder"
        let clusterPrajnaDataFolder = new TreeViewItem()
        do clusterPrajnaDataFolder.Header <- "Prajna Data Folders"
        
        // Controls for the Node View 

        // Grid for Node View: two rows: row1 for disk usage, row 2 for local folder and data folder usage
        let nodeViewGrid = new Controls.Grid()
        let nodeViewGridRow1 = new RowDefinition()        
        let nodeViewGridRow2 = new RowDefinition()
        do 
           nodeViewGridRow1.Height <- new GridLength(0.5, GridUnitType.Star)
           nodeViewGridRow2.Height <- new GridLength(0.5, GridUnitType.Star)
           nodeViewGrid.RowDefinitions.Add(nodeViewGridRow1)
           nodeViewGrid.RowDefinitions.Add(nodeViewGridRow2)

        // first row is for disk usages
        let nodeViewFstGrid = new Controls.Grid()
        do
            nodeViewGrid.Children.Add(nodeViewFstGrid) |> ignore
            Grid.SetColumn(nodeViewFstGrid, 0)
            Grid.SetRow(nodeViewFstGrid, 0)
        let maxDisks = 6
        let diskColumnDefs = Array.init maxDisks (fun _ -> 
                                                        let col = new ColumnDefinition()
                                                        col.Width <- new GridLength(0.5, GridUnitType.Star)
                                                        col )
        let diskHosts = Array.init maxDisks (fun _ -> new WindowsFormsHost())

        // split the second row into 2 columns, 1 for local folders, 1 for data folders
        let nodeViewSndGrid = new Controls.Grid()
        do
            nodeViewGrid.Children.Add(nodeViewSndGrid) |> ignore
            Grid.SetColumn(nodeViewSndGrid, 0)
            Grid.SetRow(nodeViewSndGrid, 1)

        // 1st column for local folder
        let localColumn = new ColumnDefinition()
        do 
            localColumn.Width <- new GridLength(0.5, GridUnitType.Star)
            nodeViewSndGrid.ColumnDefinitions.Add(localColumn)            
        
        let localHost = new WindowsFormsHost()
        do
            nodeViewSndGrid.Children.Add(localHost) |> ignore
            Grid.SetColumn(localHost, 0)
            Grid.SetRow(localHost, 0)

        // 2nd column: for data folder
        let dataColumn = new ColumnDefinition()
        do
            dataColumn.Width <- new GridLength(0.5, GridUnitType.Star)
            nodeViewSndGrid.ColumnDefinitions.Add(dataColumn)            
        
        let dataHost = new WindowsFormsHost()
        do
            nodeViewSndGrid.Children.Add(dataHost) |> ignore
            Grid.SetColumn(dataHost, 1)
            Grid.SetRow(dataHost, 0)

        // =========================================================================
        // Utilities 
        // =========================================================================

        // cluster file
        let mutable cluster : Cluster option = None
        
        // Helpers
        let showWarning (msg : string) =
            MessageBox.Show(msg) |> ignore

        let mutable clusterStatus = None
        let mutable diskUsage = None
        let mutable prajnaLocalFolderUsage = None
        let mutable prajnaDataFolderUsage = None

        let mutable rightGridIndex = None

        // =========================================================================
        // Management logic
        // =========================================================================
        let shutdown () =
            use t = new LogTimer("shutdown")
            if Option.isSome cluster 
            then
                Cluster.Stop() 
                cluster <- None
            Application.Current.Shutdown()    

        let clearRightGrid () =
            if Option.isSome rightGridIndex then
                grid.Children.RemoveAt(rightGridIndex.Value)
                rightGridIndex <- None

        let showStatusLoadingIndicator () =
            if Option.isNone clusterStatus then
                clearRightGrid()
                rightGridIndex <- (grid.Children.Add(busyIndicator) |> Some)
                Grid.SetColumn(busyIndicator, 1)
                Grid.SetRow(busyIndicator, 0)

        let drawDiskUsageSummaryChart () =
            use t = new LogTimer("drawDiskUsageSummaryChart")
            let chart = getDiskUsageSummaryChart(diskUsage.Value)
            rightChartHost.Child <- new ChartControl(chart)

        let drawDiskUsageChart drive =
            use t = new LogTimer("drawDiskUsageChart")
            let chart = getDiskUsageChart(diskUsage.Value, drive)
            rightChartHost.Child <- new ChartControl(chart)

        let updateClusterDiskUsageTree () =
            use t = new LogTimer("updateClusterDiskUsageTree")
            clusterDiskUsage.Items.Clear()
            leftTree.Items.Add(clusterDiskUsage) |> ignore
            clusterDiskUsage.Selected.Add(fun _ -> drawDiskUsageSummaryChart())
            diskUsage.Value.Keys  // Keys are drive names
            |> Seq.iter (fun name ->
                            let drive = new TreeViewItem()
                            drive.Header <- name
                            drive.Selected.Add(fun e -> drawDiskUsageChart(name)
                                                        e.Handled <- true
                                              )
                            clusterDiskUsage.Items.Add(drive) |> ignore)

        let drawPrajnaLocalFolderSummaryChart () =
            use t = new LogTimer("drawPrajnaLocalFolderSummaryChart")
            let chart = getPrajnaLocalFolderSummaryChart(fst prajnaLocalFolderUsage.Value)
            rightChartHost.Child <- new ChartControl(chart)

        let drawPrajnaLocalFolderSubChart folder =
            use t = new LogTimer("drawPrajnaLocalFolderSubChart")
            let chart = getPrajnaLocalFolderSubChart(snd prajnaLocalFolderUsage.Value, folder)
            rightChartHost.Child <- new ChartControl(chart)

        let updatePrajnaLocalFolderUsageTree () =
            use t = new LogTimer("updatePrajnaLocalFolderUsageTree")
            clusterPrajnaLocalFolder.Items.Clear()
            leftTree.Items.Add(clusterPrajnaLocalFolder) |> ignore
            clusterPrajnaLocalFolder.Selected.Add(fun _ -> drawPrajnaLocalFolderSummaryChart())
            (snd prajnaLocalFolderUsage.Value).Keys  // Keys are folder names
            |> Seq.iter (fun name ->
                            let folder = new TreeViewItem()
                            folder.Header <- name
                            folder.Selected.Add(fun e -> drawPrajnaLocalFolderSubChart(name)
                                                         e.Handled <- true
                                              )
                            clusterPrajnaLocalFolder.Items.Add(folder) |> ignore)

        let drawPrajnaDataFolderSummaryChart () =
            use t = new LogTimer("drawPrajnaDataFolderSummaryChart")
            let chart = getPrajnaDataFolderSummaryChart(fst prajnaDataFolderUsage.Value)
            rightChartHost.Child <- new ChartControl(chart)

        let drawPrajnaDataFolderSubChart folder =
            use t = new LogTimer("drawPrajnaDataFolderSubChart")
            let chart = getPrajnaDataFolderSubChart(snd prajnaDataFolderUsage.Value, folder)
            rightChartHost.Child <- new ChartControl(chart)

        let updatePrajnaDataFolderUsageTree () =
            use t = new LogTimer("updatePrajnaDataFolderUsageTree")
            clusterPrajnaDataFolder.Items.Clear()
            leftTree.Items.Add(clusterPrajnaDataFolder) |> ignore
            clusterPrajnaDataFolder.Selected.Add(fun _ -> drawPrajnaDataFolderSummaryChart())
            (snd prajnaDataFolderUsage.Value).Keys  // Keys are dir info
            |> Seq.iter (fun name ->
                            let drive = new TreeViewItem()
                            drive.Header <- name
                            drive.Selected.Add(fun e -> drawPrajnaDataFolderSubChart(name)
                                                        e.Handled <- true
                                              )
                            clusterPrajnaDataFolder.Items.Add(drive) |> ignore)

        let updateClusterTreeView () =
            updateClusterDiskUsageTree()
            updatePrajnaLocalFolderUsageTree()
            updatePrajnaDataFolderUsageTree()

            // Add the WindowsFormsHost for the chart 
            clearRightGrid()

            rightGridIndex <- (grid.Children.Add(rightChartHost) |> Some)
            Grid.SetColumn(rightChartHost, 1)
            Grid.SetRow(rightChartHost, 0)

            rightChartHost.Child <- null            

        let updateClusterViewAsync =
            async {
                use t = new LogTimer("updateClusterViewAsync")
                if Option.isNone clusterStatus then
                    clusterStatus <- (fetchClusterStatus(cluster.Value) |> Some)
                diskUsage <- (getDiskUsage(clusterStatus.Value) |> Some)
                prajnaLocalFolderUsage <- (getPrajnaLocalFolderUsage(clusterStatus.Value) |> Some)
                prajnaDataFolderUsage <- (getPrajnaDataFolderUsage(clusterStatus.Value) |> Some)
                leftTree.Dispatcher.BeginInvoke(DispatcherPriority.Background, Action( fun _ -> updateClusterTreeView()) ) |> ignore
            }

        let showClusterView () =  
            use t = new LogTimer("showClusterView")                  
            selectNodesViewMenuItem.IsChecked <- false
            leftTree.Items.Clear()
            
            showStatusLoadingIndicator()

            Async.StartAsTask( updateClusterViewAsync ) |> ignore

        let drawNodeUsage (status : MachineDrivesStatus) =
            use t = new LogTimer("drawNodeUsage")
            clearRightGrid()
           
            // split the first row into N columns, N = # of drives
            let drivesCharts = getDrivesCharts(status.Drives) 
            let chartsToUse = 
                if drivesCharts.Length > maxDisks then
                    Array.sub drivesCharts 0 maxDisks
                else
                    drivesCharts
            if nodeViewFstGrid.ColumnDefinitions.Count = chartsToUse.Length then
                // matches, just reuse
                for i = 0 to (chartsToUse.Length - 1) do
                    let chart = chartsToUse.[i]
                    let host = diskHosts.[i]
                    host.Child <- new ChartControl(chart)
            else
                // re-construct
                nodeViewFstGrid.ColumnDefinitions.Clear()
                for i = 0 to (chartsToUse.Length - 1) do
                    let chart = chartsToUse.[i]
                    let column = diskColumnDefs.[i]
                    nodeViewFstGrid.ColumnDefinitions.Add(column)
                    let host = diskHosts.[i]
                    host.Child <- new ChartControl(chart)
                    nodeViewFstGrid.Children.Add(host) |> ignore
                    Grid.SetColumn(host, i)
                    Grid.SetRow(host, 0)
           
            // The second row has 2 columns, 1 for local folders, 1 for data folders           
            let localFoldersChart = getPrajnaLocalFoldersUsageChart(status.PrajnaLocalFolderStatus)
            localHost.Child <- new ChartControl(localFoldersChart)

            // for data folder
            if Option.isSome status.PrajnaDataFoldersStatus then                  
                let dataFoldersChart = getPrajnaDataFoldersUsageChart(status.PrajnaDataFoldersStatus.Value)
                dataHost.Child <- new ChartControl(dataFoldersChart)       
            
            // bind nodeViewGrid to grid
            rightGridIndex <- (grid.Children.Add(nodeViewGrid) |> Some)
            Grid.SetColumn(nodeViewGrid, 1)
            Grid.SetRow(nodeViewGrid, 0)

        let updateNodeViewTree () =
            use t = new LogTimer("updateNodeViewTree")
            clusterStatus.Value 
            |> Array.sortBy ( fun m -> m.Name)
            |> Array.iter (fun m -> 
                             let machineItem = new TreeViewItem()
                             machineItem.Header <- m.Name
                             machineItem.Selected.Add ( fun _ -> drawNodeUsage(m))
                             leftTree.Items.Add(machineItem) |> ignore
                          )
            clearRightGrid()

        let updateNodesViewAsync =
            async {
                use t = new LogTimer("updateNodesViewAsync")
                if Option.isNone clusterStatus then
                    clusterStatus <- (fetchClusterStatus(cluster.Value) |> Some)
                leftTree.Dispatcher.BeginInvoke(DispatcherPriority.Background, Action( fun _ -> updateNodeViewTree())) |> ignore
            }

        let showNodesView () =
            use t = new LogTimer("showNodesView")
            selectClusterViewMenuItem.IsChecked <- false
            leftTree.Items.Clear()

            showStatusLoadingIndicator()

            Async.StartAsTask( updateNodesViewAsync ) |> ignore

        let openClusterFile () =
            use t = new LogTimer("openClusterFile")
            let dlg = new Microsoft.Win32.OpenFileDialog()
            dlg.DefaultExt <- "*.inf"
            dlg.Filter <- "Cluster Information Files|*.inf"
            let r = dlg.ShowDialog()
            if r.HasValue && r.Value = true 
            then 
               let file = dlg.FileName
               try
                   selectClusterViewMenuItem.IsChecked <- false
                   viewMenuItem.Visibility <- Visibility.Hidden
                   Cluster.StartCluster( file )
                   cluster <- Cluster.Current 
               with
                  | ex -> showWarning ("Fail to start cluster with '" + file + "': " + ex.Message)

            if Option.isSome cluster 
            then                 
                viewMenuItem.Visibility <- Visibility.Visible                
                selectClusterViewMenuItem.IsChecked <- true
                selectNodesViewMenuItem.IsChecked <- false

        let refreshView () =
            use t = new LogTimer("refreshView")   
            clusterStatus <- None
            if selectClusterViewMenuItem.IsChecked then
                showClusterView()
            else if selectNodesViewMenuItem.IsChecked then
                showNodesView()

        // =========================================================================
        // Initialize observers for the control events
        // =========================================================================
        do                        
            viewMenuItem.Visibility <- Visibility.Hidden
            exitMenuItem.Click.Add (fun _ -> shutdown())  
            openMenuItem.Click.Add (fun _ -> openClusterFile())
            selectClusterViewMenuItem.Checked.Add (fun _ -> showClusterView())
            selectNodesViewMenuItem.Checked.Add (fun _ -> showNodesView())
            refreshButton.Click.Add(fun _ -> refreshView())

        // the main window
        member this.Window with get() = window
    
    // Setup log
    let verbose = ConfigurationManager.AppSettings.["Verbose"]
    let logFile = ConfigurationManager.AppSettings.["Logfile"]

    let logfilePath = StringTools.BuildFName Environment.CurrentDirectory logFile
    System.IO.File.Delete( logfilePath )
    Logger.ParseArgs( [| "-log"; logfilePath; "-verbose"; verbose |] )
    
    FInfo ( fun _ -> sprintf "Program %s" (Process.GetCurrentProcess().MainModule.FileName) )
    
    let app = new Application() 
    let clusterStatusWin = ClusterStatusWindow "ClusterStatusWindow.xaml"  

    [<STAThread>]
    do app.Run clusterStatusWin.Window |> ignore
    ()
