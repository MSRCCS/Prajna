(*---------------------------------------------------------------------------
    Copyright 2013 Microsoft

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
        PrajnaController.fs
  
    Description: 
        Prajna Controller Application. Each Prajna Cluster only needs to run one Prajna Controller application. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com
    Date:
        Aug. 2013
    
 ---------------------------------------------------------------------------*)
namespace Prajna.Toolkit.PrajnaController 
module internal MainApp = 

    open System
    open System.Collections.Generic
    open System.Windows
    open System.Windows.Media
    open System.Windows.Controls
    open System.Windows.Threading
    open Prajna.Tools
    open Prajna.Tools.FSharp
    open Prajna.Tools.StringTools
    open Prajna.Core

    type PrajnaGridSortBy =
        | MachineName = 0 
        | ClientVer = 1
        | HomeIn = 2
        | Cores = 3
        | RAM = 4
        | Disk = 5
        | MachineID = 6
        | Usage = 7
        | Network = 8

    type GeneralGrid() = 
        inherit Grid()
        let RowColor = [| Colors.AntiqueWhite;  Colors.LightYellow; Colors.LightBlue;Colors.LightPink; Colors.LightGreen |]
        member gd.AddColumn( width ) =
            let col = new ColumnDefinition( Width = new GridLength( width) )  
            gd.ColumnDefinitions.Add( col )
        member gd.AddRow( height ) = 
            let row = new RowDefinition( Height = new GridLength( height) )  
            gd.RowDefinitions.Add( row )       

        member gd.AddTextBlock( text, row, col, ?color )  = 
            let txtBlock = new TextBlock()
            txtBlock.Text <- text
            txtBlock.FontSize <- 14.
            txtBlock.FontWeight <- FontWeights.Normal
            txtBlock.Foreground <- new SolidColorBrush( Colors.Black) 
            if row >= 1 then 
                if (color = None) then
                    txtBlock.Background <- new SolidColorBrush( RowColor.[ row % RowColor.Length ] )
                else
                    txtBlock.Background <- new SolidColorBrush( color.Value )
            txtBlock.VerticalAlignment <- VerticalAlignment.Top
            Grid.SetRow( txtBlock, row )
            Grid.SetColumn( txtBlock, col)
            gd.Children.Add( txtBlock ) |> ignore
            ()

        member gd.AddControl( control: Control, row, col ) = 
            if row >= 1 then 
                control.Background <- new SolidColorBrush( RowColor.[ row % RowColor.Length ] )
            Grid.SetRow( control, row )
            Grid.SetColumn( control, col )
            gd.Children.Add( control ) |> ignore

        member gd.AddButton( text, row, col ) = 
            let button = new Button( Content = text, FontSize = 14., FontWeight = FontWeights.Normal, 
                                     Foreground = new SolidColorBrush( Colors.Black), VerticalAlignment = VerticalAlignment.Top )
    //        if row >= 1 then 
    //            button.Background <- new SolidColorBrush( RowColor.[ row % RowColor.Length ] )
            gd.AddControl( button, row, col )
            button
        member gd.AddRadioButtons( groupName, text:string[], row, col ) = 
            let stackPanel = new StackPanel( Orientation = Orientation.Horizontal )
            let buttons = text |> Array.map ( fun info -> 
                                                let radioButton = new RadioButton( GroupName = groupName, Name=info ) 
                                                stackPanel.Children.Add( radioButton ) |> ignore
                                                radioButton )
            Grid.SetRow( stackPanel, row )
            Grid.SetColumn( stackPanel, col )
            gd.Children.Add( stackPanel ) |> ignore
            buttons

    type PrajnaNodeGrid() = 
        inherit GeneralGrid()
        member val SortBy = PrajnaGridSortBy.MachineName with get, set
        member val IncrementSort = true with get, set
        member val SortCluster = 0 with get, set

        member gd.Show( ) =
            let listOfClients = HomeInServer.ListOfClients
            let listOfClusters = HomeInServer.ListOfClusters |> Seq.mapi( fun i (cname, _, _) -> (cname, i) ) |> Seq.toArray
            let FwdComparer = 
                { new IComparer< (string*int) > with 
                    member self.Compare(y, z) = 
                        let yname, _ = y
                        let zname, _ = z
                        yname.CompareTo(zname ) }
            let InvComparer =
                { new IComparer< (string*int) > with 
                    member self.Compare(y, z) = 
                        let yname, _ = y
                        let zname, _ = z
                        zname.CompareTo(yname ) }
            let sortedListOfClusters = 
                match gd.SortCluster with 
                | 0 ->
                    listOfClusters
                | 1 ->
                    Array.Sort( listOfClusters, FwdComparer )
                    listOfClusters
                | 2 ->
                    Array.Sort( listOfClusters, InvComparer )
                    listOfClusters
                | _ ->
                    failwith "Wrong value of SortCluster"

            let snapshotCluster (bSave:bool) = 
                let ver = (DateTime.UtcNow) 
                let clusterName = ref ("Cluster_" + VersionToString( ver ))
                let bInputReceived = ref false
                let st = new StackPanel( Background = new SolidColorBrush(Colors.LightSteelBlue), 
                                         Orientation = Orientation.Vertical );
                let txtBlock = new TextBlock( Text = "Please input the name of the cluster: ", 
                                              FontSize = 30.,
                                              TextWrapping = TextWrapping.Wrap,
                                              FontWeight = FontWeights.Normal, 
                                              Margin = new Thickness( 10. ), 
                                              Foreground = new SolidColorBrush( Colors.Black), 
                                              Background = new SolidColorBrush( Colors.LightSteelBlue)  )
                let canvas = new Canvas( Width = 600., Height = 200., Background = new SolidColorBrush( Colors.LightSteelBlue) )
                let inputBlock = new TextBox( Text = !clusterName, 
                                              Width = 550., Height = 50., 
                                              FontSize = 25.,
                                              FontWeight = FontWeights.Normal, 
                                              IsReadOnly = false,
                                              AcceptsReturn = false, 
                                              AcceptsTab = false,
                                              Margin = new Thickness( 10., 50., 10., 50. ), 
                                              Foreground = new SolidColorBrush( Colors.Black), 
                                              Background = new SolidColorBrush( Colors.White)  )

                canvas.Children.Add( inputBlock ) |> ignore
                let button = new Button( Content = "OK", FontSize = 30., FontWeight = FontWeights.Heavy, 
                                         Foreground = new SolidColorBrush( Colors.Black), 
                                         Margin = new Thickness( 250., 10., 250., 10.) )

                st.Children.Add( txtBlock ) |> ignore
                st.Children.Add( canvas ) |> ignore
                st.Children.Add( button ) |> ignore
                let inputWin = new Window( Title = "Cluster Name Dialog Window", 
                                        Content = st, 
                                        Width = 600., 
                                        Height = 400. )
                let nameInputed() = 
                    clusterName := inputBlock.Text
                    bInputReceived := true
                    inputWin.Close()
                    HomeInServer.TakeSnapShot( ver )
                    HomeInServer.CurrentCluster.Name <- !clusterName
                    if bSave then 
                        HomeInServer.CurrentCluster.SaveShort()
                        HomeInServer.CurrentCluster.SaveLst()
                        MessageBox.Show( "Cluster Name is " + !clusterName ) |> ignore

                button.Click.Add( fun _ -> nameInputed() )
                inputBlock.KeyDown.Add( fun e -> if e.Key=System.Windows.Input.Key.Return then nameInputed() )

                inputWin.Topmost <- true
                inputWin.Show()

           
                ()

            gd.Children.Clear()
            gd.RowDefinitions.Clear()
            gd.ColumnDefinitions.Clear()
            gd.AddColumn( 130. )
            gd.AddColumn( 100. )
            gd.AddColumn( 100. )
            gd.AddColumn( 50. )
            gd.AddColumn( 100. )
            gd.AddColumn( 100. )
            gd.AddColumn( 130. )
            gd.AddColumn( 50. )
            gd.AddColumn( 100. )
            gd.AddColumn( 50. )
            gd.AddColumn( 150. )
            gd.AddColumn( 120. )
            gd.AddColumn( 120. )
        
            let cntClient = if Utils.IsNull listOfClients then 0 else listOfClients.Count
            let cntCluster = sortedListOfClusters.Length
            let cnt = Math.Max( cntClient, cntCluster )

            gd.AddRow( 25.0 )
            for j=1 to cnt do
                gd.AddRow( 20. )
            gd.Height <- 20. * ( float (cnt+1)*20. )

            let btnMachineName = gd.AddButton( "Machine Name", 0, 0 )
            btnMachineName.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.MachineName then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.MachineName )
            let btnClientVer = gd.AddButton( "Client Version", 0, 1 )
            btnClientVer.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.ClientVer then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.ClientVer )
            let btnHomeIn = gd.AddButton( "Home In(s)", 0, 2 )
            btnHomeIn.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.HomeIn then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.HomeIn )
            let btnCores = gd.AddButton( "Cores", 0, 3 )
            btnCores.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.Cores then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.Cores )
            let btnRAM = gd.AddButton( "RAM(MB)", 0, 4 )
            btnRAM.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.RAM then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.RAM )
            let btnDisk = gd.AddButton( "Disk(GB)", 0, 5 )
            btnDisk.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.Disk then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.Disk )
            let btnMachineID = gd.AddButton( "MachineID", 0, 6 )
            btnMachineID.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.MachineID then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.MachineID )
            let btnUsage = gd.AddButton( "Usage", 0, 7 )
            btnUsage.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.Usage then 
                                            gd.IncrementSort <- not gd.IncrementSort
                                         else
                                            gd.SortBy <- PrajnaGridSortBy.Usage )

            let btnNetwork = gd.AddButton( "Network", 0, 8 )
            btnNetwork.Click.Add( fun _ -> if gd.SortBy = PrajnaGridSortBy.Network then 
                                                    gd.IncrementSort <- not gd.IncrementSort
                                               else
                                                    gd.SortBy <- PrajnaGridSortBy.Network )

            let btnChecked = gd.AddButton( "Check", 0, 9 )
            btnChecked.Click.Add( fun _ -> lock ( HomeInServer.ListOfClients ) ( fun () -> for pair in HomeInServer.ListOfClients do pair.Value.Selected <- not pair.Value.Selected ) )

            let btnCluster = gd.AddButton( "Cluster", 0, 10 )
            btnCluster.Click.Add( fun _ -> gd.SortCluster <- ( gd.SortCluster + 1 ) % 3 )

            let btnFilterBy = gd.AddButton( "Filter:Inc,Exc,Only", 0, 11 )
            btnFilterBy.Click.Add( fun _ -> HomeInServer.UpdateListOfClients() )


            let btnSaveSnapShot = gd.AddButton( "Save SnapShot", 0, 12 )    
            btnSaveSnapShot.Click.Add( fun _ -> snapshotCluster true )

            let btnTakeSnapShot = gd.AddButton( "Take SnapShot", 1, 12 )    
            btnTakeSnapShot.Click.Add( fun _ -> snapshotCluster false )

    //        gd.AddTextBlock( "Check", 0, 8 )

            // Show Client Status
            if Utils.IsNotNull listOfClients then 
                let mutable row = 1
                let partialSortSeq = 
                    listOfClients.Values 
                    |> Seq.sortBy( fun info -> match gd.SortBy with 
                                               | PrajnaGridSortBy.MachineName -> info.Name 
                                               | PrajnaGridSortBy.ClientVer -> info.ClientVersionInfo
                                               | PrajnaGridSortBy.HomeIn -> 
                                                    let timespan = (DateTime.UtcNow).Subtract( info.LastHomeIn )
                                                    timespan.ToString( @"d\.hh\:mm\:ss" )
                                               | PrajnaGridSortBy.Cores -> info.ProcessorCount.ToString()
                                               | PrajnaGridSortBy.RAM -> info.MemorySpace.ToString()
                                               | PrajnaGridSortBy.Disk -> info.DriveSpace.ToString()
                                               | PrajnaGridSortBy.MachineID -> info.MachineID.ToString("x")
                                               | PrajnaGridSortBy.Usage -> info.CpuUsage
                                               | PrajnaGridSortBy.Network -> info.NetworkSpeed.ToString("D18")
                                               | _ -> info.Name  )
                let sortedListOfClients = 
                    if gd.IncrementSort then 
                        Seq.toList(partialSortSeq)
                    else
                        List.rev( Seq.toList(partialSortSeq) )

                for info in sortedListOfClients do
                
                    gd.AddTextBlock( info.Name, row, 0 )
                    gd.AddTextBlock( info.ClientVersionInfo, row, 1 )
                    let timespan = (DateTime.UtcNow).Subtract( info.LastHomeIn )
                    let showTime = timespan.ToString( @"d\.hh\:mm\:ss" )
                    if (timespan.TotalSeconds > 11.0) then
                        gd.AddTextBlock( showTime, row, 2, Colors.Red )
                    else
                        gd.AddTextBlock( showTime, row, 2 )
                    gd.AddTextBlock( info.ProcessorCount.ToString(), row, 3 )
                    gd.AddTextBlock( info.MemorySpace.ToString() + "/" + info.RamCapacity.ToString(), row, 4 )
                    gd.AddTextBlock( info.DriveSpace.ToString() + "/" + info.DiskCapacity.ToString(), row, 5 )
                    gd.AddTextBlock( info.MachineID.ToString("x"), row, 6 )
                    gd.AddTextBlock( info.CpuUsage, row, 7 )
                    gd.AddTextBlock( info.NetworkMtu.ToString()+"/"+Config.NetworkSpeedToString( info.NetworkSpeed), row, 8 )
                    let checkBox = new CheckBox( IsChecked = Nullable(info.Selected) )
                    checkBox.Click.Add( fun _ -> lock ( HomeInServer.ListOfClients ) ( fun () -> info.Selected <- not info.Selected ) )
                    gd.AddControl( checkBox, row, 9 )
                    row <- row+1

                

            let mutable row = 1
            let strArray = Array.create 4 ""
            for cluster in sortedListOfClusters do
                let cname, idx = cluster
                gd.AddTextBlock( cname, row, 10 )
                let radioButtons = gd.AddRadioButtons( cname, strArray, row, 11 )
                let cname, cl, status = HomeInServer.ListOfClusters.[idx]
                for i = 0 to radioButtons.Length-1 do
                    radioButtons.[i].IsChecked <- if i=status then Nullable( true ) else Nullable (false)
                    radioButtons.[i].Click.Add( fun _ -> HomeInServer.ListOfClusters.[idx] <- cname, cl, i 
                                                         HomeInServer.UpdateListOfClients()
                                              )                                    
                row <- row + 1

    type PrajnaStorageGrid() =
        inherit GeneralGrid()
        member val SortBy = PrajnaGridSortBy.MachineName with get, set
        member val IncrementSort = true with get, set
        member val SortCluster = 0 with get, set
        member x.Start() = 
            ()
        member x.Stop() = 
            ()
        member x.Show( ) =
            ()

    type GeneralTab() = 
        inherit TabControl() 
        let mutable tabItems = List<TabItem>()
        member x.AddTab( header, c:ContentControl ) =
            let newitem = new TabItem( Header = header, Content=c )
            tabItems.Add( newitem )
            x.Items.Add( newitem ) |> ignore
            ()
        member x.Tab( index ) = 
            tabItems.[index]



    // type MainWindow = XAML<"MainWindow.xaml">
    type PrajnaControllerWindow(info, port, cport ) as cv = 
        inherit Canvas( ) 
        let tabs = new GeneralTab( Width=1400., Height=800. )
        let scroll = new ScrollViewer( Width=1400., Height=768., VerticalScrollBarVisibility = ScrollBarVisibility.Visible )
        let gd = new PrajnaNodeGrid( Width=1400., Height=2400., HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top, 
                                 ShowGridLines = false, Background = new SolidColorBrush(Colors.LightSteelBlue) )
        let scroll1 = new ScrollViewer( Width=1400., Height=768., VerticalScrollBarVisibility = ScrollBarVisibility.Visible )
        let gd1 = new PrajnaStorageGrid( Width=1400., Height=2400., HorizontalAlignment = HorizontalAlignment.Left, VerticalAlignment = VerticalAlignment.Top, 
                                 ShowGridLines = false, Background = new SolidColorBrush(Colors.LightSteelBlue) )

        // timer for updating frame, 1 frame every 25ms
        let timerNodeUpdate = new DispatcherTimer(Interval = new TimeSpan(0,0,0,0,1000))
        // timer for updating frame, 1 frame every 25ms
        let timerStorageUpdate = new DispatcherTimer(Interval = new TimeSpan(0,0,0,0,1000))
        let init() = 
            cv.Width <- 1100.
            cv.Height <- 800.
            tabs.AddTab( "Node View", scroll )
            tabs.AddTab( "Storage View", scroll1 ) 
            scroll1.Content <- gd1
            scroll.Content <- gd      
            cv.Children.Add( tabs ) |> ignore
            // set up the timer
            timerNodeUpdate.Tick.Add(fun _ ->
                lock(HomeInServer.ListOfClients ) ( fun () -> gd.Show(  ) )
                cv.InvalidateVisual())
            tabs.SelectionChanged.Add( PrajnaControllerWindow.TabSelectionChanged )
        do 
            init()
        member x.ScrollView with get() = scroll
        member x.Grid with get() = gd
        member x.NodeTimer with get() = timerNodeUpdate
        member x.ChangeSelection( idx ) = 
            if idx=0 then 
                timerNodeUpdate.Start()
                gd1.Stop()
                timerStorageUpdate.Stop()
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start tracking nodes." ))
            else
                timerNodeUpdate.Stop()
                gd1.Start()
                timerStorageUpdate.Start()
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Start tracking storage" ))
            ()
        static member TabSelectionChanged( e ) = 
            let tabs = e.Source :?> GeneralTab
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Tab %d is selected" tabs.SelectedIndex ))
            let cv = tabs.Parent :?> PrajnaControllerWindow
            cv.ChangeSelection(tabs.SelectedIndex)
            ()     
        member val Status = 
            new HomeInServer(info=info, port=port, cport=cport ) 
            with get, set
        member val Content = gd with get
    

    module Main = 
        // Show Prajna controller window
        let PrajnaMasterFile = "master.info"
        let masterInfo = ClientMasterConfig.Parse( PrajnaMasterFile )
        let logFile = StringTools.BuildFName masterInfo.CurPath "PrajnaController.log"
        System.IO.File.Delete( logFile )
        Logger.ParseArgs( [| "-log"; logFile; "-verbose"; "4" |])

    //    let serverInfo = ( sprintf "%s\t%s" masterInfo.VersionInfo DeploymentSettings.DeploymentFolder )
        let serverInfo = ( sprintf "%s\t%s\t%s\t%s" masterInfo.VersionInfo masterInfo.RootDeployPath "DeploymentSettings.exe" "PrajnaClient.exe" )
        let cv = new PrajnaControllerWindow( serverInfo, masterInfo.MasterPort, masterInfo.ControlPort )
        let win = new Window( Title = "Prajna Controller", 
                                Content = cv, 
                                Width = 1400., 
                                Height = 768. )
        win.SizeChanged.Add( fun arg -> 
                                cv.Width <- arg.NewSize.Width - 20.
                                cv.ScrollView.Width <- cv.Width
                                cv.Grid.Width <- cv.Width
                                cv.Height <- arg.NewSize.Height 
                                cv.ScrollView.Height <- cv.Height
                                )
        cv.Content.Show( )

        // HomeIn Listening Thread
        let HomeInThreadStart = new Threading.ParameterizedThreadStart( HomeInServer.StartServer )
        let HomeInThread = new Threading.Thread( HomeInThreadStart )
        // IsBackground Flag true: thread will abort when main thread is killed. 
        HomeInThread.IsBackground <- true
        HomeInThread.Start( cv.Status )

        // Controller Listening Thread
        let ControllerThreadStart = new Threading.ParameterizedThreadStart( HomeInServer.StartController )
        let ControllerThread = new Threading.Thread( ControllerThreadStart )
        // IsBackground Flag true: thread will abort when main thread is killed.
        ControllerThread.IsBackground <- true
        ControllerThread.Start( cv.Status )

        HomeInServer.LoadAllClusters()

        win.Closed.Add( fun x -> 
            Application.Current.Shutdown()
    //        HomeInThread.Abort() 
    //        ControllerThread.Abort()
            )

    //
        let app = new Application()
        [<STAThread>]
        do app.Run win |> ignore

        ()
