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
        Grids.fs
  
    Description: 
        A generic grid with scroll bars. 

    Author:																	
        Jin Li, Principal Researcher
        Microsoft Research, One Microsoft Way
        Email: jinl at microsoft dot com

    Date:
        Oct. 2014
    
 ---------------------------------------------------------------------------*)
namespace Prajna.WPFTools

open System
open System.Threading
open System.Linq
open System.Collections
open System.Collections.Generic
open System.Collections.Concurrent
open Prajna.Tools
open Prajna.Tools.StringTools
open Prajna.Tools.FSharp

open System.ComponentModel
open System.Windows
open System.Windows.Data
open System.Windows.Media
open System.Windows.Controls
open System.Windows.Threading

[<AllowNullLiteral>]
type internal GeneralGrid() = 
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

[<AllowNullLiteral;AbstractClass>]
/// A Grid with both horizontal and vertical scroll bar 
type GridWithScrollBar( ) as x = 
    inherit DockPanel()
    /// Get and Set size of the scroll bar
    static member val ScrollBarWidth = 30. with get, set
    member val private Grid = new Grid( ShowGridLines = false, Background = new SolidColorBrush(Colors.LightSteelBlue) ) with get
    member val private HorizontalScrollBar = new Primitives.ScrollBar ( Width = x.Width - GridWithScrollBar.ScrollBarWidth, Height = GridWithScrollBar.ScrollBarWidth, Orientation = Orientation.Horizontal  ) with get
    member val private VerticalScrollBar =  new Primitives.ScrollBar ( Width = GridWithScrollBar.ScrollBarWidth, Height = x.Height - GridWithScrollBar.ScrollBarWidth, Orientation = Orientation.Vertical  ) with get
    member val private bNewContent = true with get, set
    member val private bHorizontalScrollBarVisible=false with get, set
    member val private bVerticalScrollBarVisible=false with get, set
    member val private ContentWidth = -1. with get, set
    member val private ContentHeight = -1. with get, set
    /// Get and set whether first row always show (i.e., does not scroll) 
    member val bFirstRowAlwaysShow = true with get, set
    /// Get and set whether first column always show (i.e., does not scroll) 
    member val bFirstColumnAlwaysShow = true with get, set
    member val private RowStart = 0 with get, set
    member val private RowShown = 30 with get, set
    member val private ColStart = 0 with get, set
    member val private ColShown = 5 with get, set
    member val private RowHeights = null with get, set
    member val private ColWidths = null with get, set
    member val private AggregateRowHeights = null with get, set
    member val private AggregateColWidths = null with get, set
    member val private HorizontalScrollValue = ref Double.MinValue with get, set
    member val private VerticalScrollValue = ref Double.MinValue with get, set
    /// The function is called to return the width of column (in pixel) of the grid. 
    abstract GetColumnWidths : unit -> float[]
    /// The function is called to return the height of row (in pixel) of the grid. 
    abstract GetRowHeights : unit -> float[]
    /// Get a particular UIElement for the scrolling grid.
    /// The first tuple is the shown row and column. 
    /// The second tuple is the row and column in the data. 
    /// We have use both shown and grid row/column as some row and column (e.g., the first row) may not 
    /// scroll
    abstract GetUIElement : int * int -> int * int -> UIElement 
    member internal x.UpdateContentArea( width, height ) = 
        x.bNewContent <- true
        x.ContentWidth <- width
        x.ContentHeight <- height
        x.ScrollBarVisibility()
    member internal x.UpdateContentArea() = 
        x.RowHeights <- x.GetRowHeights()
        if Utils.IsNull x.RowHeights then 
            failwith "overGridWithScrollBar.GetRowHeights should return at least 1 row"
        let agg = ref 0.0
        x.AggregateRowHeights <- x.RowHeights |> Array.map( fun h -> let cur = !agg
                                                                     agg := cur + h
                                                                     cur )
        x.ColWidths <- x.GetColumnWidths()
        if Utils.IsNull x.ColWidths then 
            failwith "overGridWithScrollBar.GetColumnWidths should return at least 1 column"
        agg := 0.0
        x.AggregateColWidths <- x.ColWidths |> Array.map( fun h -> let cur = !agg
                                                                   agg := cur + h
                                                                   cur )
        let width = x.ColWidths |> Array.sum
        let height = x.RowHeights |> Array.sum
        x.UpdateContentArea( width, height )
    member internal x.ValidateShowArea() = 
        if Utils.IsNull x.RowHeights || Utils.IsNull x.ColWidths then 
            x.UpdateContentArea()
        let rowStart = if x.bFirstRowAlwaysShow then 1 else 0
        x.RowStart <- Math.Max( x.RowStart, rowStart )
        let colStart = if x.bFirstColumnAlwaysShow then 1 else 0 
        x.ColStart <- Math.Max( x.ColStart, colStart )
        let width, height = x.Width, x.Height
        let mutable cw = if x.bFirstColumnAlwaysShow then x.ColWidths.[0] else 0.0
        let mutable ch = if x.bFirstRowAlwaysShow then x.RowHeights.[0] else 0.0
        let mutable nrow, ncol = 0, 0
        let mutable bDone = false
        // Find # of rows. 
        while not bDone do
            let row = x.RowStart + nrow 
            if row >= x.RowHeights.Length then 
                bDone <- true
            else
                ch <- ch + x.RowHeights.[ row ]
                nrow <- nrow + 1
                if ch >= x.Height then 
                    bDone <- true
        x.RowShown <- nrow
        // Find # of columns 
        bDone <- false
        while not bDone do
            let col = x.ColStart + ncol 
            if col >= x.ColWidths.Length then 
                bDone <- true
            else
                cw <- cw + x.ColWidths.[ col ]
                ncol <- ncol + 1
                if cw >= x.Width then 
                    bDone <- true
        x.ColShown <- ncol   
        x.SetHorizontalSmallChange()
        x.SetVerticalSmallChange()
    member internal x.DoSetup() = 
        x.Children.Clear()
        x.LastChildFill <- true
        x.bHorizontalScrollBarVisible <- x.Width < x.ContentWidth
        if x.bHorizontalScrollBarVisible then 
            x.SetHorizontalSmallChange()
            x.HorizontalScrollBar.Scroll.Add( x.ScrollHorizontalBar )
            x.HorizontalScrollBar.ValueChanged.Add( x.ValueChangedHorizontalBar )
            DockPanel.SetDock( x.HorizontalScrollBar, Dock.Bottom )
            x.Children.Add( x.HorizontalScrollBar ) |> ignore
        x.bVerticalScrollBarVisible <- x.Height < x.ContentHeight
        if x.bVerticalScrollBarVisible then 
            x.SetVerticalSmallChange()
            x.VerticalScrollBar.Scroll.Add( x.ScrollVerticalBar )
            x.VerticalScrollBar.ValueChanged.Add( x.ValueChangedVerticalBar )
            DockPanel.SetDock( x.VerticalScrollBar, Dock.Right )
            x.Children.Add( x.VerticalScrollBar ) |> ignore
        DockPanel.SetDock( x.Grid, Dock.Top )
        x.Grid.Width <- if x.bVerticalScrollBarVisible then x.Width - GridWithScrollBar.ScrollBarWidth else x.Width
        x.Grid.Height <- if x.bHorizontalScrollBarVisible then x.Height - GridWithScrollBar.ScrollBarWidth else x.Height
        x.Children.Add( x.Grid ) |> ignore
    member internal x.SetHorizontalSmallChange() = 
        if x.bHorizontalScrollBarVisible then 
            if Utils.IsNotNull x.ColWidths && x.ColStart < x.ColWidths.Length && x.ColStart >=0 then 
                let smallchange = if x.ColStart > 0 then Math.Max( x.ColWidths.[ x.ColStart], x.ColWidths.[ x.ColStart - 1]) else x.ColWidths.[ x.ColStart]
                x.HorizontalScrollBar.SmallChange <- smallchange
    member internal x.ScrollHorizontalBar e = 
        x.HorizontalScrollTo e.NewValue
    member internal x.ValueChangedHorizontalBar e = 
        x.HorizontalScrollTo e.NewValue
    member internal x.HorizontalScrollTo value = 
        let oldvalue = !x.HorizontalScrollValue
        if value <> oldvalue && Interlocked.CompareExchange( x.HorizontalScrollValue, value, oldvalue )=oldvalue then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Horizontal scroll to %f" value ))
            let width = if Utils.IsNull x.ColWidths then x.Width else Math.Max( 0.0, x.Width - x.ColWidths.[x.ColWidths.Length - 1] )
            let newvalue = Math.Max( 0.0, Math.Min( value, x.ContentWidth ) ) / x.ContentWidth * (x.ContentWidth - width )
            let mutable first = if x.bFirstColumnAlwaysShow then 1 else 0
            let mutable last = x.ColWidths.Length - 1
            let mutable bFind = last <= first
            // Binary search
            while not bFind do 
                let mid = ( first + last ) / 2
                if x.AggregateColWidths.[mid] > newvalue then 
                    if last > mid then 
                        last <- mid
                    else
                        bFind <- true
                else 
                    if first < mid then 
                        first <- mid
                    else
                        if first < last then 
                            first <- last 
                        else
                            bFind <- true
            let bScrolled = x.ColStart <> first
            x.ColStart <- first
            if bScrolled then 
                x.ValidateShowArea()
                x.SetHorizontalSmallChange()
                x.Show()
    member internal x.SetVerticalSmallChange() = 
        if x.bVerticalScrollBarVisible then 
            if Utils.IsNotNull x.RowHeights && x.RowStart < x.RowHeights.Length && x.RowStart >=0 then 
                let smallchange = if x.RowStart > 0 then Math.Max( x.RowHeights.[ x.RowStart], x.RowHeights.[ x.RowStart - 1]) else x.RowHeights.[ x.RowStart]
                x.VerticalScrollBar.SmallChange <- smallchange
    member internal x.ScrollVerticalBar e = 
        x.VerticalScrollTo e.NewValue
    member internal x.ValueChangedVerticalBar e = 
        x.VerticalScrollTo e.NewValue
    member internal x.VerticalScrollTo value = 
        let oldvalue = !x.VerticalScrollValue
        if value <> oldvalue && Interlocked.CompareExchange( x.VerticalScrollValue, value, oldvalue )=oldvalue then 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Vertical scroll to %f" value ))
            let height = if Utils.IsNull x.RowHeights then x.Height else Math.Max(0.0, x.Height - x.RowHeights.[x.RowHeights.Length - 1] )
            let newvalue = Math.Max( 0.0, Math.Min( value, x.ContentHeight ) ) / x.ContentHeight * (x.ContentHeight - height )
            let mutable first = if x.bFirstRowAlwaysShow then 1 else 0
            let mutable last = x.RowHeights.Length - 1
            let mutable bFind = last <= first
            // Binary search
            while not bFind do 
                let mid = ( first + last ) / 2
                if x.AggregateRowHeights.[mid] > newvalue then 
                    if last > mid then 
                        last <- mid
                    else
                        bFind <- true
                else 
                    if first < mid then 
                        first <- mid
                    else
                        if first < last then 
                            first <- last
                        else
                            bFind <- true
            let bScrolled = x.RowStart <> first
            x.RowStart <- first
            if bScrolled then 
                x.ValidateShowArea()
                x.SetVerticalSmallChange()
                x.Show()
    member internal x.ScrollBarVisibility() = 
        x.Dispatcher.BeginInvoke( DispatcherPriority.Background, Action( x.DoScrollBarVisibility ) ) |> ignore
    member internal x.DoScrollBarVisibility() = 
        let width, height = x.Width, x.Height
        x.HorizontalScrollBar.Width <- width - GridWithScrollBar.ScrollBarWidth
        x.HorizontalScrollBar.Maximum <- Math.Max( x.ContentWidth, x.Width )
        x.HorizontalScrollBar.LargeChange <- x.Width
        x.VerticalScrollBar.Height <- height - GridWithScrollBar.ScrollBarWidth
        x.VerticalScrollBar.Maximum <- Math.Max( x.ContentHeight, x.Height )
        x.VerticalScrollBar.LargeChange <- x.Height
        x.DoSetup()
    member internal x.AdjustThumbSize() = 
        if x.bHorizontalScrollBarVisible then 
            let th = x.HorizontalScrollBar.Track
            if Utils.IsNotNull th then 
                th.Thumb.Background <- Brushes.Blue
                th.ViewportSize <- x.Width
        if x.bVerticalScrollBarVisible then 
            let th = x.VerticalScrollBar.Track
            if Utils.IsNotNull th then 
                th.Thumb.Background <- Brushes.Blue
                th.ViewportSize <- x.Height
    /// This function should be called whenever the window size has changed
    member x.SetSize( width, height ) = 
        // Any pending visual change? 
        let bCallExecute, bUseContinuation = x.UseContinuationForShowGrid() 
        if bUseContinuation then 
            Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Reschedule Resize to %fx%f via continuation ..." width height ))
            x.ShowGridContiuetion <-  ( fun _ ->  x.DoSetSize( width, height ) )
        elif bCallExecute then
            x.Dispatcher.BeginInvoke( DispatcherPriority.Background, Action( fun _ -> x.DoSetSize( width, height )
                                                                                  ) ) |> ignore
    member val private DoSetSizeTicks = ref Int64.MinValue with get, set
    member val private DoSetSizeContiuetion = fun _ -> () with get, set
    /// Change the size of the grid
    member x.DoSetSizeWithDelayUpdate( width, height ) = 
        let newTicks = (PerfDateTime.UtcNowTicks())
        x.DoSetSizeContiuetion <- fun _ -> x.DoSetSize( width, height )
        if Interlocked.CompareExchange( x.DoSetSizeTicks, newTicks, Int64.MinValue ) = Int64.MinValue then             
            x.DoSetSizeContiuetion <- fun _ -> ()
            x.Dispatcher.BeginInvoke( DispatcherPriority.Background, Action( fun _ -> x.DoSetSize( width, height ) ) ) |> ignore
        
    /// This function should be called whenever the window size has changed
    member internal x.DoSetSize( width, height ) = 
        x.Width <- width
        x.Height <- height
        x.bNewContent <- true
        x.DoScrollBarVisibility()
        x.ShowGrid() 
        x.DoSetSizeTicks := Int64.MinValue
        let cont = x.DoSetSizeContiuetion
        x.DoSetSizeContiuetion <- fun _ -> ()
        cont()
    /// Show grid 
    member x.Show() = 
        Logger.LogStackTrace( LogLevel.ExtremeVerbose )
        if x.IsVisible then 
        // Action<_> takes 1 parameter, Action() take 0 parameter. 
            Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "ShowGrid called for grid %dx%d ..." ( x.RowShown + if x.bFirstRowAlwaysShow then 1 else 0) ( x.ColShown + if x.bFirstColumnAlwaysShow then 1 else 0)))
            x.Dispatcher.BeginInvoke( DispatcherPriority.Background, Action( x.ShowGrid ) ) |> ignore
        // gd.Dispatcher.Invoke( fun _ -> gd.ShowAll() ) |> ignore
    member val private ShowGridTicks = ref Int64.MinValue with get, set
    member val private ShowGridContiuetion = x.ShowGrid with get, set
    // Transitted to UI thread
    member internal x.ShowGrid() = 
        let gd = x.Grid
        if gd.IsVisible then 
            let newTicks = (PerfDateTime.UtcNowTicks())
            if Interlocked.CompareExchange( x.ShowGridTicks, newTicks, Int64.MinValue ) = Int64.MinValue then 
                Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "ShowGrid called for grid %dx%d ..." ( x.RowShown + if x.bFirstRowAlwaysShow then 1 else 0) ( x.ColShown + if x.bFirstColumnAlwaysShow then 1 else 0)))
                if x.bNewContent then 
                    x.ValidateShowArea()
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "ShowGrid complete adjust show areas ..." ))
                    x.bNewContent <- false
                gd.Children.Clear()
                gd.RowDefinitions.Clear()
                gd.ColumnDefinitions.Clear()
                let colShown = List<int>(100)
                let rowShown = List<int>(100)
                let colDic = Dictionary<_,_>()
                let rowDic = Dictionary<_,_>()
                // Figure out column definitions. 
                if x.bFirstColumnAlwaysShow then 
                    let co = new ColumnDefinition( Width = new GridLength( x.ColWidths.[ 0 ] ) )  
                    gd.ColumnDefinitions.Add( co )
                    colShown.Add(0)
                    colDic.Add( 0, co ) 
                for col = x.ColStart to x.ColStart + x.ColShown - 1 do
                    let co = new ColumnDefinition( Width = new GridLength( x.ColWidths.[ col ] ) )  
                    gd.ColumnDefinitions.Add( co )
                    colShown.Add( col ) 
                    colDic.Add( col, co ) 
                if x.bFirstRowAlwaysShow then 
                    let ro = new RowDefinition( Height = new GridLength( x.RowHeights.[ 0 ] ) )
                    gd.RowDefinitions.Add( ro )
                    rowShown.Add(0)
                    rowDic.Add( 0, ro )
                for row = x.RowStart to x.RowStart + x.RowShown - 1 do
                    let ro = new RowDefinition( Height = new GridLength( x.RowHeights.[ row ] ) )
                    gd.RowDefinitions.Add( ro )
                    rowShown.Add(row)
                    rowDic.Add( row, ro )
                let mutable showrow = -1 
                for row in rowShown do
                    showrow <- showrow + 1
                    let mutable showcol = -1 
                    for col in colShown do
                        showcol <- showcol + 1
                        let el = x.GetUIElement (showrow, showcol ) ( row, col)
//                        let size = el.RenderSize
//                        if size.Width > colDic.Item( col ).Width.Value then 
//                            colDic.Item( col ).Width <-  new GridLength( size.Width )
//                        if size.Height > rowDic.Item( row ).Height.Value then 
//                            rowDic.Item( row ).Height <- new GridLength( size.Height )
                            
                        Grid.SetRow( el, showrow ) 
                        Grid.SetColumn( el, showcol ) 
                        gd.Children.Add( el ) |> ignore
                x.AdjustThumbSize() 
                gd.InvalidateVisual()
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "ShowGrid completed filling in primitives of %dx%d ..." rowShown.Count colShown.Count ))
                let bDoneExecution = Interlocked.CompareExchange( x.ShowGridTicks, Int64.MinValue, newTicks ) = newTicks
                x.ShowGridTicks := Int64.MinValue // Unlock
                if not bDoneExecution then 
                    let cont = x.ShowGridContiuetion
                    x.ShowGridContiuetion <- x.ShowGrid
                    cont() // Some new request comes in, reexecute
            else 
                let bCallExecute, _ = x.UseContinuationForShowGrid() 
                if bCallExecute then 
                    // Loopback
                    x.ShowGrid()
    member internal x.UseContinuationForShowGrid() = 
        let newTicks = (PerfDateTime.UtcNowTicks())
        let mutable bDone = false
        let mutable bCallExecute = false
        let mutable bUseContinuation = false
        while not bDone do 
            let oldTicks = !x.ShowGridTicks    
            if oldTicks = Int64.MinValue then 
                bCallExecute <- true
                bDone <- true
            elif newTicks < oldTicks then 
                bDone <- true
            else
                // Put in a new timestamp
                bDone <- Interlocked.CompareExchange( x.ShowGridTicks, newTicks, oldTicks ) = oldTicks
                if bDone then 
                    bUseContinuation <- true
                    Logger.LogF( LogLevel.ExtremeVerbose, ( fun _ -> sprintf "Reschedule ShowGrid for grid %dx%d ..." ( x.RowShown + if x.bFirstRowAlwaysShow then 1 else 0) ( x.ColShown + if x.bFirstColumnAlwaysShow then 1 else 0)))
        bCallExecute, bUseContinuation      
                                   
[<AllowNullLiteral>]
/// A Grid with Scrollbar, in which each element and column head of the Grid is a string 
type GridWithStringContent( nameGrid: string ) as x = 
    inherit GridWithScrollBar()
    do
        x.DoSetup() 
        x.IsVisibleChanged.Add( x.VisbilityChanged ) 

    let RowColor = [| Colors.AntiqueWhite;  Colors.LightYellow; Colors.LightBlue;Colors.LightPink; Colors.LightGreen |]
    member val internal Content = null with get, set
    member val internal Header = null with get, set
    member val internal SortedContent : string[][] = null with get, set
    member val internal SortedRow = null with get, set
    member val internal ContentVersion = ref Int64.MinValue with get, set
    member val internal SortedContentVersion = ref Int64.MinValue with get, set
    member val internal Lock = SpinLockSlim(true) with get, set
    member val internal SortBy = 0 with get, set
    member val internal IncrementSort = true with get, set
    member val internal NumRows = 0 with get, set
    member val internal NumCols = 0 with get, set
    member val internal ColWidth = null with get, set
    member val internal MaxColChars = 40 with get, set           
    member val internal TextColor = new SolidColorBrush( Colors.Black) with get, set
    member val internal TextBlockBackgrounds = RowColor |> Array.map ( fun color -> new SolidColorBrush( color ) ) with get, set
    member val internal ButtonForeGround = new SolidColorBrush( Colors.Black) with get, set
    member val internal CRowHeights = null with get, set
    member val internal CColumnWidths = null with get, set
    member internal x.HasContent() = 
        not (Utils.IsNull x.SortedContent) && x.SortedContent.Length > 0
    member val internal Selected = ConcurrentDictionary<_,_>() with get
    member internal gd.VisbilityChanged ev = 
        if gd.IsVisible && !gd.ContentVersion > Int64.MinValue then 
            gd.ToShow() 
    /// Set content and column header of the grid 
    member gd.SetContent( inputcontent: string[][], header:string[] ) = 
            Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "start to convert input data to write format for grid %s ..." nameGrid ))
            let v0 = (PerfADateTime.UtcNowTicks())
            let cl = !gd.ContentVersion
            if v0 > cl && Interlocked.CompareExchange( gd.ContentVersion, v0, cl)=cl then 
                let numrows = inputcontent.Length
                let mutable numcols = 0 
                let contentarr = Array.zeroCreate<_> numrows
                let mutable colwidth = Array.zeroCreate<_> (Math.Max( 100, header.Length + 1))
                numcols <- header.Length + 1
                for c = 0 to header.Length - 1 do 
                    if header.[c].Length > colwidth.[c] then 
                        colwidth.[c] <- header.[c].Length

                for row = 0 to numrows - 1 do 
                    let line = inputcontent.[row]
                    let col = if Utils.IsNull line then 0 else line.Length
                    if col + 1 > numcols then 
                        numcols <- col + 1
                        if numcols > 100 && numcols > colwidth.Length then 
                            let colWidthTemp = colwidth
                            colwidth <- Array.zeroCreate<_> ( gd.NumCols * 2 )
                            Array.Copy( colWidthTemp, 0, colwidth, 0, colWidthTemp.Length )
                    contentarr.[row] <- Array.zeroCreate<_> ( col + 1 )
                    if col > 0 then 
                        Array.Copy( line, 0, contentarr.[row], 1, col )
                    for c = 0 to col - 1 do 
                        if line.[c].Length > colwidth.[c] then 
                            colwidth.[c] <- line.[c].Length
                let v1 = Math.Max( (PerfDateTime.UtcNowTicks()), v0 + 1L)
                let lv = !gd.ContentVersion
                let bLockTaken = ref false
                let bContentAssigned = ref false
                gd.Lock.Enter()
                if !bLockTaken then 
                    if v1 > lv && Interlocked.CompareExchange( gd.ContentVersion, v1, lv )=lv then 
                        gd.Selected.Clear()
                        gd.Header <- header
                        gd.NumRows <- numrows
                        gd.NumCols <- numcols
                        gd.Content <- contentarr
                        gd.ColWidth <- colwidth
                        gd.CRowHeights <- Array.create ( numrows + 1 ) 20.
                        gd.CColumnWidths <- Array.zeroCreate ( numcols ) 
                        gd.CColumnWidths.[0] <- 20.
                        for c = 0 to numcols - 2 do 
                            let colwidth = 12. * float (Math.Min( colwidth.[c], x.MaxColChars ))
                            gd.CColumnWidths.[c + 1] <- colwidth
                        bContentAssigned := true
                    else
                        Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Grid %s, fail to store recent version of content when preparation job is completed ..." nameGrid ))
                    gd.Lock.Exit()
                else
                    Logger.LogF( LogLevel.Warning, ( fun _ -> sprintf "Grid %s, fail to lock when preparation job is completed ..." nameGrid ))
                if !bContentAssigned then 
                    gd.ToShow()
            else
                Logger.LogF( LogLevel.Info, ( fun _ -> sprintf "Grid %s, some process has already store a more recent version of content" nameGrid ))
    override x.GetRowHeights( ) = 
        if x.HasContent() then x.CRowHeights else [| 800. |]
    override x.GetColumnWidths() =  
        if x.HasContent() then 
            x.CColumnWidths 
        else 
            [| 1000. |]
    /// Show grid 
    member gd.ToShow( ) =
        if gd.IsVisible then 
            let GridComparer sortBy incrementSort= 
                { new IComparer< (int*string[]) > with 
                    member self.Compare(y0, z0) = 
                        let yn, y = y0
                        let zn, z = z0
                        let ycount = if Utils.IsNull y then 0 else y.Length
                        let zcount = if Utils.IsNull z then 0 else z.Length
                        if ycount > sortBy && zcount > sortBy then 
                            let yfloat = ref 0.0
                            let zfloat = ref 0.0 
                            if Double.TryParse( y.[sortBy], yfloat ) && Double.TryParse( z.[sortBy], zfloat ) then 
                                if incrementSort then 
                                    Math.Sign( (!yfloat) - (!zfloat) )
                                else
                                    Math.Sign( (!zfloat) - (!yfloat) )
                            else    
                                if incrementSort then 
                                    y.[sortBy].CompareTo( z.[sortBy] ) 
                                else
                                    z.[sortBy].CompareTo( y.[sortBy] ) 
                        elif ycount > sortBy then 
                            // For z=null
                            if incrementSort then 1 else -1 
                        elif zcount > sortBy then 
                            if incrementSort then -1 else 1 
                        else
                            0
                }
            if not (Utils.IsNull gd.Content) then 
                Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "start to sort for grid %s ..." nameGrid ))
                let v0 = (PerfADateTime.UtcNowTicks())
                let ol = !gd.SortedContentVersion
                if v0 > ol && Interlocked.CompareExchange( gd.SortedContentVersion, v0, ol) = ol then 
                    let sortNum, sortContent =
                        if gd.SortBy<=0 then 
                            Array.init gd.Content.Length ( fun i -> i ), gd.Content
                        else 
                            let presort = gd.Content |> Array.mapi ( fun i line -> i, line )
                            Array.Sort( presort, GridComparer gd.SortBy gd.IncrementSort )
                            presort |> Array.map ( fun (i, line) -> i ), presort |> Array.map ( fun (i, line) -> line)
                    gd.Lock.Enter()
                    let ol = !gd.SortedContentVersion
                    /// System.Warning? 
                    /// UtcNow may go back? Some times the statement below doesn't fire if I don't do the math.Max
                    let v1 = Math.Max( (PerfDateTime.UtcNowTicks()), v0 + 1L)
                    if v1 > ol && Interlocked.CompareExchange( gd.SortedContentVersion, v1, ol ) = ol then 
                        gd.SortedContent <- sortContent
                        gd.SortedRow <- sortNum
                    else
                        Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Grid %s, a more recent version of sorted content is available after the content has been sorted (A process enter late but finished earlier)" nameGrid ))
                    gd.Lock.Exit()
                        
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "sort is done grid %s ..." nameGrid ))
                    if gd.IsVisible then 
                        gd.UpdateContentArea()
                        gd.Show()
                else
                    Logger.LogF( LogLevel.MildVerbose, ( fun _ -> sprintf "Grid %s, a more recent version of sorted content is available " nameGrid ))
    override x.GetUIElement (showrow, showcol) (row, col ) = 
        if row = 0 && Utils.IsNotNull x.SortedContent then 
            let headertxt = if col = 0 then " " else 
                                let headerlen = if Utils.IsNull x.Header then 0 else x.Header.Length
                                if col <= headerlen then x.Header.[col-1] else "C"+col.ToString()
            let button = new Button( Content = headertxt, FontSize = 14., FontWeight = FontWeights.Normal, 
                                     Foreground = x.ButtonForeGround, VerticalAlignment = VerticalAlignment.Top )
            button.Click.Add( fun _ -> if x.SortBy = col then 
                                            x.IncrementSort <- not x.IncrementSort
                                       else
                                            x.SortBy <- col
                                       x.ToShow() )
            button :> UIElement
        else
//            let sortedContent = x.SortedContent
            if col = 0 && Utils.IsNotNull x.SortedContent then 
                let rowContent = x.SortedContent.[row-1]
                let checkBox = new CheckBox( IsChecked = Nullable(Utils.IsNotNull rowContent.[0] ) )
                                    // Toggle status
                checkBox.Click.Add( fun _ -> if Utils.IsNull rowContent.[0] then 
                                                rowContent.[0] <- ""
                                                if Utils.IsNotNull x.SortedRow && row-1 < x.SortedRow.Length then 
                                                    x.Selected.Item( x.SortedRow.[row-1] ) <- true           
                                             else
                                                rowContent.[0] <- null
                                                if Utils.IsNotNull x.SortedRow && row-1 < x.SortedRow.Length then 
                                                    x.Selected.TryRemove( x.SortedRow.[row-1]) |> ignore )
                checkBox :> UIElement
            else    
                let text = if Utils.IsNull x.SortedContent then "Waiting for data ....." else
                                if row - 1 < x.SortedContent.Length && col < x.SortedContent.[row-1].Length then 
                                    x.SortedContent.[row-1].[col]
                                else
                                    " "
                let txtBlock = new TextBlock()
                txtBlock.Text <- text
                txtBlock.FontSize <- 14.
                txtBlock.FontWeight <- FontWeights.Normal
                txtBlock.Foreground <- x.TextColor
                txtBlock.Background <- x.TextBlockBackgrounds.[ showrow % x.TextBlockBackgrounds.Length ]
                txtBlock.VerticalAlignment <- VerticalAlignment.Top
                txtBlock :> UIElement
    /// Find content tthat is being selected. 
    member x.SelectedContent() = 
        x.Selected |> Seq.choose( fun kv -> let row = kv.Key
                                            if row >=0 && row < x.Content.Length then 
                                                Some x.Content.[row]
                                            else
                                                None )
                   
