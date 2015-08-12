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
		PersistedTab.fs
  
	Description: 
		Persisted Visual Tree when changing Tabs. 

	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com

    Date:
        Oct. 2014
	
 ---------------------------------------------------------------------------*)
namespace Prajna.WPFTools

open System
open System.Linq
open System.Collections
open System.Collections.Generic
open System.Collections.Specialized
open Prajna.Tools
open Prajna.Tools.FSharp

open System.ComponentModel
open System.Windows
open System.Windows.Data
open System.Windows.Media
open System.Windows.Controls
open System.Windows.Threading

[<AllowNullLiteral>]
/// A Customized Tab Control
type GeneralTab() = 
    inherit TabControl() 
    let mutable tabItems = List<TabItem>()
    /// Add a tab with header information 
    member x.AddTab( header, c:FrameworkElement ) =
        let newitem = new TabItem( Header = header, Content=c )
        tabItems.Add( newitem )
        x.Items.Add( newitem ) |> ignore
        ()
    /// Return a certain tab
    member x.Tab( index ) = 
        tabItems.[index]
    /// Set a certain tab as the current displying tab, update its window size 
    member x.SetActive ( index, width, height ) = 
        let item = x.Tab( index ) 
        item.InvalidateVisual( ) 
        let c = item.Content :?> FrameworkElement
        c.InvalidateVisual( )
        c.Width <- width - 50.
        c.Height <- height - 50.


[<AllowNullLiteral>]
type private PersistedTab( tab: TabControl) as x=
    static let ItemsSourceProperty = 
            DependencyProperty.RegisterAttached("ItemsSource",
                                                typeof<IEnumerable>,
                                                typeof<PersistedTab>,
                                                new UIPropertyMetadata( null,
                                                                        new PropertyChangedCallback( PersistedTab.OnItemsSourcePropertyChanged) ) )
    static let SelectedItemProperty = 
            DependencyProperty.RegisterAttached("SelectedItem",
                                                typeof<Object>,
                                                typeof<PersistedTab>,
                                                new FrameworkPropertyMetadata(null,
                                                    FrameworkPropertyMetadataOptions.BindsTwoWayByDefault,
                                                    new PropertyChangedCallback( PersistedTab.OnSelectedItemPropertyChanged ) ))
    static let ev = new Event<_,_>()
    do
        let binding = new Binding("SelectedTabItem", Source = x)
        tab.SetBinding( TabControl.SelectedItemProperty, binding ) |> ignore
        tab.Loaded.Add( x.OnTabLoaded )
    interface INotifyPropertyChanged with
        [<CLIEvent>]
        member x.PropertyChanged = 
            ev.Publish
    static member SetItemSource (parent:DependencyObject) source = 
        parent.SetValue( ItemsSourceProperty, source )
    static member GetItemSource (tab:DependencyObject ) =
        tab.GetValue( ItemsSourceProperty )
    static member SetSelectedItem ( tab: DependencyObject) source = 
        tab.SetValue( SelectedItemProperty, source )
    static member GetSelectedItem( tab: DependencyObject ) = 
        tab.GetValue( SelectedItemProperty )
    member val private ItemsSource : System.Collections.IEnumerable = null with get, set
    member val private InnerSelection : TabItem = null with get, set
    member val private TabControl = tab with get
    member x.SelectedTabItem with get() = x.InnerSelection
                              and set( value ) = 
                                        if not (Object.ReferenceEquals( value, x.InnerSelection )) then 
                                             x.InnerSelection <- value
                                             x.TabControl.SetValue( SelectedItemProperty, if Utils.IsNull value then null else value.DataContext )
    static member internal GetHandler (parent:TabControl ) = 
        let binding = parent.GetBindingExpression( TabControl.SelectedItemProperty )
        if Utils.IsNull binding then 
            PersistedTab ( parent ) 
        else
            binding.DataItem :?> PersistedTab
    static member internal OnItemsSourcePropertyChanged parent e = 
        let inst = PersistedTab.GetHandler( parent :?> TabControl )
        inst.DoItemsSourcePropertyChanged parent e
    member x.DoItemsSourcePropertyChanged parent e =
        let value = e.NewValue :?> IEnumerable
        if not(Object.ReferenceEquals( e.NewValue, (x.ItemsSource) )) then 
            if not ( Utils.IsNull x.ItemsSource ) then 
                ( x.ItemsSource :?> INotifyCollectionChanged ).CollectionChanged.RemoveHandler( new NotifyCollectionChangedEventHandler(x.OnSourceCollectionChanged) )
                x.TabControl.Items.Clear()
                x.ItemsSource <- null 
            if not (Utils.IsNull value) then 
                x.ItemsSource <- value
                ( value :?> INotifyCollectionChanged ).CollectionChanged.AddHandler( new NotifyCollectionChangedEventHandler(x.OnSourceCollectionChanged) )
    static member internal OnSelectedItemPropertyChanged parent e = 
        let inst = PersistedTab.GetHandler( parent :?> TabControl )
        inst.InnerSelection <- if Utils.IsNull e.NewValue then null else inst.TabControl.Items.Cast<TabItem>().FirstOrDefault( fun t -> e.NewValue.Equals( t.DataContext ) )
        let ch = ( inst :> INotifyPropertyChanged).PropertyChanged
        // Don't know how to do property change
        ()
    member internal x.OnTabLoaded( e ) =
        for item in x.ItemsSource do 
            x.AddTabItem( item ) 

    member internal x.OnSourceCollectionChanged sender e = 
        match e.Action with 
        | NotifyCollectionChangedAction.Add -> 
            for item in e.NewItems do 
                x.AddTabItem item
        | NotifyCollectionChangedAction.Remove -> 
            for item in e.OldItems do 
                x.RemoveTabItem item  
        | NotifyCollectionChangedAction.Replace -> 
            x.ReplaceItems e.NewItems e.OldItems
        | NotifyCollectionChangedAction.Reset -> 
            x.TabControl.Items.Clear()
        | _ -> 
            failwith (sprintf "OnSourceCollectionChanged: This action %A has not been implemented yet." e.Action )
    member x.AddTabItem item = 
        let contentControl = new ContentControl() 
        let tab = new TabItem( DataContext = item, Content = contentControl, HeaderTemplate = x.TabControl.ItemTemplate )
        contentControl.SetBinding( ContentControl.ContentProperty, new Binding()) |> ignore
        tab.SetBinding( TabItem.HeaderProperty, new Binding() ) |> ignore
        x.TabControl.Items.Add( tab ) |> ignore
    member x.RemoveTabItem item = 
        let foundItem = x.TabControl.Items.Cast<TabItem>().FirstOrDefault( fun t -> t.DataContext = item )
        if not ( Utils.IsNull foundItem ) then
            x.TabControl.Items.Remove( foundItem ) 

    member x.ReplaceItems newItems oldItems =
        let newEnum = newItems.GetEnumerator()
        let oldEnum = oldItems.GetEnumerator()
        while ( newEnum.MoveNext() && oldEnum.MoveNext() ) do
            let tab = x.TabControl.Items.Cast<TabItem>().FirstOrDefault( fun t -> t.DataContext = oldEnum.Current ) 
            if not ( Utils.IsNull tab ) then 
                tab.DataContext <- newEnum.Current

