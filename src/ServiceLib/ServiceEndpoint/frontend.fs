(*---------------------------------------------------------------------------
	Copyright 2013, Microsoft.	All rights reserved                                                      

	File: 
		frontend.fs
  
	Description: 
		Front end distributed gateway service. 

	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl@microsoft.com, Tel. (425) 703-8451
    Date:
        Jan. 2015
	
 ---------------------------------------------------------------------------*)
namespace Prajna.QueryService

open System
open System.Collections.Concurrent
open Prajna
open Tools
open Tools.FTrace

/// <summary>
/// This portion of the front end service will not be serialized during service Launch
/// </summary> 
[<AllowNullLiteral; AbstractClass>]
type FrontEndInstanceBasic() =
    /// <summary>
    /// User need to override this function to construct a fully instantiated front end instance class. 
    /// </summary>
    abstract WaitForCloseAll: unit -> unit
    abstract Run: unit -> unit 

/// <summary>
/// For a user to use Prajna.QueryService service, the developer needs to extend FrontEndInstance class, 
/// to implement the missing functions
/// </summary> 
[<AllowNullLiteral; AbstractClass>]
type FrontEndInstance<'Request, 'Reply, 'ServiceInstance>() = 
    inherit FrontEndInstanceBasic() 
    member val ServiceCollections = ConcurrentDictionary<_,_>() with get
    abstract OnStart : unit -> unit 
    override x.WaitForCloseAll() =
        ()
    override x.Run()=
        ()
    
/// <summary>
/// This class contains the parameter used to start the front end service. The class (and if you have derived from the class, any additional information) 
/// will be serialized and send over the network to be executed on Prajna platform. 
/// </summary> 
[<AllowNullLiteral>]
type FrontEndServiceParam() =
    /// <summary> 
    /// Number of threads of that runs the front end query distribution service. 
    /// </summary>
    member val NumThreads = 2 with get, set
    /// <summary>
    /// User will need to override this function to instantiate the right front end instance to be used for service. 
    /// </summary>
    member val NewInstanceFunc : unit -> FrontEndInstanceBasic = fun () -> null with get, set

/// <summary>
/// The code abstract a front end distributed gateway service. The front end gateway is designed to be running on a public facing machine (e.g., in an Azure VM, 
/// in a VM in Amazon Web Service, in a machine with DTAP access, etc.) During its operation, the functions are:
/// 1. Connected by backend service to provide query service. 
/// 2. Connected by user (e.g., through Web API) for submitting query. 
/// The front end service manages the request, select 1 or more backend service to service the request, and aggregate the request to return to the user.
/// </summary> 
type FrontEndService<'StartParamType when 'StartParamType :> FrontEndServiceParam and 'StartParamType: null >() =
    inherit PrajnaRoleEntryPointGeneric<'StartParamType>()
    [<field:NonSerialized>]
    let mutable serviceInternal = null
    [<field:NonSerialized>]
    let mutable paramInternal = null
    [<field:NonSerialized>]
    member val bTerminate = false with get, set
    override x.OnStart (param) = 
        paramInternal <- param
        serviceInternal <- null
        x.bTerminate <- Utils.IsNull paramInternal
        not x.bTerminate
    override x.Run() = 
        try
            serviceInternal <- paramInternal.NewInstanceFunc()
        with
        | e -> 
            serviceInternal <- null
            Logger.LogF( LogLevel.Error, ( fun _ -> sprintf "!!! Unexpected exception !!! Message: %s, %A" e.Message e ))
            x.bTerminate <- true            
    override x.OnStop() = 
        // Cancel all pending jobs. 
        if Utils.IsNotNull serviceInternal then 
            serviceInternal.WaitForCloseAll()
        x.bTerminate <- true
    override x.IsRunning() = 
        not x.bTerminate


