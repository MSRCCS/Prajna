using System;
using System.IO;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Runtime.Serialization;

using Prajna.Tools;
using Prajna.Core;
using Prajna.Service.ServiceEndpoint;
using VMHub.Data;
using VMHub.ServiceEndpoint;

using Prajna.Service.CSharp;


namespace SampleRecogServerCSharp
{
    [Serializable]
    public class SampleRecogInstanceCSharpParam : VHubBackendStartParam
    {
        public string SaveImageDirectory { get; set; }

        public static SampleRecogInstanceCSharp ConstructSampleRecogInstanceCSharp()
        {
            return new SampleRecogInstanceCSharp();
        }

    }


    public class SampleRecogInstanceCSharp : VHubBackEndInstance<SampleRecogInstanceCSharpParam>
    {
        public static SampleRecogInstanceCSharp Current { get; set; }
        public string SaveImageDirectory { get; set; }
        internal SampleRecogInstanceCSharp() :
            /// Fill in your recognizing engine name here
            base("Your Engine Name")
        {
            /// CSharp does support closure, so we have to pass the recognizing instance somewhere, 
            /// We use a static variable for this example. But be aware that this method of variable passing will hold a reference to SampleRecogInstanceCSharp
            
            SampleRecogInstanceCSharp.Current = this;
            Func<SampleRecogInstanceCSharpParam, bool> del = InitializeRecognizer;
            this.OnStartBackEnd.Add(del);
        }

        public static bool InitializeRecognizer(SampleRecogInstanceCSharpParam pa)
        {
            var bInitialized = true;
            var x = SampleRecogInstanceCSharp.Current;
            x.SaveImageDirectory = pa.SaveImageDirectory;
            if (!Object.ReferenceEquals( x, null ))
            {
                /// <remarks>
                /// To implement your own image recognizer, please obtain a connection Guid by contacting jinl@microsoft.com
                /// </remarks> 
                x.RegisterAppInfo(new Guid("843EF294-C635-42DA-9AD8-E79E82F9A357"), "0.0.0.1");
                Func<Guid, int, RecogRequest, RecogReply> del = QueryFunc;
                /// <remarks>
                /// Register your query function here. 
                /// </remarks> 
                x.RegisterClassifierCS("#dog", "dog.jpg", 100, del); 
            }
            else
            {
                bInitialized = false;
            }
            return bInitialized;
        }
        public static RecogReply QueryFunc( Guid id,int timeBudgetInMS,RecogRequest req )
        {
            var resultString = "Unknown";
            return VHubRecogResultHelper.FixedClassificationResult(resultString, resultString); 
        }
    }

    class Program
    {
        static void Main(string[] args)
        {
            var usage = @"
    Usage: Launch an intance of SampleRecogServerFSharp. The application is intended to run on any machine. It will home in to an image recognition hub gateway for service\n\
    Command line arguments: \n\
        -start       Launch a Prajna Recognition service in cluster\n\
        -stop        Stop existing Prajna Recognition service in cluster\n\
        -exe         Execute in exe mode \n\
        -cluster     CLUSTER_NAME    Name of the cluster for service launch \n\
        -port        PORT            Port used for service \n\
        -node        NODE_Name       Launch the recognizer on the node of the cluster only (note that the cluster parameter still need to be provided \n\
        -rootdir     Root_Directory  this directories holds DLLs, data model files that need to be remoted for execution\n\
        -gateway     SERVERURI       ServerUri\n\
        -only        SERVERURI       Only register with this server, disregard default. \n\
        -saveimage   DIRECTORY       Directory where recognized image is saved \n\
    ";
            var parse = new ArgumentParser(args);
            var prajnaClusterFile = parse.ParseString("-cluster", "");
            var usePort = parse.ParseInt( "-port", VHubSetting.RegisterServicePort );
            var nodeName = parse.ParseString( "-node", "" );
            var curfname = System.Diagnostics.Process.GetCurrentProcess().MainModule.FileName;
            var curdir = Directory.GetParent( curfname ).FullName;
            var upperdir = Directory.GetParent( curdir ).FullName;
            var upper2dir = Directory.GetParent( upperdir ).FullName;
            var defaultRootdir = upper2dir;
            var rootdir = parse.ParseString( "-rootdir", defaultRootdir);
            var dlldir = parse.ParseString( "-dlldir", defaultRootdir );
            var bStart = parse.ParseBoolean("-start", false);
            var bStop = parse.ParseBoolean( "-stop", false);
            var bExe = parse.ParseBoolean( "-exe", true);
            var saveImageDirectory = parse.ParseString( "-saveimage", @".");
            var gatewayServers = parse.ParseStrings( "-gateway", new string[]{});
            var onlyServers = parse.ParseStrings( "-only", new string[]{});
            var serviceName = "SampleRecognitionServiceCS";

            /// <remark> 
            /// The code below is from PrajnaRecogServer for register multiple classifier (one for each domain)
            /// You should modify the code to retrieve your own classifiers
            /// </remark>
            var bAllParsed = parse.AllParsed( usage ); 
            if (bAllParsed) 
            {
                Cluster.StartCluster(prajnaClusterFile );
                if (!(StringTools.IsNullOrEmpty( nodeName )) )
                {
                    var cluster = Cluster.GetCurrent().GetSingleNodeCluster( nodeName );
                    if (Object.ReferenceEquals(cluster,null))
                    {
                        throw new Exception( "Can't find node in cluster" );
                    }
                    else
                        Cluster.SetCurrent( cluster );
                }
            }

            if ( bExe )
            {
                JobDependencies.DefaultTypeOfJobMask = JobTaskKind.ApplicationMask;
            }

            var curJob = JobDependencies.setCurrentJob( "SampleRecogServerCSharp" );

            if (bStart)
            {
            // add other file dependencies
            // allow mapping local to different location in remote in case user specifies different model files
                    var exeName = System.Reflection.Assembly.GetExecutingAssembly().Location;
                    var exePath = Path.GetDirectoryName( exeName );
                    
                    /// If you need to change the current directory at remote, uncomment and set the following variable  the following                         
//                    curJob.EnvVars.Add(DeploymentSettings.EnvStringSetJobDirectory, "." )
                    /// All data files are placed under rootdir locally, and will be placed at root (relative directory at remote ).
                    var dirAtRemote = curJob.AddDataDirectoryWithPrefix( null, rootdir, "root", "*", SearchOption.AllDirectories );
                    /// Add all 
                    var dlls = curJob.AddDataDirectory( dlldir, "*", SearchOption.AllDirectories );
                    var managedDlls = curJob.AddDataDirectoryWithPrefix( null, exePath, "dependencies", "*", SearchOption.AllDirectories );
                    var startParam = new SampleRecogInstanceCSharpParam { SaveImageDirectory = saveImageDirectory } ;
                    /// Add traffic manager gateway, see http://azure.microsoft.com/en-us/services/traffic-manager/, 
                    /// Gateway that is added as traffic manager will be repeatedly resovled via DNS resolve
                    foreach ( var gatewayServer in gatewayServers )
                    {
                        if (!(StringTools.IsNullOrEmpty( gatewayServer)) ) 
                            startParam.AddOneTrafficManager( gatewayServer, usePort  );
                    };
                    /// Add simple gateway, no repeated DNS service. 
                    foreach ( var onlyServer in onlyServers )
                    {
                        if (!(StringTools.IsNullOrEmpty( onlyServer)) )
                            startParam.AddOneServer( onlyServer, usePort );
                    }

                    
                    var curCluster = Cluster.GetCurrent();
                    if (Object.ReferenceEquals(curCluster, null))
                    {
                        
                        // If no cluster parameter, start a local instance. 
                        RemoteInstance.StartLocal(serviceName, startParam, SampleRecogInstanceCSharpParam.ConstructSampleRecogInstanceCSharp );

                        while (RemoteInstance.IsRunningLocal(serviceName))
                        {
                            if (Console.KeyAvailable)
                            {
                                var cki = Console.ReadKey(true);
                                if (cki.Key == ConsoleKey.Enter)
                                {
                                    RemoteInstance.StopLocal(serviceName);
                                }
                                else
                                {
                                    System.Threading.Thread.Sleep(10);
                                }
                            }
                        }
                    }
                    else
                        // If there is cluster parameter, start remote recognition cluster. 
                        RemoteInstance.Start(serviceName, startParam, SampleRecogInstanceCSharpParam.ConstructSampleRecogInstanceCSharp);
            }
            else if (bStop)
            {
                RemoteInstance.Stop(serviceName);
            }
        }
    }
}
