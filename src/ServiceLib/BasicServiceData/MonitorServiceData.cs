/********************************************************************
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
		MonitorServiceData.fs
  
	Description: 
		Class that defines the monitored status of a gateway server 

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        May. 2015
 *******************************************************************/

namespace Prajna.Service.CoreServices.Data
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Monitored Server status
    /// </summary>
    public class OneServerMonitoredStatus
    {
        /// <summary>
        /// DNS name of the host being monitored 
        /// </summary>
        [DataMember]
        public String HostName { get; set; }

        /// <summary>
        /// IP Address of the host being monitored 
        /// </summary>
        [DataMember]
        public Byte[] Address { get; set; }

        /// <summary>
        /// Port of the host being monitored
        /// </summary>
        [DataMember]
        public Int32 Port { get; set; }


        /// <summary>
        /// The period being monitored (in seconds) 
        /// </summary>
        [DataMember]
        public Int64 PeriodMonitored { get; set; }

        /// <summary>
        /// The period that the service is alive (in seconds)
        /// </summary>
        [DataMember]
        public Int64 PeriodAlive {get; set; }

        /// <summary>
        /// The period that the server (VM) is alive (in seconds)
        /// </summary>
        [DataMember]
        public Int64 GatewayAlive { get; set; }

        /// <summary>
        /// Network round trip time (in millisecond)
        /// </summary>
        [DataMember]
        public Int32 RttInMs { get; set; }

        /// <summary>
        /// Performance (in millisecond)
        /// </summary>
        [DataMember]
        public Int32 PerfInMs { get; set; }
    }
}
