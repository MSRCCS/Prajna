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
		RecogReply.fs
  
	Description: 
		Class that defines a recognition reply

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Jan. 2015
        Revised Jan. 2016. 
 *******************************************************************/
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace Prajna.Service.Hub.Visual.Data
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// A reply
    /// </summary>
    public class RecogReply
    {
        // Request ID will not be included in the RecogReply class, but will be sent outside. 

        /// <summary>
        /// A recognition score that can be used by the aggregation service to determine which reply to send back to user. 
        /// </summary>
        [DataMember]
        [System.ComponentModel.DefaultValue(0.0)]
        public double Confidence { get; set; }

        /// <summary>
        /// A brief description of the returned result 
        /// </summary>
        [DataMember]
        public string Description { get; set; }

        /// <summary>
        /// A text description on performance information
        /// </summary>
        [DataMember]
        public string PerfInformation { get; set; }

        /// <summary>
        /// Primary recognition result
        /// </summary>
        [DataMember]
        public byte[] Result { get; set; }

        /// <summary>
        /// Auxilary data of the recognition reply, when the vHub doesn't have the relevant Guid, it will retrieve the blob
        /// in the form of byte[] from client. 
        /// </summary>
        [DataMember]
        public Guid[] AuxData { get; set; }
    }
}

namespace VMHub.Data
{
    /// <summary>
    /// A reply
    /// </summary>
    public class RecogReply : Prajna.Service.Hub.Visual.Data.RecogReply { };
}

