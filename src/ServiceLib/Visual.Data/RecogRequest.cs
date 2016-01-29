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
		RecogRequest.fs
  
	Description: 
		Class that defines a recognition request

	Author:																	
 		Jin Li, Principal Researcher
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Jan. 2015
        Revised. Jan. 2016. 
 *******************************************************************/
namespace Prajna.Service.Hub.Visual.Data
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// A request 
    /// </summary>
    public class RecogRequest
    {
        // Request ID will not be included in the RecogRequest class, but will be managed outside. 

        // Service ID will also not be included in the request, it will be sent externally 

        /// <summary>
        /// Data of the recognition request, e.g., image stream
        /// </summary>
        [DataMember]
        public byte[] Data { get; set; }

        /// <summary>
        /// Auxilary data of the recognition request, e.g., media type, frame info, etc..
        /// </summary>
        [DataMember]
        public byte[] AuxData { get; set; }

    }
}

namespace VMHub.Data
{
    /// <summary>
    /// A request 
    /// </summary>
    public class RecogRequest : Prajna.Service.Hub.Visual.Data.RecogRequest { }
}