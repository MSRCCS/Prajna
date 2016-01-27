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
		RecognInstance.fs
  
	Description: 
		Class that defines a recognition class. 

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Jan. 2015
 *******************************************************************/
namespace Prajna.Service.Hub.Visual.Data
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Recognition Instance 
    /// </summary>
    public class RecogInstance
    {
        /// <summary>
        /// ID that identifies the recognition instance (will be used by App)
        /// </summary>
        [DataMember]
        public Guid ServiceID { get; set; }

        /// <summary>
        /// Name of the recognition instance
        /// </summary>
        [DataMember]
        public string Name { get; set; }

        /// <summary>
        /// Name of the Recognition Engine
        /// </summary>
        [DataMember]
        public string EngineName { get; set; }

        /// <summary>
        /// A Guid that defines the entity info. If the corresponding Guid doesn't exist, it will be retrieved 
        /// and returned as a byte[], we use Guid as the equality is well defined in all .Net platform. 
        /// </summary>
        [DataMember]
        public Guid[] EntityGuid { get; set; }

        /// <summary>
        /// Version of the recognizer, can be expanded to x.x.x.x format. 
        /// </summary>
        [DataMember]
        public UInt32 Version { get; set; }

        /// <summary>
        /// Parameter, any parameter to be used to define the instance. 
        /// </summary>
        [DataMember]
        public byte[] Parameter { get; set; }
    }
}

namespace VMHub.Data
{
    /// <summary>
    /// Recognition Instance, deprecated, please use Prajna.Service.Hub.Visual.Data.RecogInstance
    /// </summary>
    public class RecogInstance : Prajna.Service.Hub.Visual.Data.RecogInstance { };
}

