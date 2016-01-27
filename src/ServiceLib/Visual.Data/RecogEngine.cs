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
		RecogEngine.fs
  
	Description: 
		Recognition Engie class structure

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com
    Date:
        Jan. 2015
        Revised Jan. 2016
 *******************************************************************/
namespace Prajna.Service.Hub.Visual.Data
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Recognition Engine 
    /// </summary>
    public class RecogEngine
    {
        /// <summary>
        /// ID of the recognition engine, we are going to use a byte array, 
        /// which is more general than a Guid. 
        /// </summary>
        [DataMember]
        public Guid RecogEngineID { get; set; }

        /// <summary>
        /// Name of the recognition engine.
        /// </summary>
        [DataMember]
        public string RecogEngineName { get; set; }

        /// <summary>
        /// Name of the instituion that provides the recognition engine.
        /// </summary>
        [DataMember]
        public string InstituteName { get; set; }

        /// <summary>
        /// Name of the organization that provides the recognition engine.
        /// </summary>
        [DataMember]
        public string InstituteURL { get; set; }

        /// <summary>
        /// Contact of the organization that provides the recognition service
        /// </summary>
        [DataMember]
        public string ContactEmail { get; set; }
    }
}

namespace VMHub.Data
{
    /// <summary>
    /// Recognition Engine, deprecated, please use Prajna.Service.Hub.Visual.Data.RecogEngine
    /// </summary>
    public class RecogEngine : Prajna.Service.Hub.Visual.Data.RecogEngine { };
}
