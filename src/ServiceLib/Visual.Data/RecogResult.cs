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
		RecogResult.cs
  
	Description: 
		Class that defines a recognition result, including both the image 
        and region level result.

	Author:																	
 		Jin Li, Partner Research Manager
 		Microsoft Research, One Microsoft Way
 		Email: jinl at microsoft dot com

 		Lei Zhang, Senior Researcher
 		Microsoft Research, One Microsoft Way
 		Email: leizhang at microsoft dot com

    Date:
        Sep. 2015
        Revised Jan. 2016
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
    /// Recognition result that will be encapsulated into RecogReply.Result using Json
    /// serialization and sent to the client app.
    /// </summary>
    [DataContract]
    public class RecogResult
    {
        /// <summary>
        /// Class for category recognition result
        /// </summary>
        [DataContract]
        public class CategResult
        {
            /// <summary>
            /// Category name
            /// </summary>
            [DataMember]
            public string CategoryName;
            /// <summary>
            /// Confidence score for this category
            /// </summary>
            [DataMember]
            public double Confidence;
            /// <summary>
            /// Auxilliary result defined by recognizer providers, e.g. entity id
            /// </summary>
            [DataMember]
            public string AuxResult;
        }

        /// <summary>
        /// Class for rectangle
        /// </summary>
        [DataContract]
        public class Rectangle
        {
            /// <summary>
            /// The x-coordinate of the upper-left corner of the rectangle.
            /// </summary>
            [DataMember]
            public int X;
            /// <summary>
            /// The y-coordinate of the upper-left corner of the rectangle.
            /// </summary>
            [DataMember]
            public int Y;
            /// <summary>
            /// The width of the rectangle.
            /// </summary>
            [DataMember]
            public int Width;
            /// <summary>
            /// The height of the rectangle.
            /// </summary>
            [DataMember]
            public int Height;

            /// <summary>
            /// Tests whether all numeric properties of this Rectangle have values of zero.
            /// </summary>
            /// <returns>returns true if the Width, Height, X, and Y properties of this 
            /// Rectangle all have values of zero; otherwise, false.</returns>
            public bool IsEmpty()
            {
                return X == 0 && Y == 0 && Width == 0 && Height == 0;
            }
        }

        /// <summary>
        /// Rectangle of this detected region, e.g. a face bounding box
        /// 
        /// </summary>
        [DataMember]
        public Rectangle Rect;
        /// <summary>
        /// Confidence score for this rectangle
        /// </summary>
        [DataMember]
        public double Confidence;
        /// <summary>
        /// Auxilliary result for this rectangle, e.g. facial landmark points
        /// </summary>
        [DataMember]
        public string AuxResult;
        /// <summary>
        /// Top K categories recognized for this rectangle, e.g. the most possible peoples
        /// </summary>
        [DataMember]
        public CategResult[] CategoryResult;
    }
}

namespace VMHub.Data
{
    using System;
    using System.Runtime.Serialization;

    /// <summary>
    /// Recognition result that will be encapsulated into RecogReply.Result using Json
    /// serialization and sent to the client app.
    /// </summary>
    [DataContract]
    public class RecogResult : Prajna.Service.Hub.Visual.Data.RecogResult { }
}