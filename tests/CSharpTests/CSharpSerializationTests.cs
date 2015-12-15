using System;
using System.Linq;
using System.Collections.Generic;
using System.IO;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using Prajna.Core;
using Prajna.Tools;
using Prajna.Tools.Tests;

namespace CSharpTests
{
    [TestClass]
    public class CSharpSerializationTests
    {
        [TestMethod]
        public void CircularCSharpClosure()
        {
            object[] arr = new object[] { 1, 2, 3 };
            Func<object[]> getArray = () => arr;
            arr[2] = getArray;
            foreach (var streamConstructors in SerializationTests.MemoryStreamConstructors)
            {
                var otherGetArray = (Func<object[]>) SerializationTests.roundTrip(streamConstructors.Item1, streamConstructors.Item2, getArray);
                var otherGetArrayCircularRef = (Func<object[]>)(otherGetArray()[2]);
                Assert.AreEqual(getArray()[0], otherGetArray()[0]);
                Assert.AreEqual(getArray()[0], otherGetArrayCircularRef()[0]);
                Assert.AreSame(otherGetArray(), otherGetArrayCircularRef());
            }
        }
    }
}
