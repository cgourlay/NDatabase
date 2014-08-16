﻿using System.Collections.Generic;
using NDatabase.Tool;
using NUnit.Framework;

namespace NDatabase.UnitTests.Utils
{
    public class Test_list_extensions
    {
        [Test]
        public void It_should_return_false_when_list_is_null()
        {
            IList<string> nullList = null;
            Assert.That(() => nullList.IsNullOrEmpty(), Is.True);
        }
    }
}