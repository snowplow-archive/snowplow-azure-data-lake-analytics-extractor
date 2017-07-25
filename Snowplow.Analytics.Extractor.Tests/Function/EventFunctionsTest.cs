/*
 * EventFunctionsTest.cs
 * 
 * Copyright (c) 2017 Snowplow Analytics Ltd. All rights reserved.
 * This program is licensed to you under the Apache License Version 2.0,
 * and you may not use this file except in compliance with the Apache License
 * Version 2.0. You may obtain a copy of the Apache License Version 2.0 at
 * http://www.apache.org/licenses/LICENSE-2.0.
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the Apache License Version 2.0 is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the Apache License Version 2.0 for the specific
 * language governing permissions and limitations there under.
 * Authors: Devesh Shetty
 * Copyright: Copyright (c) 2017 Snowplow Analytics Ltd
 * License: Apache License Version 2.0
 */

using Microsoft.VisualStudio.TestTools.UnitTesting;
using Snowplow.Analytics.Extractor.Function;
using System.Collections.Generic;
using System.Linq;

namespace Snowplow.Analytics.Extractor.Tests.Function
{
    [TestClass]
    public class EventFunctionsTest
    {
        [TestMethod]
        public void TestGetUnstructuredEvent()
        {
            var expectedDict =
                new Dictionary<string, string>() {
                  {"targetUrl", "http://www.example.com"},
                  {"elementClasses", "[\"foreground\"]"},
                  {"elementId", "exampleLink"},
                  {"boolField", "true" },
                  {"intField", "1" },
                  {"floatField", "20.2" }
                };
            var inputJson = @"{
                  'targetUrl' : 'http://www.example.com',
                  'elementClasses' : [ 'foreground' ],
                  'elementId' : 'exampleLink',
                  'boolField' : true,
                  'intField' : 1,
                  'floatField' : 20.2
                }";
            var actualMap = EventFunctions.GetUnstructuredEvent(inputJson);
            expectedDict.ToList().ForEach(pair => {
                var expected = pair.Value;
                var actual = actualMap[pair.Key];
                Assert.AreEqual(expected, actual);
            });
        }

        [TestMethod]
        public void TestGetContext()
        {
            var dict =
                new Dictionary<string, string>() { 
                  {"genre", "blog"},
                  {"inLanguage", "en-US"},
                  {"datePublished", "2014-11-06T00:00:00Z"},
                  {"author", "Devesh Shetty"},
                  {"breadcrumb", "[\"blog\",\"releases\"]" },
                  {"keywords", "[\"snowplow\",\"javascript\",\"tracker\",\"event\"]" }
                };
            var inputJson = @"[ {
                  'genre' : 'blog',
                  'inLanguage' : 'en-US',
                  'datePublished' : '2014-11-06T00:00:00Z',
                  'author' : 'Devesh Shetty',
                  'breadcrumb' : ['blog', 'releases'],
                  'keywords' : [ 'snowplow', 'javascript', 'tracker', 'event' ]
                } ]";
            var actualMap = EventFunctions.GetContext(inputJson)[0];
            dict.ToList().ForEach(pair => {
                var expected = pair.Value;
                var actual = actualMap[pair.Key];
                Assert.AreEqual(expected, actual);
            });
        }

        [TestMethod]
        public void TestGetObject()
        {
            var dict =
                new Dictionary<string, string>() {
                  {"genre", "blog"},
                  {"inLanguage", "en-US"},
                  {"datePublished", "2014-11-06T00:00:00Z"},
                  {"author", "Devesh Shetty"},
                  {"breadcrumb", "[\"blog\",\"releases\"]" },
                  {"keywords", "[\"snowplow\",\"javascript\",\"tracker\",\"event\"]" }
                };
            var inputJson = @"{
                  'genre' : 'blog',
                  'inLanguage' : 'en-US',
                  'datePublished' : '2014-11-06T00:00:00Z',
                  'author' : 'Devesh Shetty',
                  'breadcrumb' : ['blog', 'releases'],
                  'keywords' : [ 'snowplow', 'javascript', 'tracker', 'event' ]
                }";
            var actualMap = EventFunctions.GetObject(inputJson);
            dict.ToList().ForEach(pair => {
                var expected = pair.Value;
                var actual = actualMap[pair.Key];
                Assert.AreEqual(expected, actual);
            });
        }

        [TestMethod]
        public void TestGetArray()
        {
            var expectedArray = new string[] { "hello", "world", "test", "precise" };
            var inputJson = @"['hello', 'world', 'test', 'precise']";
            var actual = EventFunctions.GetArray(inputJson);
            Assert.AreEqual(expectedArray.Length, actual.Count);
            for (int i = 0; i < expectedArray.Length; i++)
            {
                Assert.AreEqual(expectedArray[i], actual[i]);
            }
        }

        [TestMethod]
        public void TestMultipleContextElements()
        {
            var expectedDict =
                new Dictionary<string, string>() {
                  {"genre", "blog"},
                  {"inLanguage", "en-US"},
                  {"datePublished", "2014-11-06T00:00:00Z"},
                  {"author", "Devesh Shetty"},
                  {"breadcrumb", "[\"blog\",\"releases\"]" },
                  {"keywords", "[\"snowplow\",\"javascript\",\"tracker\",\"event\"]" }
                };
            var inputJson = @"[ {
                  'genre' : 'blog',
                  'inLanguage' : 'en-US',
                  'datePublished' : '2014-11-06T00:00:00Z',
                  'author' : 'Devesh Shetty',
                  'breadcrumb' : ['blog', 'releases'],
                  'keywords' : [ 'snowplow', 'javascript', 'tracker', 'event' ]
                },
                {
                  'targetUrl' : 'http://www.example.com',
                  'elementClasses' : [ 'foreground' ],
                  'elementId' : 'exampleLink'
                }
                ]";
            var result = EventFunctions.GetContext(inputJson);
            var actualMap = result[0]; 
            expectedDict.ToList().ForEach(pair => {
                var expected = pair.Value;
                var actual = actualMap[pair.Key];
                Assert.AreEqual(expected, actual);
            });

            expectedDict =
                new Dictionary<string, string>() {
                  {"targetUrl", "http://www.example.com"},
                  {"elementClasses", "[\"foreground\"]"},
                  {"elementId", "exampleLink"}
                };
            actualMap = result[1];
            expectedDict.ToList().ForEach(pair => {
                var expected = pair.Value;
                var actual = actualMap[pair.Key];
                Assert.AreEqual(expected, actual);
            });
        }



    }
}
