/*
 * EventExtractor.cs
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

using Microsoft.Analytics.Interfaces;
using Microsoft.Analytics.Types.Sql;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Snowplow.Analytics.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;

namespace Snowplow.Analytics.Extractor
{
    public class EventExtractor : IExtractor
    {
        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            string line;
            using (var reader = new StreamReader(input.BaseStream))
            {
                while ((line = reader.ReadLine()) != null)
                {
                    //catch SnowplowEventTransformationException and throw SnowplowEventExtractionException
                    string json = EventTransformer.Transform(line);
                    ExtractJson(json, output);
                    yield return output.AsReadOnly();
                }
            }
        }


        private void ExtractJson(string json, IUpdatableRow output)
        {
            JObject transformedEvent = JObject.Parse(json);

            var properties = transformedEvent.Properties().Select(p => p.Name).ToList();

            properties.ForEach(property => Console.WriteLine(property));

            
        }

    }
}