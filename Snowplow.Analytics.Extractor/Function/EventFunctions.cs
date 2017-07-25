/*
 * EventFunctions.cs
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
using System;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Microsoft.Analytics.Types.Sql;

namespace Snowplow.Analytics.Extractor.Function
{

    /// <summary>
    /// EventFunctions
    ///
    /// </summary>
    public static class EventFunctions
    {
        /// <summary>
        /// GetUnstructuredEvent("json", [$e1], [$e2], ...)
        ///     1. Parse Json (once for all paths)
        ///     2. Apply the path expressions
        ///     3. Tuples are returned as MAP[path, value]
        ///             Path  = Path of resolved node (matching the expression)
        ///             Value = Node contents (of the matching nodes)
        ///   ie:
        ///     GetUnstructuredEvent(json, "id", "name")              -> field names          MAP{ {id, 1 }, {name, Ed } }
        ///     GetUnstructuredEvent(json, "$.address.zip")           -> nested fields        MAP{ {address.zip, 98052}  }
        ///     GetUnstructuredEvent(json, "$..address")              -> recursive children   MAP{ {address, 98052}, {order[0].address, 98065}, ...           }
        ///     GetUnstructuredEvent(json, "$[?(@.id > 1)].id")       -> path expression      MAP{ {id, 2 }, {order[7].id, 4}, ...                            }
        ///     GetUnstructuredEvent(json)                            -> children             MAP{ {id, 1 }, {name, Ed}, { email, donotreply@live,com }, ...  }
        /// </summary>
        public static SqlMap<string, string> GetUnstructuredEvent(string json, params string[] paths)
        {
            // Delegate
            return GetObject<string>(json, paths);
        }

        public static SqlMap<string, string> GetObject(string json, params string[] paths)
        {
            // Delegate
            return GetObject<string>(json, paths);
        }

        /// <summary/>
        private static SqlMap<string, T> GetObject<T>(string json, params string[] paths)
        {
            // Parse (once)
            //  Note: Json.Net NullRefs on <null> input Json
            //        Given <null> is a common column/string value, map to empty set for composability
            var root = string.IsNullOrEmpty(json) ? new JObject() : JToken.Parse(json);
            // Apply paths
            if (paths != null && paths.Length > 0)
            {
                return SqlMap.Create(paths.SelectMany(path => ApplyPath<T>(root, path)));
            }

            // Children
            return SqlMap.Create(ApplyPath<T>(root, null));
        }

        public static SqlArray<SqlMap<string, string>> GetContext(string json)
        {
            // Delegate
            return GetArray<SqlMap<string, string>>(json);
        }

        public static SqlArray<string> GetArray(string json)
        {
            // Delegate
            return GetArray<string>(json);
        }

        /// <summary/>
        private static SqlArray<T> GetArray<T>(string json)
        {
            // Parse (once)
            //  Note: Json.Net NullRefs on <null> input Json
            //        Given <null> is a common column/string value, map to empty set for composability
            var root = string.IsNullOrEmpty(json) ? new JObject() : JToken.Parse(json);
            // Children
            return SqlArray.Create(FetchElement<T>(root));
        }

        /// <summary/>
        private static IEnumerable<T> FetchElement<T>(JToken root)
        {
            // Children
            var children = SelectChildren<T>(root, null);
            foreach (var token in children)
            {
                // Token => T
                var value = (T)EventFunctions.ConvertToken(token, typeof(T));
                yield return value;
            }
        }

        /// <summary/>
        private static IEnumerable<KeyValuePair<string, T>> ApplyPath<T>(JToken root, string path)
        {
            // Children
            var children = SelectChildren<T>(root, path);
            foreach (var token in children)
            {
                // Token => T
                var value = (T)EventFunctions.ConvertToken(token, typeof(T));
                // Tuple(path, value)
                yield return new KeyValuePair<string, T>(token.Path, value);
            }
        }

        /// <summary/>
        private static IEnumerable<JToken> SelectChildren<T>(JToken root, string path)
        {
            // Path specified
            if (!string.IsNullOrEmpty(path))
            {
                return root.SelectTokens(path);
            }

            // Single JObject
            var o = root as JObject;
            if (o != null)
            {
                //  Note: We have to special case JObject.
                //      Since JObject.Children() => JProperty.ToString() => "{"id":1}" instead of value "1".
                return o.PropertyValues();
            }

            // Multiple JObjects
            return root.Children();
        }

        /// <summary/>
        internal static string GetTokenString(JToken token)
        {
            switch (token.Type)
            {
                case JTokenType.Null:
                case JTokenType.Undefined:
                    return null;

                case JTokenType.String:
                    return (string)token;

                case JTokenType.Integer:
                case JTokenType.Float:
                case JTokenType.Boolean:
                    // For scalars we simply delegate to Json.Net (JsonConvert) for string conversions
                    //  This ensures the string conversion matches the JsonTextWriter
                    return JsonConvert.ToString(((JValue)token).Value);

                case JTokenType.Date:
                case JTokenType.TimeSpan:
                case JTokenType.Guid:
                    // For scalars we simply delegate to Json.Net (JsonConvert) for string conversions
                    //  Note: We want to leverage JsonConvert to ensure the string conversion matches the JsonTextWriter
                    //        However that places surrounding quotes for these data types.
                    var v = JsonConvert.ToString(((JValue)token).Value);
                    return v != null && v.Length > 2 && v[0] == '"' && v[v.Length - 1] == '"' ? v.Substring(1, v.Length - 2) : v;

                default:
                    // For containers we delegate to Json.Net (JToken.ToString/WriteTo) which is capable of serializing all data types, including nested containers
                    return token.ToString(Formatting.None);
            }
        }

        /// <summary/>
        internal static object ConvertToken(JToken token, Type type)
        {
            try
            {
                if (type == typeof(string))
                {
                    return EventFunctions.GetTokenString(token);
                }
                else if (type.IsGenericType)
                {
                    if (type.GetGenericTypeDefinition() == typeof(SqlMap<,>))
                    {
                        return GetObject(token.ToString(Formatting.None));
                    }
                    else if (type.GetGenericTypeDefinition() == typeof(SqlArray<>))
                    {
                        return GetArray(token.ToString(Formatting.None));
                    }
                }

                // We simply delegate to Json.Net for data conversions
                return token.ToObject(type);
            }
            catch (Exception e)
            {
                // Make this easier to debug (with field and type context)
                //  Note: We don't expose the actual value to be converted in the error message (since it might be sensitive, information disclosure)
                throw new JsonSerializationException(
                    string.Format(typeof(JsonToken).Namespace + " failed to deserialize '{0}' from '{1}' to '{2}'", token.Path, token.Type.ToString(), type.FullName),
                    e);
            }
        }

    }
}
