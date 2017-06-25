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
using Newtonsoft.Json.Linq;
using Snowplow.Analytics.Exceptions;
using Snowplow.Analytics.Extractor.Exceptions;
using Snowplow.Analytics.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Snowplow.Analytics.Extractor
{
    public class EventExtractor : IExtractor
    {
        enum FieldTypes
        {
            Property_Boolean = 0,
            Property_Int32 = 1,
            Property_Double = 2,
            Property_DateTime = 3,
            Property_String = 4,
            Property_SqlArray = 5,
            Property_SqlMap = 6
        }

        private static readonly Dictionary<string, FieldTypes>
            ENRICHED_EVENT_FIELD_TYPES = new Dictionary<string, FieldTypes>()
            {
                {"app_id", FieldTypes.Property_String},
                {"platform", FieldTypes.Property_String},
                {"etl_tstamp", FieldTypes.Property_DateTime},
                {"collector_tstamp", FieldTypes.Property_DateTime},
                {"dvce_created_tstamp", FieldTypes.Property_DateTime},
                {"event", FieldTypes.Property_String},
                {"event_id", FieldTypes.Property_String},
                {"txn_id", FieldTypes.Property_Int32},
                {"name_tracker", FieldTypes.Property_String},
                {"v_tracker", FieldTypes.Property_String},
                {"v_collector", FieldTypes.Property_String},
                {"v_etl", FieldTypes.Property_String},
                {"user_id", FieldTypes.Property_String},
                {"user_ipaddress", FieldTypes.Property_String},
                {"user_fingerprint", FieldTypes.Property_String},
                {"domain_userid", FieldTypes.Property_String},
                {"domain_sessionidx", FieldTypes.Property_Int32},
                {"network_userid", FieldTypes.Property_String},
                {"geo_country", FieldTypes.Property_String},
                {"geo_region", FieldTypes.Property_String},
                {"geo_city", FieldTypes.Property_String},
                {"geo_zipcode", FieldTypes.Property_String},
                {"geo_location", FieldTypes.Property_String},
                {"geo_latitude", FieldTypes.Property_Double},
                {"geo_longitude", FieldTypes.Property_Double},
                {"geo_region_name", FieldTypes.Property_String},
                {"ip_isp", FieldTypes.Property_String},
                {"ip_organization", FieldTypes.Property_String},
                {"ip_domain", FieldTypes.Property_String},
                {"ip_netspeed", FieldTypes.Property_String},
                {"page_url", FieldTypes.Property_String},
                {"page_title", FieldTypes.Property_String},
                {"page_referrer", FieldTypes.Property_String},
                {"page_urlscheme", FieldTypes.Property_String},
                {"page_urlhost", FieldTypes.Property_String},
                {"page_urlport", FieldTypes.Property_Int32},
                {"page_urlpath", FieldTypes.Property_String},
                {"page_urlquery", FieldTypes.Property_String},
                {"page_urlfragment", FieldTypes.Property_String},
                {"refr_urlscheme", FieldTypes.Property_String},
                {"refr_urlhost", FieldTypes.Property_String},
                {"refr_urlport", FieldTypes.Property_Int32},
                {"refr_urlpath", FieldTypes.Property_String},
                {"refr_urlquery", FieldTypes.Property_String},
                {"refr_urlfragment", FieldTypes.Property_String},
                {"refr_medium", FieldTypes.Property_String},
                {"refr_source", FieldTypes.Property_String},
                {"refr_term", FieldTypes.Property_String},
                {"mkt_medium", FieldTypes.Property_String},
                {"mkt_source", FieldTypes.Property_String},
                {"mkt_term", FieldTypes.Property_String},
                {"mkt_content", FieldTypes.Property_String},
                {"mkt_campaign", FieldTypes.Property_String},
                {"contexts", FieldTypes.Property_SqlArray},
                {"se_category", FieldTypes.Property_String},
                {"se_action", FieldTypes.Property_String},
                {"se_label", FieldTypes.Property_String},
                {"se_property", FieldTypes.Property_String},
                {"se_value", FieldTypes.Property_String},
                {"unstruct_event", FieldTypes.Property_SqlMap},
                {"tr_orderid", FieldTypes.Property_String},
                {"tr_affiliation", FieldTypes.Property_String},
                {"tr_total", FieldTypes.Property_Double},
                {"tr_tax", FieldTypes.Property_Double},
                {"tr_shipping", FieldTypes.Property_Double},
                {"tr_city", FieldTypes.Property_String},
                {"tr_state", FieldTypes.Property_String},
                {"tr_country", FieldTypes.Property_String},
                {"ti_orderid", FieldTypes.Property_String},
                {"ti_sku", FieldTypes.Property_String},
                {"ti_name", FieldTypes.Property_String},
                {"ti_category", FieldTypes.Property_String},
                {"ti_price", FieldTypes.Property_Double},
                {"ti_quantity", FieldTypes.Property_Int32},
                {"pp_xoffset_min", FieldTypes.Property_Int32},
                {"pp_xoffset_max", FieldTypes.Property_Int32},
                {"pp_yoffset_min", FieldTypes.Property_Int32},
                {"pp_yoffset_max", FieldTypes.Property_Int32},
                {"useragent", FieldTypes.Property_String},
                {"br_name", FieldTypes.Property_String},
                {"br_family", FieldTypes.Property_String},
                {"br_version", FieldTypes.Property_String},
                {"br_type", FieldTypes.Property_String},
                {"br_renderengine", FieldTypes.Property_String},
                {"br_lang", FieldTypes.Property_String},
                {"br_features_pdf", FieldTypes.Property_Boolean},
                {"br_features_flash", FieldTypes.Property_Boolean},
                {"br_features_java", FieldTypes.Property_Boolean},
                {"br_features_director", FieldTypes.Property_Boolean},
                {"br_features_quicktime", FieldTypes.Property_Boolean},
                {"br_features_realplayer", FieldTypes.Property_Boolean},
                {"br_features_windowsmedia", FieldTypes.Property_Boolean},
                {"br_features_gears", FieldTypes.Property_Boolean},
                {"br_features_silverlight", FieldTypes.Property_Boolean},
                {"br_cookies", FieldTypes.Property_Boolean},
                {"br_colordepth", FieldTypes.Property_String},
                {"br_viewwidth", FieldTypes.Property_Int32},
                {"br_viewheight", FieldTypes.Property_Int32},
                {"os_name", FieldTypes.Property_String},
                {"os_family", FieldTypes.Property_String},
                {"os_manufacturer", FieldTypes.Property_String},
                {"os_timezone", FieldTypes.Property_String},
                {"dvce_type", FieldTypes.Property_String},
                {"dvce_ismobile", FieldTypes.Property_Boolean},
                {"dvce_screenwidth", FieldTypes.Property_Int32},
                {"dvce_screenheight", FieldTypes.Property_Int32},
                {"doc_charset", FieldTypes.Property_String},
                {"doc_width", FieldTypes.Property_Int32},
                {"doc_height", FieldTypes.Property_Int32},
                {"tr_currency", FieldTypes.Property_String},
                {"tr_total_base", FieldTypes.Property_Double},
                {"tr_tax_base", FieldTypes.Property_Double},
                {"tr_shipping_base", FieldTypes.Property_Double},
                {"ti_currency", FieldTypes.Property_String},
                {"ti_price_base", FieldTypes.Property_Double},
                {"base_currency", FieldTypes.Property_String},
                {"geo_timezone", FieldTypes.Property_String},
                {"mkt_clickid", FieldTypes.Property_String},
                {"mkt_network", FieldTypes.Property_String},
                {"etl_tags", FieldTypes.Property_String},
                {"dvce_sent_tstamp", FieldTypes.Property_DateTime},
                {"refr_domain_userid", FieldTypes.Property_String},
                {"refr_device_tstamp", FieldTypes.Property_DateTime},
                {"derived_contexts", FieldTypes.Property_SqlArray},
                {"domain_sessionid", FieldTypes.Property_String},
                {"derived_tstamp", FieldTypes.Property_DateTime},
                {"event_vendor", FieldTypes.Property_String},
                {"event_name", FieldTypes.Property_String},
                {"event_format", FieldTypes.Property_String},
                {"event_version", FieldTypes.Property_String},
                {"event_fingerprint", FieldTypes.Property_String},
                {"true_tstamp", FieldTypes.Property_DateTime}
            };

        public override IEnumerable<IRow> Extract(IUnstructuredReader input, IUpdatableRow output)
        {
            string line;
            using (var reader = new StreamReader(input.BaseStream))
            {
                while ((line = reader.ReadLine()) != null)
                {

                    //check for schema
                    var totalCount = output.Schema.Count;
                    var errors = new List<string>();
                    for (int i = 0; i < totalCount; i++)
                    {
                        var columnName = output.Schema[i].Name;
                        var actualColumnType = output.Schema[i].Type;
                        FieldTypes expectedColumnType;

                        if (ENRICHED_EVENT_FIELD_TYPES.TryGetValue(columnName, out expectedColumnType))
                        {
                            switch (expectedColumnType)
                            {
                                case FieldTypes.Property_Boolean:
                                    if (actualColumnType != typeof(bool?))
                                    {
                                        errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(bool?)}");
                                    }
                                    break;
                                case FieldTypes.Property_Int32:
                                    if (actualColumnType != typeof(int?))
                                    {
                                        errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(int?)}");
                                    }
                                    break;
                                case FieldTypes.Property_Double:
                                    if (actualColumnType != typeof(double?))
                                    {
                                        errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(double?)}");
                                    }
                                    break;
                                case FieldTypes.Property_DateTime:
                                    if (actualColumnType != typeof(DateTime?))
                                    {
                                        errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(DateTime?)}");
                                    }
                                    break;
                                case FieldTypes.Property_String:
                                    if (actualColumnType != typeof(string))
                                    {
                                        errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(string)}");
                                    }
                                    break;
                                case FieldTypes.Property_SqlArray:
                                    if (actualColumnType != typeof(SqlArray<object>))
                                    {
                                        errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(SqlArray<object>)}");
                                    }
                                    break;
                                case FieldTypes.Property_SqlMap:
                                    if (actualColumnType != typeof(SqlMap<string, object>))
                                    {
                                        errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(SqlMap<string, object>)}");
                                    }
                                    break;
                                default:
                                    errors.Add($"Invalid columnName {columnName}");
                                    break;
                            }
                        }
                        else
                        {
                            var contextKey = "contexts";
                            var unstructKey = "unstruct";
                            if (string.Compare(contextKey, columnName.Substring(0, contextKey.Length)) == 0)
                            {
                                if (actualColumnType != typeof(SqlArray<SqlMap<string, object>>))
                                {
                                    errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(SqlArray<SqlMap<string, object>>)}");
                                }

                            }
                            else if (string.Compare(unstructKey, columnName.Substring(0, unstructKey.Length)) == 0)
                            {
                                if (actualColumnType != typeof(SqlMap<string, object>))
                                {
                                    errors.Add($"Invalid columnType {actualColumnType} for columnName {columnName}; expected columnType: {typeof(SqlMap<string, object>)}");
                                }
                            }
                            else
                            {
                                errors.Add($"Invalid columnName {columnName}");
                            }

                        }

                    }

                    if (errors.Count() > 0)
                    {
                        throw new SnowplowEventExtractionException(errors);
                    }


                    try
                    {
                        string json = EventTransformer.Transform(line);
                        ExtractJson(json, output);
                    }
                    catch (SnowplowEventTransformationException sete)
                    {
                        throw new SnowplowEventExtractionException(sete.ErrorMessages);
                    }
                    yield return output.AsReadOnly();
                }
            }
        }


        private static void ExtractJson(string json, IUpdatableRow output)
        {
            JObject transformedEvent = JObject.Parse(json);
            var properties = transformedEvent.Properties().Select(p => p.Name).ToList();
            properties.ForEach(property =>
            {
                FieldTypes fieldType;
                var propertyValue = transformedEvent[property];
                if (ENRICHED_EVENT_FIELD_TYPES.TryGetValue(property, out fieldType))
                {
                    switch (fieldType)
                    {
                        case FieldTypes.Property_Boolean:
                            output.Set(property, (bool?)propertyValue);
                            break;

                        case FieldTypes.Property_Double:
                            output.Set(property, (double?)propertyValue);
                            break;

                        case FieldTypes.Property_Int32:
                            output.Set(property, (int?)propertyValue);
                            break;

                        case FieldTypes.Property_DateTime:
                            output.Set(property, (DateTime?)propertyValue);
                            break;

                        case FieldTypes.Property_String:
                            output.Set(property, (string)propertyValue);
                            break;
                    }
                }
                else
                {
                    //no need to check for null as contexts and unstruct_event won't be added to transformedEvent if they are null
                    //we have encountered contexts or unstruct_event
                    var type = propertyValue.Type;
                    if (type == JTokenType.Array)
                    {
                        //we have encountered contexts
                        //context is an array of Objects
                        var mapList = new List<SqlMap<string, object>>();
                        foreach (var obj in propertyValue)
                        {
                            //convert JObject to dictionary
                            var dict = ParseObject(new Dictionary<string, object>(), (JObject)obj);
                            var map = new SqlMap<string, object>(dict);
                            mapList.Add(map);
                        }
                        var contextArray = new SqlArray<SqlMap<string, object>>(mapList);
                        output.Set(property, contextArray);
                    }
                    else if (type == JTokenType.Object)
                    {
                        //we have encountered unstruct_event
                        var unstructDict = ParseObject(new Dictionary<string, object>(), (JObject)propertyValue);
                        var unstructObj = new SqlMap<string, object>(unstructDict);
                        output.Set(property, unstructObj);
                    }
                    else
                    {
                        //didn't expect this value
                        throw new SnowplowEventExtractionException($"Unexpected value {transformedEvent[property]} encountered for field {property}");
                    }
                }
            });

        }

        private static Dictionary<string, object> ParseObject(Dictionary<string, object> dict, JObject obj)
        {
            var keys = obj.Properties().Select(p => p.Name).ToList();
            keys.ForEach(key =>
            {
                var value = obj[key];
                var type = value.Type;
                if (type == JTokenType.Object)
                {
                    //if it is an object; recurse again
                    var parseDict = ParseObject(new Dictionary<string, object>(), (JObject)value);
                    //convert dict to SqlMap
                    var sqlMap = new SqlMap<string, object>(parseDict);
                    dict[key] = sqlMap;
                }
                else if (type == JTokenType.Array)
                {
                    var parseList = ParseArray(new List<object>(), (JArray)value);
                    var sqlArray = new SqlArray<object>(parseList);
                    dict[key] = sqlArray;
                }
                else
                {
                    switch (type)
                    {
                        case JTokenType.Boolean:
                            dict[key] = (bool?)value;
                            break;

                        case JTokenType.Float:
                            dict[key] = (double?)value;
                            break;

                        case JTokenType.Integer:
                            dict[key] = (long?)value;
                            break;

                        case JTokenType.Date:
                            dict[key] = (DateTime?)value;
                            break;

                        case JTokenType.String:
                        default:
                            dict[key] = (string)value;
                            break;
                    }
                }
            });

            return dict;
        }

        private static List<object> ParseArray(List<object> list, JArray array)
        {
            foreach (var element in array)
            {
                var type = element.Type;
                if (type == JTokenType.Object)
                {
                    //if it is an object; recurse again
                    var parseDict = ParseObject(new Dictionary<string, object>(), (JObject)element);
                    //convert dict to SqlMap
                    var sqlMap = new SqlMap<string, object>(parseDict);
                    list.Add(sqlMap);
                }
                else if (type == JTokenType.Array)
                {
                    var parseList = ParseArray(new List<object>(), (JArray)element);
                    var sqlArray = new SqlArray<object>(parseList);
                    list.Add(sqlArray);
                }
                else
                {
                    switch (type)
                    {
                        case JTokenType.Boolean:
                            list.Add((bool?)element);
                            break;

                        case JTokenType.Float:
                            list.Add((double?)element);
                            break;

                        case JTokenType.Integer:
                            list.Add((long?)element);
                            break;

                        case JTokenType.Date:
                            list.Add((DateTime?)element);
                            break;

                        case JTokenType.String:
                        default:
                            list.Add((string)element);
                            break;
                    }
                }
            }
            return list;
        }

    }
}