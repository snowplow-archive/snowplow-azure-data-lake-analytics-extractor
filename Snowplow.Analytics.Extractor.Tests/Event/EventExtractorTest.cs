/*
 * EventExtractorTest.cs
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
using Microsoft.Analytics.UnitTest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Newtonsoft.Json.Linq;
using Newtonsoft.Json;

namespace Snowplow.Analytics.Extractor.Tests
{
    [TestClass]
    public class EventExtractorTest
    {
        private static readonly string _unstructJson = "{\n    'schema': 'iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0',\n    'data': {\n      'schema': 'iglu:com.snowplowanalytics.snowplow/link_click/jsonschema/1-0-1',\n      'data': {\n        'targetUrl': 'http://www.example.com',\n        'elementClasses': ['foreground'],\n        'elementId': 'exampleLink'\n      }\n    }\n  }";

        private static readonly string _contextsJson = "{\n    'schema': 'iglu:com.snowplowanalytics.snowplow/contexts/jsonschema/1-0-0',\n    'data': [\n      {\n        'schema': 'iglu:org.schema/WebPage/jsonschema/1-0-0',\n        'data': {\n          'genre': 'blog',\n          'inLanguage': 'en-US',\n          'datePublished': '2014-11-06T00:00:00Z',\n          'author': 'Devesh Shetty',\n          'breadcrumb': [\n            'blog',\n            'releases'\n          ],\n          'keywords': [\n            'snowplow',\n            'javascript',\n            'tracker',\n            'event'\n          ]\n        }\n      },\n      {\n        'schema': 'iglu:org.w3/PerformanceTiming/jsonschema/1-0-0',\n        'data': {\n          'navigationStart': 1415358089861,\n          'unloadEventStart': 1415358090270,\n          'unloadEventEnd': 1415358090287,\n          'redirectStart': 0,\n          'redirectEnd': 0,\n          'fetchStart': 1415358089870,\n          'domainLookupStart': 1415358090102,\n          'domainLookupEnd': 1415358090102,\n          'connectStart': 1415358090103,\n          'connectEnd': 1415358090183,\n          'requestStart': 1415358090183,\n          'responseStart': 1415358090265,\n          'responseEnd': 1415358090265,\n          'domLoading': 1415358090270,\n          'domInteractive': 1415358090886,\n          'domContentLoadedEventStart': 1415358090968,\n          'domContentLoadedEventEnd': 1415358091309,\n          'domComplete': 0,\n          'loadEventStart': 0,\n          'loadEventEnd': 0\n        }\n      }\n    ]\n  }";

        private static readonly string _derivedContextsJson = "{\n    'schema': 'iglu:com.snowplowanalytics.snowplow\\/contexts\\/jsonschema\\/1-0-1',\n    'data': [\n      {\n        'schema': 'iglu:com.snowplowanalytics.snowplow\\/ua_parser_context\\/jsonschema\\/1-0-0',\n        'data': {\n          'useragentFamily': 'IE',\n          'useragentMajor': '7',\n          'useragentMinor': '0',\n          'useragentPatch': null,\n          'useragentVersion': 'IE 7.0',\n          'osFamily': 'Windows XP',\n          'osMajor': null,\n          'osMinor': null,\n          'osPatch': null,\n          'osPatchMinor': null,\n          'osVersion': 'Windows XP',\n          'deviceFamily': 'Other'\n        }\n      }\n    ]\n  }";

        private Dictionary<string, string> GetInputWithContextAndUnstructEvent()
        {
            return new Dictionary<string, string>(){
                { "app_id", "angry-birds"},
                { "platform", "web"},
                { "etl_tstamp", "2017-01-26 00:01:25.292"},
                { "collector_tstamp", "2013-11-26 00:02:05"},
                { "dvce_created_tstamp", "2013-11-26 00:03:57.885"},
                { "event", "page_view"},
                { "event_id", "c6ef3124-b53a-4b13-a233-0088f79dcbcb"},
                { "txn_id", "41828"},
                { "name_tracker", "cloudfront-1"},
                { "v_tracker", "js-2.1.0"},
                { "v_collector", "clj-tomcat-0.1.0"},
                { "v_etl", "serde-0.5.2"},
                { "user_id", "jon.doe@email.com"},
                { "user_ipaddress", "92.231.54.234"},
                { "user_fingerprint", "2161814971"},
                { "domain_userid", "bc2e92ec6c204a14"},
                { "domain_sessionidx", "3"},
                { "network_userid", "ecdff4d0-9175-40ac-a8bb-325c49733607"},
                { "geo_country", "US"},
                { "geo_region", "TX"},
                { "geo_city", "New York"},
                { "geo_zipcode", "94109"},
                { "geo_latitude", "37.443604"},
                { "geo_longitude", "-122.4124"},
                { "geo_region_name", "Florida"},
                { "ip_isp", "FDN Communications"},
                { "ip_organization", "Bouygues Telecom"},
                { "ip_domain", "nuvox.net"},
                { "ip_netspeed", "Cable/DSL"},
                { "page_url", "http://www.snowplowanalytics.com"},
                { "page_title", "On Analytics"},
                { "page_referrer", ""},
                { "page_urlscheme", "http"},
                { "page_urlhost", "www.snowplowanalytics.com"},
                { "page_urlport", "80"},
                { "page_urlpath", "/product/index.html"},
                { "page_urlquery", "id=GTM-DLRG"},
                { "page_urlfragment", "4-conclusion"},
                { "refr_urlscheme", ""},
                { "refr_urlhost", ""},
                { "refr_urlport", ""},
                { "refr_urlpath", ""},
                { "refr_urlquery", ""},
                { "refr_urlfragment", ""},
                { "refr_medium", ""},
                { "refr_source", ""},
                { "refr_term", ""},
                { "mkt_medium", ""},
                { "mkt_source", ""},
                { "mkt_term", ""},
                { "mkt_content", ""},
                { "mkt_campaign", ""},
                { "contexts", _contextsJson},
                { "se_category", ""},
                { "se_action", ""},
                { "se_label", ""},
                { "se_property", ""},
                { "se_value", ""},
                { "unstruct_event", _unstructJson},
                { "tr_orderid", ""},
                { "tr_affiliation", ""},
                { "tr_total", ""},
                { "tr_tax", ""},
                { "tr_shipping", ""},
                { "tr_city", ""},
                { "tr_state", ""},
                { "tr_country", ""},
                { "ti_orderid", ""},
                { "ti_sku", ""},
                { "ti_name", ""},
                { "ti_category", ""},
                { "ti_price", ""},
                { "ti_quantity", ""},
                { "pp_xoffset_min", ""},
                { "pp_xoffset_max", ""},
                { "pp_yoffset_min", ""},
                { "pp_yoffset_max", ""},
                { "useragent", ""},
                { "br_name", ""},
                { "br_family", ""},
                { "br_version", ""},
                { "br_type", ""},
                { "br_renderengine", ""},
                { "br_lang", ""},
                { "br_features_pdf", "1"},
                { "br_features_flash", "0"},
                { "br_features_java", ""},
                { "br_features_director", ""},
                { "br_features_quicktime", ""},
                { "br_features_realplayer", ""},
                { "br_features_windowsmedia", ""},
                { "br_features_gears", ""},
                { "br_features_silverlight", ""},
                { "br_cookies", ""},
                { "br_colordepth", ""},
                { "br_viewwidth", ""},
                { "br_viewheight", ""},
                { "os_name", ""},
                { "os_family", ""},
                { "os_manufacturer", ""},
                { "os_timezone", ""},
                { "dvce_type", ""},
                { "dvce_ismobile", ""},
                { "dvce_screenwidth", ""},
                { "dvce_screenheight", ""},
                { "doc_charset", ""},
                { "doc_width", ""},
                { "doc_height", ""},
                { "tr_currency", ""},
                { "tr_total_base", ""},
                { "tr_tax_base", ""},
                { "tr_shipping_base", ""},
                { "ti_currency", ""},
                { "ti_price_base", ""},
                { "base_currency", ""},
                { "geo_timezone", ""},
                { "mkt_clickid", ""},
                { "mkt_network", ""},
                { "etl_tags", ""},
                { "dvce_sent_tstamp", ""},
                { "refr_domain_userid", ""},
                { "refr_device_tstamp", ""},
                { "derived_contexts", _derivedContextsJson},
                { "domain_sessionid", "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1"},
                { "derived_tstamp", "2013-11-26 00:03:57.886"},
                { "event_vendor", "com.snowplowanalytics.snowplow"},
                { "event_name", "link_click"},
                { "event_format", "jsonschema"},
                { "event_version", "1-0-0"},
                { "event_fingerprint", "e3dbfa9cca0412c3d4052863cefb547f"},
                { "true_tstamp", "2013-11-26 00:03:57.886"}
            };
        }

        private Dictionary<string, string> GetInputWithoutContextAndUnstructEvent()
        {
            return new Dictionary<string, string>(){
                { "app_id", "angry-birds"},
                { "platform", "web"},
                { "etl_tstamp", "2017-01-26 00:01:25.292"},
                { "collector_tstamp", "2013-11-26 00:02:05"},
                { "dvce_created_tstamp", "2013-11-26 00:03:57.885"},
                { "event", "page_view"},
                { "event_id", "c6ef3124-b53a-4b13-a233-0088f79dcbcb"},
                { "txn_id", "41828"},
                { "name_tracker", "cloudfront-1"},
                { "v_tracker", "js-2.1.0"},
                { "v_collector", "clj-tomcat-0.1.0"},
                { "v_etl", "serde-0.5.2"},
                { "user_id", "jon.doe@email.com"},
                { "user_ipaddress", "92.231.54.234"},
                { "user_fingerprint", "2161814971"},
                { "domain_userid", "bc2e92ec6c204a14"},
                { "domain_sessionidx", "3"},
                { "network_userid", "ecdff4d0-9175-40ac-a8bb-325c49733607"},
                { "geo_country", "US"},
                { "geo_region", "TX"},
                { "geo_city", "New York"},
                { "geo_zipcode", "94109"},
                { "geo_latitude", "37.443604"},
                { "geo_longitude", "-122.4124"},
                { "geo_region_name", "Florida"},
                { "ip_isp", "FDN Communications"},
                { "ip_organization", "Bouygues Telecom"},
                { "ip_domain", "nuvox.net"},
                { "ip_netspeed", "Cable/DSL"},
                { "page_url", "http://www.snowplowanalytics.com"},
                { "page_title", "On Analytics"},
                { "page_referrer", ""},
                { "page_urlscheme", "http"},
                { "page_urlhost", "www.snowplowanalytics.com"},
                { "page_urlport", "80"},
                { "page_urlpath", "/product/index.html"},
                { "page_urlquery", "id=GTM-DLRG"},
                { "page_urlfragment", "4-conclusion"},
                { "refr_urlscheme", ""},
                { "refr_urlhost", ""},
                { "refr_urlport", ""},
                { "refr_urlpath", ""},
                { "refr_urlquery", ""},
                { "refr_urlfragment", ""},
                { "refr_medium", ""},
                { "refr_source", ""},
                { "refr_term", ""},
                { "mkt_medium", ""},
                { "mkt_source", ""},
                { "mkt_term", ""},
                { "mkt_content", ""},
                { "mkt_campaign", ""},
                { "contexts", ""},
                { "se_category", ""},
                { "se_action", ""},
                { "se_label", ""},
                { "se_property", ""},
                { "se_value", ""},
                { "unstruct_event", ""},
                { "tr_orderid", ""},
                { "tr_affiliation", ""},
                { "tr_total", ""},
                { "tr_tax", ""},
                { "tr_shipping", ""},
                { "tr_city", ""},
                { "tr_state", ""},
                { "tr_country", ""},
                { "ti_orderid", ""},
                { "ti_sku", ""},
                { "ti_name", ""},
                { "ti_category", ""},
                { "ti_price", ""},
                { "ti_quantity", ""},
                { "pp_xoffset_min", ""},
                { "pp_xoffset_max", ""},
                { "pp_yoffset_min", ""},
                { "pp_yoffset_max", ""},
                { "useragent", ""},
                { "br_name", ""},
                { "br_family", ""},
                { "br_version", ""},
                { "br_type", ""},
                { "br_renderengine", ""},
                { "br_lang", ""},
                { "br_features_pdf", "1"},
                { "br_features_flash", "0"},
                { "br_features_java", ""},
                { "br_features_director", ""},
                { "br_features_quicktime", ""},
                { "br_features_realplayer", ""},
                { "br_features_windowsmedia", ""},
                { "br_features_gears", ""},
                { "br_features_silverlight", ""},
                { "br_cookies", ""},
                { "br_colordepth", ""},
                { "br_viewwidth", ""},
                { "br_viewheight", ""},
                { "os_name", ""},
                { "os_family", ""},
                { "os_manufacturer", ""},
                { "os_timezone", ""},
                { "dvce_type", ""},
                { "dvce_ismobile", ""},
                { "dvce_screenwidth", ""},
                { "dvce_screenheight", ""},
                { "doc_charset", ""},
                { "doc_width", ""},
                { "doc_height", ""},
                { "tr_currency", ""},
                { "tr_total_base", ""},
                { "tr_tax_base", ""},
                { "tr_shipping_base", ""},
                { "ti_currency", ""},
                { "ti_price_base", ""},
                { "base_currency", ""},
                { "geo_timezone", ""},
                { "mkt_clickid", ""},
                { "mkt_network", ""},
                { "etl_tags", ""},
                { "dvce_sent_tstamp", ""},
                { "refr_domain_userid", ""},
                { "refr_device_tstamp", ""},
                { "derived_contexts", ""},
                { "domain_sessionid", "2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1"},
                { "derived_tstamp", "2013-11-26 00:03:57.886"},
                { "event_vendor", "com.snowplowanalytics.snowplow"},
                { "event_name", "link_click"},
                { "event_format", "jsonschema"},
                { "event_version", "1-0-0"},
                { "event_fingerprint", "e3dbfa9cca0412c3d4052863cefb547f"},
                { "true_tstamp", "2013-11-26 00:03:57.886"}
            };
        }

        private JObject GetOutputForInputWithContextAndUnstructEvent()
        {
            return JObject.Parse(@"{
                'geo_location' : '37.443604,-122.4124',
                'app_id' : 'angry-birds',
                'platform' : 'web',
                'etl_tstamp' : '2017-01-26T00:01:25.292Z',
                'collector_tstamp' : '2013-11-26T00:02:05Z',
                'dvce_created_tstamp' : '2013-11-26T00:03:57.885Z',
                'event' : 'page_view',
                'event_id' : 'c6ef3124-b53a-4b13-a233-0088f79dcbcb',
                'txn_id' : 41828,
                'name_tracker' : 'cloudfront-1',
                'v_tracker' : 'js-2.1.0',
                'v_collector' : 'clj-tomcat-0.1.0',
                'v_etl' : 'serde-0.5.2',
                'user_id' : 'jon.doe@email.com',
                'user_ipaddress' : '92.231.54.234',
                'user_fingerprint' : '2161814971',
                'domain_userid' : 'bc2e92ec6c204a14',
                'domain_sessionidx' : 3,
                'network_userid' : 'ecdff4d0-9175-40ac-a8bb-325c49733607',
                'geo_country' : 'US',
                'geo_region' : 'TX',
                'geo_city' : 'New York',
                'geo_zipcode' : '94109',
                'geo_latitude' : 37.443604,
                'geo_longitude' : -122.4124,
                'geo_region_name' : 'Florida',
                'ip_isp' : 'FDN Communications',
                'ip_organization' : 'Bouygues Telecom',
                'ip_domain' : 'nuvox.net',
                'ip_netspeed' : 'Cable/DSL',
                'page_url' : 'http://www.snowplowanalytics.com',
                'page_title' : 'On Analytics',
                'page_referrer' : null,
                'page_urlscheme' : 'http',
                'page_urlhost' : 'www.snowplowanalytics.com',
                'page_urlport' : 80,
                'page_urlpath' : '/product/index.html',
                'page_urlquery' : 'id=GTM-DLRG',
                'page_urlfragment' : '4-conclusion',
                'refr_urlscheme' : null,
                'refr_urlhost' : null,
                'refr_urlport' : null,
                'refr_urlpath' : null,
                'refr_urlquery' : null,
                'refr_urlfragment' : null,
                'refr_medium' : null,
                'refr_source' : null,
                'refr_term' : null,
                'mkt_medium' : null,
                'mkt_source' : null,
                'mkt_term' : null,
                'mkt_content' : null,
                'mkt_campaign' : null,
                'contexts_org_schema_web_page_1' : [ {
                  'genre' : 'blog',
                  'inLanguage' : 'en-US',
                  'datePublished' : '2014-11-06T00:00:00Z',
                  'author' : 'Devesh Shetty',
                  'breadcrumb' : ['blog', 'releases'],
                      'keywords' : [ 'snowplow', 'javascript', 'tracker', 'event' ]
                } ],
                'contexts_org_w3_performance_timing_1' : [ {
                  'navigationStart' : 1415358089861,
                  'unloadEventStart' : 1415358090270,
                  'unloadEventEnd' : 1415358090287,
                  'redirectStart' : 0,
                  'redirectEnd' : 0,
                  'fetchStart' : 1415358089870,
                  'domainLookupStart' : 1415358090102,
                  'domainLookupEnd' : 1415358090102,
                  'connectStart' : 1415358090103,
                  'connectEnd' : 1415358090183,
                  'requestStart' : 1415358090183,
                  'responseStart' : 1415358090265,
                  'responseEnd' : 1415358090265,
                  'domLoading' : 1415358090270,
                  'domInteractive' : 1415358090886,
                  'domContentLoadedEventStart' : 1415358090968,
                  'domContentLoadedEventEnd' : 1415358091309,
                  'domComplete' : 0,
                  'loadEventStart' : 0,
                  'loadEventEnd' : 0
                } ],
                'se_category' : null,
                'se_action' : null,
                'se_label' : null,
                'se_property' : null,
                'se_value' : null,
                'unstruct_event_com_snowplowanalytics_snowplow_link_click_1' : {
                  'targetUrl' : 'http://www.example.com',
                  'elementClasses' : [ 'foreground' ],
                  'elementId' : 'exampleLink'
                },
                'tr_orderid' : null,
                'tr_affiliation' : null,
                'tr_total' : null,
                'tr_tax' : null,
                'tr_shipping' : null,
                'tr_city' : null,
                'tr_state' : null,
                'tr_country' : null,
                'ti_orderid' : null,
                'ti_sku' : null,
                'ti_name' : null,
                'ti_category' : null,
                'ti_price' : null,
                'ti_quantity' : null,
                'pp_xoffset_min' : null,
                'pp_xoffset_max' : null,
                'pp_yoffset_min' : null,
                'pp_yoffset_max' : null,
                'useragent' : null,
                'br_name' : null,
                'br_family' : null,
                'br_version' : null,
                'br_type' : null,
                'br_renderengine' : null,
                'br_lang' : null,
                'br_features_pdf' : true,
                'br_features_flash' : false,
                'br_features_java' : null,
                'br_features_director' : null,
                'br_features_quicktime' : null,
                'br_features_realplayer' : null,
                'br_features_windowsmedia' : null,
                'br_features_gears' : null,
                'br_features_silverlight' : null,
                'br_cookies' : null,
                'br_colordepth' : null,
                'br_viewwidth' : null,
                'br_viewheight' : null,
                'os_name' : null,
                'os_family' : null,
                'os_manufacturer' : null,
                'os_timezone' : null,
                'dvce_type' : null,
                'dvce_ismobile' : null,
                'dvce_screenwidth' : null,
                'dvce_screenheight' : null,
                'doc_charset' : null,
                'doc_width' : null,
                'doc_height' : null,
                'tr_currency' : null,
                'tr_total_base' : null,
                'tr_tax_base' : null,
                'tr_shipping_base' : null,
                'ti_currency' : null,
                'ti_price_base' : null,
                'base_currency' : null,
                'geo_timezone' : null,
                'mkt_clickid' : null,
                'mkt_network' : null,
                'etl_tags' : null,
                'dvce_sent_tstamp' : null,
                'refr_domain_userid' : null,
                'refr_device_tstamp' : null,
                'contexts_com_snowplowanalytics_snowplow_ua_parser_context_1': [{
                  'useragentFamily': 'IE',
                  'useragentMajor': '7',
                  'useragentMinor': '0',
                  'useragentPatch': null,
                  'useragentVersion': 'IE 7.0',
                  'osFamily': 'Windows XP',
                  'osMajor': null,
                  'osMinor': null,
                  'osPatch': null,
                  'osPatchMinor': null,
                  'osVersion': 'Windows XP',
                  'deviceFamily': 'Other'
                }],
                'domain_sessionid': '2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1',
                'derived_tstamp': '2013-11-26T00:03:57.886Z',
                'event_vendor': 'com.snowplowanalytics.snowplow',
                'event_name': 'link_click',
                'event_format': 'jsonschema',
                'event_version': '1-0-0',
                'event_fingerprint': 'e3dbfa9cca0412c3d4052863cefb547f',
                'true_tstamp': '2013-11-26T00:03:57.886Z'
                }");
        }

        private JObject GetOutputForInputWithoutContextAndUnstructEvent()
        {
            return JObject.Parse(@"{
                'geo_location' : '37.443604,-122.4124',
                'app_id' : 'angry-birds',
                'platform' : 'web',
                'etl_tstamp' : '2017-01-26T00:01:25.292Z',
                'collector_tstamp' : '2013-11-26T00:02:05Z',
                'dvce_created_tstamp' : '2013-11-26T00:03:57.885Z',
                'event' : 'page_view',
                'event_id' : 'c6ef3124-b53a-4b13-a233-0088f79dcbcb',
                'txn_id' : 41828,
                'name_tracker' : 'cloudfront-1',
                'v_tracker' : 'js-2.1.0',
                'v_collector' : 'clj-tomcat-0.1.0',
                'v_etl' : 'serde-0.5.2',
                'user_id' : 'jon.doe@email.com',
                'user_ipaddress' : '92.231.54.234',
                'user_fingerprint' : '2161814971',
                'domain_userid' : 'bc2e92ec6c204a14',
                'domain_sessionidx' : 3,
                'network_userid' : 'ecdff4d0-9175-40ac-a8bb-325c49733607',
                'geo_country' : 'US',
                'geo_region' : 'TX',
                'geo_city' : 'New York',
                'geo_zipcode' : '94109',
                'geo_latitude' : 37.443604,
                'geo_longitude' : -122.4124,
                'geo_region_name' : 'Florida',
                'ip_isp' : 'FDN Communications',
                'ip_organization' : 'Bouygues Telecom',
                'ip_domain' : 'nuvox.net',
                'ip_netspeed' : 'Cable/DSL',
                'page_url' : 'http://www.snowplowanalytics.com',
                'page_title' : 'On Analytics',
                'page_referrer' : null,
                'page_urlscheme' : 'http',
                'page_urlhost' : 'www.snowplowanalytics.com',
                'page_urlport' : 80,
                'page_urlpath' : '/product/index.html',
                'page_urlquery' : 'id=GTM-DLRG',
                'page_urlfragment' : '4-conclusion',
                'refr_urlscheme' : null,
                'refr_urlhost' : null,
                'refr_urlport' : null,
                'refr_urlpath' : null,
                'refr_urlquery' : null,
                'refr_urlfragment' : null,
                'refr_medium' : null,
                'refr_source' : null,
                'refr_term' : null,
                'mkt_medium' : null,
                'mkt_source' : null,
                'mkt_term' : null,
                'mkt_content' : null,
                'mkt_campaign' : null,
                'se_category' : null,
                'se_action' : null,
                'se_label' : null,
                'se_property' : null,
                'se_value' : null,
                'tr_orderid' : null,
                'tr_affiliation' : null,
                'tr_total' : null,
                'tr_tax' : null,
                'tr_shipping' : null,
                'tr_city' : null,
                'tr_state' : null,
                'tr_country' : null,
                'ti_orderid' : null,
                'ti_sku' : null,
                'ti_name' : null,
                'ti_category' : null,
                'ti_price' : null,
                'ti_quantity' : null,
                'pp_xoffset_min' : null,
                'pp_xoffset_max' : null,
                'pp_yoffset_min' : null,
                'pp_yoffset_max' : null,
                'useragent' : null,
                'br_name' : null,
                'br_family' : null,
                'br_version' : null,
                'br_type' : null,
                'br_renderengine' : null,
                'br_lang' : null,
                'br_features_pdf' : true,
                'br_features_flash' : false,
                'br_features_java' : null,
                'br_features_director' : null,
                'br_features_quicktime' : null,
                'br_features_realplayer' : null,
                'br_features_windowsmedia' : null,
                'br_features_gears' : null,
                'br_features_silverlight' : null,
                'br_cookies' : null,
                'br_colordepth' : null,
                'br_viewwidth' : null,
                'br_viewheight' : null,
                'os_name' : null,
                'os_family' : null,
                'os_manufacturer' : null,
                'os_timezone' : null,
                'dvce_type' : null,
                'dvce_ismobile' : null,
                'dvce_screenwidth' : null,
                'dvce_screenheight' : null,
                'doc_charset' : null,
                'doc_width' : null,
                'doc_height' : null,
                'tr_currency' : null,
                'tr_total_base' : null,
                'tr_tax_base' : null,
                'tr_shipping_base' : null,
                'ti_currency' : null,
                'ti_price_base' : null,
                'base_currency' : null,
                'geo_timezone' : null,
                'mkt_clickid' : null,
                'mkt_network' : null,
                'etl_tags' : null,
                'dvce_sent_tstamp' : null,
                'refr_domain_userid' : null,
                'refr_device_tstamp' : null,
                'domain_sessionid': '2b15e5c8-d3b1-11e4-b9d6-1681e6b88ec1',
                'derived_tstamp': '2013-11-26T00:03:57.886Z',
                'event_vendor': 'com.snowplowanalytics.snowplow',
                'event_name': 'link_click',
                'event_format': 'jsonschema',
                'event_version': '1-0-0',
                'event_fingerprint': 'e3dbfa9cca0412c3d4052863cefb547f',
                'true_tstamp': '2013-11-26T00:03:57.886Z'
                }");
        }

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

        private TestContext testContextInstance;
        /// <summary>
        ///Gets or sets the test context which provides
        ///information about and functionality for the current test run.
        ///</summary>
        public TestContext TestContext
        {
            get
            {
                return testContextInstance;
            }
            set
            {
                testContextInstance = value;
            }
        }

        public IRow RowGenerator(JObject expected)
        {
            //Generate the schema
            List<IColumn> columns = new List<IColumn>();
            var keys = expected.Properties().Select(p => p.Name).ToList();

            keys.ForEach(key =>
            {
                var type = expected[key].GetType();
                FieldTypes fieldType;
                if (ENRICHED_EVENT_FIELD_TYPES.TryGetValue(key, out fieldType))
                {
                    switch (fieldType)
                    {
                        case FieldTypes.Property_Boolean:
                            columns.Add(new USqlColumn<bool>(key));
                            break;

                        case FieldTypes.Property_Double:
                            columns.Add(new USqlColumn<double?>(key));
                            break;

                        case FieldTypes.Property_Int32:
                            columns.Add(new USqlColumn<int?>(key));
                            break;

                        case FieldTypes.Property_DateTime:
                            columns.Add(new USqlColumn<DateTime?>(key));
                            break;

                        case FieldTypes.Property_String:
                            columns.Add(new USqlColumn<string>(key));
                            break;
                    }
                }
                else
                {
                    var unstructKey = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1";
                    var contextKeys = new List<string>() { "contexts_org_schema_web_page_1", "contexts_org_w3_performance_timing_1", "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1" };
                    if (contextKeys.Any(item => (string.Compare(item, key, StringComparison.CurrentCulture) == 0)))
                    {
                        columns.Add(new USqlColumn<SqlArray<SqlMap<string, object>>>(key));
                    }
                    else if (string.Compare(unstructKey, key, StringComparison.CurrentCulture) == 0)
                    {
                        columns.Add(new USqlColumn<SqlMap<string, object>>(key));
                    }

                }

            });
            USqlSchema schema = new USqlSchema(columns);
            return new USqlRow(schema, null);
        }

        [TestMethod]
        public void TestExtractorForInputWithContextsAndUnstructEvent()
        {
            var expected = GetOutputForInputWithContextAndUnstructEvent();
            var input = GetInputWithContextAndUnstructEvent();

            IUpdatableRow output = RowGenerator(expected).AsUpdatable();
            //convert data into TSV
            var tsv = string.Join("\t", input.Select(data => data.Value.Replace("\n", string.Empty)));
            using (MemoryStream stream = GenerateStreamFromString(tsv))
            {
                //Read input file 
                USqlStreamReader reader = new USqlStreamReader(stream);

                EventExtractor extractor = new EventExtractor();
                List<IRow> result = extractor.Extract(reader, output).ToList();

                var res = result[0];
                var keys = expected.Properties().Select(p => p.Name).ToList();
                Assert.IsTrue(CompareValues(expected, result[0]));
            }

        }

        [TestMethod]
        public void TestExtractorForInputWithoutContextsAndUnstructEvent()
        {
            var expected = GetOutputForInputWithoutContextAndUnstructEvent();
            var input = GetInputWithoutContextAndUnstructEvent();

            IUpdatableRow output = RowGenerator(expected).AsUpdatable();
            //convert data into TSV
            var tsv = string.Join("\t", input.Select(data => data.Value.Replace("\n", string.Empty)));
            using (MemoryStream stream = GenerateStreamFromString(tsv))
            {
                //Read input file 
                USqlStreamReader reader = new USqlStreamReader(stream);

                EventExtractor extractor = new EventExtractor();
                List<IRow> result = extractor.Extract(reader, output).ToList();

                var res = result[0];
                var keys = expected.Properties().Select(p => p.Name).ToList();
                Assert.IsTrue(CompareValues(expected, result[0]));
            }

        }

        private static bool CompareValues(JObject expectedObject, IRow actual)
        {
            var keys = expectedObject.Properties().Select(p => p.Name).ToList();
            var isEqual = true;
            keys.ForEach(key =>
             {
                 if (!isEqual)
                 {
                     //if any field value is not equal then don't process further
                     return;
                 }
                 var expectedValue = expectedObject[key];
                 var type = expectedObject[key].GetType();
                 FieldTypes fieldType;
                 if (ENRICHED_EVENT_FIELD_TYPES.TryGetValue(key, out fieldType))
                 {
                     switch (fieldType)
                     {
                         case FieldTypes.Property_Boolean:
                             if ((bool?)(expectedValue) != actual.Get<bool?>(key))
                             {
                                 isEqual = false;
                             }
                             break;

                         case FieldTypes.Property_Double:
                             if ((double?)(expectedValue) != actual.Get<double?>(key))
                             {
                                 isEqual = false;
                             }
                             break;

                         case FieldTypes.Property_Int32:
                             if ((int?)(expectedValue) != actual.Get<int?>(key))
                             {
                                 isEqual = false;
                             }
                             break;

                         case FieldTypes.Property_DateTime:
                             if ((DateTime?)(expectedValue) != actual.Get<DateTime?>(key))
                             {
                                 isEqual = false;
                             }
                             break;

                         case FieldTypes.Property_String:
                             if ((string)(expectedValue) != actual.Get<string>(key))
                             {
                                 isEqual = false;
                             }
                             break;
                     }
                 }
                 else
                 {
                     var unstructKey = "unstruct_event_com_snowplowanalytics_snowplow_link_click_1";
                     var contextKeys = new List<string>() { "contexts_org_schema_web_page_1", "contexts_org_w3_performance_timing_1", "contexts_com_snowplowanalytics_snowplow_ua_parser_context_1" };
                     if (contextKeys.Any(item => (string.Compare(item, key, StringComparison.CurrentCulture) == 0)))
                     {
                         var actualArray = actual.Get<SqlArray<SqlMap<string, object>>>(key);
                         for (int i = 0; i < expectedValue.Count(); i++)
                         {
                             var obj = expectedValue[i];
                             var dict = ((JObject)obj).ToObject<Dictionary<string, object>>();
                             var expectedMap = new SqlMap<string, object>(dict);
                             var actualMap = actualArray[i];
                             isEqual = CompareMap(expectedMap, actualMap);
                         }

                     }
                     else if (string.Compare(unstructKey, key, StringComparison.CurrentCulture) == 0)
                     {
                         var actualValue = actual.Get<SqlMap<string, object>>(key);
                         var expectedMap = ConvertObjectToSqlMap((JObject)expectedValue);
                         isEqual = CompareMap(expectedMap, actualValue);
                     }

                 }

             });

            return isEqual;
        }

        private static MemoryStream GenerateStreamFromString(string value)
        {
            return new MemoryStream(Encoding.UTF8.GetBytes(value ?? ""));
        }

        private static SqlMap<string, object> ConvertObjectToSqlMap(JObject obj)
        {
            var dict = obj.ToObject<Dictionary<string, object>>();
            return new SqlMap<string, object>(dict);
        }

        private static SqlArray<object> ConvertArraytoSqlArray(JArray array)
        {
            var expectedList = array.ToList();
            return new SqlArray<object>(expectedList);
        }

        private static bool CompareMap(SqlMap<string, object> expected, SqlMap<string, object> actual, bool isEqual = true)
        {
            if (!isEqual)
            {
                return false;
            }

            var keys = expected.Keys.ToList();
            keys.ForEach(key =>
            {
                if (!isEqual)
                {
                    return;
                }

                var expectedValue = expected[key];
                var actualValue = actual[key];
                if (expectedValue == null)
                {
                    isEqual = (expectedValue == actualValue);
                    return;
                }
                var expectedType = expectedValue.GetType();
                if (expectedType == typeof(JObject))
                {
                    var expectedMap = ConvertObjectToSqlMap((JObject)expectedValue);
                    isEqual = CompareMap(expectedMap, (SqlMap<string, object>)actualValue, isEqual);
                }
                else if (expectedType == typeof(JArray))
                {
                    var expectedArr = ConvertArraytoSqlArray((JArray)expectedValue);
                    isEqual = CompareArray(expectedArr, (SqlArray<object>)actualValue, isEqual);
                }
                else
                {
                    if (expectedType == typeof(string))
                    {
                        isEqual = string.Compare((string)expectedValue, (string)actualValue) == 0;
                    }
                    else if (expectedType == typeof(DateTime))
                    {
                        isEqual = DateTime.Compare((DateTime)expectedValue, (DateTime)actualValue) == 0;
                    }
                    else if (expectedType == typeof(long))
                    {
                        isEqual = ((long)expectedValue == (long)actualValue);
                    }
                    else
                    {
                        isEqual = (expectedValue == actualValue);
                    }
                }
            });

            return isEqual;
        }

        private static bool CompareArray(SqlArray<object> expected, SqlArray<object> actual, bool isEqual = true)
        {
            if (!isEqual)
            {
                return false;
            }
            var expectedCount = expected.Count();
            if (expectedCount != actual.Count())
            {
                return false;
            }

            for (int i = 0; i < expectedCount; i++)
            {
                if (!isEqual)
                {
                    return false;
                }
                var expectedValue = expected[i];
                var actualValue = actual[i];
                var expectedType = expectedValue.GetType();

                if (expectedType == typeof(JObject))
                {
                    var expectedMap = ConvertObjectToSqlMap((JObject)expectedValue);
                    isEqual = CompareMap(expectedMap, (SqlMap<string, object>)actualValue, isEqual);
                }
                else if (expectedType == typeof(JArray))
                {
                    var expectedArr = ConvertArraytoSqlArray((JArray)expectedValue);
                    isEqual = CompareArray(expectedArr, (SqlArray<object>)actualValue, isEqual);
                }
                else
                {
                    var jValue = ((JToken)expectedValue);
                    var jType = jValue.Type;
                    if (jType == JTokenType.String)
                    {
                        isEqual = string.Compare(jValue.Value<string>(), (string)actualValue) == 0;
                    }
                    else if (jType == JTokenType.Date)
                    {
                        isEqual = DateTime.Compare(jValue.Value<DateTime>(), (DateTime)actualValue) == 0;
                    }
                    else
                    {
                        isEqual = expectedValue == actualValue;
                    }

                    if (!isEqual)
                    {
                        return isEqual;
                    }
                }

            }
            return isEqual;
        }

    }
}
