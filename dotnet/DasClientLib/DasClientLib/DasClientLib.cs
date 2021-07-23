/*
 * EarthRanger client library
 * 
 * We use the google style guide for C# https://google.github.io/styleguide/csharp-style.html
 */

using System;
using System.Collections.Generic;

using RestSharp;
using System.Net;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using System.IO;

namespace DasClientLib
{
    public class AuthToken
    {
        private AuthToken()
        {
            this.CreatedAt = DateTime.UtcNow;
        }

        private AuthToken(string accessToken)
        {
            AccessToken = accessToken;
            TokenType = "Bearer";
            _isAccessTokenOnly = true;
        }

        private bool _isAccessTokenOnly = false;
        
        public DateTime CreatedAt { get; }
        public string AccessToken { get; set; }
        public string RefreshToken { get; set; }
        public int ExpiresIn { get; set; }
        public string Scope { get; set; }
        public string TokenType { get; set; }
        // Fudge the freshness check by 30 seconds, so we won't race against the very second of expiration.
        public Boolean IsFresh { get { return _isAccessTokenOnly || (this.AccessToken != null && this.CreatedAt.AddSeconds(ExpiresIn) > DateTime.UtcNow.AddSeconds(30)); } }

        public static AuthToken Parse(string value)
        {
            return JsonConvert.DeserializeObject<AuthToken>(value);
        }

        public static AuthToken FromAccessToken(string accessToken)
        {
            return new AuthToken(accessToken);
        }

    }

    public class Source
    {
        public Source(string manufacturerId, string provider,  string sourceType, string modelName, Dictionary<string, object> additional)
        {
            this.ManufacturerId = manufacturerId;
            this.source_type = sourceType;
            this.model_name = modelName;
            this.additional = additional;
            this.provider = provider;


        }
        [JsonProperty("manufacturer_id")]
        public string ManufacturerId { get; set; }
        public string source_type { get; set; }
        public string model_name { get; set; }
        public string provider { get; set; }
        public Dictionary<string, object> additional { get; set; }
    }

    public class Coordinate
    {
        public float Latitude { set; get; }
        public float Longitude { set; get; }
        public Coordinate(float longitude, float latitude)
        {
            this.Longitude = longitude;
            this.Latitude = latitude;
        }
    }
    public class Observation
    {
        public string Source { get; set; }
        public DateTime RecordedAt { get; set; }
        public Coordinate Location { get; set; }
        public Dictionary<string, object> Additional { get; set; }

        public Observation(string sourceId, DateTime recordedAt, Coordinate location, Dictionary<string, object> additional)
        {
            this.Source = sourceId;
            this.RecordedAt = recordedAt;
            this.Location = location;
            this.Additional = additional;
        }

    }

    public class Event
    {
        [JsonProperty("event_type")]
        public string EventType { get; set; }


    }


    public class DasClientException : Exception
    {
        public DasClientException()
        {

        }

        public DasClientException(string message) : base(message)
        {

        }

        public DasClientException(string message, Exception inner) : base(message, inner)
        {

        }
    }

    public class DasClient
    {
        string _username;
        string _password;
        string _dasApi;
        string _dasAuthApi;
        string _dasClientId;
        string _useragent = "dotnet-data-loader/1.0";

        AuthToken _authToken = null;

        private static int[] CREATED = { 200, 201 };

        public DasClient(string username, string password, string api, string clientId)
        {
            _username = username;
            _password = password;
            _dasApi = $"{api}/api/v1.0";
            _dasAuthApi = $"{api}/oauth2/token";
            _dasClientId = clientId;
        }

        public DasClient(AuthToken token, string api)
        {
            _authToken = token;
            _dasApi = $"{api}/api/v1.0";
            _dasAuthApi = $"{api}/oauth2/token";
        }

        private Dictionary<string, string> AuthHeaders()
        {
            Dictionary<string, string> headers = new Dictionary<string, string>();
            headers.Add("Authorization", String.Format("{0} {1}", this._authToken.TokenType, this._authToken.AccessToken));
            headers.Add("Content-Type", "application/json");
            headers.Add("User-Agent", this._useragent);
            return headers;
        }

        public Boolean Login()
        {
            var payload = String.Format("grant_type=password&username={0}&password={1}&client_id={2}", this._username, this._password, this._dasClientId);
            return this._token_request(payload);
        }

        public Boolean RefreshToken()
        {
            var payload = String.Format("grant_type=RefreshToken&RefreshToken={0}&client_id={1}", this._authToken.RefreshToken, this._dasClientId);
            return this._token_request(payload);
        }

        private Boolean _token_request(string payload)
        {
            var client = new RestClient(this._dasAuthApi);

            var request = new RestRequest(Method.POST);
            request.AddHeader("cache-control", "no-cache");
            request.AddHeader("content-type", "application/x-www-form-urlencoded");
            request.AddParameter("application/x-www-form-urlencoded", payload, ParameterType.RequestBody);
            IRestResponse response = client.Execute(request);

            if (response.StatusCode.Equals(HttpStatusCode.OK))
            {
                this._authToken = AuthToken.Parse(response.Content);
            }
            else
            {
                this._authToken = null;
            }

            return this._authToken != null;
        }

        private void AddAuthHeaders(RestRequest request)
        {
            if (this._authToken != null)
            {
                if (!this._authToken.IsFresh)
                {
                    if (!this.RefreshToken())
                    {
                        if (!this.Login())
                        {
                            throw new DasClientException("Failed to authorize.");
                        }
                    }


                }
            }
            else
            {
                if (!this.Login())
                {
                    throw new DasClientException("Failed to authorize.");
                }
            }
            var headers = this.AuthHeaders();
            foreach (var key in headers.Keys)
            {
                request.AddHeader(key, headers[key]);
            }
        }

        private JObject ApiGet(string resource)
        {
            var client = new RestClient(String.Format("{0}/{1}", this._dasApi, resource));
            var request = new RestRequest(Method.GET);
            this.AddAuthHeaders(request);

            IRestResponse response = client.Execute(request);

            if (response.StatusCode.Equals(HttpStatusCode.OK))
            {
                return JObject.Parse(response.Content);
            }

            throw new DasClientException($"Failed to get {resource}");

        }

        public JObject ApiPostFile(string resource, string filePath, string contentType = null)
        {
            var client = new RestClient(this._dasApi);
            var request = new RestRequest(resource);
            IRestResponse response = null;

            this.AddAuthHeaders(request);
            request.AddHeader("Content-Type", "multipart/form-data");
            request.AddFile("filecontent.file", filePath, contentType);

            response = client.Post(request);


            if (Array.IndexOf(CREATED, (int)response.StatusCode) >= 0)
            {
                return JObject.Parse(response.Content);
            }
            else
            {
                throw new DasClientException($"Failed to Parse DAS response for resource {resource}");
            }
        }

        public JObject ApiPost(string resource, object payload)
        {
            var client = new RestClient(this._dasApi);
            var request = new RestRequest(resource, DataFormat.Json);
            request.JsonSerializer = new RestSharp.Serializers.NewtonsoftJson.JsonNetSerializer();

            this.AddAuthHeaders(request);
            request.AddJsonBody(payload);
            IRestResponse response = client.Post(request);

            if (Array.IndexOf(CREATED, (int)response.StatusCode) >= 0)
            {
                return JObject.Parse(response.Content);
            }
            else
            {
                throw new DasClientException($"Failed to Parse DAS response for resource {resource}");
            }
        }

        public JObject Me()
        {
            var response = this.ApiGet("user/me");

            var code = (int)response["status"]["code"];
            if (code == 200)
            {
                return (JObject)response["data"];
            }
            return null;
        }

        public JObject PostSource(Source source)
        {
            var result = this.ApiPost("sources", source);
            var src = result["data"];
            return (JObject)src;
        }

        public JObject SearchSource(string manufacturerId)
        {
            var result = this.ApiGet(String.Format("source?manufacturer_id=", manufacturerId));
            var src = result["data"];
            return (JObject)src;
        }

        public JObject PostObservation(Observation observation)
        {
            var result = this.ApiPost("observations", observation);
            var obs = result["data"];
            return (JObject)obs;

        }

    }


}
