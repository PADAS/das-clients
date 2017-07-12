using System;
using System.Collections.Generic;

using RestSharp;
using System.Net;

using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace DasClientLib
{
    class DasSettings
    {
        public static string USERNAME = "chrisd";
        public static string PASSWORD = "8hubarbTwit";
        public static string DAS_API = "https://demo.pamdas.org/api/v1.0";
        public static string DAS_AUTH_API = "https://demo.pamdas.org/oauth2/token";
        public static string DAS_CLIENT_ID = "das_web_client";
    }

    class AuthToken
    {
        public AuthToken()
        {
            this.created_at = DateTime.UtcNow;
        }
        public DateTime created_at { get; }
        public string access_token { get; set; }
        public string refresh_token { get; set; }
        public int expires_in { get; set; }
        public string scope { get; set; }
        public string token_type { get; set; }
        // Fudge the freshness check by 30 seconds, so we won't race against the very second of expiration.
        public Boolean is_fresh { get { return this.access_token != null && this.created_at.AddSeconds(expires_in) > DateTime.UtcNow.AddSeconds(30); } }

        public static AuthToken parse(string value)
        {
            return JsonConvert.DeserializeObject<AuthToken>(value);
        }

    }

    //class DasResponseStatus
    //{
    //	public string message { get; set; }
    //	public int code { get; set; }
    //}
    //class DasUser
    //{
    //	public string username { get; set;}
    //	public string email { get; set;}
    //	public string first_name { get; set; }
    //	public string last_name { get; set; }

    //}

    //class DasUserResponse
    //{
    //	public DasResponseStatus status { get; set; }
    //	public DasUser data { get; set;}

    //	public static DasUserResponse parse(string value)
    //	{
    //		return JsonConvert.DeserializeObject<DasUserResponse>(value);
    //	}
    //}

    public class DasSource
    {
        public DasSource(string manufacturer_id, string provider,  string source_type, string model_name, Dictionary<string, object> additional)
        {
            this.manufacturer_id = manufacturer_id;
            this.source_type = source_type;
            this.model_name = model_name;
            this.additional = additional;
            this.provider = provider;


        }
        public string manufacturer_id { get; set; }
        public string source_type { get; set; }
        public string model_name { get; set; }
        public string provider { get; set; }
        public Dictionary<string, object> additional { get; set; }
        // public DasSubject
    }

    public class Coordinate
    {
        public float latitude { set; get; }
        public float longitude { set; get; }
        public Coordinate(float longitude, float latitude)
        {
            this.longitude = longitude;
            this.latitude = latitude;
        }
    }
    public class DasObservation
    {
        public string source { get; set; }
        public DateTime recorded_at { get; set; }
        public Coordinate location { get; set; }
        public Dictionary<string, object> additional { get; set; }

        public DasObservation(string source_id, DateTime recorded_at, Coordinate location, Dictionary<string, object> additional)
        {
            this.source = source_id;
            this.recorded_at = recorded_at;
            this.location = location;
            this.additional = additional;
        }

    }


    public class DasClientException : Exception
    {
        public DasClientException(string message) : base(message)
        {

        }
    }

    public class DasClient
    {
        string username;
        string password;
        string das_api;
        string das_auth_api;
        string das_client_id;
        string useragent = "das-data-loader/1.0";

        AuthToken auth_token = null;

        private static int[] CREATED = { 200, 201 };

        public DasClient(string username, string password, string das_api, string das_auth_api, string das_client_id)
        {
            this.username = username;
            this.password = password;
            this.das_api = das_api;
            this.das_auth_api = das_auth_api;
            this.das_client_id = das_client_id;
        }

        private Dictionary<string, string> authHeaders()
        {
            Dictionary<string, string> headers = new Dictionary<string, string>();
            headers.Add("Authorization", String.Format("{0} {1}", this.auth_token.token_type, this.auth_token.access_token));
            headers.Add("Content-Type", "application/json");
            headers.Add("User-Agent", this.useragent);
            return headers;
        }

        public Boolean login()
        {
            var payload = String.Format("grant_type=password&username={0}&password={1}&client_id={2}", this.username, this.password, this.das_client_id);
            return this._token_request(payload);
        }

        public Boolean refreshToken()
        {
            var payload = String.Format("grant_type=refresh_token&refresh_token={0}&client_id={1}", this.auth_token.refresh_token, this.das_client_id);
            return this._token_request(payload);
        }

        private Boolean _token_request(string payload)
        {
            var client = new RestClient(this.das_auth_api);

            var request = new RestRequest(Method.POST);
            request.AddHeader("cache-control", "no-cache");
            request.AddHeader("content-type", "application/x-www-form-urlencoded");
            request.AddParameter("application/x-www-form-urlencoded", payload, ParameterType.RequestBody);
            IRestResponse response = client.Execute(request);

            if (response.StatusCode.Equals(HttpStatusCode.OK))
            {
                this.auth_token = AuthToken.parse(response.Content);
            }
            else
            {
                this.auth_token = null;
            }

            return this.auth_token != null;
        }

        private void _add_auth_headers(RestRequest request)
        {
            if (this.auth_token != null)
            {
                if (!this.auth_token.is_fresh)
                {
                    if (!this.refreshToken())
                    {
                        if (!this.login())
                        {
                            throw new DasClientException("Failed to authorize.");
                        }
                    }


                }
            }
            else
            {
                if (!this.login())
                {
                    throw new DasClientException("Failed to authorize.");
                }
            }
            var headers = this.authHeaders();
            foreach (var key in headers.Keys)
            {
                request.AddHeader(key, headers[key]);
            }
        }

        private JObject _get(string resource)
        {
            var client = new RestClient(String.Format("{0}/{1}", this.das_api, resource));
            var request = new RestRequest(Method.GET);
            this._add_auth_headers(request);

            IRestResponse response = client.Execute(request);

            if (response.StatusCode.Equals(HttpStatusCode.OK))
            {
                return JObject.Parse(response.Content);
            }

            throw new DasClientException(String.Format("Failed to get {}", resource));

        }

        private JObject _post(string resource, object payload)
        {
            var client = new RestClient(String.Format("{0}/{1}", this.das_api, resource));
            var request = new RestRequest(Method.POST);
            request.RequestFormat = DataFormat.Json;
            this._add_auth_headers(request);
            request.AddJsonBody(payload);
            IRestResponse response = client.Execute(request);

            if (Array.IndexOf(CREATED, (int)response.StatusCode) >= 0)
            {
                return JObject.Parse(response.Content);
            }
            else
            {
                throw new DasClientException(String.Format("Failed to parse DAS response for resource {}", resource));
            }
        }

        public JObject getMe()
        {
            var response = this._get("user/me");

            var code = (int)response["status"]["code"];
            if (code == 200)
            {
                return (JObject)response["data"];
            }
            return null;
        }

        public JObject postSource(DasSource source)
        {
            var result = this._post("sources", source);
            var src = result["data"];
            return (JObject)src;
        }

        public JObject searchSource(string manufacturer_id)
        {
            var result = this._get(String.Format("source?manufacturer_id=", manufacturer_id));
            var src = result["data"];
            return (JObject)src;
        }

        public JObject postObservation(DasObservation observation)
        {
            var result = this._post("observations", observation);
            var obs = result["data"];
            return (JObject)obs;

        }

    }


}
