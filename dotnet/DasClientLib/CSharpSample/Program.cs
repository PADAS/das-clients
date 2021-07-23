using Newtonsoft.Json.Linq;
using System;

namespace CSharpSample
{
    class Program
    {
        static void Main(string[] args)
        {
            var accessToken = "<er authtoken here>";
            var dasAPI = "https://sandbox.pamdas.org";
            var authToken = DasClientLib.AuthToken.FromAccessToken(accessToken);

            var dasClient = new DasClientLib.DasClient(authToken, dasAPI);

            string reportJson = @"{
                title : 'Test Event Alert',
                event_type: 'cameratrap_rep',
                event_details: {
                    cameratraprep_camera-name: 'camera one'
                },
                location: {
                    longitude:'40.1353',
                    latitude:'-1.891517'
                }
            }";

            JObject report = JObject.Parse(reportJson);

            var response = dasClient.ApiPost("activity/events", report);
            var eventId = response.SelectToken("data.id");


            var responseFile = dasClient.ApiPostFile($"activity/event/{eventId}/files", @"c:/tmp/picture.jpg", "application/jpeg");

            
        }
    }
}
