Imports DasClientLib

Module DasClientSample

    Sub Main()

        ' Here are some credentials and coordinates for the DAS web service. Fill them in appropriately.
        Dim USERNAME As String = "<your username>"
        Dim PASSWORD As String = "<your password>"
        Dim DAS_API As String = "https://demo.pamdas.org/api/v1.0"
        Dim DAS_AUTH_API As String = "https://demo.pamdas.org/oauth2/token"
        Dim DAS_CLIENT_ID As String = "das_web_client"

        Dim MY_SOURCE_PROVIDER As String = "nl-demo"

        ' Create a DasClient
        Dim client As DasClient = New DasClient(USERNAME, PASSWORD, DAS_API, DAS_AUTH_API, DAS_CLIENT_ID)

        ' Login isn't strictly required -- DasClient will automatically take care of logging in and refreshing an Oauth2 token.
        client.login()

        ' Create a Source in DAS
        ' A Source will represent the tracking device that you'll post Observations for.
        Dim manufacturerId As String = "my-device-1"
        Dim sourceType As String = "tracking-device"
        Dim modelName As String = "device-model-1"
        Dim additional = New Dictionary(Of String, Object) ' additional is a dictionary of side-data to be stored with the source.
        additional.Add("installed", "2017-01-31T16:02:21Z")
        Dim source As DasSource = New DasSource(manufacturerId, MY_SOURCE_PROVIDER, sourceType, modelName, additional)

        ' Post the new source to DAS web-service.
        Dim result = client.postSource(source)

        Dim sourceId As String = result.GetValue("id").ToString()
        System.Console.WriteLine("result ID : " + sourceId)

        ' New-up data for an observation for the source we just created.
        Dim location As Coordinate = New Coordinate(37.5F, 1.454F)
        additional = New Dictionary(Of String, Object) ' additional is side-data to be stored with the observation.
        additional.Add("heading", 45.41F)
        Dim observation As DasObservation = New DasObservation(sourceId, DateTime.UtcNow, location, additional)
        result = client.postObservation(observation)
        System.Console.WriteLine(result.GetValue("id").ToString())

    End Sub

End Module
