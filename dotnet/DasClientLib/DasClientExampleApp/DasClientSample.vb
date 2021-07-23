Imports DasClientLib

Module DasClientSample

    Sub Main()

        Dim MY_SOURCE_PROVIDER As String = "nl-demo"

        Dim client As DasClient = GetDasClient()

        ' Create a Source in DAS
        ' A Source will represent the tracking device that you'll post Observations for.
        Dim manufacturerId As String = "my-device-1"
        Dim sourceType As String = "tracking-device"
        Dim modelName As String = "device-model-1"
        Dim additional = New Dictionary(Of String, Object) ' additional is a dictionary of side-data to be stored with the source.
        additional.Add("installed", "2017-01-31T16:02:21Z")
        Dim source As Source = New Source(manufacturerId, MY_SOURCE_PROVIDER, sourceType, modelName, additional)

        ' Post the new source to DAS web-service.
        Dim result = client.PostSource(source)

        Dim sourceId As String = result.GetValue("id").ToString()
        System.Console.WriteLine("result ID : " + sourceId)

        ' New-up data for an observation for the source we just created.
        Dim location As Coordinate = New Coordinate(37.5F, 1.454F)
        additional = New Dictionary(Of String, Object) ' additional is side-data to be stored with the observation.
        additional.Add("heading", 45.41F)
        Dim observation As Observation = New Observation(sourceId, DateTime.UtcNow, location, additional)
        result = client.PostObservation(observation)
        System.Console.WriteLine(result.GetValue("id").ToString())

    End Sub

    Function GetDasClient() As DasClient
        ' Here are some credentials and coordinates for the DAS web service. Fill them in appropriately.
        Dim USERNAME As String = "<your username>"
        Dim PASSWORD As String = "<your password>"
        Dim DAS_API As String = "https://sandbox.pamdas.org"
        Dim DAS_CLIENT_ID As String = "das_web_client"
        Dim ACCESS_TOKEN As String = "<your token>"

        If Not String.IsNullOrEmpty(ACCESS_TOKEN) Then
            Return New DasClient(AuthToken.FromAccessToken(ACCESS_TOKEN), DAS_API)
        Else
            ' Create a DasClient
            Dim client As DasClient = New DasClient(USERNAME, PASSWORD, DAS_API, DAS_CLIENT_ID)

            ' Login isn't strictly required -- DasClient will automatically take care of logging in and refreshing an Oauth2 token.
            client.Login()
            Return client
        End If


    End Function

End Module
