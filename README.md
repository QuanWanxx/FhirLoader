# FHIR Server Loading tool

Folk from [FhirLoader](https://github.com/hansenms/FhirLoader). Uses multi-threaded loading of FHIR data into a FHIR server.

Configurate the config.json to upload local FHIR ndjson files to FHIR Services.

```javascript
{
  "FhirServerUrl": "https://example-fhirserver.azurewebsites.net",
  "AccessToken": "{Bearer token if use PaaS Fhir server}",
  "DirectoryPath": "{Fhir NDJSON files directory}"
}
```

## New Loader

### Set up steps
1. Run ```az login``` to setup default identity.
2. [optional] set `APPLICATIONINSIGHTS_CONNECTION_STRING` environment variable to add application insights logging.

Change config in Quwan loader `appsettings.json` and set appropriate permissions to FHIR server and blob storage. 
```sh
cd QuwanLoader
dotnet run
```

