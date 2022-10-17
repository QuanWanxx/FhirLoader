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

