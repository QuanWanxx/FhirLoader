FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src

COPY ["NuGet.Config", "."]
COPY ["src/Fhir.Loader.Tool/Fhir.Loader.Tool.csproj", "src/Fhir.Loader.Tool/"]
COPY ["src/BlobListGenerator/BlobListGenerator.csproj", "src/BlobListGenerator/"]
COPY ["src/Dicom.Loader.Tool/Dicom.Loader.Tool.csproj", "src/Dicom.Loader.Tool/"]
COPY ["src/Dicom.Operation.Example/Dicom.Operation.Example.csproj", "src/Dicom.Operation.Example/"]
COPY ["src/Fhir.Loader.Tool/Fhir.Loader.Tool.csproj", "src/Fhir.Loader.Tool/"]

RUN dotnet restore "src/Fhir.Loader.Tool/Fhir.Loader.Tool.csproj"

COPY . .
WORKDIR "/src/src/Fhir.Loader.Tool"
RUN dotnet build "Fhir.Loader.Tool.csproj" -c Release -o /app/build
FROM build AS publish

RUN dotnet publish "Fhir.Loader.Tool.csproj" -c Release -o /app/publish /p:UseAppHost=false
FROM base AS final

WORKDIR /app
COPY --from=publish /app/publish .
RUN echo "/app" >> /etc/ld.so.conf.d/app.conf && ldconfig

ENTRYPOINT ["dotnet", "Fhir.Loader.Tool.dll"]