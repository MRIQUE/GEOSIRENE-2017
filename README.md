# GeoSirene Data Importer (2017 Version)

## Overview/Purpose
This C# project allows you to download and import the geocoded SIRENE database (2017 version) into a SQL Server table. This facilitates easier data processing and analysis.

## Data Source
The primary data source is the SIRENE database of French companies and their establishments, made available by the French government as part of its open data policy (OPEN DATA) on data.gouv.fr: [https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/](https://www.data.gouv.fr/fr/datasets/base-sirene-des-entreprises-et-de-leurs-etablissements-siren-siret/)

This dataset was geocoded by Christian Quest using the BAN (Base Adresse Nationale) and BANO (Base Adresse Nationale Ouverte). The geocoded dataset is refreshed monthly and was made available at [http://data.cquest.org/geo_sirene/last/](http://data.cquest.org/geo_sirene/last/). This tool is configured to download data from this URL, as specified in the `App.config` file.

*Note: Direct programmatic access to verify the content of `data.cquest.org` via automated agents may be restricted by the website's `robots.txt` file.*

The downloaded CSV file contains over 10 million entities and 91 columns: 84 from the original SIRENE database + 7 added during geocoding (including latitude and longitude in EPSG 4326). To save memory, columns with long string labels from the original SIRENE database were removed.

## Features
- Downloads the geocoded SIRENE database (2017 version).
- Imports the data into a SQL Server table.

## Prerequisites
- **.NET Framework:** Version 4.5 or higher.
- **SQL Server:** Any recent version should be compatible.

## Configuration
The `App.config` file contains key settings for the application:
1.  **`GeoSireneURL`**: The URL for downloading the GeoSirene data files (typically `http://data.cquest.org/geo_sirene/last/`).
2.  **`ArchiveFileDownloadPath`**: The local directory where the downloaded `.7z` archive files will be stored.
3.  **`ExtractFilePath`**: The local directory where the CSV files will be extracted from the archives.
4.  **`DBServer`**: The name or network address of your SQL Server instance.
5.  **`Database`**: The name of the database within your SQL Server instance where the data will be imported.
6.  **SQL Server Connection:**
    *   The application can use a **Trusted Connection** (Windows Authentication). If your connection string in `App.config` includes `Trusted_Connection=true;`, this method is used.
    *   Alternatively, you can use **SQL Server Authentication** by providing a `User ID` and `Password` in the connection string. Ensure `Trusted_Connection=false;` or remove it if using SQL authentication.

Adjust these variables in the `App.config` file to match your environment.

## Setup Instructions
1. Create the target table in your SQL Server database using the command found in the `create_table_Geo_sirene.txt` file.

## How to Run
1. Ensure your `App.config` file is correctly configured (see Configuration section).
2. Compile the C# application (e.g., using Visual Studio 2017 or a compatible MSBuild version).
3. Launch the compiled executable from the command line.
4. (Optional) Create indexes on your SQL Server table after data import, depending on the type of queries you plan to run, to improve performance.

## Disclaimer
**IMPORTANT WARNING:**
*   The SIRENE database format was expected to undergo significant changes in early 2019.
*   This tool, "GeoSirene-2017," was specifically designed for the **2017 version** of the geocoded SIRENE database.
*   Its compatibility with any later versions of the SIRENE data is **unknown and unlikely**. Users should be aware that this tool might only work correctly with the historical 2017 data version it was originally intended for. Attempting to use it with newer data may lead to errors or incorrect data processing.

## Acknowledgements
- Thank you to Christian Quest for his ongoing, intelligent, and useful contributions to open data, especially the geocoding and maintenance of the SIRENE database. His Python script for monthly updates (relevant to the data period this tool was designed for) is available on his GitHub: [https://github.com/cquest](https://github.com/cquest).
- Note related work: [https://github.com/ColinMaudry/sirene-ld](https://github.com/ColinMaudry/sirene-ld)