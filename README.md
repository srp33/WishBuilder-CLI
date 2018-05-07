# WishBuilderCI
Continuous Integration Pipeline which tests and merges pull requests to [*WishBuilder*](https://github.com/srp33/WishBuilder)

## Purpose

*WishBuilder* is an open source project that provides biology-related datasets to *Geney*, a service that makes the data easily to filter and query for
research. *WishBuilder* allows users to submit code which gathers and reformats data pulled from public Web servers into a consistent format described on
the project [wiki](https://srp33.github.io/WishBuilder/).

*WishBuilderCI* (Continuous Integration) manages the pull requests submitted to *WishBuilder* by detecting them automatically, testing that the ouput data
 is consistent with the *WishBuilder* requirements, and adding the datasets created by the code contained in each passed dataset to *Geney*.

## How it Works

### Environment
- The WB_PATH environment variable must be set to a directory path containing the WishBuilder-CLI repository
- 'private.py' must exist with the following constant variables
    - GH_TOKEN (The token associated with a collaborator in order to receive information from the GitHub API)
    - WISHBUILDER_EMAIL (The email address used to send status reports to users)
    - WISHBUILDER_PASS (The password associated with the WISHBUILDER_EMAIL account)
#### Docker
*WishBuilderCI* requires [Docker](https://docker.com) to test code in an environment container. The "wishbuilder-cli" image used to create each container
 can be pulled from the docker hub with this command:
```bash
docker pull srp33/wishbuilder-cli
```
If you would like to experiment within this container, you can explore the environment within a bash shell by using the following command:
```bash
docker run -it -v $(pwd):/app --rm srp33/wishbuilder-cli /bin/bash
```
#### App Structure

- /app/ (WB_DIRECTORY)
    - WishBuilder-CLI (This repository)
        - all files in repository plus these additional files (in .gitignore):
        - testing/ (location for downloading and testing pull requests)
        - **private.py**
        - history.sql
        - RawDatasets/
            - Dataset1/
                - data.tsv.gz
                - metadata.tsv.gz
                - config.yaml
                - description.md
            - Dataset2
                - ...
            - ...
        - GeneyDatasets/
            - Dataset1/
                - data.h5
                - metadata.sql
                - metadata.json
                - description.json
            - Dataset2/
                - ...
            - ...
    - GeneyTypeConverter/
        - ([Type Converter Repository](https://github.com/zence/GeneyTypeConverter))

This is the file structure of the app, any folder not included in the repository is in the .gitignore and will be auto-generated if it doesn't exist, except
for the private.py file which must be provided. The history.sql file keeps a record of all the pull requests, and the sha's tested for each commit to them.
The files produced by successful tests are moved into the **RawDatasets** directory. After the datasets are converted to the correct format for Geney, the
 data is stored in the **GeneyDatasets** directory.

### Execution (Docker-Compose)
1. Make sure that the WB_PATH environment variable is set.
2. Make sure that private.py exists with the required constants.
3. From the repository root, use docker-compose to start listening for pull requests:
```bash
docker-compose start
```

Stop WishBuilder-CLI with the stop command:
```bash
docker-compose stop
````

### Results
Status reports are emailed to the github user who created the last commit to the pull request, provided that they have configured their email address using
 their git --config settings.