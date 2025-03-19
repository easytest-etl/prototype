# EasyTest-ETL: An Extensible Test Automation Framework for Functional Testing of Low-Code ETL Workflows

## Demonstration Video

**A short screencast demonstrating use of our low-code ETL testing plugins is provided [here](https://drive.google.com/drive/folders/1-cwK8ck3M4UidnAhcivJnCj97y-WzRZ1?usp=drive_link)**.


## Introduction

[ETL (Extract, Transform, and Load)](https://en.wikipedia.org/wiki/Extract%2C_transform%2C_load) is a data integration process that combines data from various different sources into a single data store according to the desired requirements, which is then loaded into a data warehouse.

This repository hosts our proposed ETL testing framework (which we've decided to name EasyTest-ETL) for functional testing of ETL workflows, particularly for no/low-code ETL data pipelines. We present a prototype to demonstrate functional testing of data pipelines on the [CDAP open-source framework](https://github.com/cdapio/cdap).

_This work has been communicated as a paper to [DATA 2025](https://data.scitevents.org/?y=2025)._

## Prototype implementation of plugins on the CDAP Platform

Our proposed framework focuses on the transformation step
of the ETL processes, and consists of three plugins for the
transformation stage, viz. _assertion_, _fixture_, and _mutation_. These plugins can be added as transform steps, thereby facilitating data validation and quality tests within the existing low-code ETL data pipeline. By adding them as a part of the transformation step, we retain the basic feature of the platform continuing to be low-code with minimal effort by the user. The framework has been prototyped on CDAP as a plug-in, and can be extended with minimal effort to other platforms too.



### Installation and Requirements

#### Requirements:

1. [OpenJDK 8](https://openjdk.org/install/) or any of its other derivatives, with Java Development Kit version 8 (for ARM-based systems such as the Raspberry Pi, [Temurin](https://adoptium.net/temurin/releases/?version=8) is recommended)
2. [Apache Maven](https://maven.apache.org/download.cgi) (version 3.0+) 
3. [Node.js](https://nodejs.org/en/download/package-manager)
4. [CDAP Sandbox](https://cdap.io/get-started/) (Linux/MacOS required; consider using WSL if on Windows)


Please note that the JDK version requirement is strict, since CDAP Sandbox doesn't support newer versions. We recommend Temurin version 8-LTS.

#### Installation procedure:

1. Install the aforementioned requirements, in the given sequence. Detailed installation steps for the individual components can be found on their respective website (or from the links above).

2. Clone this repository.

    `git clone https://github.com/easytest-etl/prototype`


[comment]: <>     (- `$JAVA_HOME` and `$PATH` [to be added based on Temurin install process])


3. Ensure that the environment variables are exported and set correctly.



   - The `$CDAP_HOME` environment variable should contain the path to the extacted CDAP Sandbox directory.

   - `$JAVA_HOME` and `$PATH` for the JDK should be set appropriately.

   - Path to the `bin` directory within the extracted Maven directory should be added to `$PATH` (as mentioned [here](https://maven.apache.org/install.html) in the official Maven installation documentation).

4. Ensure that CDAP Sandbox is running before installing EasyTest plugins. [This guide](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/480313428/Installing+the+CDAP+Sandbox+on+Linux+and+Mac+OSX) may be referred to for running CDAP Sandbox on Linux/MacOS.

5. Run `make` from within the (cloned) EasyTest repo directory. This might take some time for installation of the necessary packages via Maven. Successful installation of the plugins should display a "Done" message at the end. 


6. The installed plugins should show up shortly on the CDAP Studio page under the "Transforms" section (a reload might be necessary).




[comment]: <> (Running our test plugins on CDAP)

[comment]: <> (With all the installations complete as required, running `make start` from within the cloned `easytest-etl` repository folder will start CDAP Sandbox with the installed plugins loaded in. Running `make stop` will stop the CDAP Sandbox safely.)

#### A short word on our Makefile

The `all` target serves as the entry point, executing the `build` and `install` targets sequentially. The `build` target utilizes Maven to clean and package the plugin while skipping tests for faster execution. The `install` target handles the removal of any existing artifacts and loads the newly built plugin and its configuration into the CDAP environment.

Additionally, the `start` and `stop` targets allow for easy management of the CDAP sandbox. The `start` target initiates the sandbox environment, with an option to enable debugging for troubleshooting purposes, while the `stop` target cleanly shuts down the sandbox. 

[comment]: <> (### Pipelines demonstrated currently)

[comment]: <> (#### Assertions used for the pipelines)

[comment]: <> (#### Running our example pipelines)




## References

1. OpenJDK: [Website](https://openjdk.org/install/)

2. Temurin: [Website](https://adoptium.net/temurin/)

3. Node.js: [Website](https://nodejs.org/en)

4. Apache Maven: [Website](https://maven.apache.org/)

5. Cask Data Application Platform: [Website](https://cdap.io/), [Github Repo](https://github.com/cdapio/cdap), [Sandbox Installation Documentation](https://cdap.atlassian.net/wiki/spaces/DOCS/pages/480346167/CDAP+Sandbox)
