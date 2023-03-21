# Setting up the Environment

This repository runs AWS managed Apache Airflow and a Neo4j instance locally.

## Prerequisites

- **macOS**: [Install Docker Desktop](https://docs.docker.com/desktop/).
- **Linux/Ubuntu**: [Install Docker Compose](https://docs.docker.com/compose/install/) and [Install Docker Engine](https://docs.docker.com/engine/install/).
- **Windows**: Windows Subsystem for Linux (WSL) to run the bash based command `mwaa-local-env`. Please follow [Windows Subsystem for Linux Installation (WSL)](https://docs.docker.com/docker-for-windows/wsl/) and [Using Docker in WSL 2](https://code.visualstudio.com/blogs/2020/03/02/docker-in-wsl2), to get started.

* For windows Setup, you may need to change the line endings of bash files
* Windows might have problem in operating volumes hosted in windows from docker instances


## Get started

```bash
git clone https://github.com/sujinsmailbox/neo4jingestor.git
cd neo4jingestor
```

### Step one: Building the Docker image

Build the Docker container image using the following command:

```bash
./airflow-local-env build-image
```

**Note**: it takes several minutes to build the Docker image locally.

### Step two: Running Apache Airflow and Neo4j

#### Local runner

Runs a local Apache Airflow environment and Neo4j instance

```bash
./airflow-local-env start
```

To stop the local environment, Ctrl+C on the terminal and wait till the local runner and the postgres containers are stopped.

### Step three: Accessing the Airflow UI

By default, the `bootstrap.sh` script creates a username and password for your local Airflow environment.

- Username: `admin`
- Password: `test`

#### Airflow UI

- Open the Apache Airlfow UI: <http://localhost:8080/>.

#### Accessing Neo4j UI

Open the Neo4j UI: <http://localhost:7474/>.

- Username: neo4j
- Password: neo4j

* Change in user credentials should be updated in dags/ingestor/neo4j.py ( This can be easily set as a connection in airflow)

### Step four: Setting up the repo

* All the data should be kept inside dags/data/ folder
* The xsd file should be kept for dags/config/ folder

Connection variable should be updated with the IP of the container which runs the neo4j. To do this, get the container ip first 
login into airflow webserver container and check the connection tp neo4j container
```
nc -vz <neo4jcontainer_id> 7474
```
update the output IP into the connection used in dags/ingestor/neo4j.py ( This can be easily set as a connection in airflow)

### Step four: Running the dag

* Open the Airflow UI and run the dag with the name `ingest_protien_data` and wait for its completion
* Open the Ne04j database and query the entries

```
MATCH (n) Return n LIMIT 25000

```


