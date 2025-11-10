# DFTP (DataFrame Transfer Protocol) & DACP(Data Access and Collaboration Protocol)

A high-performance protocol for dataframe transfer and processing.



## Overview

**DFTP (DataFrame Transfer Protocol)** is a fundamental data transfer protocol that provides efficient streaming data transfer capabilities, focusing on the delivery of raw data across nodes and processes (such as byte stream transfer and transmission reliability assurance).

**DACP (Data Access and Communication Protocol)** is a communication protocol designed to support cross-node, cross-process data access in scientific and distributed computing environments. DACP provides standardized streaming-based data interactions over the **Apache Arrow Flight protocol** and defines a unified Streaming DataFrame (SDF) model, which acts as a high-performance abstraction for accessing and processing both structured and unstructured data.

This project is **Powered by Apache Arrow Flight**, leveraging its efficient in-memory columnar format and high-performance RPC framework to enable fast, scalable data transfer across distributed systems.

DACP is based on the **Apache Arrow Flight protocol** to achieve standardized streaming interaction, while relying on the basic transmission capabilities of DFTP to form a protocol stack of "low-level transmission + high-level extension" to meet the full-link requirements of scientific computing from data transmission to collaborative processing.



## Key Features

- **Protocol Stack Design**: DFTP provides basic transmission capabilities, while DAP extends dataset management and collaboration functions. Together, they support end-to-end data flow.

- **Built on Apache Arrow Flight**: Utilizes Arrow's columnar memory format and Flight's RPC layer for zero-copy data transfer, minimizing serialization overhead.
- **Unified Streaming DataFrame (SDF) Model**: Abstracts structured/unstructured data access with a consistent API, supporting transformations and actions on streaming data.
- **Cross-Environment Compatibility**: Designed for scientific and distributed computing, enabling seamless data sharing between nodes, processes, and heterogeneous systems.
- **Extensible Protocol**: Supports custom authentication, data operations, and metadata exchange (via RDF/XML and JSON) for domain-specific requirements.

# Getting Started

## Prerequisites

- **Java 11+**
- **Scala 2.12+** (for Scala API)

---

## Installation

### DFTP (DataFrame Transfer Protocol)

Add the following dependencies to your Maven `pom.xml`:

```xml
<dependency>
    <groupId>link.rdcn</groupId>
    <artifactId>dftp-server</artifactId>
    <version>0.5.0-20251028</version>
</dependency>

<dependency>
    <groupId>link.rdcn</groupId>
    <artifactId>dftp-client</artifactId>
    <version>0.5.0-20251028</version>
</dependency>
```
### DACP (Data Access and Collaboration Protocol)
Add the following dependencies to your Maven `pom.xml`:

```xml
<dependency>
    <groupId>link.rdcn</groupId>
    <artifactId>catalog-module</artifactId>
    <version>0.5.0-20251028</version>
</dependency>

<dependency>
    <groupId>link.rdcn</groupId>
    <artifactId>cook-module</artifactId>
    <version>0.5.0-20251028</version>
</dependency>
```
### DFTP Usage
#### Server Deployment (Packaged Distribution)
A quick way to deploy DFTP server is using the packaged distribution, which does not require writing Scala code:
```bash
cd packaging
# Package the server
mvn clean package -P server-unix-dist

# Unpack the generated tarball in your local directory, e.g. /usr/local/
tar -zxvf dftp-server-<version>.tar.gz
cd /usr/local/dftp-server-<version>
```
#### Modify configuration (example):
```text
# Example dftp.conf
dftp.host.position=0.0.0.0
dftp.host.port=3101
```
#### Start the server:
```bash
bin/dftp-control.sh start
```
> After server startup, the data source is automatically taken from the `data` directory under the installation directory.
Clients can then connect using DftpClient as described in the client operations section.

#### DFTP Server Implementation
```scala
val userPasswordAuthService = new UserPasswordAuthService {
  override def authenticate(credentials: UsernamePassword): UserPrincipal = {
    // Perform authentication
    UserPrincipalWithCredentials(credentials)
  }
  // Return true if this module should handle the given credentials
  override def accepts(credentials: UsernamePassword): Boolean = true
}

val directoryDataSourceModule = new DirectoryDataSourceModule
// Set the root directory of the data source
directoryDataSourceModule.setRootDirectory(new File(baseDir))

val modules = Array(
  directoryDataSourceModule,
  new BaseDftpModule,
  new UserPasswordAuthModule(userPasswordAuthService),
  new PutModule,
  new ActionModule
)

// Start the DFTP server
dftpServer = DftpServer.start(DftpServerConfig("0.0.0.0"), modules)
```
#### DFTP Client Operations
##### Connect
```scala
// Connect using anonymous login
val dftpClient = DftpClient.connect("dftp://0.0.0.0:3101", Credentials.ANONYMOUS)

// Connect using username/password authentication
val dftpClient = DftpClient.connect(
  "dftp://0.0.0.0:3101",
  UsernamePassword("user", "password")
)
```
##### Data Operations
```scala
// Retrieve a DataFrame from a remote resource
val df = dftpClient.get("dftp://0.0.0.0:3101/resourcePath")

// Transformations
val dfMap = df.map(row => row._1).limit(10)
val dfFilter = df.filter(row => row._1 > 100).limit(10)
val dfSelect = df.select("col0", "col1").limit(10)

df.collect()                       // Retrieve all data
df.foreach(row => process(row))     // Process each row individually
```

### DACP Usage
#### Server Deployment (Packaged Distribution)
DACP server can be deployed in a similar way to DFTP.
```bash
cd catalog-module
# Package the server
mvn clean package -P server-unix-dist

# Unpack the generated tarball
tar -zxvf dacp-server-<version>.tar.gz
cd /usr/local/dacp-server-<version>
```
#### Modify configuration (example):
```text
dacp.host.position=0.0.0.0
dacp.host.port=3101
```
#### Start the server:
```bash
bin/dacp-control.sh start
```
> After server startup, the data source is automatically taken from the `data` directory under the installation directory.
Clients can then connect using DacpClient as described in the client operations section.


#### DACP Server Implementation
```scala
val directoryCatalogModule = new DirectoryCatalogModule()
directoryCatalogModule.setRootDirectory(new File(baseDir))

val permissionService = new PermissionService {
  override def accepts(user: UserPrincipal): Boolean = true

  // Validate whether the user has permission to perform certain operations
  override def checkPermission(user: UserPrincipal, dataFrameName: String, opList: List[DataOperationType]): Boolean =
    user.asInstanceOf[UserPrincipalWithCredentials].credentials match {
      case Credentials.ANONYMOUS => false
      case UsernamePassword(username, password) => true
    }
}

val modules = Array(
  new BaseDftpModule,
  new DacpCookModule,
  new DacpCatalogModule,
  new DirectoryDataSourceModule,
  directoryCatalogModule,
  new UserPasswordAuthModule(userPasswordAuthService), // Refer to DFTP server implementation
  new PermissionServiceModule(permissionService)
)

// Start the DACP server
server = DftpServer.start(
  DftpServerConfig("0.0.0.0", 3101)
    .withProtocolScheme("dacp"),
  modules
)
```
#### DACP Client Operations
##### Connection
```scala
// Connect using anonymous login
val dacpClient = DacpClient.connect("dacp://0.0.0.0:3101", Credentials.ANONYMOUS)

// Connect using username/password authentication
val dacpClient = DacpClient.connect(
  "dacp://0.0.0.0:3101",
  UsernamePassword("user", "password")
)
```
##### Metadata Operations
```scala
// List all available datasets
dacpClient.listDataSetNames()

// Retrieve dataset metadata
dacpClient.getDataSetMetaData("dataSet")

// List all data frames under a dataset
dacpClient.listDataFrameNames("dataSet")

// Retrieve metadata for a specific data frame
dacpClient.getDataFrameMetaData("/dataFramePath")

// Retrieve schema of a data frame
dacpClient.getSchema("/dataFramePath")

// Retrieve data frame size
dacpClient.getDataFrameSize("/dataFramePath")

// Retrieve the DataFrameDocument associated with the data frame
dacpClient.getDocument("/dataFramePath")

// Retrieve statistical information of a data frame
dacpClient.getStatistics("/dataFramePath")
```
##### Cook Operation
```scala
// Define a custom transformer
val udf = new Transformer11 {
  override def transform(dataFrame: DataFrame): DataFrame = {
    dataFrame.limit(5)
  }
}

// Define a simple transformation DAG
val transformerDAG = Flow(
  Map(
    "A" -> SourceNode("/dataFrame"),
    "B" -> udf
  ),
  Map(
    "A" -> Seq("B")
  )
)

// Execute the transformation flow
val dfs: ExecutionResult = dacpClient.cook(transformerDAG)
dfs.single().foreach(println)
```

## Powered by Apache Arrow Flight

DACP, as an extension of DFTP, enables efficient cross-node data exchange through Apache Arrow Flight, while DFTP focuses on underlying data transmission. Together, they form a complete protocol stack adapted to scientific computing scenarios.

- **Efficient Serialization**: Uses Arrow's in-memory format to avoid costly data copying between systems.
- **Flight RPC**: Leverages Flight's lightweight RPC protocol for low-latency, high-throughput data streaming.
- **Interoperability**: Compatible with other Arrow Flight-enabled systems, enabling integration with existing data processing pipelines.

Learn more about Apache Arrow Flight at https://arrow.apache.org/docs/format/Flight.html.
