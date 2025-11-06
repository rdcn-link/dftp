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



## Getting Started

### Prerequisites

- Java 8+
- Scala 2.12+ (for Scala API)

### Installation

#### Maven

```xml
<dependency>
  <groupId>link.rdcn</groupId>
  <artifactId>dftp</artifactId>
  <version>0.5.0-20250917</version>
</dependency>
```

### Usage

##### Server Implementation

```scala
val dftpServer = new DftpServer()
  .setAuthHandler(new AuthenticatedProvider)
  .setServiceHandler(new DftpServiceHandler)
  .setProtocolSchema("dftp")  // Custom protocol schema
```

### Client Operations

#### Connection

```scala
// Anonymous login
val dftpClient = DftpClient.connect("dftp://ip:3101", Credentials.ANONYMOUS)

// Username/password login
val dftpClient = DftpClient.connect(
"dftp://ip:3101",
UsernamePassword("user", "password")
)
```

### Data Operations

```scala
// Get DataFrame from resource
val df = dftpClient.get("dftp://ip:3101/resourcePath")

// Transformations
val dfMap = df.map(row => row._1).limit(10)

val dfFilter = df.filter(row => row._1 > 100).limit(10)

val dfSelect = df.select("col0", "col1").limit(10)

// Actions
df.collect()  // Retrieve all data
df.foreach(row => process(row))  // Process each row
```

### API Reference

#### DftpServer

| Method                | Description                       |
| --------------------- | --------------------------------- |
| `setAuthHandler()`    | Sets authentication provider      |
| `setServiceHandler()` | Sets service implementation       |
| `setProtocolSchema()` | Configures custom protocol schema |

#### DftpClient

| Method      | Returns    | Description                                           |
| ----------- | ---------- | ----------------------------------------------------- |
| `connect()` | DftpClient | Establishes connection                                |
| `get()`     | DataFrame  | Retrieves resource as DataFrame                       |
| `put()`     | String     | Uploads data to server (takes DataFrame as parameter) |

## Powered by Apache Arrow Flight

DACP, as an extension of DFTP, enables efficient cross-node data exchange through Apache Arrow Flight, while DFTP focuses on underlying data transmission. Together, they form a complete protocol stack adapted to scientific computing scenarios.

- **Efficient Serialization**: Uses Arrow's in-memory format to avoid costly data copying between systems.
- **Flight RPC**: Leverages Flight's lightweight RPC protocol for low-latency, high-throughput data streaming.
- **Interoperability**: Compatible with other Arrow Flight-enabled systems, enabling integration with existing data processing pipelines.

Learn more about Apache Arrow Flight at https://arrow.apache.org/docs/format/Flight.html.
