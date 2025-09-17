# DFTP (DataFrame Transfer Protocol)

A high-performance protocol for dataframe transfer and processing.

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

| Method               | Description                          |
|----------------------|--------------------------------------|
| `setAuthHandler()`   | Sets authentication provider        |
| `setServiceHandler()`| Sets service implementation         |
| `setProtocolSchema()`| Configures custom protocol schema   |

#### DftpClient

| Method      | Returns    | Description                                  |
|-------------|------------|----------------------------------------------|
| `connect()` | DftpClient | Establishes connection                      |
| `get()`     | DataFrame  | Retrieves resource as DataFrame            |
| `put()`     | String     | Uploads data to server (takes DataFrame as parameter) |