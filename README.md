# GIF Compression Service

A fully non-blocking reactive service for compressing GIF images using Spring WebFlux with Java 21.

## Features

- 100% non-blocking reactive implementation
- Single GIF compression with configurable compression level
- Batch compression with parallel processing
- Reactive ZIP archive creation
- Rate limiting by client IP address
- Comprehensive error handling
- Cross-origin resource sharing (CORS) support
- Support for constrained environments (low CPU/memory)

## Technology Stack

- Java 21 with modern language features
- Spring Boot 3.2.x with Spring WebFlux
- Project Reactor for reactive programming
- Bucket4j for rate limiting
- SpringDoc OpenAPI for API documentation

## Architecture

The application is built using a fully non-blocking architecture:

1. **Controller Layer**: Uses reactive endpoints with `Mono` and `Flux` types
2. **Service Layer**: 
   - `GifCompressionService`: Orchestrates the entire compression workflow
   - `ReactiveGifProcessor`: Non-blocking GIF processing with parallel frame processing
   - `ReactiveZipCreator`: Non-blocking ZIP archive creation with backpressure handling
3. **Validation**: Reactive validation using `Mono` sequences
4. **Configuration**: Optimized WebFlux configuration

## API Endpoints

### Single GIF Compression

```
POST /api/compress
Content-Type: multipart/form-data
```

Parameters:
- `file`: GIF file to compress
- `compressionLevel`: Value between 0 and 1 (1 = no compression, 0 = maximum compression)

### Batch GIF Compression

```
POST /api/compress/batch
Content-Type: multipart/form-data
```

Parameters:
- `files`: Multiple GIF files to compress
- `compressionLevel`: Value between 0 and 1 (1 = no compression, 0 = maximum compression)

## Implementation Notes

### Non-blocking Approach

- **Reactive GIF Processing**: 
  - Individual frames are processed in parallel using Flux
  - Backpressure handling prevents memory overflow
  - Adaptive concurrency based on system load

- **Reactive ZIP Creation**:
  - Non-blocking stream processing for ZIP creation
  - Files are added to ZIP without loading all into memory
  - Parallel processing of entries when possible

- **Scheduler Usage**:
  - CPU-bound operations use `Schedulers.parallel()`
  - I/O-bound operations use `Schedulers.boundedElastic()`
  - Validation and orchestration on default scheduler

### Rate Limiting

Rate limiting is implemented using Bucket4j in a non-blocking way. Each client IP has its own rate limit bucket.

### Running in Constrained Environments

The application includes special optimization for running in resource-constrained environments:

- **Constrained Mode**: Enables adaptive processing to reduce resource usage
  - Sequential processing instead of parallel where appropriate
  - Reduced memory usage and buffer sizes
  - More conservative resource allocation

- **Running in Constrained Mode**:
  ```bash
  # Using PowerShell script
  ./run-constrained.ps1
  
  # Or using Docker
  docker run -e CONSTRAINED_MODE=true -p 8080:8080 gif-compression-service
  ```

- **Constrained Environment Profile**:
  - Automatically reduce batch sizes
  - Lower memory footprint
  - More aggressive resource monitoring

## Building and Running

### Prerequisites

- Java 21 or higher
- Maven 3.6 or higher

### Build

```bash
./mvnw clean package
```

### Run

```bash
# Standard mode
./mvnw spring-boot:run

# Constrained mode (for limited resources)
./run-constrained.ps1
```

## Running Tests

```
./mvnw test
```

## Docker Support

```bash
# Build docker image
docker build -t gif-compression-service .

# Run in standard mode
docker run -p 8080:8080 gif-compression-service

# Run in constrained mode for limited resources
docker run -e CONSTRAINED_MODE=true -p 8080:8080 gif-compression-service
```

## Built With

- [Spring Boot 3.x](https://spring.io/projects/spring-boot) - The main framework
- [Spring WebFlux](https://docs.spring.io/spring-framework/docs/current/reference/html/web-reactive.html) - For asynchronous request processing
- [Bucket4j](https://github.com/bucket4j/bucket4j) - Implementation of rate limiting
- [Animated-GIF-Lib](https://github.com/madgag/animated-gif-lib-for-java) - GIF compression library
- [Docker](https://www.docker.com/) - Containerization

## License

This project is licensed under the MIT License - see the LICENSE file for details