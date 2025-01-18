VideoDB README 

January 17, 2025 Update
I’ve overhauled the GPU buffer usage flags so that VideoDB now allocates its 
memory directly in main VRAM, rather than relying on shared system memory. 
Previously, buffers were placed in repurposed system RAM. With this change, 
the GPU holds VideoDB data fully on its own hardware, helping preserve bandwidth 
and reduce latency for large or frequently accessed datasets.

Performance Tip:
Run a stress test while keeping your system’s performance monitor open to watch 
GPU usage. You can also leave it open when paging through a large dataset—just 
hold down the Next button—and see the GPU actively working to serve your data.
As of this writing, the VideoDB project is less than 72 hours old and remains 
under active development. Expect rapid changes and frequent updates.

See a demo here -> https://dgriebel2014.github.io/VideoDBProject/

Benchmarks Overview
Stress Test (1 KB rows)
Add: Up to ~500K records/sec (that’s ~500 MB/s of data!)
Put: ~10K records/sec
Delete: 15K–20K records/sec
Total Data Moved (Add + Put): Over 8 GB

Accuracy & GET Benchmark
Single-Record GET: ~300+ records/sec (sequential or random)
Batched GET: Up to ~200K records/sec (contiguous)

Key Takeaway: Bulk operations drastically improve throughput—so reading with 
getMultiple([...]) can reach hundreds of thousands of gets per second!

System Specs
Model: Alienware Aurora R12
CPU: 11th Gen Intel® Core™ i9-11900KF (8 cores / 16 threads)
RAM: 128 GB
GPU: NVIDIA GeForce RTX 3090
OS: Windows 11 Home (10.0.22631 Build 22631)

VideoDB leverages WebGPU on this high-end hardware, demonstrating that 
massive throughput in a browser-based data store is achievable for both 
write-intensive and read-intensive workloads.

# VideoDB

`VideoDB` is a powerful JavaScript class designed for managing object stores with **CPU-based metadata** and **GPU-backed data storage**, optimized for **AI/ML workflows**. It supports storing and retrieving key-value pairs, with data formats such as **TypedArrays**, **JSON**, and raw **ArrayBuffers**. Built on **WebGPU**, `VideoDB` enables high-performance data management, especially for applications involving embeddings, vectors, and other computational datasets.

- - -

## Features

*   **Efficient GPU Storage**: Data rows are stored in GPU buffers for fast access, while metadata is tracked on the CPU.
*   **Multi-Format Support**: Manage data in TypedArray formats (`Float32Array`, `Float64Array`, `Int32Array`, etc.), JSON, or raw binary (`ArrayBuffer`).
*   **Dynamic Store Management**:
    *   Create, list, delete object stores.
    *   Store, retrieve, and overwrite individual rows.
*   **Optimized for AI/ML Workflows**: Ideal for handling large datasets such as embeddings or tensors.
*   **Cross-Browser Compatibility**: Works in environments supporting **WebGPU** (e.g., Chrome, Edge).

- - -

## Table of Contents

*   [Installation](#installation)
*   [Usage](#usage)
*   [API Reference](#api-reference)
    *   [Constructor](#constructor)
    *   [Methods](#methods)
*   [Example Code](#example-code)
    *   [Working with JSON Stores](#working-with-json-stores)
*   [Contributing](#contributing)
*   [License](#license)

- - -

## Installation

### Prerequisites

1.  A browser or environment with **WebGPU** support:
    *   [Enable WebGPU in Chrome/Edge](https://developer.chrome.com/en/docs/webgpu)
2.  Install `VideoDB` as part of your project:
    
    ```
    npm install videodb
    ```
    

### Importing VideoDB

If you�re using a module-based environment (e.g., ES6 or TypeScript):

```
import { VideoDB } from 'videodb';
```

- - -

## Usage

### Quick Start

```
// 1. Obtain a WebGPU device
const device = await navigator.gpu.requestAdapter()
  .then(adapter => adapter.requestDevice());

// 2. Instantiate VideoDB
const videoDB = new VideoDB(device);

// 3. Create an object store
videoDB.createObjectStore("exampleStore", {
    dataType: "TypedArray",
    typedArrayType: "Float32Array",
    bufferSize: 1024 * 1024, // 1MB
    totalRows: 100
});

// 4. Add data
const vector = new Float32Array([1.23, 4.56, 7.89]);
await videoDB.put("exampleStore", "vector1", vector);

// 5. Retrieve data
const retrieved = await videoDB.get("exampleStore", "vector1");
console.log("Retrieved vector:", retrieved);

// 6. Delete the store
videoDB.deleteObjectStore("exampleStore");
```

- - -

## API Reference

### Constructor

#### `new VideoDB(device)`

*   **Parameters**:
    *   `device` (`GPUDevice`): The WebGPU device instance used to allocate GPU buffers.
*   **Description**:  
    Initializes the `VideoDB` instance with CPU-based metadata tracking and GPU buffer management.

- - -

### Methods

#### `createObjectStore(storeName, options)`

*   **Description**: Creates a new object store with specified metadata and GPU buffers.
*   **Parameters**:
    *   `storeName` (`string`): The name of the new store.
    *   `options` (`object`): Store configuration options:
        *   `dataType` (`"TypedArray"|"ArrayBuffer"|"JSON"`): Specifies the data format.
        *   `typedArrayType` (optional): Typed array constructor name (e.g., `"Float32Array"`).
        *   `bufferSize` (`number`): Size of the GPU buffer in bytes.
        *   `totalRows` (`number`): Hint for total row capacity.
*   **Example**:
    
    ```
    videoDB.createObjectStore("floatStore", {
        dataType: "TypedArray",
        typedArrayType: "Float32Array",
        bufferSize: 1024 * 1024,
        totalRows: 50
    });
    ```
    

#### `put(storeName, key, value)`

*   **Description**: Inserts or updates a row in the specified object store.
*   **Parameters**:
    *   `storeName` (`string`): The target store name.
    *   `key` (`string`): Unique key identifying the row.
    *   `value` (`any`): Data to store. For TypedArrays, it must match the `typedArrayType`.
*   **Example**:
    
    ```
    const vector = new Float32Array([1.1, 2.2, 3.3]);
    await videoDB.put("floatStore", "vectorKey", vector);
    ```
    

#### `get(storeName, key)`

*   **Description**: Retrieves a row's data by its key.
*   **Parameters**:
    *   `storeName` (`string`): The name of the object store.
    *   `key` (`string`): The key identifying the row.
*   **Returns**: A `Promise` resolving to the data or `null` if the key does not exist.
*   **Example**:
    
    ```
    const data = await videoDB.get("floatStore", "vectorKey");
    console.log(data);
    ```
    

#### `listObjectStores()`

*   **Description**: Lists all object store names.
*   **Returns**: An array of store names.
*   **Example**:
    
    ```
    const stores = videoDB.listObjectStores();
    console.log(stores);
    ```
    

#### `deleteObjectStore(storeName)`

*   **Description**: Deletes the specified object store and frees its GPU buffers.
*   **Parameters**:
    *   `storeName` (`string`): The name of the store to delete.
*   **Example**:
    
    ```
    videoDB.deleteObjectStore("floatStore");
    ```
    

- - -

## Example Code

### Working with JSON Stores

```
// Create a JSON-based object store
videoDB.createObjectStore("jsonStore", {
    dataType: "JSON",
    bufferSize: 1024 * 1024,
    totalRows: 50
});

// Add a JSON object
const user = { id: 123, name: "John Doe", preferences: { theme: "dark" } };
await videoDB.put("jsonStore", "user:123", user);

// Retrieve the JSON object
const retrievedUser = await videoDB.get("jsonStore", "user:123");
console.log("Retrieved JSON:", retrievedUser);

// Delete the store
videoDB.deleteObjectStore("jsonStore");
```

- - -

## Contributing

We welcome contributions to improve `VideoDB`. To contribute:

1.  Fork the repository on GitHub.
2.  Create a new branch for your feature or bugfix.
3.  Submit a Pull Request (PR) with a clear description of your changes.

- - -

## License

`VideoDB` is released under the [MIT License](https://opensource.org/licenses/MIT).  
Feel free to use, modify, and distribute this software under its terms.
