VideoDB README 

As of this writing, the VideoDB project is less than 72 hours old and remains under active development. Expect rapid changes and frequent updates.

See a demo here -> https://dgriebel2014.github.io/VideoDBProject/

BENCHMARK RESULTS - NVIDIA RTX 3090 24GB - EACH ROW IS 1KB

PUT

[RESULTS] Stress Test Performance:
   jsonStress: ADD=92,276 rec/sec, PUT=9,433 rec/sec, DEL=19,513 rec/sec
   float32Stress: ADD=369,846 rec/sec, PUT=9,948 rec/sec, DEL=19,454 rec/sec
   float64Stress: ADD=320,723 rec/sec, PUT=9,884 rec/sec, DEL=18,084 rec/sec
   int32Stress: ADD=374,717 rec/sec, PUT=9,806 rec/sec, DEL=18,616 rec/sec
   uint8Stress: ADD=425,966 rec/sec, PUT=9,940 rec/sec, DEL=19,257 rec/sec
[RESULTS] Total Data Transferred (ADD/PUT): 7.78 GB

GET

[TASK] Accuracy Test / GET Benchmark
[INFO] Successfully created VideoDB instance.
[STEP] Creating 1 store(s), each with 10000 records...
[SUCCESS] All stores created and populated.
[STEP] Verifying accuracy on 1000 random rows (out of 10000)...
[SUCCESS] All sampled records match exactly! Accuracy confirmed.
[STEP] Starting SEQUENTIAL GET performance test (3 seconds)...
[RESULTS] SEQUENTIAL GET: ~301 GETs/sec
[STEP] Starting RANDOM GET performance test (3 seconds)...
[RESULTS] RANDOM GET: ~297 GETs/sec
[STEP] Starting NON-CONTIGUOUS BATCHED GET performance test (3 seconds)...
[RESULTS] NON-CONTIGUOUS BATCHED GET: ~5,657 GETs/sec
[STEP] Starting CONTIGUOUS BATCHED GET performance test (3 seconds)...
[RESULTS] CONTIGUOUS BATCHED GET: ~7,392 GETs/sec
[INFO] Accuracy Test / GET Benchmark Completed.

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
