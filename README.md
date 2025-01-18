# VideoDB README

**January 17, 2025 Update**

I’ve overhauled the GPU buffer usage flags so that VideoDB now allocates its memory directly in main VRAM, rather than relying on shared system memory. Previously, buffers were placed in repurposed system RAM. With this change, the GPU holds VideoDB data fully on its own hardware, helping preserve bandwidth and reduce latency for large or frequently accessed datasets.

## Performance Tip

Run a stress test while keeping your system’s performance monitor open to watch GPU usage. You can also leave it open when paging through a large dataset—just hold down the Next button—and see the GPU actively working to serve your data.

As of this writing, the VideoDB project is less than 72 hours old and remains under active development. Expect rapid changes and frequent updates.

**See a demo here** -> [VideoDB Project Demo](https://dgriebel2014.github.io/VideoDBProject/)

## Benchmarks Overview

### Stress Test (1 KB rows)
- **Add:** Up to ~500K records/sec (that’s ~500 MB/s of data!)
- **Put:** ~10K records/sec
- **Delete:** 15K–20K records/sec
- **Total Data Moved (Add + Put):** Over 8 GB

### Accuracy & GET Benchmark
- **Single-Record GET:** ~300+ records/sec (sequential or random)
- **Batched GET:** Up to ~200K records/sec (contiguous)

**Key Takeaway:** Bulk operations drastically improve throughput—so reading with `getMultiple([...])` can reach hundreds of thousands of gets per second!

## System Specs

- **Model:** Alienware Aurora R12
- **CPU:** 11th Gen Intel® Core™ i9-11900KF (8 cores / 16 threads)
- **RAM:** 128 GB
- **GPU:** NVIDIA GeForce RTX 3090
- **OS:** Windows 11 Home (10.0.22631 Build 22631)

VideoDB leverages WebGPU on this high-end hardware, demonstrating that massive throughput in a browser-based data store is achievable for both write-intensive and read-intensive workloads.

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

# VideoDB Class Documentation

The `VideoDB` class provides functionality for managing object stores with **CPU-based metadata**. Each object store can represent a collection of key-value pairs (or “rows”), fully tracked on the CPU, while the actual row data resides in GPU buffers for efficiency. This class implements a deferred-write approach to minimize the overhead of frequent GPU operations.

---

## Constructor

### `constructor(device)`

---

- **Parameters**
  - `device` (`GPUDevice`): The WebGPU device instance used to allocate GPU buffers.

- **Behavior**  
  Initializes internal data structures for CPU-based metadata (`storeMetadataMap`) and a map of store keys to row metadata (`storeKeyMap`).  
  Also sets up a mechanism (`pendingWrites`) for batching GPU write operations.

- **Example**
  ```js
  // Suppose you obtained 'device' from navigator.gpu.requestAdapter()
  const videoDB = new VideoDB(device);
  ```

---

## Methods

### `createObjectStore(storeName, options)`

---

- **Parameters**
  - `storeName` (`string`): The name for the new store.
  - `options` (`object`):
    - `dataType` (`"TypedArray"|"ArrayBuffer"|"JSON"`): The data format.
    - `typedArrayType` (optional): The typed array constructor name (e.g. `"Float32Array"`, `"Float64Array"`, `"Uint32Array"`, `"Uint8Array"`).
    - `bufferSize` (`number`): The chunk size for GPU buffers (e.g., 250 MB).
    - `rowSize` (`number`, optional): The row’s fixed size (if relevant).
    - `totalRows` (`number`): A hint for total row capacity.

- **Behavior**
  - Throws an error if the store already exists.
  - If `dataType` is `"TypedArray"` but `typedArrayType` is missing, an error is thrown.
  - Calculates `rowsPerBuffer` if `dataType` is not `"JSON"` and `rowSize` is provided.
  - Initializes store metadata (e.g., `buffers`, `rows`, `rowSize`, `bufferSize`).
  - Stores the metadata in `storeMetadataMap` and creates a key map in `storeKeyMap`.

- **Example**
  ```js
  videoDB.createObjectStore("myFloatStore", {
    dataType: "TypedArray",
    typedArrayType: "Float32Array",
    bufferSize: 250 * 1024 * 1024,
    totalRows: 1000
  });
  ```

---

### `deleteObjectStore(storeName)`

---

- **Parameters**
  - `storeName` (`string`): The name of the store to delete.

- **Returns**
  `void`

- **Behavior**
  - If the store doesn’t exist, logs a warning and returns.
  - Removes the store’s metadata from `storeMetadataMap`.
  - Removes the store’s key map from `storeKeyMap`.

- **Example**
  ```js
  videoDB.deleteObjectStore("myFloatStore");
  ```

---

### `listObjectStores()`

---

- **Parameters**  
  None

- **Returns**
  `string[]`: The list of all object store names.

- **Behavior**
  Returns the keys of `storeMetadataMap` as an array, indicating all known stores.

- **Example**
  ```js
  const stores = videoDB.listObjectStores();
  console.log(stores); // e.g. ["myFloatStore", "jsonStore"]
  ```

---

### `add(storeName, key, value)`

---

- **Parameters**
  - `storeName` (`string`): The name of the object store.
  - `key` (`string`): A unique key identifying the row.
  - `value` (`any`): The data (JSON, TypedArray, or ArrayBuffer).

- **Returns**
  `Promise<void>`

- **Behavior**
  - Throws an error if the store does not exist.
  - Throws an error if the key already exists (in “add” mode).
  - Serializes the `value` into an `ArrayBuffer` and queues a GPU write in `pendingWrites` without immediate flush.
  - Flushes to GPU automatically after 1 second of inactivity or if a batch size threshold is reached.

- **Example**
  ```js
  // Adding a new JSON record
  await videoDB.add("jsonStore", "record1", { foo: "bar", time: Date.now() });
  ```

---

### `put(storeName, key, value)`

---

- **Parameters**
  - `storeName` (`string`): The target store name.
  - `key` (`string`): A unique string identifying the row.
  - `value` (`any`): The data (JSON, TypedArray, or ArrayBuffer).

- **Returns**
  `Promise<void>`

- **Behavior**
  - If the row doesn’t exist, it’s created; if it does, it’s overwritten (upsert behavior).
  - Serializes `value` into an `ArrayBuffer` and queues a GPU write in `pendingWrites`.
  - Flushes to GPU automatically after 1 second of inactivity or if a batch size threshold is reached.

- **Example**
  ```js
  // Inserting a Float32Array into "embeddingStore"
  const vector = new Float32Array([1, 2, 3, 4]);
  await videoDB.put("embeddingStore", "vector:001", vector);
  ```

---

### `get(storeName, key)`

---

- **Parameters**
  - `storeName` (`string`): The name of the object store.
  - `key` (`string`): The key identifying which row to read. Supports SQL wildcard patterns.

- **Returns**
  `Promise<any|null>`: Deserialized data or `null` if not found.

- **SQL Wildcards**  
  This method supports SQL Server–style `LIKE` patterns in the `key` parameter. If the key contains wildcard characters, it retrieves the first matching key in the store. Otherwise, the key is read as-is.
  - `%` matches zero or more characters (`.*` in regex).
  - `_` matches exactly one character (`.` in regex).
  - `[abc]` matches any single character in the set `a`, `b`, `c`.
  - `[^abc]` matches any single character *not* in the set `a`, `b`, `c`.
  
  These patterns must match the entire key string from start to finish.

- **Behavior**
  - Ensures all pending writes are flushed before reading (no stale data).
  - Uses the `getMultiple` method internally, allowing the use of wildcard patterns.
  - If the `key` contains wildcards, retrieves the first matching entry; otherwise, retrieves the specified key.
  - Returns `null` if no matching key is found.

- **Example**
  ```js
  // Retrieving a specific key
  const vector = await videoDB.get("embeddingStore", "vector:001");
  console.log("Retrieved vector length:", vector.length);

  // Using a wildcard pattern to retrieve the first matching key
  const firstMatchingVector = await videoDB.get("embeddingStore", "vector:%");
  console.log("First matching vector:", firstMatchingVector);
  ```

---

### `getMultiple(storeName, keys)`  
### `getMultiple(storeName, skip, take)`

---

This method supports two distinct overloads:

1. **Overload #1**: `getMultiple(storeName: string, keys: string[]): Promise<(any | null)[]>`
   - **Parameters:**
     - `storeName` (`string`): The target store name.
     - `keys` (`string[]`): An array of keys or wildcard patterns to fetch.
   - **Returns:** `Promise<(any | null)[]>`

2. **Overload #2**: `getMultiple(storeName: string, skip: number, take: number): Promise<(any | null)[]>`
   - **Parameters:**
     - `storeName` (`string`): The target store name.
     - `skip` (`number`): The zero-based offset at which to start reading rows.
     - `take` (`number`): The number of rows to read from that offset.
   - **Returns:** `Promise<(any | null)[]>`

**SQL Wildcards (Overload #1 with `keys`):**  
This overload supports SQL Server–style `LIKE` patterns in the `keys` array. Each pattern is expanded to all matching keys in the store before performing a single bulk read. Examples:
- `%` matches zero or more characters (`.*` in regex).
- `_` matches exactly one character (`.` in regex).
- `[abc]` matches any single character in the set `a`, `b`, `c`.
- `[^abc]` matches any single character *not* in that set.

These patterns must match the entire key. If no actual keys match a wildcard, the corresponding result item is `null`.

**Examples:**
```js
// Overload #1: Retrieving multiple specific keys
const items = await videoDB.getMultiple("embeddingStore", ["vector:001", "vector:002"]);
console.log("Results:", items);

// Overload #1: Using wildcard patterns
const wildcardItems = await videoDB.getMultiple("embeddingStore", ["vector:%", "config:%"]);
console.log("Wildcard results:", wildcardItems);

// Overload #2: Fetch by pagination
const pagedItems = await videoDB.getMultiple("embeddingStore", 10, 5);
console.log("Paged results:", pagedItems);
```

---

## `delete(storeName, key)`

---

- **Parameters**
  - `storeName` (`string`): The name of the object store.
  - `key` (`string`): The key identifying which row to delete.

- **Returns**
  `Promise<void>`

- **Behavior**
  - If the row is not found or already inactive, logs a message and returns.
  - Optionally writes zeros to the GPU buffer for that row (for a “true” data wipe).
  - Queues a delete operation in `pendingWrites`; actual metadata removal happens on flush.

- **Example**
  ```js
  await videoDB.delete("myFloatStore", "row123");
  ```

---

### `clear(storeName)`

---

- **Parameters**
  - `storeName` (`string`): The name of the object store to clear.

- **Returns**
  `void`

- **Behavior**
  - Removes all rows from the store’s metadata array.
  - Destroys all GPU buffers associated with the store.
  - Recreates a single fresh GPU buffer for subsequent usage.
  - Clears the store’s key map and updates metadata.

- **Example**
  ```js
  videoDB.clear("myFloatStore");
  ```

---

### `openCursor(storeName, options?)`

---

- **Parameters**
  - `storeName` (`string`): The name of the object store to iterate over.
  - `options` (optional, `object`):
    - `range` (`object`):
      - `lowerBound` (`string`): The lower bound of keys to include.
      - `upperBound` (`string`): The upper bound of keys to include.
      - `lowerInclusive` (`boolean`): Whether the lower bound is inclusive.
      - `upperInclusive` (`boolean`): Whether the upper bound is inclusive.
    - `direction` (`"next"` | `"prev"`): The direction of iteration.

- **Returns**
  `AsyncGenerator<{ key: string; value: any }, void, unknown>`

- **Behavior**
  - Retrieves store metadata and the key map.
  - Collects all active keys from the key map.
  - Optionally filters them by the specified `range`.
  - Sorts keys in ascending (`"next"`) or descending (`"prev"`) order.
  - For each key, retrieves data via `get()` and yields `{ key, value }`.

- **Example**
  ```js
  for await (const record of videoDB.openCursor("myFloatStore")) {
    console.log(record.key, record.value);
  }

  const range = { lowerBound: "100", upperBound: "200", lowerInclusive: true, upperInclusive: false };
  for await (const record of videoDB.openCursor("myFloatStore", { range, direction: "prev" })) {
    console.log(record.key, record.value);
  }
  ```

## License

`VideoDB` is released under the [MIT License](https://opensource.org/licenses/MIT).  
Feel free to use, modify, and distribute this software under its terms.
