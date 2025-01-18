// Copyright © 2025 Jon Griebel. dgriebel2014@gmail.com - All rights reserved.
// Distributed under the MIT license.
// For convenience, define a simple flag for inactive rows, e.g. 0x1.
const ROW_INACTIVE_FLAG = 0x1;
/**
 * Rounds `value` up to the nearest multiple of `align`.
 */
function roundUp(value, align) {
    return Math.ceil(value / align) * align;
}
/**
 * Ensures the length of the provided JSON string is a multiple of 4 by adding trailing spaces.
 * @param jsonString - The original JSON string to pad.
 * @returns The padded JSON string with a UTF-8 length multiple of 4.
 */
function padJsonTo4Bytes(jsonString) {
    const encoder = new TextEncoder();
    const initialBytes = encoder.encode(jsonString).length;
    const remainder = initialBytes % 4;
    if (remainder === 0) {
        return jsonString;
    }
    const needed = 4 - remainder;
    return jsonString + " ".repeat(needed);
}
/**
 * Pads the given ArrayBuffer to make its byteLength a multiple of 4.
 * If already aligned, returns the original buffer.
 */
function padTo4Bytes(ab) {
    const remainder = ab.byteLength % 4;
    if (remainder === 0) {
        // Already aligned
        return ab;
    }
    const needed = 4 - remainder;
    const padded = new Uint8Array(ab.byteLength + needed);
    padded.set(new Uint8Array(ab), 0); // copy the original bytes
    // Extra bytes remain zero
    return padded.buffer;
}
/**
 * A VideoDB class that stores all metadata in CPU memory,
 * and actual data on the GPU
 */
export class VideoDB {
    device;
    storeMetadataMap;
    storeKeyMap;
    pendingWrites = [];
    BATCH_SIZE = 10000;
    flushTimer = null;
    isReady = true;
    jsonWorker;
    /**
     * Initializes a new instance of the VideoDB class.
     * @param {GPUDevice} device - The GPU device to be used for buffer operations.
     */
    constructor(device) {
        this.device = device;
        this.storeMetadataMap = new Map();
        this.storeKeyMap = new Map();
        this.jsonWorker = new Worker('./offsetsWorker.js');
    }
    // Then you add a method that uses the worker
    getJsonFieldOffsets(dataArray, sortDefinition) {
        return new Promise((resolve, reject) => {
            const onMessage = (ev) => {
                if (!ev.data)
                    return;
                if (ev.data.cmd === 'getJsonFieldOffsets_result') {
                    this.jsonWorker.removeEventListener('message', onMessage);
                    resolve(ev.data.result);
                }
            };
            this.jsonWorker.addEventListener('message', onMessage);
            // Post the request to the worker
            this.jsonWorker.postMessage({
                cmd: 'getJsonFieldOffsets',
                data: dataArray,
                sortDefinition,
            });
        });
    }
    /**
     * Creates a new object store with the specified configuration options.
     * @param {string} storeName - The name of the store to create.
     * @param {{
     *   dataType: "TypedArray" | "ArrayBuffer" | "JSON";
     *   typedArrayType?:
     *     | "Float32Array"
     *     | "Float64Array"
     *     | "Int32Array"
     *     | "Uint32Array"
     *     | "Uint8Array";
     *   bufferSize: number;
     *   rowSize?: number;
     *   totalRows: number;
     *   sortDefinition?: {
     *     name: string;
     *     sortFields: {
     *       sortColumn: string;
     *       path: string;
     *       sortDirection: "Asc" | "Desc";
     *     }[];
     *   }[];
     * }} options - The configuration options for the new store.
     * @returns {void} This method does not return anything.
     * @throws {Error} If the store already exists or the typedArrayType is missing when dataType is "TypedArray".
     */
    createObjectStore(storeName, options) {
        if (this.storeMetadataMap.has(storeName)) {
            throw new Error(`Object store "${storeName}" already exists.`);
        }
        if (options.dataType === "TypedArray" && !options.typedArrayType) {
            throw new Error(`typedArrayType is required when dataType is "TypedArray".`);
        }
        let rowsPerBuffer;
        // If it's not JSON and rowSize was specified, compute how many rows fit into bufferSize
        if (options.dataType !== "JSON" && options.rowSize) {
            rowsPerBuffer = Math.floor(options.bufferSize / options.rowSize);
        }
        const storeMetadata = {
            storeName,
            dataType: options.dataType,
            typedArrayType: options.typedArrayType,
            bufferSize: options.bufferSize,
            rowSize: options.rowSize,
            rowsPerBuffer,
            totalRows: options.totalRows,
            buffers: [],
            rows: [],
            metadataBuffer: undefined,
            dirtyMetadata: false,
            metadataVersion: 0,
            // Assign sort definitions if provided
            sortDefinition: options.sortDefinition ?? []
        };
        this.storeMetadataMap.set(storeName, storeMetadata);
        this.storeKeyMap.set(storeName, new Map());
    }
    /**
     * Deletes an existing object store by name.
     * @param {string} storeName - The name of the store to delete.
     * @returns {void} This method does not return anything.
     */
    deleteObjectStore(storeName) {
        if (!this.storeMetadataMap.has(storeName)) {
            console.warn(`Object store "${storeName}" does not exist.`);
            return;
        }
        this.storeMetadataMap.delete(storeName);
        this.storeKeyMap.delete(storeName);
    }
    /**
     * Retrieves a list of all existing object store names.
     * @returns {string[]} An array containing the names of all object stores.
     */
    listObjectStores() {
        return Array.from(this.storeMetadataMap.keys());
    }
    /**
     * Adds a new record to the specified store without immediately writing to the GPU.
     * Instead, it caches the data in a pending-writes array. Once no writes occur
     * for 1 second, this method triggers a flush to the GPU.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @param {any} value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns {Promise<void>} A promise that resolves when the data is queued for writing.
     * @throws {Error} If the store does not exist or a record with the same key is already active (in "add" mode).
     */
    async add(storeName, key, value) {
        this.isReady = false;
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        const keyMap = this.storeKeyMap.get(storeName);
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);
        // Find/create the CPU row metadata, but do NOT write to GPU here.
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, "add");
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        // Queue up the write instead of writing immediately to GPU:
        this.pendingWrites.push({
            storeMeta,
            rowMetadata,
            arrayBuffer,
            gpuBuffer,
            operationType: 'add'
        });
        // Reset the flush timer
        this.resetFlushTimer();
        // Check if batch size threshold is met
        await this.checkAndFlush();
    }
    /**
     * Stores or updates data in the specified store without immediately writing to the GPU.
     * Instead, it caches the data in a pending-writes array. Once no writes occur
     * for 250 ms, this method triggers a flush to the GPU.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @param {any} value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns {Promise<void>} A promise that resolves when the data is queued for writing.
     * @throws {Error} If the store does not exist.
     */
    async put(storeName, key, value) {
        this.isReady = false;
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        const keyMap = this.storeKeyMap.get(storeName);
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);
        // "put" mode allows overwrites if the key already exists.
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, "put");
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        // Queue up the write instead of writing immediately to GPU:
        this.pendingWrites.push({
            storeMeta,
            rowMetadata,
            arrayBuffer,
            gpuBuffer,
            operationType: 'put'
        });
        // Reset the flush timer
        this.resetFlushTimer();
        // Check if batch size threshold is met
        await this.checkAndFlush();
    }
    /**
     * Retrieves data for a specific key from the GPU-backed store by utilizing the getMultiple method.
     * Ensures all pending writes are flushed before reading.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @returns {Promise<any | null>} A promise that resolves with the retrieved data or null if not found.
     * @throws {Error} If the store does not exist.
     */
    async get(storeName, key) {
        // Call getMultiple with a single key
        const results = await this.getMultiple(storeName, [key]);
        // Extract the first (and only) result
        const result = results[0];
        return result;
    }
    async getMultiple(storeName, param2, param3) {
        if (Array.isArray(param2)) {
            // Overload 1: Fetch by keys
            const keys = param2;
            const { results } = await this.getMultipleByKeys(storeName, keys);
            return results;
        }
        else if (typeof param2 === 'number' && typeof param3 === 'number') {
            // Overload 2: Fetch by pagination (skip and take)
            const skip = param2;
            const take = param3;
            // Flush & retrieve store metadata
            const { keyMap } = await this.flushAndGetMetadata(storeName);
            // Convert the store’s keyMap into an array of all keys
            const allKeys = Array.from(keyMap.keys());
            const { results } = await this.readRowsWithPagination(storeName, allKeys, skip, take);
            return results;
        }
        else {
            throw new Error('Invalid parameters for getMultiple. Expected either (storeName, keys[]) or (storeName, skip, take).');
        }
    }
    /**
     * Reads rows from the store based on skip and take parameters.
     *
     * @param storeName - The target store name.
     * @param storeMeta - Metadata of the store.
     * @param skip - Number of records to skip.
     * @param take - Number of records to take.
     * @returns An object containing the results and per-key metrics.
     */
    async readRowsWithPagination(storeName, allKeys, skip, take) {
        // Slice the keys array to get the paginated keys
        const paginatedKeys = allKeys.slice(skip, skip + take);
        // Use getMultipleByKeys to fetch the data
        const { results, perKeyMetrics } = await this.getMultipleByKeys(storeName, paginatedKeys);
        return { results, perKeyMetrics };
    }
    /**
     * Helper method to handle fetching multiple records by keys.
     *
     * @param storeName - The target store name.
     * @param keys - The array of keys (or wildcard patterns) to read.
     * @returns An object containing the deserialized data array and per-key metrics.
     */
    async getMultipleByKeys(storeName, keys) {
        // Flush & retrieve store metadata
        const { storeMeta, keyMap, metrics } = await this.flushAndGetMetadata(storeName);
        // Expand any wildcard patterns
        const expandedKeys = this.expandAllWildcards(keys, keyMap);
        // Read all rows based on expanded keys
        const { results, perKeyMetrics } = await this.readAllRows(storeName, storeMeta, keyMap, expandedKeys);
        // Log or accumulate performance
        this.logPerformance(metrics, perKeyMetrics);
        return { results, perKeyMetrics };
    }
    /**
     * Deletes data for a specific key from the GPU-backed store by batching the delete operation.
     * The actual GPU write and metadata update are deferred until `flushWrites` is called.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row to delete.
     * @returns {Promise<void>} A promise that resolves once the delete operation is queued.
     * @throws {Error} If the store does not exist.
     */
    async delete(storeName, key) {
        // Retrieve store metadata and key map
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        // Find row metadata for the active row
        const rowMetadata = this.findActiveRowMetadata(keyMap, key, storeMeta.rows);
        if (!rowMetadata) {
            return;
        }
        // Create a zeroed ArrayBuffer to overwrite the row data (optional)
        const zeroedArrayBuffer = new ArrayBuffer(rowMetadata.length);
        const zeroedView = new Uint8Array(zeroedArrayBuffer);
        zeroedView.fill(0); // Optional: Fill with zeros for "true" deletion
        // Queue the delete operation as a pending write
        this.pendingWrites.push({
            storeMeta,
            rowMetadata,
            arrayBuffer: zeroedArrayBuffer,
            gpuBuffer: this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex),
            operationType: 'delete',
            key // Include the key for metadata updates during flush
        });
        // Reset the flush timer
        this.resetFlushTimer();
        // Check if batch size threshold is met
        await this.checkAndFlush();
    }
    /**
     * Removes all rows from the specified object store, destroys all GPU buffers,
     * and then recreates a single fresh buffer for subsequent usage.
     *
     * @param {string} storeName - The name of the object store to clear.
     * @returns {void}
     * @throws {Error} If the specified store does not exist.
     */
    clear(storeName) {
        // Retrieve store metadata and keyMap
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        // Discard all row metadata
        storeMeta.rows = [];
        // Destroy all existing GPU buffers
        for (const bufferMeta of storeMeta.buffers) {
            if (bufferMeta.gpuBuffer) {
                bufferMeta.gpuBuffer.destroy();
            }
        }
        // Clear the array of buffers
        storeMeta.buffers = [];
        // CHANGED: Recreate a single new GPU buffer with only COPY_SRC | COPY_DST
        // REMOVED: MAP_WRITE
        const newGpuBuffer = this.device.createBuffer({
            size: storeMeta.bufferSize,
            usage: GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
            mappedAtCreation: false
        });
        // Add the newly created buffer to the store's metadata
        storeMeta.buffers.push({
            bufferIndex: 0,
            startRow: -1,
            rowCount: 0,
            gpuBuffer: newGpuBuffer
        });
        // Clear the keyMap so there are no active keys
        keyMap.clear();
        // Update store metadata and version
        this.updateStoreMetadata(storeMeta);
    }
    /**
     * Opens a cursor for iterating over records in the specified object store.
     *
     * @param {string} storeName - The name of the object store to iterate over.
     * @param {{
     *   range?: {
     *     lowerBound?: string;
     *     upperBound?: string;
     *     lowerInclusive?: boolean;
     *     upperInclusive?: boolean;
     *   };
     *   direction?: 'next' | 'prev';
     * }} [options] - Optional parameters to filter and control the iteration.
     * @returns {AsyncGenerator<{ key: string; value: any }, void, unknown>} An async generator yielding key-value pairs.
     * @throws {Error} If the specified store does not exist.
     *
     * @example
     * for await (const record of videoDB.openCursor('MyStore')) {
     *     console.log(record.key, record.value);
     * }
     *
     * @example
     * const range = { lowerBound: '100', upperBound: '200', lowerInclusive: true, upperInclusive: false };
     * for await (const record of videoDB.openCursor('MyStore', { range, direction: 'prev' })) {
     *     console.log(record.key, record.value);
     * }
     */
    async *openCursor(storeName, options) {
        // Retrieve store metadata and keyMap
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        // Retrieve all active keys
        let activeKeys = Array.from(keyMap.keys());
        // Apply key range filtering if a range is provided
        if (options?.range) {
            activeKeys = this.applyCustomRange(activeKeys, options.range);
        }
        // Sort keys based on direction
        if (options?.direction === 'prev') {
            activeKeys.sort((a, b) => this.compareKeys(b, a));
        }
        else {
            // Default to 'next' direction
            activeKeys.sort((a, b) => this.compareKeys(a, b));
        }
        // Iterate over the sorted, filtered keys and yield records
        for (const key of activeKeys) {
            const rowMetadata = keyMap.get(key);
            if (rowMetadata == null) {
                continue; // Skip if no row metadata found
            }
            const record = await this.get(storeName, key);
            if (record !== null) {
                yield { key, value: record };
            }
        }
    }
    /**
     * Checks whether the number of pending writes has reached the batch size threshold.
     * If the threshold is met, it clears any existing flush timer and immediately flushes the pending writes to the GPU.
     *
     * This method is called after each write operation (`add`, `put`, `delete`) to ensure that writes are batched efficiently.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once the flush operation (if triggered) completes.
     */
    async checkAndFlush() {
        if (this.pendingWrites.length >= this.BATCH_SIZE) {
            if (this.flushTimer !== null) {
                clearTimeout(this.flushTimer);
                this.flushTimer = null;
            }
            // Await the flush here
            await this.flushWrites();
        }
    }
    /**
     * Flushes all pending writes (ADD, PUT, DELETE) to the GPU in a batched manner.
     *
     * CHANGED: Instead of mapping the target GPU buffer (which used MAP_WRITE),
     * we now use the WebGPU `queue.writeBuffer(...)` to copy CPU data directly into the GPU buffer.
     * This removes all direct `mapAsync` calls on our store buffers.
     *
     * @returns {Promise<void>} A promise that resolves once all pending writes have been applied.
     */
    async flushWrites() {
        if (this.pendingWrites.length === 0) {
            return;
        }
        // Group by GPU buffer
        const writesByBuffer = new Map();
        for (const item of this.pendingWrites) {
            const { gpuBuffer } = item;
            if (!writesByBuffer.has(gpuBuffer)) {
                writesByBuffer.set(gpuBuffer, []);
            }
            writesByBuffer.get(gpuBuffer).push(item);
        }
        // We'll track which writes succeeded
        const successfulWrites = new Set();
        // For each GPU buffer, do a series of queue.writeBuffer
        for (const [gpuBuffer, writeGroup] of writesByBuffer.entries()) {
            // Sort the group by offset (lowest → highest) to remain consistent
            writeGroup.sort((a, b) => a.rowMetadata.offset - b.rowMetadata.offset);
            for (const pendingWrite of writeGroup) {
                try {
                    const { rowMetadata, arrayBuffer } = pendingWrite;
                    // CHANGED: Use queue.writeBuffer, not mapAsync
                    this.device.queue.writeBuffer(gpuBuffer, rowMetadata.offset, arrayBuffer);
                    successfulWrites.add(pendingWrite);
                }
                catch (singleWriteError) {
                    console.error('Error writing single item:', singleWriteError);
                }
            }
        }
        // If needed, ensure the commands complete
        // (In basic WebGPU usage, queue.writeBuffer is immediate for CPU → GPU data,
        // but you can await queue.onSubmittedWorkDone() if you need to ensure completion.)
        // await this.device.queue.onSubmittedWorkDone();
        // Remove successful writes from pendingWrites
        this.pendingWrites = this.pendingWrites.filter(write => !successfulWrites.has(write));
    }
    /**
     * Applies a custom key range to filter the provided keys.
     *
     * @param {string[]} keys - The array of keys to filter.
     * @param {{
     *   lowerBound?: string;
     *   upperBound?: string;
     *   lowerInclusive?: boolean;
     *   upperInclusive?: boolean;
     * }} range - The key range to apply.
     * @returns {string[]} The filtered array of keys that fall within the range.
     */
    applyCustomRange(keys, range) {
        return keys.filter((key) => {
            let withinLower = true;
            let withinUpper = true;
            if (range.lowerBound !== undefined) {
                if (range.lowerInclusive) {
                    withinLower = key >= range.lowerBound;
                }
                else {
                    withinLower = key > range.lowerBound;
                }
            }
            if (range.upperBound !== undefined) {
                if (range.upperInclusive) {
                    withinUpper = key <= range.upperBound;
                }
                else {
                    withinUpper = key < range.upperBound;
                }
            }
            return withinLower && withinUpper;
        });
    }
    /**
     * Retrieves the keyMap for a specific store, throwing an error if not found.
     *
     * @param {string} storeName - The name of the object store whose keyMap should be retrieved.
     * @returns {Map<string, number>} The key-to-rowId mapping for the specified store.
     * @throws {Error} If no keyMap is found for the given storeName.
     */
    getKeyMap(storeName) {
        const keyMap = this.storeKeyMap.get(storeName);
        if (!keyMap) {
            throw new Error(`Key map for store "${storeName}" is missing or uninitialized.`);
        }
        return keyMap;
    }
    /**
     * Compares two keys for sorting purposes.
     * Modify this method if your keys are not simple strings.
     *
     * @param {string} a - The first key.
     * @param {string} b - The second key.
     * @returns {number} Negative if a < b, positive if a > b, zero if equal.
     */
    compareKeys(a, b) {
        if (a < b)
            return -1;
        if (a > b)
            return 1;
        return 0;
    }
    /**
     * Finds the active row metadata for a given key.
     *
     * @param {Map<string, number>} keyMap - The key map for the store.
     * @param {string} key - The key to search for.
     * @param {RowMetadata[]} rows - The array of row metadata.
     * @returns {RowMetadata | null} The active RowMetadata or null if not found/inactive.
     */
    findActiveRowMetadata(keyMap, key, rows) {
        const rowId = keyMap.get(key);
        if (rowId == null) {
            return null;
        }
        const rowMetadata = rows.find((r) => r.rowId === rowId);
        if (!rowMetadata) {
            return null;
        }
        if ((rowMetadata.flags ?? 0) & ROW_INACTIVE_FLAG) {
            return null;
        }
        return rowMetadata;
    }
    /**
     * Updates the store metadata to indicate that a change has occurred.
     * This increments the metadata version and sets the `dirtyMetadata` flag.
     *
     * @param {StoreMetadata} storeMeta - The store’s metadata object to be updated.
     * @returns {void}
     */
    updateStoreMetadata(storeMeta) {
        storeMeta.dirtyMetadata = true;
        storeMeta.metadataVersion += 1;
    }
    /**
     * Retrieves the metadata object for a specified store.
     * @param {string} storeName - The name of the store.
     * @returns {StoreMetadata} The metadata object for the specified store.
     * @throws {Error} If the specified store does not exist.
     */
    getStoreMetadata(storeName) {
        const meta = this.storeMetadataMap.get(storeName);
        if (!meta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        return meta;
    }
    /**
     * Finds or creates a GPU buffer chunk that has enough space for the specified size.
     * @param {StoreMetadata} storeMeta - The metadata of the store where space is needed.
     * @param {number} size - The size in bytes required.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *          An object containing the GPU buffer, the buffer index, and the offset at which the data can be written.
     */
    findOrCreateSpace(storeMeta, size) {
        if (storeMeta.buffers.length === 0) {
            // No buffers exist yet; allocate the first one
            return this.allocateFirstBufferChunk(storeMeta, size);
        }
        // Otherwise, check the last buffer for available space
        const { lastBufferMeta, usedBytes } = this.getLastBufferUsage(storeMeta);
        const capacity = storeMeta.bufferSize;
        if (usedBytes + size <= capacity) {
            // There's enough space in the last buffer
            return this.useSpaceInLastBuffer(storeMeta, lastBufferMeta, usedBytes, size);
        }
        // Not enough space, so allocate a new buffer
        return this.allocateNewBufferChunk(storeMeta, size);
    }
    /**
     * CHANGED: Now creates a buffer with COPY_SRC | COPY_DST (no MAP_WRITE / MAP_READ).
     * @param {StoreMetadata} storeMeta - The metadata of the store that requires a new GPU buffer.
     * @param {number} size - The requested size (though we typically allocate storeMeta.bufferSize).
     */
    createNewBuffer(storeMeta, size) {
        // REMOVED: GPUBufferUsage.MAP_WRITE
        return this.device.createBuffer({
            size: storeMeta.bufferSize,
            usage: GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
            mappedAtCreation: false
        });
    }
    /**
     * Allocates and initializes the very first buffer in the store.
     * @param {StoreMetadata} storeMeta - The metadata of the store where the buffer is being created.
     * @param {number} size - The initial number of bytes needed in the new buffer.
     */
    allocateFirstBufferChunk(storeMeta, size) {
        const gpuBuffer = this.createNewBuffer(storeMeta, storeMeta.bufferSize);
        gpuBuffer._usedBytes = 0;
        storeMeta.buffers.push({
            bufferIndex: 0,
            startRow: -1,
            rowCount: 0,
            gpuBuffer
        });
        gpuBuffer._usedBytes = size;
        storeMeta.buffers[0].rowCount += 1;
        return {
            gpuBuffer,
            bufferIndex: 0,
            offset: 0
        };
    }
    /**
     * Retrieves usage information for the last buffer in the store.
     */
    getLastBufferUsage(storeMeta) {
        const lastIndex = storeMeta.buffers.length - 1;
        const lastBufferMeta = storeMeta.buffers[lastIndex];
        const gpuBuffer = lastBufferMeta.gpuBuffer;
        const usedBytes = gpuBuffer._usedBytes || 0;
        return { lastBufferMeta, usedBytes };
    }
    useSpaceInLastBuffer(storeMeta, lastBufferMeta, usedBytes, size) {
        const gpuBuffer = lastBufferMeta.gpuBuffer;
        const ALIGNMENT = 256;
        // Align the offset to the nearest multiple of ALIGNMENT (256)
        const alignedOffset = roundUp(usedBytes, ALIGNMENT);
        // Check if alignedOffset + size exceeds the usable buffer size
        if (alignedOffset + size > gpuBuffer.size) {
            return this.allocateNewBufferChunk(storeMeta, size);
        }
        gpuBuffer._usedBytes = alignedOffset + size;
        lastBufferMeta.rowCount += 1;
        const bufferIndex = lastBufferMeta.bufferIndex;
        return {
            gpuBuffer,
            bufferIndex,
            offset: alignedOffset
        };
    }
    /**
     * Allocates a new buffer chunk if the last one does not have enough space.
     */
    allocateNewBufferChunk(storeMeta, size) {
        const newBufferIndex = storeMeta.buffers.length;
        const capacity = storeMeta.bufferSize;
        const newGpuBuffer = this.createNewBuffer(storeMeta, capacity);
        newGpuBuffer._usedBytes = size;
        storeMeta.buffers.push({
            bufferIndex: newBufferIndex,
            startRow: -1,
            rowCount: 1,
            gpuBuffer: newGpuBuffer
        });
        return {
            gpuBuffer: newGpuBuffer,
            bufferIndex: newBufferIndex,
            offset: 0
        };
    }
    /**
     * Converts a given value into an ArrayBuffer based on the store's data type,
     * then pads it to 4 bytes (if needed) before returning.
     */
    serializeValueForStore(storeMeta, value) {
        let resultBuffer;
        switch (storeMeta.dataType) {
            case "JSON": {
                let jsonString = JSON.stringify(value);
                jsonString = padJsonTo4Bytes(jsonString);
                const cloned = new TextEncoder().encode(jsonString).slice();
                resultBuffer = cloned.buffer;
                break;
            }
            case "TypedArray": {
                if (!storeMeta.typedArrayType) {
                    throw new Error(`typedArrayType is missing for store "${storeMeta}".`);
                }
                if (!(value instanceof globalThis[storeMeta.typedArrayType])) {
                    throw new Error(`Value must be an instance of ${storeMeta.typedArrayType} for store "${storeMeta}".`);
                }
                resultBuffer = value.buffer;
                break;
            }
            case "ArrayBuffer": {
                if (!(value instanceof ArrayBuffer)) {
                    throw new Error(`Value must be an ArrayBuffer for store "${storeMeta}".`);
                }
                resultBuffer = value;
                break;
            }
            default:
                throw new Error(`Unknown dataType "${storeMeta.dataType}".`);
        }
        // *** Finally, ensure the buffer is 4-byte-aligned for WebGPU. ***
        return padTo4Bytes(resultBuffer);
    }
    /**
     * Finds or creates a RowMetadata entry for the given key.
     * The GPU write is deferred until a flush operation.
     */
    async findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, mode) {
        let rowId = keyMap.get(key);
        let rowMetadata = rowId == null
            ? null
            : storeMeta.rows.find((r) => r.rowId === rowId) || null;
        // If active row exists and we are in "add" mode, throw:
        if (mode === "add" && rowMetadata && !((rowMetadata.flags ?? 0) & ROW_INACTIVE_FLAG)) {
            throw new Error(`Record with key "${key}" already exists in store and overwriting is not allowed (add mode).`);
        }
        // Allocate space in a GPU buffer (just picks offset/bufferIndex, no write):
        const { gpuBuffer, bufferIndex, offset } = this.findOrCreateSpace(storeMeta, arrayBuffer.byteLength);
        // If row is new or inactive, create a fresh RowMetadata:
        if (!rowMetadata || ((rowMetadata.flags ?? 0) & ROW_INACTIVE_FLAG)) {
            rowId = storeMeta.rows.length + 1;
            rowMetadata = {
                rowId,
                bufferIndex,
                offset,
                length: arrayBuffer.byteLength
            };
            storeMeta.rows.push(rowMetadata);
            keyMap.set(key, rowId);
        }
        // If row is active and we’re in "put" mode, handle potential reallocation:
        else if (mode === "put") {
            rowMetadata = await this.updateRowOnOverwrite(storeMeta, rowMetadata, arrayBuffer, keyMap, key);
        }
        return rowMetadata;
    }
    /**
     * If the new data is larger than the existing row’s allocated space, this method
     * deactivates the old row and finds a new location in the GPU buffer.
     */
    async updateRowOnOverwrite(storeMeta, oldRowMeta, arrayBuffer, keyMap, key) {
        // If the new data fits in the old space:
        if (arrayBuffer.byteLength <= oldRowMeta.length) {
            // Overwrite in-place later during flushWrites.
            if (arrayBuffer.byteLength < oldRowMeta.length) {
                oldRowMeta.length = arrayBuffer.byteLength;
            }
            return oldRowMeta;
        }
        else {
            // Mark old row inactive
            oldRowMeta.flags = (oldRowMeta.flags ?? 0) | ROW_INACTIVE_FLAG;
            // Find new space for the bigger data
            const { gpuBuffer, bufferIndex, offset } = this.findOrCreateSpace(storeMeta, arrayBuffer.byteLength);
            const newRowId = storeMeta.rows.length + 1;
            const newRowMeta = {
                rowId: newRowId,
                bufferIndex,
                offset,
                length: arrayBuffer.byteLength
            };
            storeMeta.rows.push(newRowMeta);
            keyMap.set(key, newRowId);
            return newRowMeta;
        }
    }
    /**
     * Deserializes the raw data from the GPU buffer based on store metadata.
     */
    deserializeData(storeMeta, copiedData) {
        switch (storeMeta.dataType) {
            case "JSON": {
                const jsonString = new TextDecoder().decode(copiedData);
                return JSON.parse(jsonString.trim());
            }
            case "TypedArray": {
                if (!storeMeta.typedArrayType) {
                    throw new Error(`typedArrayType is missing for store with dataType "TypedArray".`);
                }
                const TypedArrayCtor = globalThis[storeMeta.typedArrayType];
                if (typeof TypedArrayCtor !== "function") {
                    throw new Error(`Invalid typedArrayType "${storeMeta.typedArrayType}".`);
                }
                return new TypedArrayCtor(copiedData.buffer);
            }
            case "ArrayBuffer": {
                return copiedData.buffer;
            }
            default:
                throw new Error(`Unknown dataType "${storeMeta.dataType}".`);
        }
    }
    /**
     * Retrieves the GPU buffer associated with the specified buffer index from the store's buffer metadata.
     */
    getBufferByIndex(storeMeta, bufferIndex) {
        const bufMeta = storeMeta.buffers[bufferIndex];
        if (!bufMeta || !bufMeta.gpuBuffer) {
            throw new Error(`Buffer index ${bufferIndex} not found or uninitialized.`);
        }
        return bufMeta.gpuBuffer;
    }
    /**
     * Resets the flush timer to delay writing pending operations to the GPU.
     */
    resetFlushTimer() {
        if (this.flushTimer !== null) {
            clearTimeout(this.flushTimer);
        }
        this.flushTimer = window.setTimeout(() => {
            this.flushWrites().catch(error => {
                console.error('Error during timed flushWrites:', error);
            });
            this.flushTimer = null;
            this.isReady = true;
        }, 250);
    }
    /**
     * Logs consolidated performance metrics to the console.
     */
    logPerformance(initialMetrics, perKeyMetrics) {
        console.log("** Performance Metrics for getMultiple **", {
            flushWrites: initialMetrics.flushWrites.toFixed(2) + "ms",
            metadataRetrieval: initialMetrics.metadataRetrieval.toFixed(2) + "ms",
            perKeyMetrics: {
                findMetadata: perKeyMetrics.findMetadata.toFixed(2) + "ms total",
                createBuffer: perKeyMetrics.createBuffer.toFixed(2) + "ms total",
                copyBuffer: perKeyMetrics.copyBuffer.toFixed(2) + "ms total",
                mapBuffer: perKeyMetrics.mapBuffer.toFixed(2) + "ms total",
                mapBufferSubsections: {
                    mapAsync: perKeyMetrics.mapBufferSubsections.mapAsync.toFixed(2) + "ms total",
                    getMappedRange: perKeyMetrics.mapBufferSubsections.getMappedRange.toFixed(2) + "ms total",
                    copyToUint8Array: perKeyMetrics.mapBufferSubsections.copyToUint8Array.toFixed(2) + "ms total",
                    unmap: perKeyMetrics.mapBufferSubsections.unmap.toFixed(2) + "ms total",
                },
                deserialize: perKeyMetrics.deserialize.toFixed(2) + "ms total",
            },
        });
    }
    /**
     * Flushes all pending writes, then retrieves the store metadata and key map.
     */
    async flushAndGetMetadata(storeName) {
        const performanceMetrics = {
            flushWrites: 0,
            metadataRetrieval: 0,
        };
        const flushStart = performance.now();
        await this.flushWrites();
        performanceMetrics.flushWrites = performance.now() - flushStart;
        const metadataStart = performance.now();
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        performanceMetrics.metadataRetrieval = performance.now() - metadataStart;
        return { storeMeta, keyMap, metrics: performanceMetrics };
    }
    /**
     * Converts a SQL Server–style LIKE pattern into a RegExp.
     */
    likeToRegex(pattern) {
        let regexPattern = pattern
            .replace(/\\/g, "\\\\")
            .replace(/[.+^${}()|[\]\\]/g, (char) => `\\${char}`)
            .replace(/%/g, ".*")
            .replace(/_/g, ".");
        // revert bracket expressions
        regexPattern = regexPattern.replace(/\\\[(.*?)]/g, "[$1]");
        return new RegExp(`^${regexPattern}$`, "u");
    }
    /**
     * Expands a single key or wildcard pattern into all matching keys from the key map.
     */
    expandWildcard(key, keyMap) {
        if (!/[%_\[\]]/.test(key)) {
            return [key];
        }
        const regex = this.likeToRegex(key);
        const allStoreKeys = Array.from(keyMap.keys());
        return allStoreKeys.filter((k) => regex.test(k));
    }
    /**
     * Applies expandWildcard to each item in the array and flattens the result.
     */
    expandAllWildcards(keys, keyMap) {
        return keys.flatMap((key) => this.expandWildcard(key, keyMap));
    }
    /**
     * High-level method to read row data for a list of keys in a two-step copy process:
     *  1) Copy from store buffers into a "big read buffer" (`bigReadBuffer`).
     *  2) Copy from `bigReadBuffer` into a staging buffer which we then map for reading.
     */
    async readAllRows(storeName, storeMeta, keyMap, keys) {
        // Prepare the results array (initialized to null)
        const results = new Array(keys.length).fill(null);
        // Metrics structure
        const perKeyMetrics = this.initializeMetrics();
        // Collect row metadata
        const { rowInfos, totalBytes } = this.collectRowInfos(keyMap, storeMeta, keys, results, perKeyMetrics);
        // Early exit if nothing to read
        if (rowInfos.length === 0) {
            return { results, perKeyMetrics };
        }
        // CHANGED: create "big read buffer" with COPY_SRC | COPY_DST (not MAP_READ).
        const bigReadBuffer = this.createBigReadBuffer(totalBytes, perKeyMetrics);
        // Step 1) copy data from each row’s GPU buffer into bigReadBuffer
        this.copyRowsIntoBigBuffer(rowInfos, storeMeta, bigReadBuffer, perKeyMetrics);
        // Step 2) copy from bigReadBuffer into a staging buffer that we map
        const bigCopiedData = await this.copyFromBigBufferToStaging(bigReadBuffer, totalBytes, perKeyMetrics);
        // Deserialize each row
        this.deserializeRows(rowInfos, storeMeta, bigCopiedData, results, perKeyMetrics);
        // Cleanup the bigReadBuffer
        bigReadBuffer.destroy();
        return { results, perKeyMetrics };
    }
    /**
     * Gathers row metadata (rowInfos) for each provided key.
     */
    collectRowInfos(keyMap, storeMeta, keys, results, perKeyMetrics) {
        const findMetadataStart = performance.now();
        const rowInfos = [];
        let totalBytes = 0;
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const rowMetadata = this.findActiveRowMetadata(keyMap, key, storeMeta.rows);
            if (!rowMetadata) {
                continue;
            }
            rowInfos.push({
                rowMetadata,
                rowIndex: i,
                offsetInFinalBuffer: totalBytes,
                length: rowMetadata.length,
            });
            totalBytes += rowMetadata.length;
        }
        perKeyMetrics.findMetadata = performance.now() - findMetadataStart;
        return { rowInfos, totalBytes };
    }
    /**
     * CHANGED: create "big read buffer" with COPY_SRC | COPY_DST, no MAP_READ.
     */
    createBigReadBuffer(totalBytes, perKeyMetrics) {
        const createBufferStart = performance.now();
        const bigReadBuffer = this.device.createBuffer({
            size: totalBytes,
            usage: GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST, // CHANGED
        });
        perKeyMetrics.createBuffer = performance.now() - createBufferStart;
        return bigReadBuffer;
    }
    /**
     * Copies data from each row’s GPU buffer into the bigReadBuffer.
     */
    copyRowsIntoBigBuffer(rowInfos, storeMeta, bigReadBuffer, perKeyMetrics) {
        const copyBufferStart = performance.now();
        const commandEncoder = this.device.createCommandEncoder();
        for (const rowInfo of rowInfos) {
            const srcBuffer = this.getBufferByIndex(storeMeta, rowInfo.rowMetadata.bufferIndex);
            commandEncoder.copyBufferToBuffer(srcBuffer, rowInfo.rowMetadata.offset, bigReadBuffer, rowInfo.offsetInFinalBuffer, rowInfo.length);
        }
        this.device.queue.submit([commandEncoder.finish()]);
        perKeyMetrics.copyBuffer = performance.now() - copyBufferStart;
    }
    /**
     * CHANGED: Instead of `mapAsync` on bigReadBuffer, we do a copy to a staging buffer
     * that we then map for reading. This eliminates direct mapAsync on the main read buffer.
     */
    async copyFromBigBufferToStaging(bigReadBuffer, totalBytes, perKeyMetrics) {
        const mapBufferStart = performance.now();
        // 1) Create a staging buffer with COPY_DST | MAP_READ
        const stagingBuffer = this.device.createBuffer({
            size: totalBytes,
            usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ
        });
        // 2) Copy from bigReadBuffer → stagingBuffer
        const commandEncoder = this.device.createCommandEncoder();
        commandEncoder.copyBufferToBuffer(bigReadBuffer, 0, stagingBuffer, 0, totalBytes);
        this.device.queue.submit([commandEncoder.finish()]);
        // Wait for GPU to finish so we can map the staging buffer
        await this.device.queue.onSubmittedWorkDone();
        // Now map the staging buffer for reading
        const mapAsyncStart = performance.now();
        await stagingBuffer.mapAsync(GPUMapMode.READ);
        perKeyMetrics.mapBufferSubsections.mapAsync = performance.now() - mapAsyncStart;
        // getMappedRange
        const getMappedRangeStart = performance.now();
        const fullMappedRange = stagingBuffer.getMappedRange();
        perKeyMetrics.mapBufferSubsections.getMappedRange = performance.now() - getMappedRangeStart;
        // copyToUint8Array
        const copyToUint8ArrayStart = performance.now();
        const bigCopiedData = new Uint8Array(fullMappedRange.slice(0));
        perKeyMetrics.mapBufferSubsections.copyToUint8Array = performance.now() - copyToUint8ArrayStart;
        // unmap
        const unmapStart = performance.now();
        stagingBuffer.unmap();
        perKeyMetrics.mapBufferSubsections.unmap = performance.now() - unmapStart;
        stagingBuffer.destroy(); // We no longer need it
        perKeyMetrics.mapBuffer = performance.now() - mapBufferStart;
        return bigCopiedData;
    }
    /**
     * Deserializes each row from the provided copied data.
     */
    deserializeRows(rowInfos, storeMeta, bigCopiedData, results, perKeyMetrics) {
        const deserializeStart = performance.now();
        for (const rowInfo of rowInfos) {
            const rowSlice = bigCopiedData.subarray(rowInfo.offsetInFinalBuffer, rowInfo.offsetInFinalBuffer + rowInfo.length);
            // Convert bytes to an object using `deserializeData`
            results[rowInfo.rowIndex] = this.deserializeData(storeMeta, rowSlice);
        }
        perKeyMetrics.deserialize = performance.now() - deserializeStart;
    }
    /**
     * Initializes and returns a fresh PerKeyMetrics object.
     */
    initializeMetrics() {
        return {
            findMetadata: 0,
            createBuffer: 0,
            copyBuffer: 0,
            mapBuffer: 0,
            deserialize: 0,
            mapBufferSubsections: {
                mapAsync: 0,
                getMappedRange: 0,
                copyToUint8Array: 0,
                unmap: 0,
            },
        };
    }
}
//# sourceMappingURL=VideoDB.js.map