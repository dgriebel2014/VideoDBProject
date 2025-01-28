// Copyright © 2025 Jon Griebel. dgriebel2014@gmail.com - All rights reserved.
// Distributed under the MIT license.
// videoDB.ts
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
    waitUntilReadyPromise = null;
    readyResolver = null;
    /**
     * Initializes a new instance of the VideoDB class.
     * @param {GPUDevice} device - The GPU device to be used for buffer operations.
     */
    constructor(device) {
        this.device = device;
        this.storeMetadataMap = new Map();
        this.storeKeyMap = new Map();
    }
    createObjectStore(storeName, options) {
        // 1) Check if an object store with this name already exists. If so, throw an error.
        if (this.storeMetadataMap.has(storeName)) {
            throw new Error(`Object store "${storeName}" already exists.`);
        }
        // 2) If the data type is "TypedArray", we must have a valid typedArrayType.
        if (options.dataType === "TypedArray" && !options.typedArrayType) {
            throw new Error(`typedArrayType is required when dataType is "TypedArray".`);
        }
        // 3) If the dataType is not "JSON" and rowSize is provided, 
        //    calculate how many rows can fit into one buffer based on bufferSize.
        //    Otherwise, it remains undefined.
        const rowsPerBuffer = options.dataType !== "JSON" && options.rowSize
            ? Math.floor(options.bufferSize / options.rowSize)
            : undefined;
        // 4) Construct a StoreMetadata object that describes this new object store.
        const storeMetadata = {
            storeName,
            dataType: options.dataType,
            typedArrayType: options.typedArrayType,
            bufferSize: options.bufferSize,
            rowSize: options.rowSize,
            rowsPerBuffer,
            totalRows: options.totalRows,
            buffers: [], // no buffers allocated yet
            rows: [], // empty row list initially
            // 4a) Convert each sortDefinition to an internal format, marking all field data types as "string" by default.
            sortDefinition: options.sortDefinition?.map(def => ({
                name: def.name,
                sortFields: def.sortFields.map(field => ({
                    dataType: "string",
                    ...field
                }))
            })) ?? [],
            sortsDirty: false
        };
        // 5) Save the new store metadata in the storeMetadataMap.
        this.storeMetadataMap.set(storeName, storeMetadata);
        // 5a) Also create an empty keyMap for this storeName.
        this.storeKeyMap.set(storeName, new Map());
        // 6) If this is a JSON-type store with one or more sort definitions, also create an "offsets" store.
        if (options.dataType === "JSON" && options.sortDefinition && options.sortDefinition.length) {
            // 6a) Determine how many fields there are in total across all sort definitions.
            const totalSortFields = options.sortDefinition.reduce((count, def) => count + def.sortFields.length, 0);
            // 6c) Create the companion offsets store with typedArrayType = "Uint32Array" and a large buffer size.
            this.createObjectStore(`${storeName}-offsets`, {
                dataType: "TypedArray",
                typedArrayType: "Uint32Array",
                bufferSize: 10 * 1024 * 1024,
                totalRows: options.totalRows
            });
        }
    }
    /**
     * Deletes an existing object store by name.
     * @param {string} storeName - The name of the store to delete.
     * @returns {void} This method does not return anything.
     */
    deleteObjectStore(storeName) {
        if (!this.storeMetadataMap.has(storeName)) {
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
     * Adds a new record to the specified store, with delayed GPU writes.
     * If the store has JSON sort definitions, it also computes and stores offsets.
     *
     * @async
     * @function
     * @name add
     * @memberof YourClassName
     * @param {string} storeName - The name of the store to which the record should be added.
     * @param {string} key - The key under which the record will be stored.
     * @param {*} value - The record data to add.
     * @returns {Promise<void>} Promise that resolves when the record has been added.
     * @throws {Error} If the specified object store does not exist.
     */
    async add(storeName, key, value) {
        this.isReady = false;
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        // 1) Write main record
        await this.writeRecordToStore(storeMeta, key, value, "add");
        // 2) If JSON-based with sort definitions, handle all offsets
        if (storeMeta.dataType === "JSON" && storeMeta.sortDefinition?.length) {
            storeMeta.sortsDirty = true; // Mark as dirty
            await this.writeOffsetsForAllDefinitions(storeMeta, key, value, "add");
        }
        // 3) Reset flush timer and possibly flush
        this.resetFlushTimer();
        await this.checkAndFlush();
    }
    /**
     * Updates (or adds) a record in the specified store, with delayed GPU writes.
     * If the store has JSON sort definitions, it also computes and stores offsets.
     *
     * @async
     * @function
     * @name put
     * @memberof YourClassName
     * @param {string} storeName - The name of the store to which the record should be written or updated.
     * @param {string} key - The key under which the record will be stored or updated.
     * @param {*} value - The record data to put or update.
     * @returns {Promise<void>} Promise that resolves when the record has been put or updated.
     * @throws {Error} If the specified object store does not exist.
     */
    async put(storeName, key, value) {
        this.isReady = false;
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        // 1) Write main record
        await this.writeRecordToStore(storeMeta, key, value, "put");
        // 2) If JSON-based with sort definitions, handle all offsets
        if (storeMeta.dataType === "JSON" && storeMeta.sortDefinition?.length) {
            storeMeta.sortsDirty = true;
            await this.writeOffsetsForAllDefinitions(storeMeta, key, value, "put");
        }
        // 3) Reset flush timer and possibly flush
        this.resetFlushTimer();
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
    async clear(storeName) {
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        await this.waitUntilReady();
        // Retrieve store metadata and keyMap
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
     *     console.info(record.key, record.value);
     * }
     *
     * @example
     * const range = { lowerBound: '100', upperBound: '200', lowerInclusive: true, upperInclusive: false };
     * for await (const record of videoDB.openCursor('MyStore', { range, direction: 'prev' })) {
     *     console.info(record.key, record.value);
     * }
     */
    async *openCursor(storeName, options) {
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        // Retrieve store metadata and keyMap
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
     * Checks if the pending writes have reached a threshold or if conditions
     * dictate a flush to the GPU buffers, and performs the flush if necessary.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once the flush has been performed (if triggered).
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
     * Flushes all pending writes in batches to their respective GPU buffers.
     * Groups writes by buffer, performs the writes, then waits for GPU completion.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once all pending writes are submitted and the queue is done.
     */
    async flushWrites() {
        if (this.pendingWrites.length === 0) {
            return;
        }
        // Group all pendingWrites by their GPUBuffer
        const writesByBuffer = new Map();
        for (const item of this.pendingWrites) {
            const { gpuBuffer } = item;
            if (!writesByBuffer.has(gpuBuffer)) {
                writesByBuffer.set(gpuBuffer, []);
            }
            writesByBuffer.get(gpuBuffer).push(item);
        }
        // Keep track of which writes succeed
        const successfulWrites = new Set();
        // Sort by offset and write each group
        for (const [gpuBuffer, writeGroup] of writesByBuffer.entries()) {
            // Sort by offset ascending
            writeGroup.sort((a, b) => a.rowMetadata.offset - b.rowMetadata.offset);
            for (const pendingWrite of writeGroup) {
                try {
                    const { rowMetadata, arrayBuffer } = pendingWrite;
                    this.device.queue.writeBuffer(gpuBuffer, rowMetadata.offset, arrayBuffer);
                    successfulWrites.add(pendingWrite);
                }
                catch (singleWriteError) {
                    console.error('Error writing single item:', singleWriteError);
                }
            }
        }
        // Wait for the GPU queue to finish
        await this.device.queue.onSubmittedWorkDone();
        // Remove successful writes from pending
        this.pendingWrites = this.pendingWrites.filter(write => !successfulWrites.has(write));
    }
    /**
     * Applies a custom string key range filter (lower/upper bounds, inclusivity)
     * to an array of keys.
     *
     * @private
     * @param {string[]} keys - The keys to be filtered.
     * @param {{
     *   lowerBound?: string;
     *   upperBound?: string;
     *   lowerInclusive?: boolean;
     *   upperInclusive?: boolean;
     * }} range - Defines the comparison bounds and inclusivity.
     * @returns {string[]} The subset of keys that meet the range criteria.
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
     * Retrieves the key-to-row-index map for the specified store name.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @returns {Map<string, number>} The store's key map.
     * @throws {Error} If the store does not exist.
     */
    getKeyMap(storeName) {
        const keyMap = this.storeKeyMap.get(storeName);
        if (!keyMap) {
            return new Map();
        }
        return keyMap;
    }
    /**
     * Compares two string keys for sorting purposes.
     *
     * @private
     * @param {string} a - The first key to compare.
     * @param {string} b - The second key to compare.
     * @returns {number} Negative if a < b, 0 if equal, or positive if a > b.
     */
    compareKeys(a, b) {
        if (a < b)
            return -1;
        if (a > b)
            return 1;
        return 0;
    }
    /**
     * Finds active row metadata for a given key if it exists (and is not flagged inactive).
     *
     * @private
     * @param {Map<string, number>} keyMap - Map of key → row ID.
     * @param {string} key - The key being searched.
     * @param {RowMetadata[]} rows - The array of row metadata for the store.
     * @returns {RowMetadata | null} Metadata if found and active, otherwise null.
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
     * Retrieves the metadata object for a given store by name.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @returns {StoreMetadata} The metadata for the specified store.
     * @throws {Error} If the store does not exist.
     */
    getStoreMetadata(storeName) {
        const meta = this.storeMetadataMap.get(storeName);
        if (!meta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        return meta;
    }
    /**
     * Finds or creates space in an existing GPU buffer (or a new buffer) for a given size.
     * Returns the GPU buffer reference, its index, and the offset where the data should be written.
     *
     * @private
     * @param {StoreMetadata} storeMeta - Metadata of the store to which we're allocating space.
     * @param {number} size - The size in bytes needed in the GPU buffer.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *  An object containing the GPU buffer, the buffer index, and the offset in the buffer.
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
     * Creates and returns a new GPU buffer for the store.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store that requires a new GPU buffer.
     * @param {number} size - The requested size (usually equal to storeMeta.bufferSize).
     * @returns {GPUBuffer} The newly created GPU buffer.
     */
    createNewBuffer(storeMeta, size) {
        return this.device.createBuffer({
            size: storeMeta.bufferSize,
            usage: GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
            mappedAtCreation: false
        });
    }
    /**
     * Allocates and initializes the very first buffer in the store.
     * Dynamically upsizes the buffer if the needed size is bigger than storeMeta.bufferSize.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata where the buffer is being created.
     * @param {number} size - The number of bytes initially needed in the new buffer.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *   An object containing the new GPU buffer, the assigned buffer index,
     *   and the offset at which data can be written (always 0 for the first chunk).
     */
    allocateFirstBufferChunk(storeMeta, size) {
        // Dynamically pick a capacity that can hold 'size'
        const neededCapacity = Math.max(storeMeta.bufferSize, roundUp(size, 256));
        const gpuBuffer = this.device.createBuffer({
            size: neededCapacity,
            usage: GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
            mappedAtCreation: false
        });
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
     * Retrieves usage information for the last buffer in the store, including
     * a reference to its metadata and the number of bytes already used.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata containing the buffer list.
     * @returns {{ lastBufferMeta: BufferMetadata; usedBytes: number }}
     *   An object containing the last buffer's metadata and how many bytes have been used.
     */
    getLastBufferUsage(storeMeta) {
        const lastIndex = storeMeta.buffers.length - 1;
        const lastBufferMeta = storeMeta.buffers[lastIndex];
        const gpuBuffer = lastBufferMeta.gpuBuffer;
        const usedBytes = gpuBuffer._usedBytes || 0;
        return { lastBufferMeta, usedBytes };
    }
    /**
     * Attempts to use available space in the last buffer for the requested size.
     * If alignment causes an overflow, a new buffer is allocated instead.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata containing buffer info.
     * @param {BufferMetadata} lastBufferMeta - Metadata for the last GPU buffer in the store.
     * @param {number} usedBytes - The currently used bytes in that buffer.
     * @param {number} size - The size (in bytes) needed.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *   The buffer, its index, and the aligned offset at which new data should be written.
     */
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
     * Allocates a new GPU buffer chunk if the last one does not have enough space.
     * Dynamically upsizes the buffer if the needed size is bigger than storeMeta.bufferSize.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata where the buffer is being created.
     * @param {number} size - The number of bytes needed.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *   An object containing the new GPU buffer, the assigned buffer index,
     *   and the offset (always 0 for new buffers).
     */
    allocateNewBufferChunk(storeMeta, size) {
        const newBufferIndex = storeMeta.buffers.length;
        // Dynamically pick a capacity that can hold 'size'
        const neededCapacity = Math.max(storeMeta.bufferSize, roundUp(size, 256));
        const newGpuBuffer = this.device.createBuffer({
            size: neededCapacity,
            usage: GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
            mappedAtCreation: false
        });
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
     * Serializes a value (JSON, TypedArray, ArrayBuffer, etc.) into an ArrayBuffer
     * suitable for writing to the GPU buffer.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store being written to.
     * @param {any} value - The original value to serialize.
     * @returns {ArrayBuffer} The serialized value as an ArrayBuffer.
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
     * Finds existing row metadata for a given key or creates a new row entry if one does not exist.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store.
     * @param {Map<string, number>} keyMap - A mapping of keys to row indices.
     * @param {string} key - The unique key identifying the row.
     * @param {ArrayBuffer} arrayBuffer - The data to be associated with this row.
     * @param {"add"|"put"} operationType - The operation type (whether we're adding or putting).
     * @returns {Promise<RowMetadata>} A promise that resolves with the row metadata.
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
     * deactivates the old row and allocates a new buffer space. Otherwise, it
     * simply updates the length field for in-place overwriting.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store.
     * @param {RowMetadata} oldRowMeta - The existing row metadata.
     * @param {ArrayBuffer} arrayBuffer - The new data to overwrite or reallocate.
     * @param {Map<string, number>} keyMap - A mapping of keys to row indices.
     * @param {string} key - The unique key identifying the row being overwritten.
     * @returns {Promise<RowMetadata>} A promise that resolves with the (possibly new) row metadata.
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
     * Deserializes raw data from a GPU buffer into its original form (JSON, TypedArray, or ArrayBuffer).
     *
     * This method ensures that if the dataType is "TypedArray", we correctly
     * construct the typed array from the subarray’s offset and length rather
     * than using the entire underlying buffer.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata (contains dataType, typedArrayType, etc.).
     * @param {Uint8Array} copiedData - A Uint8Array representing the raw copied bytes from the GPU.
     * @returns {any} The deserialized value, whose type depends on `storeMeta.dataType`.
     * @throws {Error} If the store's dataType or typedArrayType is invalid.
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
                // Ensure we use the subarray's offset and length in elements
                const bytesPerElement = this.getBytesPerElement(storeMeta.typedArrayType);
                return new TypedArrayCtor(copiedData.buffer, copiedData.byteOffset, copiedData.byteLength / bytesPerElement);
            }
            case "ArrayBuffer": {
                return copiedData.buffer;
            }
            default:
                throw new Error(`Unknown dataType "${storeMeta.dataType}".`);
        }
    }
    /**
     * Determines the number of bytes per element for a given typed array type name.
     *
     * @private
     * @param {string} typedArrayType - The name of the typed array constructor (e.g. "Float32Array").
     * @returns {number} The number of bytes each element in the typed array occupies.
     * @throws {Error} If the typed array type is unsupported.
     */
    getBytesPerElement(typedArrayType) {
        switch (typedArrayType) {
            case "Float32Array":
            case "Int32Array":
            case "Uint32Array":
                return 4;
            case "Float64Array":
                return 8;
            case "Uint8Array":
                return 1;
            default:
                throw new Error(`Unsupported typedArrayType: ${typedArrayType}`);
        }
    }
    /**
     * Retrieves the GPU buffer instance corresponding to a specific buffer index.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store.
     * @param {number} bufferIndex - The index of the buffer to retrieve.
     * @returns {GPUBuffer} The GPU buffer at the specified index.
     */
    getBufferByIndex(storeMeta, bufferIndex) {
        const bufMeta = storeMeta.buffers[bufferIndex];
        if (!bufMeta || !bufMeta.gpuBuffer) {
            throw new Error(`Buffer index ${bufferIndex} not found or uninitialized.`);
        }
        return bufMeta.gpuBuffer;
    }
    /**
     * Resets the timer that triggers an automatic flush of pending writes.
     * If the timer is already running, it is cleared and restarted.
     * Once the timer fires, this method:
     *  1) flushes all pending writes,
     *  2) rebuilds all dirty sorts by calling `rebuildAllDirtySorts`,
     *  3) resolves the internal `readyResolver` if present,
     *  4) sets `isReady` to `true`.
     *
     * @private
     * @returns {void}
     */
    resetFlushTimer() {
        if (this.flushTimer !== null) {
            clearTimeout(this.flushTimer);
        }
        this.flushTimer = window.setTimeout(async () => {
            try {
                await this.flushWrites();
                await this.rebuildAllDirtySorts();
            }
            catch (error) {
                console.error('Error during timed flush operation:', error);
            }
            finally {
                this.flushTimer = null;
                if (this.readyResolver) {
                    this.readyResolver();
                    this.readyResolver = null;
                    this.waitUntilReadyPromise = null;
                }
                this.isReady = true;
            }
        }, 250);
    }
    /**
     * Rebuilds all dirty sorts across all stores that have `sortsDirty = true`.
     * For each such store, we iterate over each `sortDefinition`.
     *
     * Now that we store per-row numeric data in the offsets store (for JSON),
     * we’ll gather that numeric data into a GPU buffer for sorting.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once all dirty sorts have been rebuilt.
     */
    async rebuildAllDirtySorts() {
        for (const [storeName, storeMeta] of this.storeMetadataMap.entries()) {
            if (!storeMeta.sortsDirty) {
                continue;
            }
            storeMeta.sortsDirty = false; // Reset the dirty flag
            // If there are no sort definitions, skip
            if (!storeMeta.sortDefinition || storeMeta.sortDefinition.length === 0) {
                continue;
            }
            // Rebuild each definition in sequence
            // jdg jdg jdg
            //    for (const def of storeMeta.sortDefinition) {
            //        await this.runGpuSortForDefinition(storeMeta, def);
            //    }
        }
    }
    /**
     * Logs performance metrics for debugging and analysis purposes.
     *
     * @private
     * @param {InitialMetrics} initialMetrics - Overall metrics (e.g., flushWrites time, metadata retrieval time).
     * @param {PerKeyMetrics} perKeyMetrics - Detailed metrics for per-key operations (e.g. copyBuffer, deserialize, etc.).
     * @returns {void}
     */
    logPerformance(initialMetrics, perKeyMetrics) {
        //    console.info("** Performance Metrics for getMultiple **", {
        //        flushWrites: initialMetrics.flushWrites.toFixed(2) + "ms",
        //        metadataRetrieval: initialMetrics.metadataRetrieval.toFixed(2) + "ms",
        //        perKeyMetrics: {
        //            findMetadata: perKeyMetrics.findMetadata.toFixed(2) + "ms total",
        //            createBuffer: perKeyMetrics.createBuffer.toFixed(2) + "ms total",
        //            copyBuffer: perKeyMetrics.copyBuffer.toFixed(2) + "ms total",
        //            mapBuffer: perKeyMetrics.mapBuffer.toFixed(2) + "ms total",
        //            mapBufferSubsections: {
        //                mapAsync: perKeyMetrics.mapBufferSubsections.mapAsync.toFixed(2) + "ms total",
        //                getMappedRange: perKeyMetrics.mapBufferSubsections.getMappedRange.toFixed(2) + "ms total",
        //                copyToUint8Array: perKeyMetrics.mapBufferSubsections.copyToUint8Array.toFixed(2) + "ms total",
        //                unmap: perKeyMetrics.mapBufferSubsections.unmap.toFixed(2) + "ms total",
        //            },
        //            deserialize: perKeyMetrics.deserialize.toFixed(2) + "ms total",
        //        },
        //    });
    }
    /**
     * Flushes all pending writes to the GPU and then returns the store metadata and key map.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @returns {Promise<{ storeMeta: StoreMetadata; keyMap: Map<string, number> }>}
     *    An object containing the store's metadata and key map.
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
     * Converts a SQL Server–style LIKE pattern into a JavaScript regular expression.
     * - `%` becomes `.*`
     * - `_` becomes `.`
     * - Special regex characters are escaped except for bracket expressions.
     *
     * @private
     * @param {string} pattern - The SQL-style LIKE pattern.
     * @returns {RegExp} A JavaScript RegExp object equivalent to the LIKE pattern.
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
     * Expands a single SQL-style wildcard key (possibly containing `%`, `_`, `[`, etc.)
     * into all matching keys from the given key map.
     *
     * @private
     * @param {string} key - The (potentially) wildcard pattern.
     * @param {Map<string, any>} keyMap - A map of all available keys in the store.
     * @returns {string[]} An array of matched keys.
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
     * Applies `expandWildcard` to each key in the given array, then flattens
     * the results into a single array of resolved keys.
     *
     * @private
     * @param {string[]} keys - An array of (possibly) wildcard patterns.
     * @param {Map<string, any>} keyMap - A map of all available keys in the store.
     * @returns {string[]} A flattened array of all expanded keys.
     */
    expandAllWildcards(keys, keyMap) {
        return keys.flatMap((key) => this.expandWildcard(key, keyMap));
    }
    /**
     * Reads row data for a list of keys in two steps:
     *  1) Copy from the store's GPU buffers into a single "big read buffer" (`bigReadBuffer`).
     *  2) Copy from `bigReadBuffer` into a staging buffer that is mapped to CPU memory for reading.
     *
     * @private
     * @param {string} storeName - The name of the store from which to read.
     * @param {StoreMetadata} storeMeta - The metadata of the store being read.
     * @param {Map<string, any>} keyMap - The key-to-row-index map for the store.
     * @param {string[]} keys - The list of keys to read.
     * @returns {Promise<{ results: (any | null)[]; perKeyMetrics: PerKeyMetrics }>}
     *   A promise resolving to an object containing:
     *   - `results`: an array of deserialized values (or null if not found).
     *   - `perKeyMetrics`: timing info for various stages of the read operation.
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
     * Gathers metadata (rowInfos) for each key to be read. Determines offsets and total
     * byte length required to hold all requested rows in a single buffer.
     *
     * @private
     * @param {Map<string, any>} keyMap - The key-to-row map for the store.
     * @param {StoreMetadata} storeMeta - The store metadata containing row info.
     * @param {string[]} keys - The list of keys to read.
     * @param {(any | null)[]} results - An array to store the resulting deserialized data.
     * @param {PerKeyMetrics} perKeyMetrics - Metrics used for performance measurement.
     * @returns {{ rowInfos: RowInfo[], totalBytes: number }}
     *   An object containing:
     *   - `rowInfos`: a list of row metadata including offsets and lengths.
     *   - `totalBytes`: the total number of bytes needed for all rows.
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
     * Creates a GPU buffer (bigReadBuffer) large enough to hold a specified total size,
     * used to consolidate row data before mapping.
     *
     * @private
     * @param {number} totalBytes - The total number of bytes needed.
     * @param {PerKeyMetrics} perKeyMetrics - Metrics used for performance measurement.
     * @returns {GPUBuffer} The newly created GPU buffer.
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
     * Copies data for each row from its source GPU buffer to the bigReadBuffer.
     * This consolidates multiple row reads into a single buffer for more efficient mapping.
     *
     * @private
     * @param {RowInfo[]} rowInfos - An array of row metadata (source offset, length, etc.).
     * @param {StoreMetadata} storeMeta - The store metadata containing all buffers.
     * @param {GPUBuffer} bigReadBuffer - The destination buffer into which all rows are copied.
     * @param {PerKeyMetrics} perKeyMetrics - Metrics used for performance measurement.
     * @returns {void}
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
     * Performs a copy from the bigReadBuffer into a staging buffer (with MAP_READ usage),
     * then maps the staging buffer for CPU access and returns the combined byte data.
     *
     * @private
     * @param {GPUBuffer} bigReadBuffer - The consolidated read buffer.
     * @param {number} totalBytes - The total number of bytes to copy.
     * @param {PerKeyMetrics} perKeyMetrics - Metrics used for performance measurement.
     * @returns {Promise<Uint8Array>} A promise that resolves to a Uint8Array containing all copied data.
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
     * Deserializes each row from the combined `bigCopiedData` buffer into its original form,
     * placing the result into the corresponding index of the `results` array.
     *
     * @private
     * @param {RowInfo[]} rowInfos - An array of row metadata (offsets, lengths, etc.).
     * @param {StoreMetadata} storeMeta - The store metadata (data types, etc.).
     * @param {Uint8Array} bigCopiedData - The combined data holding all row bytes.
     * @param {(any | null)[]} results - The result array to store deserialized values.
     * @param {PerKeyMetrics} perKeyMetrics - Metrics used for performance measurement.
     * @returns {void}
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
     * Creates and returns a fresh PerKeyMetrics object with all timing values initialized to 0.
     *
     * @private
     * @returns {PerKeyMetrics} A fresh metrics object.
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
    /**
     * Waits until the VideoDB is ready, i.e., all pending operations have settled
     * and the internal ready state is resolved.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once VideoDB is ready.
     */
    waitUntilReady() {
        // If already ready, just return immediately
        if (this.isReady)
            return Promise.resolve();
        // If a wait promise is *already* in progress, return the same one
        if (this.waitUntilReadyPromise)
            return this.waitUntilReadyPromise;
        // Otherwise, create a fresh promise and store its resolver
        this.waitUntilReadyPromise = new Promise((resolve) => {
            this.readyResolver = resolve;
        });
        return this.waitUntilReadyPromise;
    }
    /**
     * Reads records for a given store using skip/take pagination.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @param {string[]} allKeys - An array of all keys in the store.
     * @param {number} skip - The number of records to skip.
     * @param {number} take - The number of records to return after skipping.
     * @returns {Promise<{ results: (any | null)[] }>}
     *    A promise resolving with an object that contains a `results` array.
     */
    async readRowsWithPagination(storeName, allKeys, skip, take) {
        // Slice the keys array to get the paginated keys
        const paginatedKeys = allKeys.slice(skip, skip + take);
        // Use getMultipleByKeys to fetch the data
        const { results, perKeyMetrics } = await this.getMultipleByKeys(storeName, paginatedKeys);
        return { results, perKeyMetrics };
    }
    /**
     * Retrieves multiple records by an array of keys from the specified store.
     * Used internally by the overloaded `getMultiple` method.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @param {string[]} keys - The array of keys to fetch.
     * @returns {Promise<{ results: (any | null)[] }>}
     *    A promise resolving with an object that contains a `results` array.
     */
    async getMultipleByKeys(storeName, keys) {
        // Flush & retrieve store metadata
        const { storeMeta, keyMap, metrics } = await this.flushAndGetMetadata(storeName);
        // Expand any wildcard patterns
        const expandedKeys = this.expandAllWildcards(keys, keyMap);
        // Read all rows based on expanded keys
        const { results, perKeyMetrics } = await this.readAllRows(storeName, storeMeta, keyMap, expandedKeys);
        // Log or accumulate performance
        // this.logPerformance(metrics, perKeyMetrics);
        return { results, perKeyMetrics };
    }
    /**
     * Computes a flat `Uint32Array` representing *serialized field values*
     * for one object, based on a single SortDefinition (one or multiple fields).
     *
     * @param objectData - The source object whose fields we want to convert.
     * @param sortDefinition - The definition containing a `name` and `sortFields`.
     * @returns A flat array of 32-bit integers encoding the field values for that one definition.
     */
    getJsonFieldOffsetsForSingleDefinition(objectData, sortDefinition) {
        // Build up the per-field numeric arrays
        const fieldArrays = [];
        for (const field of sortDefinition.sortFields) {
            const rawValue = this.getValueByPath(objectData, field.path);
            const numericArray = this.convertValueToUint32Array(rawValue, field.dataType, field.sortDirection);
            fieldArrays.push(numericArray);
        }
        // Concatenate into one final Uint32Array
        let totalLength = 0;
        for (const arr of fieldArrays) {
            totalLength += arr.length;
        }
        const finalResult = new Uint32Array(totalLength);
        let offset = 0;
        for (const arr of fieldArrays) {
            finalResult.set(arr, offset);
            offset += arr.length;
        }
        return finalResult;
    }
    /**
     * Retrieve a value by a dot-delimited path (e.g. "user.address.street") from an object.
     */
    getValueByPath(obj, path) {
        if (!path)
            return obj;
        const segments = path.split(".");
        let current = obj;
        for (const seg of segments) {
            if (current == null)
                return undefined;
            current = current[seg];
        }
        return current;
    }
    /**
     * Convert a JS value (date, number, or string) into a Uint32Array.
     * Applies ascending or descending transformations as needed.
     */
    convertValueToUint32Array(value, dataType, direction) {
        // If descending, we invert the bits. Ascending => no inversion.
        const invert = (direction === "Desc");
        switch (dataType) {
            case "date":
                return this.serializeDate(value, invert);
            case "number":
                return this.serializeNumber(value, invert);
            case "string":
                return this.serializeString(value, invert);
            default:
                // Fallback for unknown or null
                const fallback = new Uint32Array([0]);
                if (invert)
                    fallback[0] = 0xFFFFFFFF;
                return fallback;
        }
    }
    /**
     * Example: store a Date (or date-string) as 64-bit => two 32-bit words [hi, lo].
     */
    serializeDate(rawValue, invert) {
        if (rawValue == null) {
            // e.g. store "null date" as [0,0] or [0xFFFFFFFF, 0xFFFFFFFF] if invert
            return new Uint32Array([invert ? 0xFFFFFFFF : 0, invert ? 0xFFFFFFFF : 0]);
        }
        const ms = new Date(rawValue).getTime();
        // We'll store as two 32-bit words: the high 32 bits and the low 32 bits
        const hi = Math.floor(ms / 0x100000000) >>> 0;
        const lo = (ms >>> 0);
        let arr = new Uint32Array([hi, lo]);
        if (invert) {
            arr[0] = 0xFFFFFFFF - arr[0];
            arr[1] = 0xFFFFFFFF - arr[1];
        }
        return arr;
    }
    /**
     * Serialize a JS number into either one 32-bit integer or a 64-bit float (2 words).
     */
    serializeNumber(rawValue, invert) {
        if (typeof rawValue !== "number" || !Number.isFinite(rawValue)) {
            // store 0 or 0xFFFFFFFF as a fallback
            return new Uint32Array([invert ? 0xFFFFFFFF : 0]);
        }
        // If integer in [0, 2^32-1], store in one word for compactness
        if (Number.isInteger(rawValue) && rawValue >= 0 && rawValue <= 0xFFFFFFFF) {
            const val32 = invert ? (0xFFFFFFFF - rawValue) : rawValue;
            return new Uint32Array([val32]);
        }
        // Otherwise store the 64-bit float bit pattern in 2 words.
        const buffer = new ArrayBuffer(8);
        const view = new DataView(buffer);
        // big-endian or little-endian depends on how you want to handle cross-platform
        // for typical usage, let's do little-endian:
        view.setFloat64(0, rawValue, true);
        // read out the 2 words
        let lo = view.getUint32(0, true);
        let hi = view.getUint32(4, true);
        if (invert) {
            // Bitwise inversion of floats in descending mode is an approximation
            // that may not strictly invert ordering across positive/negative boundaries
            // but might be acceptable if your domain is known (e.g. all positive).
            lo = 0xFFFFFFFF - lo;
            hi = 0xFFFFFFFF - hi;
        }
        return new Uint32Array([hi, lo]); // store [hi, lo]
    }
    /**
     * Serialize a string by storing each codepoint in a 32-bit word.
     */
    serializeString(rawValue, invert) {
        if (typeof rawValue !== "string") {
            // store "empty" if not a proper string
            return new Uint32Array([invert ? 0xFFFFFFFF : 0]);
        }
        // Convert each codepoint to one 32-bit
        const codePoints = [];
        for (const char of rawValue) {
            const cp = char.codePointAt(0);
            const word = invert ? (0xFFFFFFFF - cp) : cp;
            codePoints.push(word);
        }
        return Uint32Array.from(codePoints);
    }
    /**
     * Sorts rows for a given store and definition using a GPU bitonic approach,
     * with a two-buffer method (staging + storage) to avoid mapping the STORAGE buffer directly.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata being sorted.
     * @param {SortDefinition} sortDef - One definition specifying how to sort the rows.
     * @returns {Promise<void>} Resolves once the GPU sort is complete or aborts if over limit.
     */
    async runGpuSortForDefinition(storeMeta, sortDef) {
        const offsetsStoreName = `${storeMeta.storeName}-offsets`;
        const offsetsStoreMeta = this.storeMetadataMap.get(offsetsStoreName);
        if (!offsetsStoreMeta) {
            return;
        }
        const { sortItems, rowCount } = await this.buildSortItemsArray(storeMeta, offsetsStoreMeta, sortDef);
        if (rowCount < 2) {
            return;
        }
        console.log('sortItems: ', sortItems, 'rowCount: ', rowCount);
        const totalBytes = sortItems.byteLength;
        // Check device limit
        const maxBinding = this.device.limits.maxStorageBufferBindingSize || (128 * 1024 * 1024);
        if (totalBytes > maxBinding) {
            console.error(`Sort data requires ${totalBytes} bytes, ` +
                `exceeding GPU limit of ${maxBinding}. Aborting.`);
            return;
        }
        // Create a staging buffer for CPU → GPU
        const stagingBuffer = this.device.createBuffer({
            size: totalBytes,
            usage: GPUBufferUsage.MAP_WRITE | GPUBufferUsage.COPY_SRC,
            mappedAtCreation: true
        });
        new Uint32Array(stagingBuffer.getMappedRange()).set(sortItems);
        stagingBuffer.unmap();
        // Create the STORAGE buffer for compute
        const sortItemsBuffer = this.device.createBuffer({
            size: totalBytes,
            usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_DST | GPUBufferUsage.COPY_SRC
        });
        // Copy staging → STORAGE
        {
            const encoder = this.device.createCommandEncoder();
            encoder.copyBufferToBuffer(stagingBuffer, 0, sortItemsBuffer, 0, totalBytes);
            this.device.queue.submit([encoder.finish()]);
        }
        stagingBuffer.destroy();
        // Build pipeline and helper buffers
        const { pipeline } = this.createBitonicSortPipelineForJson();
        const paramBuffer = this.createParamBuffer();
        const debugAtomicBuffer = this.createDebugAtomicBuffer();
        const zeroBuffer = this.createZeroBuffer();
        // Standard bitonic pattern
        const paddedCount = 1 << Math.ceil(Math.log2(rowCount));
        const itemFieldCount = this.computeFieldCountForDefinition(sortDef);
        // Create bind group
        const bindGroup = this.device.createBindGroup({
            layout: pipeline.getBindGroupLayout(0),
            entries: [
                { binding: 0, resource: { buffer: sortItemsBuffer } },
                { binding: 1, resource: { buffer: paramBuffer } },
                { binding: 2, resource: { buffer: debugAtomicBuffer } }
            ]
        });
        // Execute passes
        for (let size = 2; size <= paddedCount; size <<= 1) {
            for (let halfSize = size >> 1; halfSize > 0; halfSize >>= 1) {
                await this.runBitonicPassJson(pipeline, bindGroup, paramBuffer, debugAtomicBuffer, zeroBuffer, size, halfSize, rowCount, paddedCount, itemFieldCount);
            }
        }
        const finalRowIds = await this.readBackSortedRowIds(sortItemsBuffer, rowCount, itemFieldCount);
        // Cleanup
        sortItemsBuffer.destroy();
        paramBuffer.destroy();
        debugAtomicBuffer.destroy();
        zeroBuffer.destroy();
    }
    /****
     * Computes how many 32-bit words each row’s offset data will contain
     * for the given SortDefinition.
     *
     * In the earlier offsets logic, each field used 8 bytes (2 × 32-bit words).
     * So if we have N fields, the total is (N × 2).
     ****/
    computeFieldCountForDefinition(sortDef) {
        // Each field is stored as two 32-bit words in the offsets array:
        return sortDef.sortFields.length * 2;
    }
    /**
     * Reads back the (rowId) portion of each item, ignoring the numeric fields,
     * returning them in sorted order.
     *
     * @private
     * @param {GPUBuffer} itemsBuffer - The final sorted items.
     * @param {number} rowCount - The real number of items (un-padded).
     * @param {number} fieldsPerItem - The # of numeric fields per item (excluding rowId).
     * @returns {Promise<Uint32Array>} The sorted row IDs in ascending order.
     */
    async readBackSortedRowIds(itemsBuffer, rowCount, fieldsPerItem) {
        if (rowCount === 0)
            return new Uint32Array();
        // Each item is (1 + fieldsPerItem) u32s
        const stride = 1 + fieldsPerItem;
        const totalWords = rowCount * stride;
        const totalBytes = totalWords * 4;
        // Create a staging buffer
        const staging = this.device.createBuffer({
            size: totalBytes,
            usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ
        });
        // Copy the entire itemsBuffer to the staging
        {
            const cmd = this.device.createCommandEncoder();
            console.log('copyBufferToBuffer4');
            cmd.copyBufferToBuffer(itemsBuffer, 0, staging, 0, totalBytes);
            this.device.queue.submit([cmd.finish()]);
            await this.device.queue.onSubmittedWorkDone();
        }
        // Map the staging buffer
        await staging.mapAsync(GPUMapMode.READ);
        const copyArray = new Uint32Array(staging.getMappedRange().slice(0));
        staging.unmap();
        staging.destroy();
        // The first word of each item is the rowId.
        const result = new Uint32Array(rowCount);
        for (let i = 0; i < rowCount; i++) {
            const base = i * stride;
            result[i] = copyArray[base];
        }
        return result;
    }
    async runBitonicPassJson(pipeline, bindGroup, paramBuffer, debugAtomicBuffer, zeroBuffer, size, halfSize, rowCount, paddedCount, fieldsPerItem) {
        // 1) Reset the debug atomic to zero
        await this.resetDebugAtomicBuffer(debugAtomicBuffer, zeroBuffer);
        // 2) Write param buffer: [size, halfSize, rowCount, paddedCount, fieldsPerItem]
        const paramData = new Uint32Array([
            size,
            halfSize,
            rowCount,
            paddedCount,
            fieldsPerItem
        ]);
        this.device.queue.writeBuffer(paramBuffer, 0, paramData);
        // 3) Dispatch
        const commandEncoder = this.device.createCommandEncoder();
        const pass = commandEncoder.beginComputePass();
        pass.setPipeline(pipeline);
        pass.setBindGroup(0, bindGroup);
        const workgroups = Math.ceil(paddedCount / 256);
        pass.dispatchWorkgroups(workgroups);
        pass.end();
        this.device.queue.submit([commandEncoder.finish()]);
        await this.device.queue.onSubmittedWorkDone();
    }
    /**
     * Gathers offsets-store data for a single (store, definition) pair,
     * producing a single typed array of "sort items."
     *
     * Each matched row gets:
     *   [ rowId, field0, field1, ..., fieldN ]
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the main store.
     * @param {StoreMetadata} offsetsStoreMeta - The metadata of the offsets store for this main store.
     * @param {SortDefinition} sortDef - The definition whose offsets we want.
     * @returns {Promise<{ sortItems: Uint32Array; rowCount: number }>}
     */
    async buildSortItemsArray(storeMeta, offsetsStoreMeta, sortDef) {
        const offsetsKeyMap = this.storeKeyMap.get(offsetsStoreMeta.storeName);
        const allKeys = Array.from(offsetsKeyMap.keys());
        const matchedKeys = allKeys.filter(k => k.endsWith(`::${sortDef.name}`));
        if (matchedKeys.length === 0) {
            return { sortItems: new Uint32Array(0), rowCount: 0 };
        }
        // Fetch all relevant offset rows
        const { results } = await this.getMultipleByKeys(offsetsStoreMeta.storeName, matchedKeys);
        const mainKeyMap = this.storeKeyMap.get(storeMeta.storeName);
        // Sum up total words. Each row uses 1 word for rowId + offsetData.length words.
        let totalWords = 0;
        for (const r of results) {
            if (r && r instanceof Uint32Array) {
                totalWords += (1 + r.length);
            }
        }
        // Build one big typed array for all items
        const rowCount = matchedKeys.length;
        const combined = new Uint32Array(totalWords);
        // Fill the combined array
        let writePos = 0;
        for (let i = 0; i < matchedKeys.length; i++) {
            const offsetKey = matchedKeys[i];
            const offsetData = results[i];
            if (!offsetData) {
                continue;
            }
            const mainKey = offsetKey.replace(`::${sortDef.name}`, "");
            const rowId = mainKeyMap.get(mainKey) ?? 0;
            combined[writePos++] = rowId;
            combined.set(offsetData, writePos);
            writePos += offsetData.length;
        }
        return { sortItems: combined, rowCount };
    }
    createBitonicSortPipelineForJson() {
        const code = /* wgsl */ `
struct Params {
  size: u32,
  halfSize: u32,
  rowCount: u32,
  paddedCount: u32,
  fieldsPerItem: u32
}

@group(0) @binding(0) var<storage, read_write> items: array<u32>; // [rowId, f0, f1, ...]
@group(0) @binding(1) var<uniform> params: Params;
@group(0) @binding(2) var<storage, read_write> debugAtomic: atomic<u32>;

fn lexCompare(aStart: u32, bStart: u32, fields: u32) -> bool {
  // Return true if A > B (for ascending-swap checks).
  // 
  // rowId is at items[aStart], fields start at aStart+1
  // We'll compare items[aStart+1 + i] vs items[bStart+1 + i]
  // for i in [0..fields).
  for (var i = 0u; i < fields; i++) {
    let av = items[aStart + 1u + i];
    let bv = items[bStart + 1u + i];
    if (av < bv) { return false; }  // means A < B
    if (av > bv) { return true; }   // means A > B
  }
  return false; // if all fields are equal, treat as "A == B" => no swap
}

fn compareAndSwap(i: u32, j: u32) {
  let stride = 1u + params.fieldsPerItem;
  let aStart = i * stride;
  let bStart = j * stride;

  let aShouldSwap = lexCompare(aStart, bStart, params.fieldsPerItem);
  // If aShouldSwap==true, that means itemA > itemB, so swap to get ascending
  if (aShouldSwap) {
    // swap each 32-bit word
    for (var w = 0u; w < stride; w++) {
      let tmp = items[aStart + w];
      items[aStart + w] = items[bStart + w];
      items[bStart + w] = tmp;
    }
    atomicStore(&debugAtomic, 1u);
  }
}

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) gid : vec3<u32>) {
  let i = gid.x;
  if (i >= params.paddedCount) {
    return;
  }

  let size = params.size;
  let halfSize = params.halfSize;

  let flip = (i & (size >> 1u)) != 0u;
  // For this simple example, let's always do ascending sorts
  // If you want descending, set a bool or do "if (flip) invert"
  let mate = i ^ halfSize;
  if (mate < params.paddedCount && mate != i) {
    if (i < mate) {
      compareAndSwap(i, mate);
    } else {
      compareAndSwap(mate, i);
    }
  }
}
`;
        const module = this.device.createShaderModule({ code });
        const pipeline = this.device.createComputePipeline({
            layout: "auto",
            compute: { module, entryPoint: "main" },
        });
        return { pipeline };
    }
    /**
     * Creates a uniform buffer to store bitonic sorting parameters.
     *
     * @private
     * @returns {GPUBuffer} A GPU buffer suitable for storing 5 x u32 parameters.
     */
    createParamBuffer() {
        return this.device.createBuffer({
            size: 5 * 4, // 5 x u32
            usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST
        });
    }
    /**
     * Creates a buffer used as an atomic "debug" buffer (e.g., to detect swaps).
     *
     * @private
     * @returns {GPUBuffer} A GPU buffer that can be used with atomic operations.
     */
    createDebugAtomicBuffer() {
        const buffer = this.device.createBuffer({
            size: 4,
            usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
            mappedAtCreation: true
        });
        new Uint32Array(buffer.getMappedRange()).set([0]);
        buffer.unmap();
        return buffer;
    }
    /**
     * Creates a small GPU buffer containing a single `0` value.
     * Used for resetting other buffers atomically.
     *
     * @private
     * @returns {GPUBuffer} A tiny buffer with one 32-bit zero.
     */
    createZeroBuffer() {
        const zeroBuffer = this.device.createBuffer({
            size: 4,
            usage: GPUBufferUsage.COPY_SRC,
            mappedAtCreation: true
        });
        new Uint32Array(zeroBuffer.getMappedRange()).set([0]);
        zeroBuffer.unmap();
        return zeroBuffer;
    }
    /**
     * Resets the debug atomic buffer to zero by copying from a small zero buffer.
     *
     * @private
     * @param {GPUBuffer} debugAtomicBuffer - The buffer storing the atomic debug value.
     * @param {GPUBuffer} zeroBuffer - A small GPU buffer containing a single zero value.
     * @returns {Promise<void>} A promise that resolves once the reset copy is done.
     */
    async resetDebugAtomicBuffer(debugAtomicBuffer, zeroBuffer) {
        const cmd = this.device.createCommandEncoder();
        cmd.copyBufferToBuffer(zeroBuffer, 0, debugAtomicBuffer, 0, 4);
        this.device.queue.submit([cmd.finish()]);
        await this.device.queue.onSubmittedWorkDone();
    }
    /**
     * Handles the main store write (row metadata, buffer, etc.).
     * Re-used by both `add` and `put`.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store to write into.
     * @param {string} key - The unique key for the row.
     * @param {any} value - The data to serialize and write.
     * @param {"add"|"put"} operationType - Whether this is an "add" or a "put" operation.
     * @returns {Promise<void>} Resolves when the metadata has been updated and the write is queued.
     */
    async writeRecordToStore(storeMeta, key, value, operationType) {
        const keyMap = this.storeKeyMap.get(storeMeta.storeName);
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);
        // Find or create row metadata
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, operationType);
        // Get GPU buffer
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        // Queue the main store write
        this.pendingWrites.push({
            storeMeta,
            rowMetadata,
            arrayBuffer,
            gpuBuffer,
            operationType,
        });
    }
    /**
     * Writes offset arrays to `<storeName>-offsets` for **all** sort definitions
     * defined on this store. Each definition is processed independently.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata for the main store.
     * @param {string} key - The unique key for the row in the main store.
     * @param {any} value - The JSON data to derive offsets from.
     * @param {"add"|"put"} operationType - The operation type (add or put).
     * @returns {Promise<void>} A promise that resolves when all offset writes are queued.
     */
    async writeOffsetsForAllDefinitions(storeMeta, key, value, operationType) {
        const offsetsStoreName = `${storeMeta.storeName}-offsets`;
        const offsetsStoreMeta = this.storeMetadataMap.get(offsetsStoreName);
        if (!offsetsStoreMeta) {
            // If there's no offsets store at all, nothing to do
            return;
        }
        const offsetsKeyMap = this.storeKeyMap.get(offsetsStoreName);
        // Process each definition independently
        for (const singleDefinition of storeMeta.sortDefinition) {
            // 1) Compute numeric keys for this definition
            const singleDefinitionOffsets = this.getJsonFieldOffsetsForSingleDefinition(value, singleDefinition);
            // 3) Create ArrayBuffer copy
            const offsetsCopy = new Uint32Array(singleDefinitionOffsets);
            const offsetsArrayBuffer = offsetsCopy.buffer;
            // 4) Use a composite key e.g. `<originalKey>::<definitionName>`
            const offsetRowKey = `${key}::${singleDefinition.name}`;
            // 5) Find or create row metadata for offsets store
            const offsetsRowMetadata = await this.findOrCreateRowMetadata(offsetsStoreMeta, offsetsKeyMap, offsetRowKey, offsetsArrayBuffer, operationType);
            // 6) Get GPU buffer for offsets
            const offsetsGpuBuffer = this.getBufferByIndex(offsetsStoreMeta, offsetsRowMetadata.bufferIndex);
            // 7) Queue offsets write
            this.pendingWrites.push({
                storeMeta: offsetsStoreMeta,
                rowMetadata: offsetsRowMetadata,
                arrayBuffer: offsetsArrayBuffer,
                gpuBuffer: offsetsGpuBuffer,
                operationType,
            });
        }
    }
    /**
     * Logs how much GPU buffer space is actually used (in bytes) across all
     * buffers in the offsets store for a particular main store.
     *
     * @param {string} storeName - The name of the **main** store. We'll look for "<storeName>-offsets".
     */
    async logOffsetsStoreUsage(storeName) {
        const offsetsStoreName = `${storeName}-offsets`;
        const offsetsStoreMeta = this.storeMetadataMap.get(offsetsStoreName);
        if (!offsetsStoreMeta) {
            console.warn(`No offsets store found for ${offsetsStoreName}.`);
            return;
        }
        let totalUsed = 0;
        for (const bufferMeta of offsetsStoreMeta.buffers) {
            const buffer = bufferMeta.gpuBuffer;
            if (!buffer)
                continue;
            // We track used bytes in (gpuBuffer as any)._usedBytes. 
            // If missing, assume 0.
            const usedBytes = buffer._usedBytes || 0;
            totalUsed += usedBytes;
        }
    }
}
// For convenience, define a simple flag for inactive rows, e.g. 0x1.
const ROW_INACTIVE_FLAG = 0x1;
/**
 * Rounds the given value up to the nearest multiple of `align`.
 *
 * @param {number} value - The original value.
 * @param {number} align - The alignment boundary.
 * @returns {number} The smallest integer >= `value` that is a multiple of `align`.
 */
function roundUp(value, align) {
    return Math.ceil(value / align) * align;
}
/**
 * Ensures the length of the UTF-8 representation of `jsonString` is a multiple of 4
 * by appending spaces as needed.
 *
 * @param {string} jsonString - The original JSON string to pad.
 * @returns {string} The padded JSON string, whose UTF-8 length is a multiple of 4.
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
 * Pads the given ArrayBuffer so that its byte length is a multiple of 4.
 * If it is already aligned, returns the original buffer. Otherwise, returns
 * a new buffer with zero-padding at the end.
 *
 * @param {ArrayBuffer} ab - The original ArrayBuffer to pad.
 * @returns {ArrayBuffer} A 4-byte-aligned ArrayBuffer.
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
//# sourceMappingURL=VideoDB.js.map