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
            // Assign sort definitions if provided
            sortDefinition: options.sortDefinition ?? []
        };
        this.storeMetadataMap.set(storeName, storeMetadata);
        this.storeKeyMap.set(storeName, new Map());
        if (options.dataType === "JSON" && options.sortDefinition && options.sortDefinition.length) {
            const totalSortFields = options.sortDefinition
                .reduce((count, def) => count + def.sortFields.length, 0);
            const bytesPerField = 2 * 4;
            const rowSize = totalSortFields * bytesPerField;
            this.createObjectStore(`${storeName}-offsets`, {
                dataType: "TypedArray",
                typedArrayType: "Uint32Array",
                bufferSize: 10 * 1024 * 1024,
                totalRows: options.totalRows,
                rowSize: rowSize
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
     * Adds a new record to the specified store without immediately writing to the GPU.
     * Instead, it caches the data in a pending-writes array. Once no writes occur for 1 second,
     * this method triggers a flush to the GPU. If the store is defined as `dataType: "JSON"` and
     * has one or more `sortDefinition`s, this method will also compute and store field-offset data
     * in the `<storeName>-offsets` store under the same key.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @param {any} value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns {Promise<void>} A promise that resolves once the data (and offsets, if any) is queued for writing.
     * @throws {Error} If the store does not exist or a record with the same key already exists (add mode does not overwrite).
     *
     * @remarks
     * - The new data is added to a batch (`pendingWrites`) and won’t be written to GPU memory until
     *   either there is a 1-second pause in writes or the batch size threshold is reached.
     * - If `storeName` is configured with `sortDefinition`, a `<storeName>-offsets` store is
     *   automatically created. This method will compute JSON field offsets and queue them
     *   for writing to the offset store simultaneously.
     * - The `isReady` flag is only set to `true` once all writes (including offsets) are fully flushed
     *   to the GPU, ensuring consistency between the primary and offset data.
     */
    async add(storeName, key, value) {
        this.isReady = false;
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        const keyMap = this.storeKeyMap.get(storeName);
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);
        // Compute fieldOffsets if JSON + sortDefinition
        const fieldOffsets = (storeMeta.dataType === "JSON" && storeMeta.sortDefinition?.length)
            ? this.getJsonFieldOffsetsFlattened(value, storeMeta.sortDefinition)
            : null;
        // findOrCreateRowMetadata for the main store
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, "add");
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        // Queue the main store write
        this.pendingWrites.push({
            storeMeta,
            rowMetadata,
            arrayBuffer,
            gpuBuffer,
            operationType: 'add'
        });
        // If we have fieldOffsets, write them to the `-offsets` store
        if (fieldOffsets) {
            const offsetsStoreName = `${storeName}-offsets`;
            const offsetsStoreMeta = this.storeMetadataMap.get(offsetsStoreName);
            if (offsetsStoreMeta) {
                const offsetsKeyMap = this.storeKeyMap.get(offsetsStoreName);
                // Create a guaranteed ArrayBuffer copy
                const offsetsCopy = new Uint32Array(fieldOffsets);
                const offsetsArrayBuffer = offsetsCopy.buffer; // Now strictly ArrayBuffer
                const offsetsRowMetadata = await this.findOrCreateRowMetadata(offsetsStoreMeta, offsetsKeyMap, key, offsetsArrayBuffer, "add");
                const offsetsGpuBuffer = this.getBufferByIndex(offsetsStoreMeta, offsetsRowMetadata.bufferIndex);
                // Queue the offsets write
                this.pendingWrites.push({
                    storeMeta: offsetsStoreMeta,
                    rowMetadata: offsetsRowMetadata,
                    arrayBuffer: offsetsArrayBuffer,
                    gpuBuffer: offsetsGpuBuffer,
                    operationType: 'add'
                });
            }
        }
        // Reset flush timer and possibly flush
        this.resetFlushTimer();
        await this.checkAndFlush();
    }
    /**
     * Stores or updates data in the specified store without immediately writing to the GPU.
     * Instead, it caches the data in a pending-writes array. Once no writes occur for 250 ms,
     * this method triggers a flush to the GPU. If the store is defined as `dataType: "JSON"` and
     * has one or more `sortDefinition`s, this method will also compute and store field-offset data
     * in the `<storeName>-offsets` store under the same key.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @param {any} value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns {Promise<void>} A promise that resolves once the data (and offsets, if any) is queued for writing.
     * @throws {Error} If the store does not exist.
     *
     * @remarks
     * - Unlike `add`, `put` allows overwriting an existing key if it is found.
     * - The updated data is added to a batch (`pendingWrites`) and won’t be written to GPU memory until
     *   either there is a 250 ms pause in writes or the batch size threshold is reached.
     * - If `storeName` is configured with `sortDefinition`, a `<storeName>-offsets` store is
     *   automatically created. This method will compute JSON field offsets and queue them
     *   for writing to the offset store simultaneously.
     * - The `isReady` flag is only set to `true` once all writes (including offsets) are fully flushed
     *   to the GPU, ensuring consistency between the primary and offset data.
     */
    async put(storeName, key, value) {
        this.isReady = false;
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        const keyMap = this.storeKeyMap.get(storeName);
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);
        // 1) Compute fieldOffsets if JSON + sortDefinition
        const fieldOffsets = (storeMeta.dataType === "JSON" && storeMeta.sortDefinition?.length)
            ? this.getJsonFieldOffsetsFlattened(value, storeMeta.sortDefinition)
            : null;
        // findOrCreateRowMetadata for the main store in "put" mode
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, "put");
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        // Queue the main store write
        this.pendingWrites.push({
            storeMeta,
            rowMetadata,
            arrayBuffer,
            gpuBuffer,
            operationType: 'put'
        });
        // If we have fieldOffsets, write them to the `-offsets` store
        if (fieldOffsets) {
            const offsetsStoreName = `${storeName}-offsets`;
            const offsetsStoreMeta = this.storeMetadataMap.get(offsetsStoreName);
            if (offsetsStoreMeta) {
                const offsetsKeyMap = this.storeKeyMap.get(offsetsStoreName);
                // Create a guaranteed ArrayBuffer copy
                const offsetsCopy = new Uint32Array(fieldOffsets);
                const offsetsArrayBuffer = offsetsCopy.buffer; // guaranteed ArrayBuffer
                const offsetsRowMetadata = await this.findOrCreateRowMetadata(offsetsStoreMeta, offsetsKeyMap, key, offsetsArrayBuffer, "put");
                const offsetsGpuBuffer = this.getBufferByIndex(offsetsStoreMeta, offsetsRowMetadata.bufferIndex);
                // Queue the offsets write
                this.pendingWrites.push({
                    storeMeta: offsetsStoreMeta,
                    rowMetadata: offsetsRowMetadata,
                    arrayBuffer: offsetsArrayBuffer,
                    gpuBuffer: offsetsGpuBuffer,
                    operationType: 'put'
                });
            }
        }
        // Reset flush timer and possibly flush
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
     * @returns {Promise<void>}
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
     * It groups pending writes by their GPUBuffer, performs the writes,
     * and then waits for the GPU queue to finish.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once all writes have been submitted to the GPU.
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
     * Applies a custom key range filter (lower/upper bounds, inclusivity)
     * to an array of string keys.
     *
     * @private
     * @param {string[]} keys - The array of keys to be filtered.
     * @param {{
     *   lowerBound?: string;
     *   upperBound?: string;
     *   lowerInclusive?: boolean;
     *   upperInclusive?: boolean;
     * }} range - The range constraints.
     * @returns {string[]} A filtered array of keys that satisfy the provided range.
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
     * Retrieves the key-to-row-index map for a given store by name.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @returns {Map<string, number>} The map of keys to row indices.
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
     * Compares two string keys to determine their sort order.
     * Return value is negative if a < b, zero if they are equal, and positive if a > b.
     *
     * @private
     * @param {string} a - The first key.
     * @param {string} b - The second key.
     * @returns {number} Comparison result for sorting.
     */
    compareKeys(a, b) {
        if (a < b)
            return -1;
        if (a > b)
            return 1;
        return 0;
    }
    /**
     * Finds the existing row metadata for a key within the store,
     * if it is currently active (i.e., exists in the row array).
     *
     * @private
     * @param {Map<string, number>} keyMap - The store's key map.
     * @param {string} key - The unique key identifying the row.
     * @param {RowMetadata[]} rows - The array of row metadata in the store.
     * @returns {RowMetadata | undefined} The row metadata if found, otherwise undefined.
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
     * Allocates and initializes the very first buffer in the store. This method
     * creates the buffer, sets its used bytes, updates metadata, and returns
     * the reference to the buffer, its index, and offset (starting at 0).
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata where the buffer is being created.
     * @param {number} size - The number of bytes initially needed in the new buffer.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *   An object containing the new GPU buffer, the assigned buffer index, and the offset at which data can be written.
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
     * Sets the newly allocated buffer's used bytes and updates the store metadata.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata where the buffer is being created.
     * @param {number} size - The number of bytes needed.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *   An object containing the new GPU buffer, the assigned buffer index, and the offset (always 0 for new buffers).
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
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata (contains dataType, typedArrayType, etc.).
     * @param {Uint8Array} copiedData - A Uint8Array representing the raw copied bytes.
     * @returns {any} The deserialized value, whose type depends on `storeMeta.dataType`.
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
     *
     * @private
     * @returns {void}
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
            if (this.readyResolver) {
                this.readyResolver();
                this.readyResolver = null;
                this.waitUntilReadyPromise = null;
            }
            this.isReady = true;
        }, 250);
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
        this.logPerformance(metrics, perKeyMetrics);
        return { results, perKeyMetrics };
    }
    /**
     * Combines multiple sort definitions into one by concatenating their `sortFields`.
     *
     * @private
     * @param {SortDefinition[]} definitions - An array of sort definitions to combine.
     * @returns {SortDefinition} A single sort definition containing all fields.
     */
    combineSortDefinitions(definitions) {
        const combined = { name: "combined", sortFields: [] };
        for (const def of definitions) {
            if (!def || !Array.isArray(def.sortFields)) {
                console.warn("A sort definition is missing 'sortFields':", def);
                continue;
            }
            for (const field of def.sortFields) {
                combined.sortFields.push({
                    sortColumn: field.sortColumn,
                    path: field.path,
                    sortDirection: field.sortDirection,
                });
            }
        }
        return combined;
    }
    /**
     * Builds a mapping from each path (e.g. "user.name") to an array of indices in the final offset array.
     * If a path appears in multiple sort fields, all relevant indices are collected.
     *
     * @private
     * @param {SortDefinition} sortDefinition - The combined sort definition containing multiple fields.
     * @returns {Record<string, number[]>} An object whose keys are paths and values are arrays of field indices.
     */
    buildPathIndexMap(sortDefinition) {
        const map = {};
        sortDefinition.sortFields.forEach((field, i) => {
            if (!map[field.path]) {
                map[field.path] = [];
            }
            map[field.path].push(i);
        });
        return map;
    }
    /**
     * Recursively measures how many characters a value would occupy in JSON,
     * without building the full JSON string. Updates `offsets` if `currentPath`
     * matches any field in `pathIndexMap`.
     *
     * @private
     * @param {any} value - The current value (object, array, primitive, etc.).
     * @param {string} currentPath - The dot-delimited path to this value (e.g. "user.address.street").
     * @param {Record<string, number[]>} pathIndexMap - Maps paths to an array of field indices.
     * @param {Uint32Array} offsets - The output array where offset pairs (start/end) are written.
     * @param {number} offsetBaseIndex - The starting index in `offsets` for this object/row.
     * @param {number} currentOffset - The current character offset in our hypothetical JSON string.
     * @returns {number} The new offset position after including the current value's JSON length.
     */
    measureValueWithOffsets(value, currentPath, pathIndexMap, offsets, offsetBaseIndex, currentOffset) {
        const relevantFieldIndices = pathIndexMap[currentPath];
        // Handle null
        if (value === null) {
            // "null" => length 4
            if (relevantFieldIndices) {
                for (const fieldIndex of relevantFieldIndices) {
                    const outIndex = offsetBaseIndex + fieldIndex * 2;
                    offsets[outIndex] = currentOffset;
                    offsets[outIndex + 1] = currentOffset + 4;
                }
            }
            return currentOffset + 4;
        }
        const valueType = typeof value;
        // Boolean
        if (valueType === "boolean") {
            // "true" => 4, "false" => 5
            const boolLen = value ? 4 : 5;
            if (relevantFieldIndices) {
                for (const fieldIndex of relevantFieldIndices) {
                    const outIndex = offsetBaseIndex + fieldIndex * 2;
                    offsets[outIndex] = currentOffset;
                    offsets[outIndex + 1] = currentOffset + boolLen;
                }
            }
            return currentOffset + boolLen;
        }
        // Number
        if (valueType === "number") {
            if (!Number.isFinite(value)) {
                // "null" => length 4
                if (relevantFieldIndices) {
                    for (const fieldIndex of relevantFieldIndices) {
                        const outIndex = offsetBaseIndex + fieldIndex * 2;
                        offsets[outIndex] = currentOffset;
                        offsets[outIndex + 1] = currentOffset + 4;
                    }
                }
                return currentOffset + 4;
            }
            else {
                // e.g. 1234 => length 4
                const strVal = String(value);
                const len = strVal.length;
                if (relevantFieldIndices) {
                    for (const fieldIndex of relevantFieldIndices) {
                        const outIndex = offsetBaseIndex + fieldIndex * 2;
                        offsets[outIndex] = currentOffset;
                        offsets[outIndex + 1] = currentOffset + len;
                    }
                }
                return currentOffset + len;
            }
        }
        // String
        if (valueType === "string") {
            // Measure length of JSON.stringify(value)
            const strJson = JSON.stringify(value);
            const len = strJson.length;
            if (relevantFieldIndices) {
                // Exclude the surrounding quotes from the offset range
                for (const fieldIndex of relevantFieldIndices) {
                    const outIndex = offsetBaseIndex + fieldIndex * 2;
                    offsets[outIndex] = currentOffset + 1; // skip leading quote
                    offsets[outIndex + 1] = currentOffset + len - 1; // skip trailing quote
                }
            }
            return currentOffset + len;
        }
        // Array
        if (Array.isArray(value)) {
            // '[' + ']' + commas + children
            let localOffset = currentOffset + 1; // '['
            for (let i = 0; i < value.length; i++) {
                if (i > 0) {
                    localOffset += 1; // comma
                }
                const nextPath = currentPath ? `${currentPath}.${i}` : String(i);
                localOffset = this.measureValueWithOffsets(value[i], nextPath, pathIndexMap, offsets, offsetBaseIndex, localOffset);
            }
            return localOffset + 1; // ']'
        }
        // Object
        let localOffset = currentOffset + 1; // '{'
        const keys = Object.keys(value);
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const propPath = currentPath ? `${currentPath}.${key}` : key;
            if (i > 0) {
                localOffset += 1; // comma
            }
            // Key in quotes
            const keyJson = JSON.stringify(key);
            const keyLen = keyJson.length;
            localOffset += keyLen; // length of the serialized key
            // colon
            localOffset += 1;
            // measure the child value
            localOffset = this.measureValueWithOffsets(value[key], propPath, pathIndexMap, offsets, offsetBaseIndex, localOffset);
        }
        return localOffset + 1; // '}'
    }
    /**
     * For one object, computes offsets for all fields in the given sort definition(s) and
     * stores them in a `finalOffsets` array (two indices per field: start and end).
     *
     * @private
     * @param {any} obj - The JavaScript object for which to compute field offsets.
     * @param {SortDefinition} sortDefinition - A combined sort definition containing multiple fields.
     * @param {Uint32Array} finalOffsets - A 2*N array of offset ranges, where N is number of fields.
     * @returns {void}
     */
    computeOffsetsSingleObject(obj, sortDefinition, finalOffsets) {
        const pathIndexMap = this.buildPathIndexMap(sortDefinition);
        // There's only one object, so offsetBaseIndex always starts at 0
        this.measureValueWithOffsets(obj, "", pathIndexMap, finalOffsets, 0, 0);
    }
    /**
     * Computes a single flat `Uint32Array` of offsets for one object,
     * across all fields in one or more sort definitions (two offsets per field).
     *
     * @param {any} objectData - A single JavaScript object whose field offsets we want.
     * @param {SortDefinition | SortDefinition[]} sortDefinitionOrDefinitions - One or multiple definitions.
     * @returns {Uint32Array} A flat array of offsets (two per field, for start and end).
     */
    getJsonFieldOffsetsFlattened(objectData, sortDefinitionOrDefinitions) {
        // Normalize to an array of definitions
        const definitions = Array.isArray(sortDefinitionOrDefinitions)
            ? sortDefinitionOrDefinitions
            : [sortDefinitionOrDefinitions];
        // Combine them
        const combinedDefinition = this.combineSortDefinitions(definitions);
        const totalFields = combinedDefinition.sortFields.length;
        // Allocate a buffer for offsets: (2 offsets) * (number of fields)
        const finalOffsets = new Uint32Array(totalFields * 2);
        // Populate offsets for this single object
        this.computeOffsetsSingleObject(objectData, combinedDefinition, finalOffsets);
        return finalOffsets;
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