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
 * A simplified VideoDB class that:
 * - Uses 250MB GPU buffers as "chunks."
 * - Never destroys or reclaims space in old buffers.
 * - Stores all metadata in CPU memory.
 */
export class VideoDB {
    device;
    storeMetadataMap;
    storeKeyMap;
    // The new properties that enable caching/batching:
    pendingWrites = [];
    BATCH_SIZE = 10000; // e.g. auto-flush after 10000 writes
    flushTimer = null;
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
     *   typedArrayType?: "Float32Array" | "Float64Array" | "Int32Array" | "Uint32Array" | "Uint8Array";
     *   bufferSize: number;
     *   rowSize?: number;
     *   totalRows: number;
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
            metadataVersion: 0
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
     * for 1 second, this method triggers a flush to the GPU.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @param {any} value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns {Promise<void>} A promise that resolves when the data is queued for writing.
     * @throws {Error} If the store does not exist.
     */
    async put(storeName, key, value) {
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
     * Retrieves data for a specific key from the GPU-backed store.
     * Ensures all pending writes are flushed before reading.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @returns {Promise<any | null>} A promise that resolves with the retrieved data or null if not found.
     * @throws {Error} If the store does not exist.
     */
    async get(storeName, key) {
        // Ensure all pending writes are flushed before reading
        await this.flushWrites();
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        const rowMetadata = this.findActiveRowMetadata(keyMap, key, storeMeta.rows);
        if (!rowMetadata) {
            console.log(`Key "${key}" not found or inactive in store "${storeName}".`);
            return null;
        }
        const data = await this.readDataFromGPU(storeMeta, rowMetadata);
        return this.deserializeData(storeMeta, data);
    }
    /**
     * Reads an array of keys or wildcard patterns from the specified store,
     * using a staging buffer for each row, mirroring how the single `get()` method
     * works behind the scenes. Supports SQL Server–style wildcards in key names.
     *
     * @param {string} storeName - The target store name.
     * @param {string[]} keys - The array of keys (or wildcard patterns) to read.
     * @returns {Promise<(any|null)[]>} An array of deserialized data, matching the input order.
     */
    async getMultiple(storeName, keys) {
        // 1. Flush & retrieve store metadata
        const { storeMeta, keyMap, metrics } = await this.flushAndGetMetadata(storeName);
        // 2. Expand any wildcard patterns
        const expandedKeys = this.expandAllWildcards(keys, keyMap);
        // 3. Read rows
        const { results, perKeyMetrics } = await this.readAllRows(storeName, storeMeta, keyMap, expandedKeys);
        // 4. Log performance
        this.logPerformance(metrics, perKeyMetrics);
        return results;
    }
    /**
     * Flushes all pending writes, then retrieves the store metadata and key map.
     * Also tracks performance time for these operations.
     */
    async flushAndGetMetadata(storeName) {
        const performanceMetrics = {
            flushWrites: 0,
            metadataRetrieval: 0,
        };
        const flushStart = performance.now();
        await this.flushWrites(); // your method
        performanceMetrics.flushWrites = performance.now() - flushStart;
        const metadataStart = performance.now();
        const storeMeta = this.getStoreMetadata(storeName); // throws if undefined
        const keyMap = this.getKeyMap(storeName); // throws if undefined
        performanceMetrics.metadataRetrieval = performance.now() - metadataStart;
        return { storeMeta, keyMap, metrics: performanceMetrics };
    }
    /**
     * Converts a SQL Server–style LIKE pattern into a RegExp.
     * Supports the following:
     *   - % => .*  (any string)
     *   - _ => .   (any single character)
     *   - [abc] => [abc] (character class)
     *   - [^abc] => [^abc] (negated character class)
     */
    likeToRegex(pattern) {
        // Escape special regex chars, except for our placeholders: %, _, [, ]
        let regexPattern = pattern
            // Escape backslash first to avoid double-escape issues
            .replace(/\\/g, "\\\\")
            // Escape everything else that might conflict with regex
            .replace(/[.+^${}()|[\]\\]/g, (char) => `\\${char}`)
            // Convert SQL wildcards into regex equivalents
            .replace(/%/g, ".*")
            .replace(/_/g, ".");
        // Because we escaped '[' and ']' above, we need to revert them
        // for bracket expressions. We'll do a simple approach:
        regexPattern = regexPattern.replace(/\\\[(.*?)]/g, "[$1]");
        // Build final anchored regex
        return new RegExp(`^${regexPattern}$`, "u"); // "u" (Unicode) can help with extended chars
    }
    /**
     * Expands a single key or wildcard pattern into all matching keys from the key map.
     * If the string does not contain wildcard characters, returns array with just [key].
     */
    expandWildcard(key, keyMap) {
        // Quick check for wildcard chars. If none, just return [key].
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
        // This assumes you’ve created likeToRegex, expandWildcard, etc.
        return keys.flatMap((key) => this.expandWildcard(key, keyMap));
    }
    /**
     * Reads row data for a list of keys. Returns both the results array and
     * cumulative per-key performance metrics.
     */
    async readAllRows(storeName, storeMeta, keyMap, keys) {
        const results = new Array(keys.length).fill(null);
        // Initialize aggregated metrics
        const perKeyMetrics = {
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
        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const { value, metrics: singleKeyMetrics, mapBufferMetrics } = await this.getOneRow(storeName, storeMeta, keyMap, key);
            // Place value
            results[i] = value;
            // Accumulate metrics
            perKeyMetrics.findMetadata += singleKeyMetrics.findMetadata;
            perKeyMetrics.createBuffer += singleKeyMetrics.createBuffer;
            perKeyMetrics.copyBuffer += singleKeyMetrics.copyBuffer;
            perKeyMetrics.mapBuffer += singleKeyMetrics.mapBuffer;
            perKeyMetrics.deserialize += singleKeyMetrics.deserialize;
            perKeyMetrics.mapBufferSubsections.mapAsync += mapBufferMetrics.mapAsync;
            perKeyMetrics.mapBufferSubsections.getMappedRange += mapBufferMetrics.getMappedRange;
            perKeyMetrics.mapBufferSubsections.copyToUint8Array += mapBufferMetrics.copyToUint8Array;
            perKeyMetrics.mapBufferSubsections.unmap += mapBufferMetrics.unmap;
        }
        return { results, perKeyMetrics };
    }
    /**
     * Reads a single row from the store, including GPU copy and deserialization.
     * Returns the row's value (or null) plus timing metrics.
     */
    async getOneRow(storeName, storeMeta, keyMap, key) {
        // Partial metrics for a single key
        const metrics = {
            findMetadata: 0,
            createBuffer: 0,
            copyBuffer: 0,
            mapBuffer: 0,
            deserialize: 0,
        };
        const mapBufferMetrics = {
            mapAsync: 0,
            getMappedRange: 0,
            copyToUint8Array: 0,
            unmap: 0,
        };
        // 1. Find row metadata
        const findMetadataStart = performance.now();
        const rowMetadata = this.findActiveRowMetadata(keyMap, key, storeMeta.rows);
        metrics.findMetadata = performance.now() - findMetadataStart;
        if (!rowMetadata) {
            return { value: null, metrics, mapBufferMetrics };
        }
        // 2. Create read buffer
        const createBufferStart = performance.now();
        const readBuffer = this.device.createBuffer({
            size: rowMetadata.length,
            usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST,
        });
        metrics.createBuffer = performance.now() - createBufferStart;
        // 3. Copy GPU buffer data
        const copyBufferStart = performance.now();
        const commandEncoder = this.device.createCommandEncoder();
        commandEncoder.copyBufferToBuffer(this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex), // source
        rowMetadata.offset, readBuffer, 0, rowMetadata.length);
        this.device.queue.submit([commandEncoder.finish()]);
        metrics.copyBuffer = performance.now() - copyBufferStart;
        // 4. Map buffer (with sub-metrics)
        const mapBufferStart = performance.now();
        // 4a. mapAsync
        const mapAsyncStart = performance.now();
        await readBuffer.mapAsync(GPUMapMode.READ);
        mapBufferMetrics.mapAsync = performance.now() - mapAsyncStart;
        // 4b. getMappedRange
        const getMappedRangeStart = performance.now();
        const mappedRange = readBuffer.getMappedRange(0, rowMetadata.length);
        mapBufferMetrics.getMappedRange = performance.now() - getMappedRangeStart;
        // 4c. Copy data
        const copyToUint8ArrayStart = performance.now();
        const copiedData = new Uint8Array(mappedRange.slice(0));
        mapBufferMetrics.copyToUint8Array = performance.now() - copyToUint8ArrayStart;
        // 4d. unmap
        const unmapStart = performance.now();
        readBuffer.unmap();
        mapBufferMetrics.unmap = performance.now() - unmapStart;
        metrics.mapBuffer = performance.now() - mapBufferStart;
        // 5. Deserialize
        const deserializeStart = performance.now();
        const deserialized = this.deserializeData(storeMeta, copiedData);
        metrics.deserialize = performance.now() - deserializeStart;
        // 6. Cleanup
        readBuffer.destroy();
        return { value: deserialized, metrics, mapBufferMetrics };
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
     * Deletes data for a specific key from the GPU-backed store by batching the delete operation.
     * The actual GPU write and metadata update are deferred until `flushWrites` is called.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row to delete.
     * @returns {Promise<void>} A promise that resolves once the delete operation is queued.
     * @throws {Error} If the store does not exist.
     */
    async delete(storeName, key) {
        // 1. Retrieve store metadata and key map
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        // 2. Find row metadata for the active row
        const rowMetadata = this.findActiveRowMetadata(keyMap, key, storeMeta.rows);
        if (!rowMetadata) {
            console.log(`Key "${key}" not found or already inactive in store "${storeName}".`);
            return;
        }
        // 3. Create a zeroed ArrayBuffer to overwrite the row data (optional)
        const zeroedArrayBuffer = new ArrayBuffer(rowMetadata.length);
        const zeroedView = new Uint8Array(zeroedArrayBuffer);
        zeroedView.fill(0); // Optional: Fill with zeros for "true" deletion
        // 4. Queue the delete operation as a pending write
        this.pendingWrites.push({
            storeMeta,
            rowMetadata,
            arrayBuffer: zeroedArrayBuffer,
            gpuBuffer: this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex),
            operationType: 'delete',
            key // Include the key for metadata updates during flush
        });
        // 5. Reset the flush timer
        this.resetFlushTimer();
        // 6. Check if batch size threshold is met
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
        // 1. Retrieve store metadata and keyMap
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        // 2. Discard all row metadata
        storeMeta.rows = [];
        // 3. Destroy all existing GPU buffers
        for (const bufferMeta of storeMeta.buffers) {
            if (bufferMeta.gpuBuffer) {
                bufferMeta.gpuBuffer.destroy();
            }
        }
        // 4. Clear the array of buffers
        storeMeta.buffers = [];
        // 5. Recreate a single new GPU buffer (index = 0)
        const newGpuBuffer = this.device.createBuffer({
            size: storeMeta.bufferSize,
            usage: GPUBufferUsage.MAP_WRITE | GPUBufferUsage.COPY_SRC,
            mappedAtCreation: false
        });
        // 6. Add the newly created buffer to the store's metadata
        storeMeta.buffers.push({
            bufferIndex: 0,
            startRow: -1,
            rowCount: 0,
            gpuBuffer: newGpuBuffer
        });
        // 7. Clear the keyMap so there are no active keys
        keyMap.clear();
        // 8. Update store metadata and version
        this.updateStoreMetadata(storeMeta);
        console.log(`Cleared store "${storeName}", destroyed all GPU buffers, and recreated one fresh buffer.`);
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
        // 1. Retrieve store metadata and keyMap
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        // 2. Retrieve all active keys
        let activeKeys = Array.from(keyMap.keys());
        // 3. Apply key range filtering if a range is provided
        if (options?.range) {
            activeKeys = this.applyCustomRange(activeKeys, options.range);
        }
        // 4. Sort keys based on direction
        if (options?.direction === 'prev') {
            activeKeys.sort((a, b) => this.compareKeys(b, a));
        }
        else {
            // Default to 'next' direction
            activeKeys.sort((a, b) => this.compareKeys(a, b));
        }
        // 5. Iterate over the sorted, filtered keys and yield records
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
     * Flushes all pending writes (ADD, PUT, DELETE) to the GPU in a batched and optimized manner.
     *
     * This method performs the following steps:
     * 1. Groups all pending writes by their target GPU buffer.
     * 2. Sorts each group of writes by their byte offsets within the buffer.
     * 3. Maps each GPU buffer once, writes all relevant data sequentially, and then unmaps.
     * 4. Updates the associated row and store metadata accordingly.
     *
     * This approach ensures that multiple writes to the same buffer are handled efficiently,
     * reducing the number of GPU buffer mappings and maximizing write throughput.
     *
     * @returns {Promise<void>} A promise that resolves once all pending writes have been applied.
     */
    async flushWrites() {
        if (this.pendingWrites.length === 0) {
            return;
        }
        // 1. Group by GPU buffer
        const writesByBuffer = new Map();
        for (const item of this.pendingWrites) {
            const { gpuBuffer } = item;
            if (!writesByBuffer.has(gpuBuffer)) {
                writesByBuffer.set(gpuBuffer, []);
            }
            writesByBuffer.get(gpuBuffer).push(item);
        }
        // 2. We'll track which writes succeeded
        const successfulWrites = new Set();
        // 3. For each GPU buffer, attempt to write
        for (const [gpuBuffer, writeGroup] of writesByBuffer.entries()) {
            // Sort the group by offset
            writeGroup.sort((a, b) => a.rowMetadata.offset - b.rowMetadata.offset);
            try {
                // Map once
                await gpuBuffer.mapAsync(GPUMapMode.WRITE);
                const mappedRange = gpuBuffer.getMappedRange();
                const mappedView = new Uint8Array(mappedRange);
                // For each write
                for (const pendingWrite of writeGroup) {
                    try {
                        const { rowMetadata, arrayBuffer } = pendingWrite;
                        mappedView.set(new Uint8Array(arrayBuffer), rowMetadata.offset);
                        // If no error, mark as successful
                        successfulWrites.add(pendingWrite);
                    }
                    catch (singleWriteError) {
                        // Possibly keep it for retry or log it
                        console.error('Error writing single item:', singleWriteError);
                    }
                }
                // Unmap
                gpuBuffer.unmap();
            }
            catch (mapError) {
                // If we can’t even map, everything in this group fails
                console.error('Error mapping GPU buffer:', mapError);
            }
        }
        // 4. Remove successful writes from pendingWrites
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
         * @param keyMap - The key map for the store.
         * @param key - The key to search for.
         * @param rows - The array of row metadata.
         * @returns The active RowMetadata or null if not found/inactive.
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
     * Overwrites the GPU buffer region for the specified row with zeros.
     * This effectively wipes out the data in GPU memory for that row.
     *
     * @param {StoreMetadata} storeMeta - The metadata of the store containing the GPU buffer.
     * @param {RowMetadata} rowMetadata - The metadata for the row to be wiped.
     * @returns {Promise<void>} A promise that resolves once the wipe operation completes.
     */
    async wipeRowDataInGPU(storeMeta, rowMetadata) {
        try {
            const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
            const zeroArray = new ArrayBuffer(rowMetadata.length);
            await this.writeDataToBuffer(gpuBuffer, rowMetadata.offset, zeroArray);
        }
        catch (error) {
            console.error(`Error zeroing out data for rowId=${rowMetadata.rowId}:`, error);
        }
    }
    /**
     * Marks the given row as inactive in CPU metadata (e.g., logically deleted).
     *
     * @param {RowMetadata} rowMetadata - The metadata for the row to be marked inactive.
     * @returns {void}
     */
    markRowInactive(rowMetadata) {
        rowMetadata.flags = (rowMetadata.flags ?? 0) | ROW_INACTIVE_FLAG;
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
     * Retrieves the row metadata for a specific key from the given store.
     * Ensures that the row is active and exists in the store.
     *
     * @param {StoreMetadata} storeMeta - The metadata of the store containing the row.
     * @param {Map<string, number>} keyMap - The map of keys to row IDs.
     * @param {string} key - The unique key identifying the row.
     * @returns {RowMetadata | null} The metadata for the row, or null if not found.
     */
    getRowMetadataForKey(storeMeta, keyMap, key) {
        const rowId = keyMap.get(key);
        if (rowId == null) {
            return null;
        }
        const rowMetadata = storeMeta.rows.find((r) => r.rowId === rowId);
        if (!rowMetadata || (rowMetadata.flags ?? 0) & ROW_INACTIVE_FLAG) {
            return null;
        }
        return rowMetadata;
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
        const capacity = storeMeta.bufferSize; // e.g. 250MB
        if (usedBytes + size <= capacity) {
            // There's enough space in the last buffer
            return this.useSpaceInLastBuffer(lastBufferMeta, usedBytes, size);
        }
        // Not enough space, so allocate a new buffer
        return this.allocateNewBufferChunk(storeMeta, size);
    }
    /**
     * Creates a new GPU buffer for the given store metadata with the specified size.
     * @param {StoreMetadata} storeMeta - The metadata of the store that requires a new GPU buffer.
     * @param {number} size - The size of the new GPU buffer in bytes.
     * @returns {GPUBuffer} The newly created GPU buffer.
     */
    createNewBuffer(storeMeta, size) {
        return this.device.createBuffer({
            size,
            usage: GPUBufferUsage.MAP_WRITE | GPUBufferUsage.COPY_SRC,
            mappedAtCreation: false
        });
    }
    /**
     * Allocates and initializes the very first buffer in the store.
     * @param {StoreMetadata} storeMeta - The metadata of the store where the buffer is being created.
     * @param {number} size - The initial number of bytes needed in the new buffer.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *          An object containing the GPU buffer, its index, and the offset where data will be written.
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
        // Place data at offset 0
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
     * @param {StoreMetadata} storeMeta - The metadata of the store.
     * @returns {{ lastBufferMeta: BufferMetadata; usedBytes: number }}
     *          An object containing the last buffer's metadata and how many bytes have been used so far.
     */
    getLastBufferUsage(storeMeta) {
        const lastIndex = storeMeta.buffers.length - 1;
        const lastBufferMeta = storeMeta.buffers[lastIndex];
        const gpuBuffer = lastBufferMeta.gpuBuffer;
        const usedBytes = gpuBuffer._usedBytes || 0;
        return { lastBufferMeta, usedBytes };
    }
    /**
     * Uses existing space in the last buffer if there is enough capacity.
     * @param {BufferMetadata} lastBufferMeta - The metadata of the last buffer in the store.
     * @param {number} usedBytes - How many bytes have been used so far in this buffer.
     * @param {number} size - The number of bytes needed.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *          An object containing the GPU buffer, its index, and the offset where data will be written.
     */
    useSpaceInLastBuffer(lastBufferMeta, usedBytes, size) {
        const gpuBuffer = lastBufferMeta.gpuBuffer;
        // Align the offset to 256
        const alignedOffset = roundUp(usedBytes, 256);
        // Check capacity after alignment
        if (alignedOffset + size > gpuBuffer.size) {
            throw new Error("No space left in the last buffer after alignment.");
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
     * @param {StoreMetadata} storeMeta - The metadata of the store that needs a new buffer.
     * @param {number} size - The number of bytes to reserve in the new buffer.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *          An object containing the GPU buffer, its index, and the starting offset where data will be written.
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
     *
     * @param storeMeta - Metadata defining the store's dataType (JSON, TypedArray, or ArrayBuffer).
     * @param value - The data to be serialized.
     * @returns A 4-byte-aligned ArrayBuffer containing the serialized data.
     */
    serializeValueForStore(storeMeta, value) {
        let resultBuffer;
        switch (storeMeta.dataType) {
            case "JSON": {
                // Existing JSON logic
                let jsonString = JSON.stringify(value);
                jsonString = padJsonTo4Bytes(jsonString); // Your existing JSON-specific string padding
                const cloned = new TextEncoder().encode(jsonString).slice();
                resultBuffer = cloned.buffer;
                break;
            }
            case "TypedArray": {
                // For typed arrays, we just grab .buffer
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
     * Finds or creates a RowMetadata entry for the given key. Unlike the previous version,
     * this no longer does the actual GPU write. It only determines where the data
     * should go (offset, bufferIndex) and updates CPU-side metadata. The GPU write is
     * deferred until a flush operation.
     *
     * @param {StoreMetadata} storeMeta - The metadata of the target store.
     * @param {Map<string, number>} keyMap - A map of key-to-rowId for the store.
     * @param {string} key - The unique key identifying the row.
     * @param {ArrayBuffer} arrayBuffer - The data to be stored, already serialized/padded.
     * @param {"add" | "put"} mode - "add" disallows overwrites, "put" allows them.
     * @returns {Promise<RowMetadata>} A promise that resolves with the row’s metadata.
     * @throws {Error} If the key already exists while in "add" mode.
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
     * deactivates the old row and finds a new location in the GPU buffer. Note that
     * we do NOT write the data to the GPU here; we only choose the new offset.
     *
     * @param {StoreMetadata} storeMeta - The metadata of the store.
     * @param {RowMetadata} oldRowMeta - The existing row metadata to be overwritten.
     * @param {ArrayBuffer} arrayBuffer - The new data (serialized/padded).
     * @param {Map<string, number>} keyMap - The store’s key→rowId mapping.
     * @param {string} key - The unique key for the row.
     * @returns {Promise<RowMetadata>} The newly updated or created row metadata.
     */
    async updateRowOnOverwrite(storeMeta, oldRowMeta, arrayBuffer, keyMap, key) {
        // If the new data fits in the old space:
        if (arrayBuffer.byteLength <= oldRowMeta.length) {
            // We'll overwrite in-place later during flushWrites.
            // Just adjust length if the new data is smaller.
            if (arrayBuffer.byteLength < oldRowMeta.length) {
                oldRowMeta.length = arrayBuffer.byteLength;
            }
            return oldRowMeta;
        }
        else {
            // Mark old row inactive:
            oldRowMeta.flags = (oldRowMeta.flags ?? 0) | 0x1;
            // Find new space for the bigger data:
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
            // The actual GPU write is deferred until flushWrites().
            return newRowMeta;
        }
    }
    /**
     * Completes a write operation by writing data again for alignment and updating row metadata.
     * @param storeMeta - The metadata of the target store.
     * @param rowMetadata - The row metadata associated with the current write.
     * @param arrayBuffer - The serialized data to be written.
     * @param gpuBuffer - The target GPU buffer to be written to.
     * @returns A promise that resolves once the aligned write is completed and metadata is updated.
     */
    async finalizeWrite(storeMeta, rowMetadata, arrayBuffer, gpuBuffer) {
        const alignedLength = await this.writeDataToBuffer(gpuBuffer, rowMetadata.offset, arrayBuffer);
        rowMetadata.length = alignedLength;
        storeMeta.dirtyMetadata = true;
        storeMeta.metadataVersion += 1;
    }
    /**
     * Reads data from the GPU buffer based on row metadata.
     *
     * @param {StoreMetadata} storeMeta - The metadata of the target store.
     * @param {RowMetadata} rowMetadata - The row metadata specifying which buffer/offset to read.
     * @returns {Promise<Uint8Array>} A promise resolving to a Uint8Array containing the copied data.
     */
    async readDataFromGPU(storeMeta, rowMetadata) {
        const chunkBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        // 1. Create a read buffer for GPU → CPU transfer
        const readBuffer = this.device.createBuffer({
            size: rowMetadata.length,
            usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST
        });
        // 2. Copy data from the chunk buffer into readBuffer
        const commandEncoder = this.device.createCommandEncoder();
        commandEncoder.copyBufferToBuffer(chunkBuffer, // Source buffer
        rowMetadata.offset, readBuffer, // Destination buffer
        0, rowMetadata.length);
        this.device.queue.submit([commandEncoder.finish()]);
        // 3. Map the read buffer to access the data
        await readBuffer.mapAsync(GPUMapMode.READ);
        const mappedRange = readBuffer.getMappedRange(0, rowMetadata.length);
        // 4. Copy out the data before unmapping
        const copiedData = new Uint8Array(mappedRange.slice(0));
        readBuffer.unmap();
        return copiedData;
    }
    /**
         * Deserializes the raw data from the GPU buffer based on store metadata.
         *
         * @param {StoreMetadata} storeMeta - The metadata of the store.
         * @param {Uint8Array} copiedData - The raw bytes read from the GPU buffer.
         * @returns {any} The deserialized data.
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
    *
    * @param {StoreMetadata} storeMeta - The metadata of the store containing the buffer.
    * @param {number} bufferIndex - The index of the buffer to retrieve.
    * @returns {GPUBuffer} The GPU buffer at the specified index.
    * @throws {Error} If the buffer is not found or is uninitialized.
    */
    getBufferByIndex(storeMeta, bufferIndex) {
        const bufMeta = storeMeta.buffers[bufferIndex];
        if (!bufMeta || !bufMeta.gpuBuffer) {
            throw new Error(`Buffer index ${bufferIndex} not found or uninitialized.`);
        }
        return bufMeta.gpuBuffer;
    }
    /**
     * Writes data to the specified GPU buffer at a given offset, ensuring
     * the data is aligned to 4-byte boundaries.
     *
     * @param {GPUBuffer} gpuBuffer - The GPU buffer to write to.
     * @param {number} offset - The offset in the buffer where the data will be written.
     * @param {ArrayBuffer} arrayBuffer - The data to write to the buffer.
     * @returns {Promise<number>} The length of the data written (aligned size).
     * @throws {Error} If the write operation fails.
     */
    async writeDataToBuffer(gpuBuffer, offset, arrayBuffer) {
        // --- 4-byte alignment fix ---
        const remainder = arrayBuffer.byteLength % 4;
        if (remainder !== 0) {
            // Create a padded copy
            const needed = 4 - remainder;
            const padded = new Uint8Array(arrayBuffer.byteLength + needed);
            padded.set(new Uint8Array(arrayBuffer), 0);
            arrayBuffer = padded.buffer;
        }
        console.log("Buffer size:", gpuBuffer.size);
        console.log("Offset:", offset, "Write length:", arrayBuffer.byteLength);
        try {
            await gpuBuffer.mapAsync(GPUMapMode.WRITE);
            console.log("Buffer successfully mapped.");
            const mappedRange = gpuBuffer.getMappedRange(offset, arrayBuffer.byteLength);
            new Uint8Array(mappedRange).set(new Uint8Array(arrayBuffer));
            gpuBuffer.unmap();
            console.log("Data successfully written to GPU buffer.");
        }
        catch (err) {
            console.error("Error writing to GPU buffer:", err);
            throw err;
        }
        return arrayBuffer.byteLength; // new padded length
    }
    /**
     * Resets the flush timer to delay writing pending operations to the GPU.
     * If a timer is already set, it clears it and starts a new one.
     *
     * @private
     */
    resetFlushTimer() {
        // If a timer is already set, clear it
        if (this.flushTimer !== null) {
            clearTimeout(this.flushTimer);
        }
        // Set a new timer
        this.flushTimer = window.setTimeout(() => {
            this.flushWrites().catch(error => {
                console.error('Error during timed flushWrites:', error);
            });
            this.flushTimer = null; // Reset the timer handle
        }, 250); // 250 ms
    }
}
//# sourceMappingURL=VideoDB.js.map