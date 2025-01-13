// For convenience, define a simple flag for inactive rows, e.g. 0x1.
const ROW_INACTIVE_FLAG = 0x1;
/**
 * Rounds `value` up to the nearest multiple of `align`.
 */
function roundUp(value, align) {
    return Math.ceil(value / align) * align;
}
/** The Web Worker reference (lazy initialized) */
let gpuWorker = null;
function getGpuWorker() {
    if (!gpuWorker) {
        gpuWorker = new Worker(new URL("./gpuWorker.js", import.meta.url), {
            type: "module",
        });
        gpuWorker.onmessage = handleWorkerMessage;
        // Optionally, send an INIT_DEVICE message to the worker
        gpuWorker.postMessage({ type: "INIT_DEVICE" });
    }
    return gpuWorker;
}
/**
 * Handle responses coming back from the worker.
 */
function handleWorkerMessage(evt) {
    const msg = evt.data;
    if (!msg?.type)
        return;
    switch (msg.type) {
        case "DEVICE_READY": {
            console.log("[gpuWorker] Device is ready in worker:", msg.payload);
            break;
        }
        case "WRITE_DONE": {
            // The worker has successfully written the batch
            const { storeName, rowCount } = msg.payload;
            console.log(`[gpuWorker] WRITE_DONE for store="${storeName}" rowCount=${rowCount}`);
            // If we have a pending flush promise for this store, resolve it
            //if (storeFlushResolvers.has(storeName)) {
            //    storeFlushResolvers.get(storeName)!.resolve(msg.payload);
            //    storeFlushResolvers.delete(storeName);
            //}
            break;
        }
        case "ERROR": {
            console.error("[gpuWorker] Error:", msg.payload);
            // If we have a pending flush promise for some store, reject it
            // NOTE: in a real app, you might want a storeName in the error, etc.
            //for (const [storeName, resolvers] of storeFlushResolvers.entries()) {
            //    resolvers.reject(msg.payload);
            //    storeFlushResolvers.delete(storeName);
            //}
            break;
        }
    }
}
/**
 * Example flush helper that returns a Promise which resolves
 * once the worker has written everything successfully.
 */
async function sendBatchToWorker(storeName, dataToWrite, rowCount) {
    const worker = getGpuWorker();
    // Create a new promise + store the resolve/reject
    const promise = new Promise((resolve, reject) => { });
    // Post the data to the worker
    worker.postMessage({
        type: "WRITE_BATCH",
        payload: { storeName, batch: dataToWrite, rowCount },
    });
    // Wait for the worker to confirm (WRITE_DONE) or error
    await promise;
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
        console.log(`Created object store: ${storeName}`);
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
        console.log(`Deleted object store: ${storeName}`);
    }
    /**
     * Retrieves a list of all existing object store names.
     * @returns {string[]} An array containing the names of all object stores.
     */
    listObjectStores() {
        return Array.from(this.storeMetadataMap.keys());
    }
    /**
     * Adds data to a GPU-backed store but fails if the key already exists.
     * @param storeName - The name of the object store.
     * @param key - The unique key identifying the row.
     * @param value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns A promise that resolves once the data is stored.
     * @throws {Error} If the store does not exist or a record with the same key is already active.
     */
    async add(storeName, key, value) {
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        const keyMap = this.storeKeyMap.get(storeName);
        if (!keyMap) {
            // Should never happen unless the store is half-initialized
            throw new Error(`Key map for store "${storeName}" is missing or uninitialized.`);
        }
        // Serialize the incoming value to an ArrayBuffer
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);
        // Use the new "add" mode so that if a row exists and is active, it will fail
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, "add");
        // Once we have the rowMetadata, finalize the write
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        await this.finalizeWrite(storeMeta, rowMetadata, arrayBuffer, gpuBuffer);
        console.log(`Data added for key "${key}" in object store "${storeName}", row ${rowMetadata.rowId}.`);
    }
    /**
     * Stores or updates data in a GPU-backed store.
     * @param storeName - The name of the object store.
     * @param key - The unique key identifying the row.
     * @param value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns A promise that resolves once the data is stored.
     */
    async put(storeName, key, value) {
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }
        const keyMap = this.storeKeyMap.get(storeName);
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);
        // Use the "put" mode so overwrites are allowed
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, "put");
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        await this.finalizeWrite(storeMeta, rowMetadata, arrayBuffer, gpuBuffer);
        console.log(`Data stored (put) for key "${key}" in object store "${storeName}", row ${rowMetadata.rowId}.`);
    }
    /**
         * Retrieves data for a given key from the GPU buffer based on CPU metadata.
         * If the key does not exist or is inactive, it returns null.
         *
         * @param {string} storeName - The name of the object store.
         * @param {string} key - The unique identifier for the data.
         * @returns {Promise<any | null>} A promise resolving to the deserialized data (object, typed array, or raw bytes),
         * or null if not found or flagged inactive.
         */
    async get(storeName, key) {
        const storeMeta = this.getStoreMetadata(storeName);
        // Explicit check instead of "!"
        const keyMap = this.storeKeyMap.get(storeName);
        if (!keyMap) {
            console.warn(`No keyMap found for store "${storeName}".`);
            return null;
        }
        // 1. Lookup row metadata or return null if not found/inactive
        const rowMetadata = this.getRowMetadataForKey(storeMeta, keyMap, key);
        if (!rowMetadata) {
            return null;
        }
        // 2. Read GPU data into a CPU-based Uint8Array
        const copiedData = await this.readDataFromGPU(storeMeta, rowMetadata);
        // 3. Deserialize the bytes into the correct data type
        return this.deserializeData(storeMeta, copiedData);
    }
    /**
     * Deletes data for a specific key from the GPU-backed store.
     * @param storeName - The name of the object store.
     * @param key - The unique key identifying the row to delete.
     * @returns A promise that resolves once the data is marked inactive (and optionally zeroed out).
     */
    async delete(storeName, key) {
        // 1. Get store metadata & keyMap
        const storeMeta = this.getStoreMetadata(storeName);
        const keyMap = this.getKeyMap(storeName);
        // 2. Find row metadata for the active row
        const rowMetadata = this.findActiveRowMetadata(keyMap, key, storeMeta.rows);
        if (!rowMetadata) {
            console.log(`Key "${key}" not found or already inactive in store "${storeName}".`);
            return;
        }
        // 3. Wipe the GPU data (optional, but recommended for "true" deletion)
        await this.wipeRowDataInGPU(storeMeta, rowMetadata);
        // 4. Mark row as inactive and remove it from the keyMap
        this.markRowInactive(rowMetadata);
        keyMap.delete(key);
        // 5. Update store metadata to reflect the change
        this.updateStoreMetadata(storeMeta);
        console.log(`Deleted data for key "${key}" in object store "${storeName}".`);
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
     * Finds a row’s metadata for the given key if it is active (not flagged as inactive).
     *
     * @param {Map<string, number>} keyMap - The mapping of keys to row IDs for this store.
     * @param {string} key - The unique key identifying the row to look up.
     * @param {RowMetadata[]} rows - The array of RowMetadata objects for the store.
     * @returns {RowMetadata | null} The row’s metadata if found and active, or `null` otherwise.
     */
    findActiveRowMetadata(keyMap, key, rows) {
        const rowId = keyMap.get(key);
        if (rowId == null) {
            return null; // Key not found
        }
        const rowMetadata = rows.find((r) => r.rowId === rowId);
        if (!rowMetadata) {
            return null; // Row metadata missing
        }
        // Check if the row is flagged inactive
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
    * Retrieves the row metadata for a given key, ensuring it's active.
    *
    * @param {StoreMetadata} storeMeta - The metadata of the target store.
    * @param {Map<string, number>} keyMap - A mapping of keys to row IDs for the store.
    * @param {string} key - The key identifying which row to find.
    * @returns {RowMetadata | null} The RowMetadata if found and active, or null otherwise.
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
        console.log("Allocated first buffer chunk for store:", storeMeta);
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
        console.log("Allocated a new buffer chunk at index", newBufferIndex, "for store, usage size:", size);
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
     * Finds or creates a RowMetadata entry for the given key.
     * - If mode is "add", fail if the row already exists and is active.
     * - If mode is "put", overwrite if the row already exists (the default put behavior).
     *
     * @param storeMeta - The metadata of the target store.
     * @param keyMap - A map of key → rowId for the store.
     * @param key - The key identifying the row.
     * @param arrayBuffer - The serialized data to be written.
     * @param mode - Either "add" (disallow overwrite) or "put" (allow overwrite).
     * @returns A promise that resolves to the relevant RowMetadata (new or existing/updated).
     */
    async findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer, mode) {
        let rowId = keyMap.get(key);
        // Check if there's an existing row in CPU metadata
        let rowMetadata = rowId == null
            ? null
            : storeMeta.rows.find((r) => r.rowId === rowId) || null;
        // If an active row exists and we are in "add" mode, throw an error immediately
        if (mode === "add" && rowMetadata && !((rowMetadata.flags ?? 0) & ROW_INACTIVE_FLAG)) {
            // The row is active, so adding a duplicate is not allowed
            throw new Error(`Record with key "${key}" already exists in store and overwriting is not allowed (add mode).`);
        }
        // Allocate space in a GPU buffer
        const { gpuBuffer, bufferIndex, offset } = this.findOrCreateSpace(storeMeta, arrayBuffer.byteLength);
        // If row does not exist or is inactive, treat it as a new row
        if (!rowMetadata || ((rowMetadata.flags ?? 0) & ROW_INACTIVE_FLAG)) {
            // Create a new RowMetadata
            rowId = storeMeta.rows.length + 1;
            rowMetadata = {
                rowId,
                bufferIndex,
                offset,
                length: arrayBuffer.byteLength
            };
            storeMeta.rows.push(rowMetadata);
            keyMap.set(key, rowId);
            // Initial write of data for the newly created row
            await this.writeDataToBuffer(gpuBuffer, offset, arrayBuffer);
        }
        // Else if row is active and we're in "put" mode, we can overwrite
        else if (mode === "put") {
            // Reuse existing row (or possibly relocate it if new data is bigger)
            rowMetadata = await this.updateRowOnOverwrite(storeMeta, rowMetadata, arrayBuffer, keyMap, key);
        }
        return rowMetadata;
    }
    /**
     * Updates or reassigns a row when new data is larger or smaller than the existing row's capacity.
     * @param storeMeta - The metadata of the target store.
     * @param oldRowMeta - The existing row metadata being updated.
     * @param arrayBuffer - The new data to be written.
     * @param keyMap - A map of key-to-rowId for the store.
     * @param key - The key identifying the row.
     * @returns A promise that resolves to the updated or newly created RowMetadata.
     */
    async updateRowOnOverwrite(storeMeta, oldRowMeta, arrayBuffer, keyMap, key) {
        if (arrayBuffer.byteLength <= oldRowMeta.length) {
            // Overwrite in place
            const oldBuf = this.getBufferByIndex(storeMeta, oldRowMeta.bufferIndex);
            await this.writeDataToBuffer(oldBuf, oldRowMeta.offset, arrayBuffer);
            // Adjust length if new data is smaller
            if (arrayBuffer.byteLength < oldRowMeta.length) {
                oldRowMeta.length = arrayBuffer.byteLength;
            }
            return oldRowMeta;
        }
        else {
            // Mark old row inactive
            oldRowMeta.flags = (oldRowMeta.flags ?? 0) | 0x1;
            // Create new allocation
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
            // Write data to new allocation
            await this.writeDataToBuffer(gpuBuffer, offset, arrayBuffer);
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
}
//# sourceMappingURL=VideoDB.js.map