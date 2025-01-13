import { StoreMetadata, BufferMetadata, RowMetadata, RowMetadataAndData } from "./types/StoreMetadata";

// For convenience, define a simple flag for inactive rows, e.g. 0x1.
const ROW_INACTIVE_FLAG = 0x1;

/**
 * Rounds `value` up to the nearest multiple of `align`.
 */
function roundUp(value: number, align: number): number {
    return Math.ceil(value / align) * align;
}

/** The Web Worker reference (lazy initialized) */
let gpuWorker: Worker | null = null;
function getGpuWorker(): Worker {
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
function handleWorkerMessage(evt: MessageEvent) {
    const msg = evt.data;
    if (!msg?.type) return;

    switch (msg.type) {
        case "DEVICE_READY": {
            console.log("[gpuWorker] Device is ready in worker:", msg.payload);
            break;
        }
        case "WRITE_DONE": {
            // The worker has successfully written the batch
            const { storeName, rowCount } = msg.payload;
            console.log(`[gpuWorker] WRITE_DONE for store="${storeName}" rowCount=${rowCount}`);
            break;
        }
        case "ERROR": {
            console.error("[gpuWorker] Error:", msg.payload);
            break;
        }
    }
}

/**
 * Example flush helper that returns a Promise which resolves
 * once the worker has written everything successfully.
 */
async function sendBatchToWorker(
    storeName: string,
    dataToWrite: ArrayBuffer,
    rowCount: number
): Promise<void> {
    const worker = getGpuWorker();

    // Create a new promise + store the resolve/reject
    const promise = new Promise<void>((resolve, reject) => { });

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
function padJsonTo4Bytes(jsonString: string): string {
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
function padTo4Bytes(ab: ArrayBuffer): ArrayBuffer {
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
    public storeMetadataMap: Map<string, StoreMetadata>;
    public storeKeyMap: Map<string, Map<string, number>>;

    // The new properties that enable caching/batching:
    private pendingWrites: Array<{
        storeMeta: StoreMetadata;
        rowMetadata: RowMetadata;
        arrayBuffer: ArrayBuffer;
        gpuBuffer: GPUBuffer;
    }> = [];

    private readonly BATCH_SIZE = 2000; // e.g. auto-flush after 1000 writes

    /**
     * Initializes a new instance of the VideoDB class.
     * @param {GPUDevice} device - The GPU device to be used for buffer operations.
     */
    constructor(private device: GPUDevice) {
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
    public createObjectStore(
        storeName: string,
        options: {
            dataType: "TypedArray" | "ArrayBuffer" | "JSON";
            typedArrayType?: "Float32Array" | "Float64Array" | "Int32Array" | "Uint32Array" | "Uint8Array";
            bufferSize: number; // Typically 250 * 1024 * 1024
            rowSize?: number;
            totalRows: number;
        }
    ): void {
        if (this.storeMetadataMap.has(storeName)) {
            throw new Error(`Object store "${storeName}" already exists.`);
        }

        if (options.dataType === "TypedArray" && !options.typedArrayType) {
            throw new Error(`typedArrayType is required when dataType is "TypedArray".`);
        }

        let rowsPerBuffer: number | undefined;
        if (options.dataType !== "JSON" && options.rowSize) {
            rowsPerBuffer = Math.floor(options.bufferSize / options.rowSize);
        }

        const storeMetadata: StoreMetadata = {
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
    public deleteObjectStore(storeName: string): void {
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
    public listObjectStores(): string[] {
        return Array.from(this.storeMetadataMap.keys());
    }

    /**
     * Adds a new record to the specified store without immediately writing to the GPU.
     * Instead, it caches the data in a pending-writes array. Once the pending array
     * reaches 1000 entries, this method triggers a flush to the GPU.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @param {any} value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns {Promise<void>} A promise that resolves when the data is queued for writing.
     * @throws {Error} If the store does not exist or a record with the same key is already active (in "add" mode).
     */
    public async add(storeName: string, key: string, value: any): Promise<void> {
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }

        const keyMap = this.storeKeyMap.get(storeName)!;
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);

        // Find/create the CPU row metadata, but do NOT write to GPU here.
        const rowMetadata = await this.findOrCreateRowMetadata(
            storeMeta,
            keyMap,
            key,
            arrayBuffer,
            "add"
        );

        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);

        // Queue up the write instead of writing immediately to GPU:
        this.pendingWrites.push({ storeMeta, rowMetadata, arrayBuffer, gpuBuffer });

        // Auto-flush if we've reached the batch threshold:
        if (this.pendingWrites.length >= this.BATCH_SIZE) {
            await this.flushWrites();
        }

        console.log(
            `Data added for key "${key}" in "${storeName}", row ${rowMetadata.rowId} (pending write).`
        );
    }

    /**
     * Stores or updates data in the specified store without immediately writing to the GPU.
     * Instead, it caches the data in a pending-writes array. Once the pending array
     * reaches 1000 entries, this method triggers a flush to the GPU.
     *
     * @param {string} storeName - The name of the object store.
     * @param {string} key - The unique key identifying the row.
     * @param {any} value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns {Promise<void>} A promise that resolves when the data is queued for writing.
     * @throws {Error} If the store does not exist.
     */
    public async put(storeName: string, key: string, value: any): Promise<void> {
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }

        const keyMap = this.storeKeyMap.get(storeName)!;
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);

        // "put" mode allows overwrites if the key already exists.
        const rowMetadata = await this.findOrCreateRowMetadata(
            storeMeta,
            keyMap,
            key,
            arrayBuffer,
            "put"
        );

        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);

        // Queue up the write instead of writing immediately to GPU:
        this.pendingWrites.push({ storeMeta, rowMetadata, arrayBuffer, gpuBuffer });

        // Auto-flush if we've reached the batch threshold:
        if (this.pendingWrites.length >= this.BATCH_SIZE) {
            await this.flushWrites();
        }

        console.log(
            `Data stored (put) for key "${key}" in "${storeName}", row ${rowMetadata.rowId} (pending write).`
        );
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
    public async get(storeName: string, key: string): Promise<any | null> {
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
    public async delete(storeName: string, key: string): Promise<void> {
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
    public clear(storeName: string): void {
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
    public async *openCursor(
        storeName: string,
        options?: {
            range?: {
                lowerBound?: string;
                upperBound?: string;
                lowerInclusive?: boolean;
                upperInclusive?: boolean;
            };
            direction?: 'next' | 'prev';
        }
    ): AsyncGenerator<{ key: string; value: any }, void, unknown> {
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
        } else {
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
     * Flushes all pending writes to the GPU in a batched and optimized manner.
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
    public async flushWrites(): Promise<void> {
        if (this.pendingWrites.length === 0) {
            console.info(`No pending writes to flush.`);
            return;
        }

        console.info(`Flushing ${this.pendingWrites.length} writes to GPU...`);

        // 1. Group pendingWrites by their target GPU buffer.
        const writesByBuffer: Map<GPUBuffer, RowMetadataAndData[]> = new Map();

        for (const item of this.pendingWrites) {
            const { gpuBuffer, rowMetadata, arrayBuffer, storeMeta } = item;

            if (!writesByBuffer.has(gpuBuffer)) {
                writesByBuffer.set(gpuBuffer, []);
            }

            writesByBuffer.get(gpuBuffer)!.push({ rowMetadata, arrayBuffer, storeMeta });
        }

        // 2. Iterate over each GPU buffer group and process writes.
        for (const [gpuBuffer, writes] of writesByBuffer.entries()) {
            if (writes.length === 0) continue; // Safety check.

            // a. Sort writes by their byte offsets within the buffer (ascending order).
            writes.sort((a, b) => a.rowMetadata.offset - b.rowMetadata.offset);

            try {
                // b. Map the GPU buffer once for all writes.
                await gpuBuffer.mapAsync(GPUMapMode.WRITE);
                const mappedRange = gpuBuffer.getMappedRange();
                const mappedView = new Uint8Array(mappedRange);

                // c. Write each row's data into the mapped buffer at the specified offset.
                for (const write of writes) {
                    const { rowMetadata, arrayBuffer, storeMeta } = write;

                    // Ensure alignment (assuming already handled during serialization).
                    mappedView.set(new Uint8Array(arrayBuffer), rowMetadata.offset);

                    // Update row metadata.
                    rowMetadata.length = arrayBuffer.byteLength;
                    storeMeta.dirtyMetadata = true;
                    storeMeta.metadataVersion += 1;

                    console.debug(
                        `Written rowId=${rowMetadata.rowId} to GPU buffer at offset=${rowMetadata.offset}.`
                    );
                }

                // d. Unmap the buffer after all writes are completed.
                gpuBuffer.unmap();

                console.info(`Successfully flushed writes to GPU buffer.`);
            } catch (error) {
                console.error(`Error flushing writes to GPU buffer:`, error);
                // Optionally, handle specific errors or retry logic here.
            }
        }

        // 3. Clear the pendingWrites array after all writes have been processed.
        this.pendingWrites = [];
        console.info(`All pending writes have been flushed to GPU.`);
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
    private applyCustomRange(
        keys: string[],
        range: {
            lowerBound?: string;
            upperBound?: string;
            lowerInclusive?: boolean;
            upperInclusive?: boolean;
        }
    ): string[] {
        return keys.filter((key) => {
            let withinLower = true;
            let withinUpper = true;

            if (range.lowerBound !== undefined) {
                if (range.lowerInclusive) {
                    withinLower = key >= range.lowerBound;
                } else {
                    withinLower = key > range.lowerBound;
                }
            }

            if (range.upperBound !== undefined) {
                if (range.upperInclusive) {
                    withinUpper = key <= range.upperBound;
                } else {
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
    private getKeyMap(storeName: string): Map<string, number> {
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
    private compareKeys(a: string, b: string): number {
        if (a < b) return -1;
        if (a > b) return 1;
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
    private findActiveRowMetadata(
        keyMap: Map<string, number>,
        key: string,
        rows: RowMetadata[]
    ): RowMetadata | null {
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
    private async wipeRowDataInGPU(
        storeMeta: StoreMetadata,
        rowMetadata: RowMetadata
    ): Promise<void> {
        try {
            const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
            const zeroArray = new ArrayBuffer(rowMetadata.length);
            await this.writeDataToBuffer(gpuBuffer, rowMetadata.offset, zeroArray);
        } catch (error) {
            console.error(`Error zeroing out data for rowId=${rowMetadata.rowId}:`, error);
        }
    }

    /**
     * Marks the given row as inactive in CPU metadata (e.g., logically deleted).
     *
     * @param {RowMetadata} rowMetadata - The metadata for the row to be marked inactive.
     * @returns {void}
     */
    private markRowInactive(rowMetadata: RowMetadata): void {
        rowMetadata.flags = (rowMetadata.flags ?? 0) | ROW_INACTIVE_FLAG;
    }

    /**
     * Updates the store metadata to indicate that a change has occurred.
     * This increments the metadata version and sets the `dirtyMetadata` flag.
     *
     * @param {StoreMetadata} storeMeta - The store’s metadata object to be updated.
     * @returns {void}
     */
    private updateStoreMetadata(storeMeta: StoreMetadata): void {
        storeMeta.dirtyMetadata = true;
        storeMeta.metadataVersion += 1;
    }

    /**
     * Retrieves the metadata object for a specified store.
     * @param {string} storeName - The name of the store.
     * @returns {StoreMetadata} The metadata object for the specified store.
     * @throws {Error} If the specified store does not exist.
     */
    private getStoreMetadata(storeName: string): StoreMetadata {
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
    private getRowMetadataForKey(
        storeMeta: StoreMetadata,
        keyMap: Map<string, number>,
        key: string
    ): RowMetadata | null {
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
    private findOrCreateSpace(
        storeMeta: StoreMetadata,
        size: number
    ): {
        gpuBuffer: GPUBuffer;
        bufferIndex: number;
        offset: number;
    } {
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
    private createNewBuffer(storeMeta: StoreMetadata, size: number): GPUBuffer {
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
    private allocateFirstBufferChunk(
        storeMeta: StoreMetadata,
        size: number
    ): {
        gpuBuffer: GPUBuffer;
        bufferIndex: number;
        offset: number;
    } {
        const gpuBuffer = this.createNewBuffer(storeMeta, storeMeta.bufferSize);
        (gpuBuffer as any)._usedBytes = 0;

        storeMeta.buffers.push({
            bufferIndex: 0,
            startRow: -1,
            rowCount: 0,
            gpuBuffer
        });
        console.log("Allocated first buffer chunk for store:", storeMeta);

        // Place data at offset 0
        (gpuBuffer as any)._usedBytes = size;
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
    private getLastBufferUsage(
        storeMeta: StoreMetadata
    ): {
        lastBufferMeta: BufferMetadata;
        usedBytes: number;
    } {
        const lastIndex = storeMeta.buffers.length - 1;
        const lastBufferMeta = storeMeta.buffers[lastIndex];
        const gpuBuffer = lastBufferMeta.gpuBuffer!;
        const usedBytes = (gpuBuffer as any)._usedBytes || 0;
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
    private useSpaceInLastBuffer(
        lastBufferMeta: BufferMetadata,
        usedBytes: number,
        size: number
    ): {
        gpuBuffer: GPUBuffer;
        bufferIndex: number;
        offset: number;
    } {
        const gpuBuffer = lastBufferMeta.gpuBuffer!;
        // Align the offset to 256
        const alignedOffset = roundUp(usedBytes, 256);

        // Check capacity after alignment
        if (alignedOffset + size > gpuBuffer.size) {
            throw new Error("No space left in the last buffer after alignment.");
        }

        (gpuBuffer as any)._usedBytes = alignedOffset + size;
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
    private allocateNewBufferChunk(
        storeMeta: StoreMetadata,
        size: number
    ): {
        gpuBuffer: GPUBuffer;
        bufferIndex: number;
        offset: number;
    } {
        const newBufferIndex = storeMeta.buffers.length;
        const capacity = storeMeta.bufferSize;
        const newGpuBuffer = this.createNewBuffer(storeMeta, capacity);

        (newGpuBuffer as any)._usedBytes = size;
        storeMeta.buffers.push({
            bufferIndex: newBufferIndex,
            startRow: -1,
            rowCount: 1,
            gpuBuffer: newGpuBuffer
        });

        console.log(
            "Allocated a new buffer chunk at index",
            newBufferIndex,
            "for store, usage size:",
            size
        );

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
    private serializeValueForStore(storeMeta: StoreMetadata, value: any): ArrayBuffer {
        let resultBuffer: ArrayBuffer;

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
                    throw new Error(
                        `Value must be an instance of ${storeMeta.typedArrayType} for store "${storeMeta}".`
                    );
                }
                resultBuffer = (value as { buffer: ArrayBuffer }).buffer;
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
    private async findOrCreateRowMetadata(
        storeMeta: StoreMetadata,
        keyMap: Map<string, number>,
        key: string,
        arrayBuffer: ArrayBuffer,
        mode: "add" | "put"
    ): Promise<RowMetadata> {
        let rowId = keyMap.get(key);
        let rowMetadata = rowId == null
            ? null
            : storeMeta.rows.find((r) => r.rowId === rowId) || null;

        // If active row exists and we are in "add" mode, throw:
        if (mode === "add" && rowMetadata && !((rowMetadata.flags ?? 0) & ROW_INACTIVE_FLAG)) {
            throw new Error(
                `Record with key "${key}" already exists in store and overwriting is not allowed (add mode).`
            );
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
            rowMetadata = await this.updateRowOnOverwrite(
                storeMeta,
                rowMetadata,
                arrayBuffer,
                keyMap,
                key
            );
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
    private async updateRowOnOverwrite(
        storeMeta: StoreMetadata,
        oldRowMeta: RowMetadata,
        arrayBuffer: ArrayBuffer,
        keyMap: Map<string, number>,
        key: string
    ): Promise<RowMetadata> {
        // If the new data fits in the old space:
        if (arrayBuffer.byteLength <= oldRowMeta.length) {
            // We'll overwrite in-place later during flushWrites.
            // Just adjust length if the new data is smaller.
            if (arrayBuffer.byteLength < oldRowMeta.length) {
                oldRowMeta.length = arrayBuffer.byteLength;
            }
            return oldRowMeta;
        } else {
            // Mark old row inactive:
            oldRowMeta.flags = (oldRowMeta.flags ?? 0) | 0x1;

            // Find new space for the bigger data:
            const { gpuBuffer, bufferIndex, offset } = this.findOrCreateSpace(storeMeta, arrayBuffer.byteLength);

            const newRowId = storeMeta.rows.length + 1;
            const newRowMeta: RowMetadata = {
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
    private async finalizeWrite(
        storeMeta: StoreMetadata,
        rowMetadata: RowMetadata,
        arrayBuffer: ArrayBuffer,
        gpuBuffer: GPUBuffer
    ): Promise<void> {
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
    private async readDataFromGPU(
        storeMeta: StoreMetadata,
        rowMetadata: RowMetadata
    ): Promise<Uint8Array> {
        const chunkBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);

        // 1. Create a read buffer for GPU → CPU transfer
        const readBuffer = this.device.createBuffer({
            size: rowMetadata.length,
            usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST
        });

        // 2. Copy data from the chunk buffer into readBuffer
        const commandEncoder = this.device.createCommandEncoder();
        commandEncoder.copyBufferToBuffer(
            chunkBuffer,          // Source buffer
            rowMetadata.offset,
            readBuffer,           // Destination buffer
            0,
            rowMetadata.length
        );
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
    private deserializeData(storeMeta: StoreMetadata, copiedData: Uint8Array): any {
        switch (storeMeta.dataType) {
            case "JSON": {
                const jsonString = new TextDecoder().decode(copiedData);
                return JSON.parse(jsonString.trim());
            }
            case "TypedArray": {
                if (!storeMeta.typedArrayType) {
                    throw new Error(
                        `typedArrayType is missing for store with dataType "TypedArray".`
                    );
                }
                const TypedArrayCtor = (globalThis as any)[storeMeta.typedArrayType];
                if (typeof TypedArrayCtor !== "function") {
                    throw new Error(
                        `Invalid typedArrayType "${storeMeta.typedArrayType}".`
                    );
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
    private getBufferByIndex(storeMeta: StoreMetadata, bufferIndex: number): GPUBuffer {
        const bufMeta = storeMeta.buffers[bufferIndex];
        if (!bufMeta || !bufMeta.gpuBuffer) {
            throw new Error(`Buffer index ${bufferIndex} not found or uninitialized.`);
        }
        return bufMeta.gpuBuffer;
    }

    private async writeDataToBuffer(
        gpuBuffer: GPUBuffer,
        offset: number,
        arrayBuffer: ArrayBuffer
    ): Promise<number> {
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
        } catch (err) {
            console.error("Error writing to GPU buffer:", err);
            throw err;
        }

        return arrayBuffer.byteLength; // new padded length
    }
}