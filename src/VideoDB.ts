// Copyright © 2025 Jon Griebel. dgriebel2014@gmail.com - All rights reserved.
// Distributed under the MIT license.

import {
    StoreMetadata,
    BufferMetadata,
    RowMetadata,
    InitialMetrics,
    MapBufferSubsections,
    PerKeyMetrics,
    RowInfo
} from "./types/StoreMetadata";

// For convenience, define a simple flag for inactive rows, e.g. 0x1.
const ROW_INACTIVE_FLAG = 0x1;

/**
 * Rounds `value` up to the nearest multiple of `align`.
 */
function roundUp(value: number, align: number): number {
    return Math.ceil(value / align) * align;
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
 * Represents a single write operation's relevant metadata and data,
 * including the type of operation: 'add', 'put', or 'delete'.
 */
interface PendingWrite {
    storeMeta: StoreMetadata;
    rowMetadata: RowMetadata;
    arrayBuffer: ArrayBuffer;
    gpuBuffer: GPUBuffer;
    operationType: 'add' | 'put' | 'delete';
    key?: string; // Required for 'delete' operations
}

/**
 * A VideoDB class that stores all metadata in CPU memory, 
 * and actual data on the GPU
 */
export class VideoDB {
    public storeMetadataMap: Map<string, StoreMetadata>;
    public storeKeyMap: Map<string, Map<string, number>>;
    // The new properties that enable caching/batching:
    public pendingWrites: PendingWrite[] = [];
    private readonly BATCH_SIZE = 10000; // e.g. auto-flush after 10000 writes
    private flushTimer: number | null = null;

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
    public deleteObjectStore(storeName: string): void {
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
    public listObjectStores(): string[] {
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
    public async get(storeName: string, key: string): Promise<any | null> {
        // Call getMultiple with a single key
        const results = await this.getMultiple(storeName, [key]);

        // Extract the first (and only) result
        const result = results[0];

        return result;
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
    public async getMultiple(storeName: string, keys: string[]): Promise<(any | null)[]> {
        // Flush & retrieve store metadata
        const { storeMeta, keyMap, metrics } = await this.flushAndGetMetadata(storeName);

        // Expand any wildcard patterns
        const expandedKeys = this.expandAllWildcards(keys, keyMap);

        // One readAllRows call that does exactly ONE mapAsync
        const { results, perKeyMetrics } = await this.readAllRows(
            storeName,
            storeMeta,
            keyMap,
            expandedKeys
        );

        // Log or accumulate performance
        this.logPerformance(metrics, perKeyMetrics);

        return results;
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
    public async delete(storeName: string, key: string): Promise<void> {
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
    public clear(storeName: string): void {
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

        // Recreate a single new GPU buffer (index = 0)
        const newGpuBuffer = this.device.createBuffer({
            size: storeMeta.bufferSize,
            usage: GPUBufferUsage.MAP_WRITE | GPUBufferUsage.COPY_SRC,
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
        } else {
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
    private async checkAndFlush(): Promise<void> {
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
    private async flushWrites(): Promise<void> {
        if (this.pendingWrites.length === 0) {
            return;
        }

        // Group by GPU buffer
        const writesByBuffer: Map<GPUBuffer, PendingWrite[]> = new Map();
        for (const item of this.pendingWrites) {
            const { gpuBuffer } = item;
            if (!writesByBuffer.has(gpuBuffer)) {
                writesByBuffer.set(gpuBuffer, []);
            }
            writesByBuffer.get(gpuBuffer)!.push(item);
        }

        // We'll track which writes succeeded
        const successfulWrites = new Set<PendingWrite>();

        // For each GPU buffer, attempt to write
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
                    } catch (singleWriteError) {
                        // Possibly keep it for retry or log it
                        console.error('Error writing single item:', singleWriteError);
                    }
                }

                // Unmap
                gpuBuffer.unmap();
            } catch (mapError) {
                // If we can’t even map, everything in this group fails
                console.error('Error mapping GPU buffer:', mapError);
            }
        }

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
     * Finds the active row metadata for a given key.
     *
     * @param {Map<string, number>} keyMap - The key map for the store.
     * @param {string} key - The key to search for.
     * @param {RowMetadata[]} rows - The array of row metadata.
     * @returns {RowMetadata | null} The active RowMetadata or null if not found/inactive.
     */
    private findActiveRowMetadata(
        keyMap: Map<string, number>,
        key: string,
        rows: RowMetadata[]
    ): RowMetadata | null {
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
     * Deserializes the raw data from the GPU buffer based on store metadata.
     *
     * @param {StoreMetadata} storeMeta - The metadata of the store (dataType, typedArrayType, etc.).
     * @param {Uint8Array} copiedData - The raw bytes read from the GPU buffer.
     * @returns {any} The deserialized data (JSON, TypedArray, ArrayBuffer, etc.).
     * @throws {Error} If the dataType is unknown or invalid.
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

    /**
     * Resets the flush timer to delay writing pending operations to the GPU.
     * If a timer is already set, it clears it and starts a new one.
     * 
     * @private
     */
    private resetFlushTimer(): void {
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
        }, 250);
    }

    /**
     * Logs consolidated performance metrics to the console.
     */
    private logPerformance(initialMetrics: InitialMetrics, perKeyMetrics: PerKeyMetrics) {
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
     * Also tracks performance time for these operations.
     */
    private async flushAndGetMetadata(storeName: string): Promise<{
        storeMeta: StoreMetadata;
        keyMap: Map<string, any>;
        metrics: InitialMetrics;
    }> {
        const performanceMetrics: InitialMetrics = {
            flushWrites: 0,
            metadataRetrieval: 0,
        };

        const flushStart = performance.now();
        await this.flushWrites(); // your method
        performanceMetrics.flushWrites = performance.now() - flushStart;

        const metadataStart = performance.now();
        const storeMeta = this.getStoreMetadata(storeName) as StoreMetadata; // throws if undefined
        const keyMap = this.getKeyMap(storeName) as Map<string, any>;       // throws if undefined
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
    private likeToRegex(pattern: string): RegExp {
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
    private expandWildcard(key: string, keyMap: Map<string, any>): string[] {
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
    private expandAllWildcards(keys: string[], keyMap: Map<string, any>): string[] {
        // This assumes you’ve created likeToRegex, expandWildcard, etc.
        return keys.flatMap((key) => this.expandWildcard(key, keyMap));
    }

    /**
     * High-level method to read row data for a list of keys in a single GPU mapAsync call.
     * Orchestrates all the steps to gather metadata, copy buffers, and deserialize the data.
     *
     * @param {string} storeName - The name of the store.
     * @param {StoreMetadata} storeMeta - The metadata describing the store.
     * @param {Map<string, any>} keyMap - A mapping of keys to row identifiers.
     * @param {string[]} keys - The keys for which data is requested.
     * @returns {Promise<{ results: (any | null)[]; perKeyMetrics: PerKeyMetrics }>}
     *   An object containing the array of results and collected performance metrics.
     */
    private async readAllRows(
        storeName: string,
        storeMeta: StoreMetadata,
        keyMap: Map<string, any>,
        keys: string[]
    ): Promise<{ results: (any | null)[]; perKeyMetrics: PerKeyMetrics }> {
        // Prepare the results array (initialized to null)
        const results = new Array<(any | null)>(keys.length).fill(null);

        // Metrics structure
        const perKeyMetrics: PerKeyMetrics = this.initializeMetrics();

        // 1. Collect row metadata for each key
        const { rowInfos, totalBytes } = this.collectRowInfos(
            keyMap,
            storeMeta,
            keys,
            results,
            perKeyMetrics
        );

        // Early exit if nothing to read
        if (rowInfos.length === 0) {
            return { results, perKeyMetrics };
        }

        // 2. Create a single large GPU buffer
        const bigReadBuffer = this.createBigReadBuffer(totalBytes, perKeyMetrics);

        // 3. Copy data from each row’s GPU buffer into the large buffer
        this.copyRowsIntoBigBuffer(rowInfos, storeMeta, bigReadBuffer, perKeyMetrics);

        // 4. Read all data at once (mapAsync, getMappedRange, unmap)
        const bigCopiedData = await this.mapAndReadBuffer(bigReadBuffer, perKeyMetrics);

        // 5. Deserialize each row
        this.deserializeRows(rowInfos, storeMeta, bigCopiedData, results, perKeyMetrics);

        // Cleanup the buffer
        bigReadBuffer.destroy();

        return { results, perKeyMetrics };
    }

    /**
     * Gathers row metadata (rowInfos) for each provided key.
     * Also tracks total bytes to be copied.
     *
     * @param {Map<string, any>} keyMap - A mapping of keys to row IDs.
     * @param {StoreMetadata} storeMeta - The store metadata, including rows.
     * @param {string[]} keys - The list of keys to look up.
     * @param {(any | null)[]} results - The results array to fill with data.
     * @param {PerKeyMetrics} perKeyMetrics - The object tracking performance metrics.
     * @returns {{ rowInfos: RowInfo[], totalBytes: number }} - The row metadata and the total size in bytes.
     */
    private collectRowInfos(
        keyMap: Map<string, any>,
        storeMeta: StoreMetadata,
        keys: string[],
        results: (any | null)[],
        perKeyMetrics: PerKeyMetrics
    ): { rowInfos: RowInfo[]; totalBytes: number } {
        const findMetadataStart = performance.now();

        const rowInfos: RowInfo[] = [];
        let totalBytes = 0;

        for (let i = 0; i < keys.length; i++) {
            const key = keys[i];
            const rowMetadata = this.findActiveRowMetadata(keyMap, key, storeMeta.rows);
            if (!rowMetadata) {
                // If row not found, results[i] remains null
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
    * Creates a single large GPU buffer for reading. 
    * Tracks time in the performance metrics.
    *
    * @param {number} totalBytes - The total size in bytes for all row data.
    * @param {PerKeyMetrics} perKeyMetrics - The metrics object to update.
    * @returns {GPUBuffer} The newly created GPU buffer for reading.
    */
    private createBigReadBuffer(
        totalBytes: number,
        perKeyMetrics: PerKeyMetrics
    ): GPUBuffer {
        const createBufferStart = performance.now();

        const bigReadBuffer = this.device.createBuffer({
            size: totalBytes,
            usage: GPUBufferUsage.MAP_READ | GPUBufferUsage.COPY_DST,
        });

        perKeyMetrics.createBuffer = performance.now() - createBufferStart;
        return bigReadBuffer;
    }

    /**
     * Copies data from each row’s GPU buffer into a larger buffer for subsequent read.
     *
     * @param {RowInfo[]} rowInfos - Metadata about where each row resides in the final buffer.
     * @param {StoreMetadata} storeMeta - The store's metadata, including buffer references.
     * @param {GPUBuffer} bigReadBuffer - The destination GPU buffer for reads.
     * @param {PerKeyMetrics} perKeyMetrics - Performance metrics object to update.
     */
    private copyRowsIntoBigBuffer(
        rowInfos: RowInfo[],
        storeMeta: StoreMetadata,
        bigReadBuffer: GPUBuffer,
        perKeyMetrics: PerKeyMetrics
    ): void {
        const copyBufferStart = performance.now();

        const commandEncoder = this.device.createCommandEncoder();

        for (const rowInfo of rowInfos) {
            const srcBuffer = this.getBufferByIndex(storeMeta, rowInfo.rowMetadata.bufferIndex);
            commandEncoder.copyBufferToBuffer(
                srcBuffer,
                rowInfo.rowMetadata.offset,        // source offset
                bigReadBuffer,
                rowInfo.offsetInFinalBuffer,       // dest offset
                rowInfo.length
            );
        }

        // Submit the copy commands
        this.device.queue.submit([commandEncoder.finish()]);

        perKeyMetrics.copyBuffer = performance.now() - copyBufferStart;
    }

    /**
     * Maps the buffer for reading, copies data to a Uint8Array, and unmaps it.
     * Updates performance metrics accordingly.
     *
     * @param {GPUBuffer} bigReadBuffer - The GPU buffer holding the concatenated row data.
     * @param {PerKeyMetrics} perKeyMetrics - The metrics object to update.
     * @returns {Promise<Uint8Array>} The copied data from the mapped buffer.
     */
    private async mapAndReadBuffer(
        bigReadBuffer: GPUBuffer,
        perKeyMetrics: PerKeyMetrics
    ): Promise<Uint8Array> {
        const mapBufferStart = performance.now();

        // mapAsync
        const mapAsyncStart = performance.now();
        await bigReadBuffer.mapAsync(GPUMapMode.READ);
        perKeyMetrics.mapBufferSubsections.mapAsync = performance.now() - mapAsyncStart;

        // getMappedRange
        const getMappedRangeStart = performance.now();
        const fullMappedRange = bigReadBuffer.getMappedRange();
        perKeyMetrics.mapBufferSubsections.getMappedRange =
            performance.now() - getMappedRangeStart;

        // copyToUint8Array
        const copyToUint8ArrayStart = performance.now();
        const bigCopiedData = new Uint8Array(fullMappedRange.slice(0));
        perKeyMetrics.mapBufferSubsections.copyToUint8Array =
            performance.now() - copyToUint8ArrayStart;

        // unmap
        const unmapStart = performance.now();
        bigReadBuffer.unmap();
        perKeyMetrics.mapBufferSubsections.unmap = performance.now() - unmapStart;

        perKeyMetrics.mapBuffer = performance.now() - mapBufferStart;

        return bigCopiedData;
    }

    /**
     * Deserializes each row from the provided copied data according to the store metadata.
     * Updates the results array in-place.
     *
     * @param {RowInfo[]} rowInfos - Array of metadata about each row location/length.
     * @param {StoreMetadata} storeMeta - The store's metadata (dataType, typedArrayType, etc.).
     * @param {Uint8Array} bigCopiedData - The full set of copied row bytes.
     * @param {(any | null)[]} results - The array where deserialized results are placed.
     * @param {PerKeyMetrics} perKeyMetrics - The metrics object to update.
     */
    private deserializeRows(
        rowInfos: RowInfo[],
        storeMeta: StoreMetadata,
        bigCopiedData: Uint8Array,
        results: (any | null)[],
        perKeyMetrics: PerKeyMetrics
    ): void {
        const deserializeStart = performance.now();

        for (const rowInfo of rowInfos) {
            const rowSlice = bigCopiedData.subarray(
                rowInfo.offsetInFinalBuffer,
                rowInfo.offsetInFinalBuffer + rowInfo.length
            );

            // Convert bytes to an object using `deserializeData`
            results[rowInfo.rowIndex] = this.deserializeData(storeMeta, rowSlice);
        }

        perKeyMetrics.deserialize = performance.now() - deserializeStart;
    }

    /**
     * Initializes and returns a fresh PerKeyMetrics object.
     *
     * @returns {PerKeyMetrics} - The initialized performance metrics object.
     */
    private initializeMetrics(): PerKeyMetrics {
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