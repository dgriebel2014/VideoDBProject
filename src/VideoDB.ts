﻿// Copyright © 2025 Jon Griebel. dgriebel2014@gmail.com - All rights reserved.
// Distributed under the MIT license.

// videoDB.ts

/**
 * A VideoDB class that stores all metadata in CPU memory, 
 * and actual data on the GPU
 */
export class VideoDB {
    public storeMetadataMap: Map<string, StoreMetadata>;
    public storeKeyMap: Map<string, Map<string, number>>;
    public pendingWrites: PendingWrite[] = [];
    private readonly BATCH_SIZE = 10000;
    private flushTimer: number | null = null;
    public isReady: boolean | null = true;
    private waitUntilReadyPromise: Promise<void> | null = null;
    private readyResolver: (() => void) | null = null;

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
     * If the store uses `dataType: "JSON"` and has one or more `sortDefinition` objects,
     * this method will also create a companion offsets store named `<storeName>-offsets`
     * (with `dataType: "TypedArray"` and `typedArrayType: "Uint32Array"`) to store numeric
     * sorting keys extracted from JSON fields.
     *
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
     * @returns {void}
     * @throws {Error} If the store already exists or the typedArrayType is missing when dataType is "TypedArray".
     */
    public createObjectStore(
        storeName: string,
        options: {
            dataType: "TypedArray" | "ArrayBuffer" | "JSON";
            typedArrayType?:
            | "Float32Array"
            | "Float64Array"
            | "Int32Array"
            | "Uint32Array"
            | "Uint8Array";
            bufferSize: number;
            rowSize?: number;
            totalRows: number;
            sortDefinition?: {
                name: string;
                sortFields: {
                    sortColumn: string;
                    path: string;
                    sortDirection: "Asc" | "Desc";
                }[];
            }[];
        }
    ): void;
    public createObjectStore(
        storeName: string,
        options: {
            dataType: "TypedArray" | "ArrayBuffer" | "JSON";
            typedArrayType?:
            | "Float32Array"
            | "Float64Array"
            | "Int32Array"
            | "Uint32Array"
            | "Uint8Array";
            bufferSize: number;
            rowSize?: number;
            totalRows: number;
            sortDefinition?: {
                name: string;
                sortFields: {
                    sortColumn: string;
                    path: string;
                    sortDirection: "Asc" | "Desc";
                }[];
            }[];
        }
    ): void {
        if (this.storeMetadataMap.has(storeName)) {
            throw new Error(`Object store "${storeName}" already exists.`);
        }

        if (options.dataType === "TypedArray" && !options.typedArrayType) {
            throw new Error(
                `typedArrayType is required when dataType is "TypedArray".`
            );
        }

        const rowsPerBuffer = options.dataType !== "JSON" && options.rowSize
            ? Math.floor(options.bufferSize / options.rowSize)
            : undefined;

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
            sortDefinition: options.sortDefinition?.map(def => ({
                name: def.name,
                sortFields: def.sortFields.map(field => ({
                    dataType: "string",
                    ...field
                }))
            })) ?? [],
            sortsDirty: false
        };

        this.storeMetadataMap.set(storeName, storeMetadata);
        this.storeKeyMap.set(storeName, new Map());

        if (options.dataType === "JSON" && options.sortDefinition && options.sortDefinition.length) {
            const totalSortFields = options.sortDefinition.reduce(
                (count, def) => count + def.sortFields.length,
                0
            );
            const bytesPerField = 2 * 4;
            const rowSize = totalSortFields * bytesPerField;

            this.createObjectStore(`${storeName}-offsets`, {
                dataType: "TypedArray",
                typedArrayType: "Uint32Array",
                bufferSize: 10 * 1024 * 1024,
                totalRows: options.totalRows,
                rowSize
            });
        }
    }

    /**
     * Deletes an existing object store by name.
     * @param {string} storeName - The name of the store to delete.
     * @returns {void} This method does not return anything.
     */
    public deleteObjectStore(storeName: string): void {
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
    public listObjectStores(): string[] {
        return Array.from(this.storeMetadataMap.keys());
    }

    /**
     * Adds a new record to the specified store, with delayed GPU writes.
     * If the store has JSON sort definitions, also computes & stores offsets.
     */
    public async add(storeName: string, key: string, value: any): Promise<void> {
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
     * If the store has JSON sort definitions, also computes & stores offsets.
     */
    public async put(storeName: string, key: string, value: any): Promise<void> {
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
    public async get(storeName: string, key: string): Promise<any | null> {
        // Call getMultiple with a single key
        const results = await this.getMultiple(storeName, [key]);

        // Extract the first (and only) result
        const result = results[0];

        return result;
    }

    /**
     * Retrieves multiple records from the specified store, supporting two different usage patterns:
     *
     * 1. **Fetch by an array of keys**:
     *    ```ts
     *    const results = await videoDB.getMultiple("MyStore", ["key1", "key2", "key3"]);
     *    ```
     *    - Returns a Promise resolving to an array of the same length as the input `keys`.
     *    - Each position corresponds to the deserialized data for that key, or `null` if the key does not exist.
     *    - Keys can also include wildcard patterns (e.g., `%`, `_`, or bracket expressions) which will be expanded to match multiple existing keys.
     *
     * 2. **Paginated fetch**:
     *    ```ts
     *    const results = await videoDB.getMultiple("MyStore", 0, 100);
     *    ```
     *    - Interprets the second parameter as `skip` and the third parameter as `take`.
     *    - Internally retrieves all keys from the store, then returns a slice of that array starting at index `skip` and spanning `take` entries.
     *    - The returned array contains data in the store’s internal keyMap order (not necessarily sorted by key).
     *
     * @param {string} storeName
     *   The name of the target store from which to retrieve data.
     *
     * @param {string[] | number} param2
     *   - If this is an array of `string`s, the method treats it as a list of specific keys (including possible wildcards) to fetch.
     *   - If this is a `number`, the method interprets it as the `skip` value for pagination.
     *
     * @param {number} [param3]
     *   - If `param2` is a number, then this parameter is the `take` value for pagination.
     *   - Ignored if `param2` is an array of keys.
     *
     * @returns {Promise<(any|null)[]>}
     *   A promise that resolves to an array of results. Each result is either the deserialized object/typed-array/ArrayBuffer from the GPU or `null` if the record does not exist.
     *   - In “fetch by keys” mode, the array matches the order of your provided keys.
     *   - In “paginated fetch” mode, the array contains results in the order of the store’s keyMap from `skip` to `skip + take - 1`.
     *
     * @throws {Error}
     *   - If invalid parameters are provided (neither an array of keys nor valid `skip`/`take`).
     *
     * @example
     * // 1) Fetch by specific keys
     * const recordsByKey = await videoDB.getMultiple("MyStore", ["key1", "key2"]);
     *
     * @example
     * // 2) Fetch by pagination
     * const firstHundredRecords = await videoDB.getMultiple("MyStore", 0, 100);
     *
     * @remarks
     * - All pending writes are flushed before reading to ensure consistency.
     * - If keys are expanded from wildcards, the results array may be larger than the original keys array.
     * - The order of results in paginated mode depends on iteration order of the store’s internal keyMap, which is not necessarily sorted.
     */
    public async getMultiple(storeName: string, keys: string[]): Promise<(any | null)[]>;
    public async getMultiple(storeName: string, skip: number, take: number): Promise<(any | null)[]>;
    public async getMultiple(
        storeName: string,
        param2: string[] | number,
        param3?: number
    ): Promise<(any | null)[]> {
        if (Array.isArray(param2)) {
            // Overload 1: Fetch by keys
            const keys = param2;
            const { results } = await this.getMultipleByKeys(storeName, keys);
            return results;
        } else if (typeof param2 === 'number' && typeof param3 === 'number') {
            // Overload 2: Fetch by pagination (skip and take)
            const skip = param2;
            const take = param3;

            // Flush & retrieve store metadata
            const { keyMap } = await this.flushAndGetMetadata(storeName);

            // Convert the store’s keyMap into an array of all keys
            const allKeys = Array.from(keyMap.keys());

            const { results } = await this.readRowsWithPagination(storeName, allKeys, skip, take);
            return results;
        } else {
            throw new Error(
                'Invalid parameters for getMultiple. Expected either (storeName, keys[]) or (storeName, skip, take).'
            );
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
    public async clear(storeName: string): Promise<void> {
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
     * Checks if the pending writes have reached a threshold or if conditions
     * dictate a flush to the GPU buffers, and performs the flush if necessary.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once the flush has been performed (if triggered).
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
     * Flushes all pending writes in batches to their respective GPU buffers.
     * Groups writes by buffer, performs the writes, then waits for GPU completion.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once all pending writes are submitted and the queue is done.
     */
    private async flushWrites(): Promise<void> {
        if (this.pendingWrites.length === 0) {
            return;
        }

        // Group all pendingWrites by their GPUBuffer
        const writesByBuffer: Map<GPUBuffer, PendingWrite[]> = new Map();
        for (const item of this.pendingWrites) {
            const { gpuBuffer } = item;
            if (!writesByBuffer.has(gpuBuffer)) {
                writesByBuffer.set(gpuBuffer, []);
            }
            writesByBuffer.get(gpuBuffer)!.push(item);
        }

        // Keep track of which writes succeed
        const successfulWrites = new Set<PendingWrite>();

        // Sort by offset and write each group
        for (const [gpuBuffer, writeGroup] of writesByBuffer.entries()) {
            // Sort by offset ascending
            writeGroup.sort((a, b) => a.rowMetadata.offset - b.rowMetadata.offset);

            for (const pendingWrite of writeGroup) {
                try {
                    const { rowMetadata, arrayBuffer } = pendingWrite;
                    this.device.queue.writeBuffer(
                        gpuBuffer,
                        rowMetadata.offset,
                        arrayBuffer
                    );
                    successfulWrites.add(pendingWrite);
                } catch (singleWriteError) {
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
     * Retrieves the key-to-row-index map for the specified store name.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @returns {Map<string, number>} The store's key map.
     * @throws {Error} If the store does not exist.
     */
    private getKeyMap(storeName: string): Map<string, number> {
        const keyMap = this.storeKeyMap.get(storeName);
        if (!keyMap) {
            return new Map<string, number>();
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
    private compareKeys(a: string, b: string): number {
        if (a < b) return -1;
        if (a > b) return 1;
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
     * Retrieves the metadata object for a given store by name.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @returns {StoreMetadata} The metadata for the specified store.
     * @throws {Error} If the store does not exist.
     */
    private getStoreMetadata(storeName: string): StoreMetadata {
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
    private createNewBuffer(storeMeta: StoreMetadata, size: number): GPUBuffer {
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

        (gpuBuffer as any)._usedBytes = size;
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
    private useSpaceInLastBuffer(
        storeMeta: StoreMetadata,
        lastBufferMeta: BufferMetadata,
        usedBytes: number,
        size: number
    ): {
        gpuBuffer: GPUBuffer;
        bufferIndex: number;
        offset: number;
    } {
        const gpuBuffer = lastBufferMeta.gpuBuffer!;
        const ALIGNMENT = 256;

        // Align the offset to the nearest multiple of ALIGNMENT (256)
        const alignedOffset = roundUp(usedBytes, ALIGNMENT);

        // Check if alignedOffset + size exceeds the usable buffer size
        if (alignedOffset + size > gpuBuffer.size) {
            return this.allocateNewBufferChunk(storeMeta, size);
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
     * Allocates a new GPU buffer chunk if the last one does not have enough space.
     * Sets the newly allocated buffer's used bytes and updates the store metadata.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The store metadata where the buffer is being created.
     * @param {number} size - The number of bytes needed.
     * @returns {{ gpuBuffer: GPUBuffer; bufferIndex: number; offset: number }}
     *   An object containing the new GPU buffer, the assigned buffer index, and the offset (always 0 for new buffers).
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
     * Serializes a value (JSON, TypedArray, ArrayBuffer, etc.) into an ArrayBuffer
     * suitable for writing to the GPU buffer.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store being written to.
     * @param {any} value - The original value to serialize.
     * @returns {ArrayBuffer} The serialized value as an ArrayBuffer.
     */
    private serializeValueForStore(storeMeta: StoreMetadata, value: any): ArrayBuffer {
        let resultBuffer: ArrayBuffer;

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
    private async updateRowOnOverwrite(
        storeMeta: StoreMetadata,
        oldRowMeta: RowMetadata,
        arrayBuffer: ArrayBuffer,
        keyMap: Map<string, number>,
        key: string
    ): Promise<RowMetadata> {
        // If the new data fits in the old space:
        if (arrayBuffer.byteLength <= oldRowMeta.length) {
            // Overwrite in-place later during flushWrites.
            if (arrayBuffer.byteLength < oldRowMeta.length) {
                oldRowMeta.length = arrayBuffer.byteLength;
            }
            return oldRowMeta;
        } else {
            // Mark old row inactive
            oldRowMeta.flags = (oldRowMeta.flags ?? 0) | ROW_INACTIVE_FLAG;

            // Find new space for the bigger data
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
     * Retrieves the GPU buffer instance corresponding to a specific buffer index.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata of the store.
     * @param {number} bufferIndex - The index of the buffer to retrieve.
     * @returns {GPUBuffer} The GPU buffer at the specified index.
     */
    private getBufferByIndex(storeMeta: StoreMetadata, bufferIndex: number): GPUBuffer {
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
    private resetFlushTimer(): void {
        if (this.flushTimer !== null) {
            clearTimeout(this.flushTimer);
        }
        this.flushTimer = window.setTimeout(async () => {
            try {
                await this.flushWrites();
                await this.rebuildAllDirtySorts();
            } catch (error) {
                console.error('Error during timed flush operation:', error);
            } finally {
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
     * For each such store, this method calls `rebuildStoreBySortDefinition(storeName, sortDefinitionName)`
     * for every sort definition in that store, awaiting completion in sequence.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once all dirty sorts have been rebuilt.
     */
    private async rebuildAllDirtySorts(): Promise<void> {
        for (const [storeName, storeMeta] of this.storeMetadataMap.entries()) {
            if (storeMeta.sortsDirty) {
                storeMeta.sortsDirty = false; // reset the flag before starting
                if (storeMeta.sortDefinition && storeMeta.sortDefinition.length > 0) {
                    for (const def of storeMeta.sortDefinition) {
                        await this.rebuildStoreBySortDefinition(storeName, def.name);
                    }
                }
            }
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
     * Flushes all pending writes to the GPU and then returns the store metadata and key map.
     *
     * @private
     * @param {string} storeName - The name of the store.
     * @returns {Promise<{ storeMeta: StoreMetadata; keyMap: Map<string, number> }>}
     *    An object containing the store's metadata and key map.
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
        await this.flushWrites();
        performanceMetrics.flushWrites = performance.now() - flushStart;

        const metadataStart = performance.now();
        const storeMeta = this.getStoreMetadata(storeName) as StoreMetadata;
        const keyMap = this.getKeyMap(storeName) as Map<string, any>;
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
    private likeToRegex(pattern: string): RegExp {
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
    private expandWildcard(key: string, keyMap: Map<string, any>): string[] {
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
    private expandAllWildcards(keys: string[], keyMap: Map<string, any>): string[] {
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

        // Collect row metadata
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
    private createBigReadBuffer(
        totalBytes: number,
        perKeyMetrics: PerKeyMetrics
    ): GPUBuffer {
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
                rowInfo.rowMetadata.offset,
                bigReadBuffer,
                rowInfo.offsetInFinalBuffer,
                rowInfo.length
            );
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
    private async copyFromBigBufferToStaging(
        bigReadBuffer: GPUBuffer,
        totalBytes: number,
        perKeyMetrics: PerKeyMetrics
    ): Promise<Uint8Array> {
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
     * Creates and returns a fresh PerKeyMetrics object with all timing values initialized to 0.
     *
     * @private
     * @returns {PerKeyMetrics} A fresh metrics object.
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

    /**
     * Waits until the VideoDB is ready, i.e., all pending operations have settled
     * and the internal ready state is resolved.
     *
     * @private
     * @returns {Promise<void>} A promise that resolves once VideoDB is ready.
     */
    private waitUntilReady(): Promise<void> {
        // If already ready, just return immediately
        if (this.isReady) return Promise.resolve();

        // If a wait promise is *already* in progress, return the same one
        if (this.waitUntilReadyPromise) return this.waitUntilReadyPromise;

        // Otherwise, create a fresh promise and store its resolver
        this.waitUntilReadyPromise = new Promise<void>((resolve) => {
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
    private async readRowsWithPagination(
        storeName: string,
        allKeys: any,
        skip: number,
        take: number
    ): Promise<{ results: (any | null)[]; perKeyMetrics: any }> {
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
    private async getMultipleByKeys(storeName: string, keys: string[]): Promise<{ results: (any | null)[]; perKeyMetrics: any }> {
        // Flush & retrieve store metadata
        const { storeMeta, keyMap, metrics } = await this.flushAndGetMetadata(storeName);

        // Expand any wildcard patterns
        const expandedKeys = this.expandAllWildcards(keys, keyMap);

        // Read all rows based on expanded keys
        const { results, perKeyMetrics } = await this.readAllRows(
            storeName,
            storeMeta,
            keyMap,
            expandedKeys
        );

        // Log or accumulate performance
        this.logPerformance(metrics, perKeyMetrics);

        return { results, perKeyMetrics };
    }

    /**
     * For one or more SortDefinition objects, compute numeric-key arrays without merging them.
     * 
     * @param {any} objectData - The source object whose fields we want to convert.
     * @param {SortDefinition | SortDefinition[]} sortDefinitionOrDefinitions - Either one definition or an array of them.
     * @returns {Uint32Array | Record<string, Uint32Array>} 
     *   - If a single definition is provided, returns a single `Uint32Array`.
     *   - If multiple definitions are provided, returns an object keyed by definition name, 
     *     with each value being its own `Uint32Array`.
     */
    public getJsonFieldOffsetsFlattened(
        objectData: any,
        sortDefinitionOrDefinitions: SortDefinition | SortDefinition[]
    ): Uint32Array | Record<string, Uint32Array> {
        // 1) Normalize to an array of definitions
        const definitions = Array.isArray(sortDefinitionOrDefinitions)
            ? sortDefinitionOrDefinitions
            : [sortDefinitionOrDefinitions];

        // 2) If only one definition, return a single Uint32Array
        if (definitions.length === 1) {
            const def = definitions[0];
            return this.getJsonFieldOffsetsForSingleDefinition(objectData, def);
        }

        // 3) Otherwise, for multiple definitions, return an object keyed by definition name
        const result: Record<string, Uint32Array> = {};
        for (const def of definitions) {
            const offsets = this.getJsonFieldOffsetsForSingleDefinition(objectData, def);
            result[def.name] = offsets;
        }
        return result;
    }

    /**
     * Computes a flat `Uint32Array` representing *serialized field values*
     * for one object, based on a single SortDefinition (one or multiple fields).
     *
     * @param objectData - The source object whose fields we want to convert.
     * @param sortDefinition - The definition containing a `name` and `sortFields`.
     * @returns A flat array of 32-bit integers encoding the field values for that one definition.
     */
    private getJsonFieldOffsetsForSingleDefinition(
        objectData: any,
        sortDefinition: SortDefinition
    ): Uint32Array {
        // Build up the per-field numeric arrays
        const fieldArrays: Uint32Array[] = [];
        for (const field of sortDefinition.sortFields) {
            const rawValue = this.getValueByPath(objectData, field.path);
            const numericArray = this.convertValueToUint32Array(
                rawValue,
                field.dataType,
                field.sortDirection
            );
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
    private getValueByPath(obj: any, path: string): any {
        if (!path) return obj;
        const segments = path.split(".");
        let current = obj;
        for (const seg of segments) {
            if (current == null) return undefined;
            current = current[seg];
        }
        return current;
    }

    /**
     * Convert a JS value (date, number, or string) into a Uint32Array.
     * Applies ascending or descending transformations as needed.
     */
    private convertValueToUint32Array(
        value: any,
        dataType: "string" | "number" | "date",
        direction: "Asc" | "Desc"
    ): Uint32Array {
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
                if (invert) fallback[0] = 0xFFFFFFFF;
                return fallback;
        }
    }

    /**
     * Example: store a Date (or date-string) as 64-bit => two 32-bit words [hi, lo].
     */
    private serializeDate(rawValue: any, invert: boolean): Uint32Array {
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
    private serializeNumber(rawValue: any, invert: boolean): Uint32Array {
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

        return new Uint32Array([hi, lo]);  // store [hi, lo]
    }

    /**
     * Serialize a string by storing each codepoint in a 32-bit word.
     */
    private serializeString(rawValue: any, invert: boolean): Uint32Array {
        if (typeof rawValue !== "string") {
            // store "empty" if not a proper string
            return new Uint32Array([invert ? 0xFFFFFFFF : 0]);
        }

        // Convert each codepoint to one 32-bit
        const codePoints: number[] = [];
        for (const char of rawValue) {
            const cp = char.codePointAt(0)!;
            const word = invert ? (0xFFFFFFFF - cp) : cp;
            codePoints.push(word);
        }
        return Uint32Array.from(codePoints);
    }

    /**
     * Triggers a GPU-based sort for a specific store and one named sort definition.
     * 
     * @param {string} storeName - The name of the object store to be sorted.
     * @param {string} sortDefinitionName - The name of the sort definition to apply.
     * @returns {Promise<void>} A promise that resolves once the sort operation is complete.
     * @throws {Error} If the store does not exist or the named sort definition is not found.
     */
    private async rebuildStoreBySortDefinition(
        storeName: string,
        sortDefinitionName: string
    ): Promise<void> {
        // Retrieve the store metadata
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }

        // Find the specified sort definition by name
        const sortDef = storeMeta.sortDefinition?.find(
            def => def.name === sortDefinitionName
        );
        if (!sortDef) {
            throw new Error(
                `Sort definition "${sortDefinitionName}" not found in store "${storeName}".`
            );
        }

        // Confirm that we actually have some fields to sort by
        if (!sortDef.sortFields || sortDef.sortFields.length === 0) {
            // No fields defined, no need to sort
            console.warn(
                `Sort definition "${sortDefinitionName}" has no fields; skipping sort.`
            );
            return;
        }

        // Execute the GPU-based sort for this definition.
        //    - This is a placeholder call to whatever GPU compute routine
        //      will reorder the row ID array for the store.
        //    - The "rowIdBuffer", "offsetBuffer", "jsonBuffer", etc. 
        //      would get wired in here.
        // await this.runGpuSortForDefinition(storeMeta, sortDef);

        // Optionally, you might mark the store as "sorted" or log completion
        storeMeta.sortsDirty = false;
    }

    /**
     * Main orchestrator method. Corresponds to the old "runGpuSortForDefinition".
     * Public so that external code can call it.
     *
     * @param {StoreMetadata} storeMeta - The metadata for the store being sorted.
     * @param {SortDefinition} sortDef - A single sort definition object specifying how to sort.
     * @returns {Promise<void>} A promise that resolves when the GPU sort is complete.
     */
    public async runGpuSortForDefinition(
        storeMeta: StoreMetadata,
        sortDef: SortDefinition
    ): Promise<void> {
        console.log(
            "Running GPU bitonic sort (with debug) for store:",
            storeMeta.storeName,
            "with sort:",
            sortDef.name
        );

        // 1) Prepare row IDs and pad
        const padResult = this.prepareRowIdsForSort(storeMeta);
        if (!padResult) {
            // nothing to do
            return;
        }
        const { paddedRowIds, rowCount, paddedCount } = padResult;

        // 2) Create rowId buffer
        const rowIdBuffer = this.createRowIdBuffer(paddedRowIds);

        // 3) Create ephemeral offsets buffer (or dummy)
        const offsetBuffer = await this.createEphemeralOffsetsBufferIfPossible(storeMeta.storeName);

        // 4) Parse sort direction
        const dirFlag = this.getDirectionFlag(sortDef);

        // 5) Create param buffer
        const paramBuffer = this.createParamBuffer();

        // 6) Create debug atomic buffer
        const debugAtomicBuffer = this.createDebugAtomicBuffer();

        // 7) Create a zero buffer
        const zeroBuffer = this.createZeroBuffer();

        // 8) Create the compute pipeline
        const pipeline = this.createBitonicPipeline(this.bitonicModuleCode);

        // 9) Create the bind group
        const bindGroup = this.createBitonicBindGroup(
            pipeline,
            rowIdBuffer,
            offsetBuffer,
            paramBuffer,
            debugAtomicBuffer
        );

        // 10) Run the bitonic sort passes
        const sizeLimit = paddedCount; // merges up to "paddedCount"

        for (let size = 2; size <= sizeLimit; size <<= 1) {
            for (let halfSize = size >> 1; halfSize > 0; halfSize >>= 1) {
                await this.runBitonicPass(
                    pipeline,
                    bindGroup,
                    paramBuffer,
                    debugAtomicBuffer,
                    zeroBuffer,
                    size,
                    halfSize,
                    dirFlag,
                    rowCount,
                    paddedCount
                );
            }
        }

        // 11) Read back the final row IDs (only the first rowCount are real)
        const finalPaddedRowIds = await this.readBackRowIds(rowIdBuffer, paddedRowIds.byteLength);
        const sortedRowIds = finalPaddedRowIds.subarray(0, rowCount);

        // 12) CPU check to confirm ascending or descending
        this.assertSortedArray(sortedRowIds, dirFlag);

        // 13) Clean up ephemeral buffers
        offsetBuffer.destroy();
        debugAtomicBuffer.destroy();
        zeroBuffer.destroy();
    }

    /**
     * WGSL code for the bitonic sort. In production, you might load this from a file;
     * here we keep it inline for brevity.
     */
    private readonly bitonicModuleCode = /* wgsl */`
struct Params {
    size: u32,
    halfSize: u32,
    dirFlag: u32,
    actualCount: u32,
    paddedCount: u32
}

@group(0) @binding(0) var<storage, read_write> rowIds: array<u32>;
@group(0) @binding(1) var<storage, read> offsets: array<u32>;
@group(0) @binding(2) var<uniform> params: Params;
@group(0) @binding(3) var<storage, read_write> debugAtomic: atomic<u32>;

fn compareAndSwap(idxA: u32, idxB: u32, ascending: bool) {
    let rowA = rowIds[idxA];
    let rowB = rowIds[idxB];

    var offsetA: u32;
    var offsetB: u32;

    if (rowA <= params.actualCount) {
        offsetA = offsets[(rowA - 1u) * 2u];
    } else {
        offsetA = select(0u, 0xffffffffu, ascending);
    }

    if (rowB <= params.actualCount) {
        offsetB = offsets[(rowB - 1u) * 2u];
    } else {
        offsetB = select(0u, 0xffffffffu, ascending);
    }

    var shouldSwap = false;
    if (ascending) {
        shouldSwap = (offsetA > offsetB);
    } else {
        shouldSwap = (offsetA < offsetB);
    }

    if (shouldSwap) {
        rowIds[idxA] = rowB;
        rowIds[idxB] = rowA;
        atomicStore(&debugAtomic, 1u);
    }
}

@compute @workgroup_size(256)
fn main(@builtin(global_invocation_id) global_id : vec3<u32>) {
    let i = global_id.x;
    if (i >= params.paddedCount) {
        return;
    }

    let size = params.size;
    let halfSize = params.halfSize;

    let flip = ((i & (size >> 1u)) != 0u);
    var ascending = (params.dirFlag == 1u);
    if (flip) {
        ascending = !ascending;
    }

    let mate = i ^ halfSize;
    if (mate < params.paddedCount && mate != i) {
        if (i < mate) {
            compareAndSwap(i, mate, ascending);
        } else {
            compareAndSwap(mate, i, ascending);
        }
    }
}
`;

    /**
     * Prepares and pads row IDs for GPU sorting if needed.
     *
     * @private
     * @param {StoreMetadata} storeMeta - The metadata for the store.
     * @returns {RowIdPaddingResult | null} The padded array and sizes, or null if too few rows.
     */
    private prepareRowIdsForSort(storeMeta: StoreMetadata): RowIdPaddingResult | null {
        const rowIdsOriginal = new Uint32Array(storeMeta.rows.map(r => r.rowId));
        const rowCount = rowIdsOriginal.length;
        if (rowCount < 2) {
            console.warn("Store has fewer than 2 rows; no sorting needed.");
            return null;
        }
        console.log("Unsorted row IDs:", rowIdsOriginal);

        // Next power of two
        const paddedCount = 1 << Math.ceil(Math.log2(rowCount));

        // Padded array: the first rowCount entries are real, the rest are sentinel row IDs.
        const paddedRowIds = new Uint32Array(paddedCount);
        paddedRowIds.set(rowIdsOriginal, 0);
        for (let i = rowCount; i < paddedCount; i++) {
            // Any ID > rowCount acts as sentinel
            paddedRowIds[i] = rowCount + (i + 1);
        }

        return { paddedRowIds, rowCount, paddedCount };
    }

    /**
     * Creates a GPU buffer to store row IDs.
     *
     * @private
     * @param {Uint32Array} rowIdData - The data (row IDs) to store in the buffer.
     * @returns {GPUBuffer} A GPU buffer containing the row IDs.
     */
    private createRowIdBuffer(rowIdData: Uint32Array): GPUBuffer {
        const bufferSize = rowIdData.byteLength;
        const buffer = this.device.createBuffer({
            size: bufferSize,
            usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_DST | GPUBufferUsage.COPY_SRC,
            mappedAtCreation: true
        });
        new Uint32Array(buffer.getMappedRange()).set(rowIdData);
        buffer.unmap();
        return buffer;
    }

    /**
     * Creates an ephemeral copy of the offsets buffer (if one exists) so we don't mutate
     * the original buffer directly during sorting. If no offsets buffer is found,
     * returns a small dummy buffer instead.
     *
     * @private
     * @param {string} storeName - The name of the store (e.g. "MyStore"), from which
     *   the offsets store name is derived ("MyStore-offsets").
     * @returns {Promise<GPUBuffer>} A buffer suitable for reading/writing during the sort.
     */
    private async createEphemeralOffsetsBufferIfPossible(
        storeName: string
    ): Promise<GPUBuffer> {
        const offsetsStoreName = `${storeName}-offsets`;
        const offsetsStoreMeta = this.storeMetadataMap.get(offsetsStoreName);

        if (offsetsStoreMeta?.buffers?.[0]?.gpuBuffer) {
            const originalOffsetBuffer = offsetsStoreMeta.buffers[0].gpuBuffer!;
            const ephemeralOffsetBuffer = this.device.createBuffer({
                size: originalOffsetBuffer.size,
                usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_DST | GPUBufferUsage.COPY_SRC
            });

            // Copy from original → ephemeral
            {
                const cmd = this.device.createCommandEncoder();
                cmd.copyBufferToBuffer(
                    originalOffsetBuffer,
                    0,
                    ephemeralOffsetBuffer,
                    0,
                    originalOffsetBuffer.size
                );
                this.device.queue.submit([cmd.finish()]);
                await this.device.queue.onSubmittedWorkDone();
            }

            return ephemeralOffsetBuffer;
        }

        // Fallback dummy buffer
        return this.device.createBuffer({
            size: 4,
            usage: GPUBufferUsage.STORAGE
        });
    }

    /**
     * Returns 1 if the first field in the sort definition is ascending, 0 if descending.
     *
     * @private
     * @param {SortDefinition} sortDef - The sort definition to evaluate.
     * @returns {number} - 1 if ascending, 0 if descending.
     */
    private getDirectionFlag(sortDef: SortDefinition): number {
        const firstField = sortDef.sortFields[0];
        const ascending = (firstField.sortDirection === "Asc");
        return ascending ? 1 : 0;
    }

    /**
     * Creates a uniform buffer to store bitonic sorting parameters.
     *
     * @private
     * @returns {GPUBuffer} A GPU buffer suitable for storing 5 x u32 parameters.
     */
    private createParamBuffer(): GPUBuffer {
        return this.device.createBuffer({
            size: 5 * 4, // 5 x u32
            usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST
        });
    }

    /**
     * Writes the bitonic sorting parameters to the specified uniform param buffer.
     *
     * @private
     * @param {GPUBuffer} paramBuffer - The GPU buffer to which param data is written.
     * @param {number} size - The current sub-block size for the bitonic pass.
     * @param {number} halfSize - Half of the sub-block size.
     * @param {number} dirFlag - 1 for ascending, 0 for descending.
     * @param {number} actualCount - The actual number of rows to sort.
     * @param {number} paddedCount - The next power-of-two size of the row ID array.
     * @returns {void}
     */
    private writeParamBuffer(
        paramBuffer: GPUBuffer,
        size: number,
        halfSize: number,
        dirFlag: number,
        actualCount: number,
        paddedCount: number
    ): void {
        const paramData = new Uint32Array([size, halfSize, dirFlag, actualCount, paddedCount]);
        this.device.queue.writeBuffer(paramBuffer, 0, paramData);
    }

    /**
     * Creates a buffer used as an atomic "debug" buffer (e.g., to detect swaps).
     *
     * @private
     * @returns {GPUBuffer} A GPU buffer that can be used with atomic operations.
     */
    private createDebugAtomicBuffer(): GPUBuffer {
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
    private createZeroBuffer(): GPUBuffer {
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
    private async resetDebugAtomicBuffer(
        debugAtomicBuffer: GPUBuffer,
        zeroBuffer: GPUBuffer
    ): Promise<void> {
        const cmd = this.device.createCommandEncoder();
        cmd.copyBufferToBuffer(zeroBuffer, 0, debugAtomicBuffer, 0, 4);
        this.device.queue.submit([cmd.finish()]);
        await this.device.queue.onSubmittedWorkDone();
    }

    /**
     * Creates the GPU compute pipeline for the bitonic sort shader module.
     *
     * @private
     * @param {string} bitonicModuleCode - WGSL code for the bitonic sort compute shader.
     * @returns {GPUComputePipeline} The created pipeline.
     */
    private createBitonicPipeline(bitonicModuleCode: string): GPUComputePipeline {
        const bitonicModule = this.device.createShaderModule({ code: bitonicModuleCode });
        return this.device.createComputePipeline({
            layout: "auto",
            compute: {
                module: bitonicModule,
                entryPoint: "main",
            },
        });
    }

    /**
     * Creates a bind group for the bitonic sort pipeline, attaching rowId buffer, offsets buffer,
     * param buffer, and debug atomic buffer.
     *
     * @private
     * @param {GPUComputePipeline} pipeline - The pipeline whose bind group layout we will use.
     * @param {GPUBuffer} rowIdBuffer - The buffer holding row IDs.
     * @param {GPUBuffer} offsetBuffer - A buffer containing offsets for each row (JSON-based).
     * @param {GPUBuffer} paramBuffer - A uniform buffer holding sorting parameters.
     * @param {GPUBuffer} debugAtomicBuffer - A storage buffer used for atomic writes to detect swaps.
     * @returns {GPUBindGroup} The bind group connecting these resources.
     */
    private createBitonicBindGroup(
        pipeline: GPUComputePipeline,
        rowIdBuffer: GPUBuffer,
        offsetBuffer: GPUBuffer,
        paramBuffer: GPUBuffer,
        debugAtomicBuffer: GPUBuffer
    ): GPUBindGroup {
        return this.device.createBindGroup({
            layout: pipeline.getBindGroupLayout(0),
            entries: [
                { binding: 0, resource: { buffer: rowIdBuffer } },
                { binding: 1, resource: { buffer: offsetBuffer } },
                { binding: 2, resource: { buffer: paramBuffer } },
                { binding: 3, resource: { buffer: debugAtomicBuffer } },
            ]
        });
    }

    /**
     * Executes one pass of the bitonic merge sort algorithm on the GPU, 
     * configuring the size and halfSize parameters.
     *
     * @private
     * @param {GPUComputePipeline} pipeline - The compute pipeline.
     * @param {GPUBindGroup} bindGroup - The bind group containing buffers.
     * @param {GPUBuffer} paramBuffer - The buffer where bitonic parameters are written.
     * @param {GPUBuffer} debugAtomicBuffer - A buffer used for atomic swap detection.
     * @param {GPUBuffer} zeroBuffer - A small buffer containing a single zero value (used for resets).
     * @param {number} size - The current size of the sub-block in bitonic sorting.
     * @param {number} halfSize - Half of that sub-block size.
     * @param {number} dirFlag - 1 for ascending, 0 for descending.
     * @param {number} actualCount - Actual number of rows to sort (un-padded).
     * @param {number} paddedCount - Power-of-two row count, possibly above actual count.
     * @returns {Promise<void>} A promise that resolves once the GPU pass is completed.
     */
    private async runBitonicPass(
        pipeline: GPUComputePipeline,
        bindGroup: GPUBindGroup,
        paramBuffer: GPUBuffer,
        debugAtomicBuffer: GPUBuffer,
        zeroBuffer: GPUBuffer,
        size: number,
        halfSize: number,
        dirFlag: number,
        actualCount: number,
        paddedCount: number
    ) {
        // 1) Reset debugAtomic to 0
        await this.resetDebugAtomicBuffer(debugAtomicBuffer, zeroBuffer);

        // 2) Write param buffer
        this.writeParamBuffer(paramBuffer, size, halfSize, dirFlag, actualCount, paddedCount);

        // 3) Dispatch
        const cmdEnc = this.device.createCommandEncoder();
        const passEnc = cmdEnc.beginComputePass();
        passEnc.setPipeline(pipeline);
        passEnc.setBindGroup(0, bindGroup);

        const workgroupCount = Math.ceil(paddedCount / 256);
        passEnc.dispatchWorkgroups(workgroupCount);

        passEnc.end();
        this.device.queue.submit([cmdEnc.finish()]);
        await this.device.queue.onSubmittedWorkDone();
    }

    /**
     * Reads back the row IDs from the GPU buffer into a Uint32Array on the CPU.
     *
     * @private
     * @param {GPUBuffer} rowIdBuffer - The GPU buffer holding the row IDs.
     * @param {number} totalBytes - The total byte length needed for the copy.
     * @returns {Promise<Uint32Array>} A promise resolving to the final array of row IDs.
     */
    private async readBackRowIds(
        rowIdBuffer: GPUBuffer,
        totalBytes: number
    ): Promise<Uint32Array> {
        // Create staging buffer
        const stagingBuffer = this.device.createBuffer({
            size: totalBytes,
            usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ
        });

        // Copy rowIdBuffer -> staging
        {
            const cmdEncoder = this.device.createCommandEncoder();
            cmdEncoder.copyBufferToBuffer(rowIdBuffer, 0, stagingBuffer, 0, totalBytes);
            this.device.queue.submit([cmdEncoder.finish()]);
            await this.device.queue.onSubmittedWorkDone();
        }

        // Map and read
        await stagingBuffer.mapAsync(GPUMapMode.READ);
        const copyArray = new Uint32Array(stagingBuffer.getMappedRange().slice(0));
        stagingBuffer.unmap();
        stagingBuffer.destroy();

        return copyArray;
    }

    /**
     * Checks whether `arr` is sorted in ascending order if `dirFlag=1`,
     * or descending order if `dirFlag=0`. Logs a warning if out-of-order.
     *
     * @private
     * @param {Uint32Array} arr - The array of row IDs to verify.
     * @param {number} dirFlag - 1 means ascending, 0 means descending.
     * @returns {void}
     */
    private assertSortedArray(arr: Uint32Array, dirFlag: number): void {
        if (arr.length < 2) {
            console.log("Array is trivially sorted (length < 2).");
            return;
        }

        const ascending = (dirFlag === 1);
        for (let i = 1; i < arr.length; i++) {
            if (ascending) {
                if (arr[i - 1] > arr[i]) {
                    console.warn(
                        "Array is NOT sorted ascending. First out-of-order pair at indices",
                        i - 1, i, arr[i - 1], arr[i]
                    );
                    return;
                }
            } else {
                // descending
                if (arr[i - 1] < arr[i]) {
                    console.warn(
                        "Array is NOT sorted descending. First out-of-order pair at indices",
                        i - 1, i, arr[i - 1], arr[i]
                    );
                    return;
                }
            }
        }

        if (ascending) {
            console.log("✔ Array is sorted ascending (CPU check).");
        } else {
            console.log("✔ Array is sorted descending (CPU check).");
        }
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
    private async writeRecordToStore(
        storeMeta: StoreMetadata,
        key: string,
        value: any,
        operationType: "add" | "put"
    ): Promise<void> {
        const keyMap = this.storeKeyMap.get(storeMeta.storeName)!;
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);

        // Find or create row metadata
        const rowMetadata = await this.findOrCreateRowMetadata(
            storeMeta,
            keyMap,
            key,
            arrayBuffer,
            operationType
        );

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
    private async writeOffsetsForAllDefinitions(
        storeMeta: StoreMetadata,
        key: string,
        value: any,
        operationType: "add" | "put"
    ): Promise<void> {
        const offsetsStoreName = `${storeMeta.storeName}-offsets`;
        const offsetsStoreMeta = this.storeMetadataMap.get(offsetsStoreName);
        if (!offsetsStoreMeta) {
            // If there's no offsets store at all, nothing to do
            return;
        }

        const offsetsKeyMap = this.storeKeyMap.get(offsetsStoreName)!;

        // Process each definition independently
        for (const singleDefinition of storeMeta.sortDefinition!) {
            // 1) Compute numeric keys for this definition
            const singleDefinitionOffsets = this.getJsonFieldOffsetsForSingleDefinition(
                value,
                singleDefinition
            );

            // 2) [Optional] Debug snippet
            if (Math.random() < 0.0002) {
                console.log(`Debugging offsets for definition "${singleDefinition.name}"`, singleDefinition);
                console.log("Object value:", value);
                console.log("Serialized Uint32Array:", singleDefinitionOffsets);
            }

            // 3) Create ArrayBuffer copy
            const offsetsCopy = new Uint32Array(singleDefinitionOffsets);
            const offsetsArrayBuffer = offsetsCopy.buffer;

            // 4) Use a composite key e.g. `<originalKey>::<definitionName>`
            const offsetRowKey = `${key}::${singleDefinition.name}`;

            // 5) Find or create row metadata for offsets store
            const offsetsRowMetadata = await this.findOrCreateRowMetadata(
                offsetsStoreMeta,
                offsetsKeyMap,
                offsetRowKey,
                offsetsArrayBuffer,
                operationType
            );

            // 6) Get GPU buffer for offsets
            const offsetsGpuBuffer = this.getBufferByIndex(
                offsetsStoreMeta,
                offsetsRowMetadata.bufferIndex
            );

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
}

import {
    StoreMetadata,
    BufferMetadata,
    RowMetadata,
    InitialMetrics,
    MapBufferSubsections,
    PerKeyMetrics,
    RowInfo,
    SortDefinition,
    SortField,
    PendingWrite,
    RowIdPaddingResult
} from "./types/StoreMetadata";

// For convenience, define a simple flag for inactive rows, e.g. 0x1.
const ROW_INACTIVE_FLAG = 0x1;

/**
 * Rounds the given value up to the nearest multiple of `align`.
 *
 * @param {number} value - The original value.
 * @param {number} align - The alignment boundary.
 * @returns {number} The smallest integer >= `value` that is a multiple of `align`.
 */
function roundUp(value: number, align: number): number {
    return Math.ceil(value / align) * align;
}

/**
 * Ensures the length of the UTF-8 representation of `jsonString` is a multiple of 4
 * by appending spaces as needed.
 *
 * @param {string} jsonString - The original JSON string to pad.
 * @returns {string} The padded JSON string, whose UTF-8 length is a multiple of 4.
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
 * Pads the given ArrayBuffer so that its byte length is a multiple of 4.
 * If it is already aligned, returns the original buffer. Otherwise, returns
 * a new buffer with zero-padding at the end.
 *
 * @param {ArrayBuffer} ab - The original ArrayBuffer to pad.
 * @returns {ArrayBuffer} A 4-byte-aligned ArrayBuffer.
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
