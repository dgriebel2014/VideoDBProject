import { StoreMetadata, BufferMetadata, RowMetadata } from "./types/StoreMetadata";

// For convenience, define a simple flag for inactive rows, e.g. 0x1.
const ROW_INACTIVE_FLAG = 0x1;

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
 * A simplified VideoDB class that:
 * - Uses 250MB GPU buffers as "chunks."
 * - Never destroys or reclaims space in old buffers.
 * - Stores all metadata in CPU memory.
 */
export class VideoDB {
    public storeMetadataMap: Map<string, StoreMetadata>;
    public storeKeyMap: Map<string, Map<string, number>>;

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
     * Stores or updates data in a GPU-backed store.
     * @param storeName - The name of the object store.
     * @param key - The unique key identifying the row.
     * @param value - The data to be written (JSON, TypedArray, or ArrayBuffer).
     * @returns A promise that resolves once the data is stored.
     */
    public async put(storeName: string, key: string, value: any): Promise<void> {
        const storeMeta = this.storeMetadataMap.get(storeName);
        if (!storeMeta) {
            throw new Error(`Object store "${storeName}" does not exist.`);
        }

        const keyMap = this.storeKeyMap.get(storeName)!;
        const arrayBuffer = this.serializeValueForStore(storeMeta, value);

        // Locate or create row metadata and perform initial data write
        const rowMetadata = await this.findOrCreateRowMetadata(storeMeta, keyMap, key, arrayBuffer);

        // Optionally re-write for alignment and finalize metadata
        const gpuBuffer = this.getBufferByIndex(storeMeta, rowMetadata.bufferIndex);
        await this.finalizeWrite(storeMeta, rowMetadata, arrayBuffer, gpuBuffer);

        console.log(`Data stored for key "${key}" in object store "${storeName}", row ${rowMetadata.rowId}.`);
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
     * Looks up the correct RowMetadata for a given key from the store’s key map.
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

        const rowMetadata = storeMeta.rows.find(r => r.rowId === rowId);
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
        const offset = usedBytes;
        (gpuBuffer as any)._usedBytes = offset + size;
        lastBufferMeta.rowCount += 1;

        const bufferIndex = lastBufferMeta.bufferIndex;
        return {
            gpuBuffer,
            bufferIndex,
            offset
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
     * Creates and returns a new GPU buffer chunk for the specified store metadata.
     * @param {StoreMetadata} storeMeta - The metadata of the store that needs a new chunk.
     * @returns {BufferMetadata} The metadata for the newly created GPU buffer chunk.
     */
    private createNewChunk(storeMeta: StoreMetadata): BufferMetadata {
        const gpuBuffer = this.device.createBuffer({
            size: storeMeta.bufferSize,
            usage: GPUBufferUsage.MAP_WRITE | GPUBufferUsage.COPY_SRC,
            mappedAtCreation: false
        });
        const bufferIndex = storeMeta.buffers.length;

        const newBufferMeta: BufferMetadata = {
            bufferIndex,
            startRow: -1, // We can set this once we know the row ID
            rowCount: 0,
            gpuBuffer
        };
        storeMeta.buffers.push(newBufferMeta);
        console.log(`Allocated a new 250MB chunk for store (bufferIndex=${bufferIndex}).`);
        return newBufferMeta;
    }

    /**
     * Allocates space for a new row in the store's GPU buffer, creating a new chunk if needed.
     * @param {StoreMetadata} storeMeta - The metadata of the store to allocate space in.
     * @param {number} size - The size in bytes required for the new row.
     * @returns {{ bufferMeta: BufferMetadata; chunkOffset: number }}
     *          An object containing the buffer metadata and the offset at which the new row is allocated.
     */
    private allocateSpaceForNewRow(storeMeta: StoreMetadata, size: number): {
        bufferMeta: BufferMetadata;
        chunkOffset: number;
    } {
        // If there's no chunk, or the last chunk is too full, create a new one
        let bufferMeta = storeMeta.buffers[storeMeta.buffers.length - 1];
        if (!bufferMeta || !bufferMeta.gpuBuffer) {
            // Create the first chunk
            bufferMeta = this.createNewChunk(storeMeta);
        }

        // Suppose we track an extra field on BufferMetadata to track usedBytes
        const usedBytes = (bufferMeta as any)._usedBytes || 0;
        if (usedBytes + size > storeMeta.bufferSize) {
            // Need a new chunk
            bufferMeta = this.createNewChunk(storeMeta);
            (bufferMeta as any)._usedBytes = 0;
        }

        // Now we allocate from bufferMeta
        const chunkOffset = (bufferMeta as any)._usedBytes || 0;
        (bufferMeta as any)._usedBytes = chunkOffset + size;
        bufferMeta.rowCount += 1;

        return { bufferMeta, chunkOffset };
    }

    /**
     * Converts a given value into an ArrayBuffer based on the store's data type.
     * @param storeMeta - Metadata defining the store's data type (JSON, TypedArray, ArrayBuffer).
     * @param value - The data to be serialized.
     * @returns An ArrayBuffer containing the serialized data.
     */
    private serializeValueForStore(storeMeta: StoreMetadata, value: any): ArrayBuffer {
        switch (storeMeta.dataType) {
            case "JSON": {
                let jsonString = JSON.stringify(value);
                jsonString = padJsonTo4Bytes(jsonString);
                return new TextEncoder().encode(jsonString).buffer;
            }
            case "TypedArray": {
                if (!storeMeta.typedArrayType) {
                    throw new Error(`typedArrayType is missing for store "${storeMeta}".`);
                }
                const TypedArrayCtor = (globalThis as any)[storeMeta.typedArrayType];
                if (!(value instanceof TypedArrayCtor)) {
                    throw new Error(
                        `Value must be an instance of ${storeMeta.typedArrayType} for store "${storeMeta}".`
                    );
                }
                return value.buffer;
            }
            case "ArrayBuffer": {
                if (!(value instanceof ArrayBuffer)) {
                    throw new Error(`Value must be an ArrayBuffer for store "${storeMeta}".`);
                }
                return value;
            }
            default:
                throw new Error(`Unknown dataType "${storeMeta.dataType}".`);
        }
    }

    /**
     * Finds or creates a row metadata entry and writes the data if necessary.
     * @param storeMeta - The metadata of the target store.
     * @param keyMap - A map of key-to-rowId for the store.
     * @param key - The key identifying the row.
     * @param arrayBuffer - The serialized data to be written.
     * @returns A promise that resolves to the relevant RowMetadata (new or existing).
     */
    private async findOrCreateRowMetadata(
        storeMeta: StoreMetadata,
        keyMap: Map<string, number>,
        key: string,
        arrayBuffer: ArrayBuffer
    ): Promise<RowMetadata> {
        let rowId = keyMap.get(key);
        let rowMetadata = rowId == null
            ? null
            : storeMeta.rows.find((r) => r.rowId === rowId) || null;

        const { gpuBuffer, bufferIndex, offset } = this.findOrCreateSpace(storeMeta, arrayBuffer.byteLength);

        if (!rowMetadata) {
            // Create a new row
            rowId = storeMeta.rows.length + 1;
            rowMetadata = {
                rowId,
                bufferIndex,
                offset,
                length: arrayBuffer.byteLength
            };
            storeMeta.rows.push(rowMetadata);
            keyMap.set(key, rowId);

            // Initial write of data
            await this.writeDataToBuffer(gpuBuffer, offset, arrayBuffer);
        } else {
            // Handle overwrite or move to new allocation
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
    private async updateRowOnOverwrite(
        storeMeta: StoreMetadata,
        oldRowMeta: RowMetadata,
        arrayBuffer: ArrayBuffer,
        keyMap: Map<string, number>,
        key: string
    ): Promise<RowMetadata> {
        if (arrayBuffer.byteLength <= oldRowMeta.length) {
            // Overwrite in place
            const oldBuf = this.getBufferByIndex(storeMeta, oldRowMeta.bufferIndex);
            await this.writeDataToBuffer(oldBuf, oldRowMeta.offset, arrayBuffer);

            // Adjust length if new data is smaller
            if (arrayBuffer.byteLength < oldRowMeta.length) {
                oldRowMeta.length = arrayBuffer.byteLength;
            }
            return oldRowMeta;
        } else {
            // Mark old row inactive
            oldRowMeta.flags = (oldRowMeta.flags ?? 0) | 0x1;

            // Create new allocation
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
     * Copies data from the store’s GPU buffer chunk into a CPU-visible buffer
     * and returns the contents as a Uint8Array.
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
     * Converts the raw bytes in a Uint8Array back into the appropriate data type based on store metadata.
     *
     * @param {StoreMetadata} storeMeta - The metadata of the target store, including `dataType` and `typedArrayType`.
     * @param {Uint8Array} copiedData - The raw bytes read from the GPU buffer.
     * @returns {any} The deserialized data, which may be a JSON object, a typed array, or an ArrayBuffer.
     */
    private deserializeData(storeMeta: StoreMetadata, copiedData: Uint8Array): any {
        switch (storeMeta.dataType) {
            case "JSON": {
                const jsonString = new TextDecoder().decode(copiedData);
                return JSON.parse(jsonString);
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
     * Writes data into a GPU buffer at the specified offset and ensures JSON-safe padding.
     * @param gpuBuffer - The GPU buffer to write into.
     * @param offset - The offset within the buffer.
     * @param arrayBuffer - The data to write.
     * @returns The aligned size of the written data.
     */
    private async writeDataToBuffer(
        gpuBuffer: GPUBuffer,
        offset: number,
        arrayBuffer: ArrayBuffer
    ): Promise<number> {
        console.log("Buffer size:", gpuBuffer.size);
        console.log("Offset:", offset, "Write length:", arrayBuffer.byteLength);

        try {
            // Map the buffer for writing
            await gpuBuffer.mapAsync(GPUMapMode.WRITE);
            console.log("Buffer successfully mapped.");

            // Copy directly into the mapped range
            const mappedRange = gpuBuffer.getMappedRange(offset, arrayBuffer.byteLength);
            new Uint8Array(mappedRange).set(new Uint8Array(arrayBuffer));
            gpuBuffer.unmap();
            console.log("Data successfully written to GPU buffer.");
        } catch (err) {
            console.error("Error writing to GPU buffer:", err);
            throw err;
        }

        // Return the actual length of the data written
        return arrayBuffer.byteLength;
    }
}
