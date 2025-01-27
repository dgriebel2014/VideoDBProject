// types/StoreMetadata.ts
export interface StoreMetadata {
    storeName: string;
    dataType: "TypedArray" | "ArrayBuffer" | "JSON";
    typedArrayType?:
    | "Float32Array"
    | "Float64Array"
    | "Int32Array"
    | "Uint32Array"
    | "Uint8Array";
    bufferSize: number;
    rowSize?: number;
    rowsPerBuffer?: number;
    totalRows: number;

    buffers: BufferMetadata[];
    rows: RowMetadata[];

    // Top-level sort definitions
    sortDefinition?: SortDefinition[];
    pendingSortOffsets?: Map<number, [number, number][]>;

    /**
     * True if the store’s sort orders are potentially out of date (due to new writes).
     * Must never be null or undefined; default to false when the store is created.
     */
    sortsDirty: boolean;
}

export interface BufferMetadata {
    bufferIndex: number;
    startRow: number;
    rowCount: number;
    // You may no longer need GPUBuffer references if you're purely CPU-based:
    gpuBuffer?: GPUBuffer;
}

export interface RowMetadata {
    rowId: number;
    bufferIndex: number;
    offset: number;
    length: number;
    flags?: number;
}

export interface InitialMetrics {
    flushWrites: number;
    metadataRetrieval: number;
}
export interface MapBufferSubsections {
    mapAsync: number;
    getMappedRange: number;
    copyToUint8Array: number;
    unmap: number;
}
export interface PerKeyMetrics {
    findMetadata: number;
    createBuffer: number;
    copyBuffer: number;
    mapBuffer: number;
    deserialize: number;
    mapBufferSubsections: MapBufferSubsections;
}

export type RowInfo = {
    rowMetadata: RowMetadata;
    rowIndex: number;
    offsetInFinalBuffer: number;
    length: number;
}

export interface SortDefinition {
    name: string;
    sortFields: {
        dataType: "string" | "number" | "date";
        sortColumn: string;
        path: string;
        sortDirection: "Asc" | "Desc";
    }[];
}
export interface SortField {
    dataType: "string" | "number" | "date";
    sortColumn: string;
    path: string;
    sortDirection: "Asc" | "Desc";
}
export interface PendingWrite {
    storeMeta: StoreMetadata;
    rowMetadata: RowMetadata;
    arrayBuffer: ArrayBuffer;
    gpuBuffer: GPUBuffer;
    operationType: 'add' | 'put' | 'delete';
    key?: string; // Required for 'delete' operations
}

export interface RowIdPaddingResult {
    paddedRowIds: Uint32Array;
    rowCount: number;
    paddedCount: number;
}