// types/StoreMetadata.ts
export interface StoreMetadata {
    dataType: "TypedArray" | "ArrayBuffer" | "JSON";
    typedArrayType?: "Float32Array" | "Float64Array" | "Int32Array" | "Uint32Array" | "Uint8Array";
    bufferSize: number;
    rowSize?: number;
    rowsPerBuffer?: number;
    totalRows: number;

    // Updated fields for the CPU-based row approach:
    buffers: BufferMetadata[];
    rows: RowMetadata[];

    // GPU mirror fields (not used yet, but in your new design for future use)
    metadataBuffer?: GPUBuffer;
    dirtyMetadata: boolean;
    metadataVersion: number;
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
