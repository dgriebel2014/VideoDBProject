/// <reference lib="webworker" />

// Types used by main thread <--> worker messages
interface GpuWorkerMessage {
    type: "INIT_DEVICE" | "WRITE_BATCH";
    payload?: {
        storeName?: string;
        batch?: ArrayBuffer;  // the data to write
        rowCount?: number;    // how many rows in the batch, if relevant
    };
}

interface GpuWorkerResponse {
    type: "DEVICE_READY" | "WRITE_DONE" | "ERROR";
    payload?: any;
}

// The GPU device this worker will use
let device: GPUDevice | null = null;

/**
 * Helper to initialize the GPU device if not already done.
 */
async function initDevice(): Promise<GPUDevice> {
    if (device) return device;
    const adapter = await navigator.gpu.requestAdapter();
    if (!adapter) {
        throw new Error("No GPU adapter found. WebGPU not supported?");
    }
    device = await adapter.requestDevice();
    return device;
}

/**
 * Worker message handler.
 */
self.onmessage = async (evt: MessageEvent<GpuWorkerMessage>) => {
    try {
        const msg = evt.data;
        switch (msg.type) {
            case "INIT_DEVICE": {
                // Initialize device if needed
                await initDevice();
                const response: GpuWorkerResponse = {
                    type: "DEVICE_READY",
                    payload: { message: "Worker GPUDevice initialized." },
                };
                self.postMessage(response);
                break;
            }

            case "WRITE_BATCH": {
                // Ensure we have a device before proceeding
                const dev = await initDevice();
                if (!msg.payload?.batch) {
                    throw new Error("Missing 'batch' in WRITE_BATCH payload.");
                }
                // Grab the raw data
                const batchBuffer = msg.payload.batch;

                // 2) Create the buffer
                const gpuBuffer = dev.createBuffer({
                    size: batchBuffer.byteLength,
                    usage: GPUBufferUsage.MAP_WRITE | GPUBufferUsage.COPY_SRC,
                    mappedAtCreation: false,
                });

                await gpuBuffer.mapAsync(GPUMapMode.WRITE, 0, batchBuffer.byteLength);
                const mappedRange = gpuBuffer.getMappedRange(0, batchBuffer.byteLength);

                // 4) Write only the real data length
                new Uint8Array(mappedRange).set(new Uint8Array(batchBuffer));
                gpuBuffer.unmap();

                // Post success back to main thread, including storeName, rowCount, etc.
                const response: GpuWorkerResponse = {
                    type: "WRITE_DONE",
                    payload: {
                        storeName: msg.payload.storeName ?? "unknownStore",
                        rowCount: msg.payload.rowCount ?? 0,
                        message: "Batch written successfully in worker.",
                    },
                };
                self.postMessage(response);
                break;
            }

            default: {
                // Unknown message type
                throw new Error(`Unknown worker message type: ${msg.type}`);
            }
        }
    } catch (err: any) {
        // On any error, post an ERROR message
        const errorResponse: GpuWorkerResponse = {
            type: "ERROR",
            payload: { message: err?.message || String(err) },
        };
        self.postMessage(errorResponse);
    }
};