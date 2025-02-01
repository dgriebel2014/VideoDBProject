/**
 * Fully-correct Bitonic Sort in one GPU dispatch (fixing struct syntax errors).
 * 
 * - Each thread i in [0..Npow2-1] performs classical bitonic sorting with 
 *   all sub-stages, ensuring a properly sorted result.
 * - Uses only global memory (no large local arrays).
 * - O(N log^2(N)) compare-swaps, which can be slow for large N.
 *
 * Usage:
 *   await demoBitonicSortGlobalAllInOne(16);
 *   await demoBitonicSortGlobalAllInOne(1024 * 10); // may be slow
 */
async function demoBitonicSortGlobalAllInOne(numRecords = 16) {
    //---------------------------------------------------------------------------
    // 1) Basic Setup & GPU Check
    //---------------------------------------------------------------------------
    if (!navigator.gpu) {
        console.error("WebGPU not available in this browser.");
        return;
    }
    const adapter = await navigator.gpu.requestAdapter();
    if (!adapter) {
        console.error("Failed to get GPU adapter.");
        return;
    }
    const device = await adapter.requestDevice();

    //---------------------------------------------------------------------------
    // 2) Generate random records (each 102 u32s)
    //---------------------------------------------------------------------------
    const MAX_LENGTH_PER_RECORD = 100;
    const RECORD_SLOT_SIZE = 1 + 1 + MAX_LENGTH_PER_RECORD; // 102

    function createRandomRecord(recordID) {
        const length = Math.floor(Math.random() * MAX_LENGTH_PER_RECORD) + 1;
        const arr = new Uint32Array(RECORD_SLOT_SIZE);
        arr[0] = recordID;  // ID
        arr[1] = length;    // length
        for (let i = 0; i < length; i++) {
            arr[2 + i] = Math.floor(Math.random() * 100);
        }
        return arr;
    }

    const records = [];
    for (let i = 0; i < numRecords; i++) {
        records.push(createRandomRecord(i + 1));
    }

    //---------------------------------------------------------------------------
    // 3) Pad N up to next power of two (bitonic sort requires it)
    //---------------------------------------------------------------------------
    function nextPowerOfTwo(x) {
        return 1 << Math.ceil(Math.log2(x));
    }
    const N = numRecords;
    const Npow2 = nextPowerOfTwo(N);
    const totalCount = Npow2 * RECORD_SLOT_SIZE;

    const combined = new Uint32Array(totalCount);
    for (let i = 0; i < N; i++) {
        combined.set(records[i], i * RECORD_SLOT_SIZE);
    }
    // Fill the remainder with sentinel
    for (let i = N; i < Npow2; i++) {
        const base = i * RECORD_SLOT_SIZE;
        combined[base + 0] = 0xffffffff; // large ID
        combined[base + 1] = MAX_LENGTH_PER_RECORD;
        for (let j = 0; j < MAX_LENGTH_PER_RECORD; j++) {
            combined[base + 2 + j] = 0xffffffff;
        }
    }

    //---------------------------------------------------------------------------
    // 4) Create GPU Buffers
    //---------------------------------------------------------------------------
    const bufferSizeBytes = combined.byteLength;
    const dataBuffer = device.createBuffer({
        size: bufferSizeBytes,
        usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC | GPUBufferUsage.COPY_DST,
    });
    device.queue.writeBuffer(dataBuffer, 0, combined);

    const resultBuffer = device.createBuffer({
        size: bufferSizeBytes,
        usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ,
    });

    //---------------------------------------------------------------------------
    // 5) Single-Kernel Bitonic Sort (All Sub-stages in Nested Loops)
    //
    // We do the classical nested loops:
    //   for (k = 2; k <= Npow2; k <<= 1) {
    //     for (j = k >> 1; j > 0; j >>= 1) {
    //       partner = i ^ j
    //       if (i < partner && i < n && partner < n) ...
    //       swap if ascending or descending
    //     }
    //   }
    //
    // We'll embed these loops in WGSL, within one dispatch. Each iteration
    // ends with a barrier so all threads proceed in lockstep.
    //---------------------------------------------------------------------------
    const wgslCode = /* wgsl */`
struct SortParams {
    n : u32
};

@group(0) @binding(0) var<storage, read_write> data: array<u32>;
@group(0) @binding(1) var<uniform> params: SortParams;

const RECORD_SIZE = 102u;

fn compareRecords(iA: u32, iB: u32) -> i32 {
    let baseA = iA * RECORD_SIZE;
    let baseB = iB * RECORD_SIZE;
    let lenA = data[baseA + 1u];
    let lenB = data[baseB + 1u];
    let minLen = min(lenA, lenB);

    for (var idx = 0u; idx < minLen; idx++) {
        let aVal = data[baseA + 2u + idx];
        let bVal = data[baseB + 2u + idx];
        if (aVal < bVal) {
            return -1;
        }
        if (aVal > bVal) {
            return 1;
        }
    }
    // Compare length if prefix matched
    if (lenA < lenB) {
        return -1;
    }
    if (lenA > lenB) {
        return 1;
    }
    return 0;
}

fn swapRecords(iA: u32, iB: u32) {
    let baseA = iA * RECORD_SIZE;
    let baseB = iB * RECORD_SIZE;
    for (var i = 0u; i < RECORD_SIZE; i++) {
        let tmp = data[baseA + i];
        data[baseA + i] = data[baseB + i];
        data[baseB + i] = tmp;
    }
}

@compute @workgroup_size(128)
fn main(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    let n = params.n;
    let inRange = i < n;

    var k = 2u;
    loop {
        if (k > n) {
            break;
        }

        var j = k >> 1u;
        loop {
            if (j == 0u) {
                break;
            }

            let partner = i ^ j;
            if (inRange && (partner < n) && (i < partner)) {
                let ascending = ((i & k) == 0u);
                let cmp = compareRecords(i, partner);
                if ((ascending && cmp > 0) || (!ascending && cmp < 0)) {
                    swapRecords(i, partner);
                }
            }

            workgroupBarrier();
            j = j >> 1u;
        }

        k = k << 1u;
    }
}
`;

    const module = device.createShaderModule({ code: wgslCode });
    const pipeline = device.createComputePipeline({
        layout: device.createPipelineLayout({
            bindGroupLayouts: [
                device.createBindGroupLayout({
                    entries: [
                        { binding: 0, visibility: GPUShaderStage.COMPUTE, buffer: { type: "storage" } },
                        { binding: 1, visibility: GPUShaderStage.COMPUTE, buffer: { type: "uniform" } },
                    ],
                }),
            ],
        }),
        compute: { module, entryPoint: "main" },
    });

    //---------------------------------------------------------------------------
    // 6) Uniform Buffer + BindGroup
    //---------------------------------------------------------------------------
    const sortParamBuf = device.createBuffer({
        size: 4, // 1x u32
        usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
    });
    device.queue.writeBuffer(sortParamBuf, 0, new Uint32Array([Npow2]));

    const bindGroup = device.createBindGroup({
        layout: pipeline.getBindGroupLayout(0),
        entries: [
            { binding: 0, resource: { buffer: dataBuffer } },
            { binding: 1, resource: { buffer: sortParamBuf } },
        ],
    });

    //---------------------------------------------------------------------------
    // 7) Dispatch a Single Pass
    //---------------------------------------------------------------------------
    console.log("----- SORTING STARTED (All-in-One Bitonic) -----");
    const t0 = performance.now();

    const commandEncoder = device.createCommandEncoder();
    {
        const pass = commandEncoder.beginComputePass();
        pass.setPipeline(pipeline);
        pass.setBindGroup(0, bindGroup);

        // We need enough threads to cover all Npow2 items
        const workgroupSize = 128;
        const numGroups = Math.ceil(Npow2 / workgroupSize);
        pass.dispatchWorkgroups(numGroups);

        pass.end();
    }
    device.queue.submit([commandEncoder.finish()]);
    await device.queue.onSubmittedWorkDone();

    const t1 = performance.now();
    console.log("----- SORTING COMPLETED (All-in-One Bitonic) -----");
    console.log(`Bitonic Sort took ${(t1 - t0).toFixed(3)} ms (global, single dispatch).`);

    //---------------------------------------------------------------------------
    // 8) Copy results back to CPU (avoid "detached" buffer)
    //---------------------------------------------------------------------------
    {
        const copyCmd = device.createCommandEncoder();
        copyCmd.copyBufferToBuffer(dataBuffer, 0, resultBuffer, 0, bufferSizeBytes);
        device.queue.submit([copyCmd.finish()]);
        await device.queue.onSubmittedWorkDone();
    }

    await resultBuffer.mapAsync(GPUMapMode.READ);
    const mappedRange = resultBuffer.getMappedRange();
    // copy into a new typed array before unmapping
    const gpuData = new Uint32Array(mappedRange.byteLength / 4);
    gpuData.set(new Uint32Array(mappedRange));
    resultBuffer.unmap();

    // Extract final sorted records
    function extractRecord(i) {
        const base = i * RECORD_SLOT_SIZE;
        const id = gpuData[base];
        const length = gpuData[base + 1];
        const arr = gpuData.slice(base + 2, base + 2 + length);
        return { recordID: id, array: Array.from(arr) };
    }
    const sortedRecords = [];
    for (let i = 0; i < N; i++) {
        sortedRecords.push(extractRecord(i));
    }

     // (Optional) Log them if desired
     console.log("----- AFTER SORT (GPU) -----");
     for (let i = 0; i < N; i++) {
         const rec = sortedRecords[i];
         const arrPreview = rec.array.slice(0, 5).join(", ");
         console.log(
             `Sorted ${i}: ID=${rec.recordID}, length=${rec.array.length}, data=[${arrPreview} ...]`
         );
     }

    return sortedRecords;
}

// Example usage:
// await demoBitonicSortGlobalOnly(1024 * 10);

/**
 * CPU-based Bitonic Sort in standard JavaScript, storing each record in 102 u32s.
 * 
 * This function duplicates the logic from `demoBitonicSortSingleSubmit()` but
 * performs the sort on the CPU instead of the GPU. It returns the final sorted
 * records, measuring the time taken using `performance.now()`.
 */
function demoBitonicSortCpu(numRecords = 16) {
    // --------------------------------------------------------------------------
    // 1) Generate random records, each occupying 102 u32s.
    // --------------------------------------------------------------------------
    const MAX_LENGTH_PER_RECORD = 100;
    const RECORD_SLOT_SIZE = 1 + 1 + MAX_LENGTH_PER_RECORD; // 102 total

    function createRandomRecord(recordID) {
        // random length [1..100]
        const length = Math.floor(Math.random() * MAX_LENGTH_PER_RECORD) + 1;
        const arr = new Uint32Array(RECORD_SLOT_SIZE);
        arr[0] = recordID; // ID
        arr[1] = length;   // length
        for (let i = 0; i < length; i++) {
            arr[2 + i] = Math.floor(Math.random() * 100);
        }
        return arr;
    }

    const records = [];
    for (let i = 0; i < numRecords; i++) {
        records.push(createRandomRecord(i + 1));
    }

    // --------------------------------------------------------------------------
    // 2) Pad N up to next power of two
    // --------------------------------------------------------------------------
    function nextPowerOfTwo(x) {
        return 1 << Math.ceil(Math.log2(x));
    }
    const N = numRecords;
    const Npow2 = nextPowerOfTwo(N);

    const totalCount = Npow2 * RECORD_SLOT_SIZE;
    const combined = new Uint32Array(totalCount);

    // Copy real records
    for (let i = 0; i < N; i++) {
        combined.set(records[i], i * RECORD_SLOT_SIZE);
    }

    // Fill the rest with sentinel (very large values so they go to the "end")
    for (let i = N; i < Npow2; i++) {
        const base = i * RECORD_SLOT_SIZE;
        combined[base + 0] = 0xffffffff; // large ID
        combined[base + 1] = MAX_LENGTH_PER_RECORD;
        for (let j = 0; j < MAX_LENGTH_PER_RECORD; j++) {
            combined[base + 2 + j] = 0xffffffff;
        }
    }

    // --------------------------------------------------------------------------
    // 3) Define compare & swap for our 102-u32 records
    // --------------------------------------------------------------------------
    function compareRecords(iA, iB) {
        const baseA = iA * RECORD_SLOT_SIZE;
        const baseB = iB * RECORD_SLOT_SIZE;

        // Compare length
        const lenA = combined[baseA + 1];
        const lenB = combined[baseB + 1];
        const minLen = Math.min(lenA, lenB);

        // Lexicographically compare combined[2..2+len-1]
        for (let idx = 0; idx < minLen; idx++) {
            const aVal = combined[baseA + 2 + idx];
            const bVal = combined[baseB + 2 + idx];
            if (aVal < bVal) {
                return -1;
            } else if (aVal > bVal) {
                return 1;
            }
        }

        // If all in common are the same, compare length
        if (lenA < lenB) {
            return -1;
        } else if (lenA > lenB) {
            return 1;
        }
        return 0;
    }

    function swapRecords(iA, iB) {
        const baseA = iA * RECORD_SLOT_SIZE;
        const baseB = iB * RECORD_SLOT_SIZE;
        for (let i = 0; i < RECORD_SLOT_SIZE; i++) {
            const tmp = combined[baseA + i];
            combined[baseA + i] = combined[baseB + i];
            combined[baseB + i] = tmp;
        }
    }

    // --------------------------------------------------------------------------
    // 4) Perform the CPU-based Bitonic Sort
    // 
    // Matches the same pass structure:
    //   for k in [2..Npow2], doubling
    //     for j in [k/2..1], halving
    //       for i in [0..N-1] (and partner < N)
    //         compare & swap if needed
    //
    // We only iterate i up to N (not Npow2), just like the GPU kernel does.
    // --------------------------------------------------------------------------
    console.log("----- SORTING STARTED (CPU) -----");
    const sortStartTime = performance.now();

    for (let k = 2; k <= Npow2; k <<= 1) {
        for (let j = (k >> 1); j > 0; j >>= 1) {
            for (let i = 0; i < N; i++) {
                const partner = i ^ j;
                if (partner < N && i < partner) {
                    // Ascending if (i & k) == 0
                    const ascending = ((i & k) === 0);
                    const cmp = compareRecords(i, partner);
                    if ((ascending && cmp > 0) || (!ascending && cmp < 0)) {
                        swapRecords(i, partner);
                    }
                }
            }
        }
    }

    const sortEndTime = performance.now();
    console.log("----- SORTING COMPLETED (CPU) -----");
    console.log(`CPU Bitonic Sort took ${(sortEndTime - sortStartTime).toFixed(3)} ms.`);

    // --------------------------------------------------------------------------
    // 5) Extract final sorted data (only the first N real records)
    // --------------------------------------------------------------------------
    function extractRecord(i) {
        const base = i * RECORD_SLOT_SIZE;
        const id = combined[base + 0];
        const length = combined[base + 1];
        const arr = combined.slice(base + 2, base + 2 + length);
        return { recordID: id, array: Array.from(arr) };
    }

    const sortedRecords = [];
    for (let i = 0; i < N; i++) {
        sortedRecords.push(extractRecord(i));
    }

    // (Optional) Log them if desired
    // console.log("----- AFTER SORT (CPU) -----");
    // for (let i = 0; i < N; i++) {
    //     const rec = sortedRecords[i];
    //     const arrPreview = rec.array.slice(0, 5).join(", ");
    //     console.log(
    //         `Sorted ${i}: ID=${rec.recordID}, length=${rec.array.length}, data=[${arrPreview} ...]`
    //     );
    // }

    return sortedRecords;
}

// Example usage:
await demoBitonicSortGlobalAllInOne(1024 * 10);

