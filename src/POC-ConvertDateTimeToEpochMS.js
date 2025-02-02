async function runAbsoluteEpochTimeShader()
{
    if (!navigator.gpu)
    {
        console.error("WebGPU is not supported in this browser.");
        return;
    }

    // Request the adapter and device.
    const adapter = await navigator.gpu.requestAdapter();
    const device = await adapter.requestDevice();

    // WGSL shader code.
    // This shader receives a uniform DateTime structure (using UTC components)
    // and computes the absolute epoch milliseconds (since 1970-01-01) as a simulated 64-bit unsigned integer,
    // split into two u32 values (low and high).
    const shaderCode = `
struct DateTime
{
    year: i32,
    month: i32,
    day: i32,
    hour: i32,
    minute: i32,
    second: i32,
    millisecond: i32,
    pad: i32,
};

struct EpochTime
{
    low: u32,
    high: u32,
};

@group(0) @binding(0)
var<uniform> currentDate: DateTime;

@group(0) @binding(1)
var<storage, read_write> output : EpochTime;

// Multiply two 32-bit unsigned integers and return a simulated 64-bit result as vec2<u32>(low, high).
fn umulExtended(u: u32, v: u32) -> vec2<u32> {
    let u0 = u & 0xFFFFu;
let u1 = u >> 16u;
let v0 = v & 0xFFFFu;
let v1 = v >> 16u;

let w0 = u0 * v0;
let t = u1 * v0 + (w0 >> 16u);
let w1 = t & 0xFFFFu;
let w2 = t >> 16u;
let t2 = u0 * v1 + w1;
let high = u1 * v1 + w2 + (t2 >> 16u);
let low = (t2 << 16u) | (w0 & 0xFFFFu);
return vec2<u32>(low, high);
}

// Add a 32-bit unsigned integer to a simulated 64-bit unsigned integer.
fn add_u32_to_u64(x: vec2<u32>, y: u32) -> vec2<u32> {
    var low = x.x + y;
    var high = x.y;
    if (low < y)
    {
        high = high + 1u;
    }
    return vec2<u32>(low, high);
}

// Compute the epoch milliseconds for a given UTC date/time.
// 1. Compute the Julian Day Number (JDN) for the given date.
// 2. Subtract 2440588 (JDN for 1970-01-01) to get the number of days since the epoch.
// 3. Multiply by 86400000 (milliseconds per day) and add the time-of-day in milliseconds.
fn date_to_epoch_millis(year: i32, month: i32, day: i32,
                          hour: i32, minute: i32, second: i32, millisecond: i32) -> vec2<u32> {
    let a = (14 - month) / 12;
let y_adj = year + 4800 - a;
let m_adj = month + 12 * a - 3;
let jdn = day + ((153 * m_adj + 2) / 5)
          + 365 * y_adj
          + (y_adj / 4)
          - (y_adj / 100)
          + (y_adj / 400)
          - 32045;
let days_since_epoch = jdn - 2440588;
let days_u = u32(days_since_epoch);

// Multiply days by 86400000 (ms per day).
let msPerDay: u32 = 86400000u;
let msFromDays = umulExtended(days_u, msPerDay);

// Compute time-of-day in milliseconds.
let tod: u32 = u32(hour * 3600000 + minute * 60000 + second * 1000 + millisecond);

// Add time-of-day.
let total = add_u32_to_u64(msFromDays, tod);
return total;
}

@compute @workgroup_size(1)
fn main()
{
    let result = date_to_epoch_millis(
        currentDate.year, currentDate.month, currentDate.day,
        currentDate.hour, currentDate.minute, currentDate.second, currentDate.millisecond
    );
    output.low = result.x;
    output.high = result.y;
}
`;

    // Create the shader module.
    const shaderModule = device.createShaderModule({
            code: shaderCode
        });

    // Create a uniform buffer for DateTime (8 × 4 bytes = 32 bytes).
    const uniformBufferSize = 32;
    const uniformBuffer = device.createBuffer({
            size: uniformBufferSize,
        usage: GPUBufferUsage.UNIFORM | GPUBufferUsage.COPY_DST,
        });

    // Create a storage buffer for the EpochTime result (2 × 4 bytes = 8 bytes).
    const outputBufferSize = 8;
    const outputBuffer = device.createBuffer({
            size: outputBufferSize,
        usage: GPUBufferUsage.STORAGE | GPUBufferUsage.COPY_SRC,
        });

    // Create a read buffer (with MAP_READ) for copying the result.
    const readBuffer = device.createBuffer({
            size: outputBufferSize,
        usage: GPUBufferUsage.COPY_DST | GPUBufferUsage.MAP_READ,
        });

    // Set up the bind group layout.
    const bindGroupLayout = device.createBindGroupLayout({
            entries: [{
                    binding: 0,
                    visibility: GPUShaderStage.COMPUTE,
                    buffer:
    {
        type: "uniform"
                    }
                },
                {
        binding: 1,
                    visibility: GPUShaderStage.COMPUTE,
                    buffer:
        {
            type: "storage"
                    }
    },
            ],
        });

    const pipelineLayout = device.createPipelineLayout({
            bindGroupLayouts: [bindGroupLayout],
        });

    const computePipeline = device.createComputePipeline({
            layout: pipelineLayout,
        compute: {

            module: shaderModule,
            entryPoint: "main"
            },
        });

    const bindGroup = device.createBindGroup({
            layout: bindGroupLayout,
        entries: [{
                    binding: 0,
                    resource:
    {
        buffer: uniformBuffer
                    }
                },
                {
        binding: 1,
                    resource:
        {
            buffer: outputBuffer
                    }
    },
            ],
        });

    // Get the current date/time from JavaScript in UTC.
    const now = new Date();
    const year = now.getUTCFullYear();
    const month = now.getUTCMonth() + 1; // Convert from 0-indexed to 1-indexed.
    const day = now.getUTCDate();
    const hour = now.getUTCHours();
    const minute = now.getUTCMinutes();
    const second = now.getUTCSeconds();
    const millisecond = now.getUTCMilliseconds();

    // Pack the UTC date/time values into an Int32Array (8 elements = 32 bytes).
    const uniformData = new Int32Array([year, month, day, hour, minute, second, millisecond, 0]);
    device.queue.writeBuffer(uniformBuffer, 0, uniformData.buffer, uniformData.byteOffset, uniformData.byteLength);

    // Compute the CPU epoch time for the same "now" value.
    // Date.now() returns the number of milliseconds since 1970-01-01 UTC.
    const cpuEpochMillis = BigInt(now.getTime());

    // Encode commands to dispatch the compute shader and copy the result.
    const commandEncoder = device.createCommandEncoder();
    const passEncoder = commandEncoder.beginComputePass();
    passEncoder.setPipeline(computePipeline);
    passEncoder.setBindGroup(0, bindGroup);
    passEncoder.dispatchWorkgroups(1);
    passEncoder.end();

    // Copy the 8-byte output from the storage buffer to the read buffer.
    commandEncoder.copyBufferToBuffer(outputBuffer, 0, readBuffer, 0, outputBufferSize);

    const gpuCommands = commandEncoder.finish();
    device.queue.submit([gpuCommands]);

    // Wait for the GPU work to complete and map the read buffer.
    await readBuffer.mapAsync(GPUMapMode.READ);
    const arrayBuffer = readBuffer.getMappedRange();
    const outputArray = new Uint32Array(arrayBuffer);
    const low = outputArray[0];
    const high = outputArray[1];
    readBuffer.unmap();

    // Combine the high and low parts into a JavaScript BigInt.
    const gpuEpochMillis = (BigInt(high) << 32n) | BigInt(low);

    // Output both values.
    console.log("Absolute epoch milliseconds (GPU computed):", gpuEpochMillis.toString());
    console.log("Absolute epoch milliseconds (CPU computed):", cpuEpochMillis.toString());

    // Check if they agree.
    if (gpuEpochMillis !== cpuEpochMillis)
    {
        console.error("ERROR: GPU and CPU results do not agree!");
    }
    else
    {
        console.log("SUCCESS: GPU and CPU results agree.");
    }
}

// Run the function.
runAbsoluteEpochTimeShader();