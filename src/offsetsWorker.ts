// offsetsWorker.ts
/// <reference lib="webworker" />

// --- Global error / rejection handlers in the worker ---
self.onerror = (evt) => {
    console.error("Worker top-level error:", evt);
};
self.onunhandledrejection = (evt) => {
    console.error("Worker unhandled promise rejection:", evt.reason);
};

// Combine multiple definitions into one
function combineSortDefinitions(definitions: any[]): any {
    const combined = { name: "combined", sortFields: [] as any[] };
    for (const def of definitions) {
        if (!def || !Array.isArray(def.sortFields)) {
            console.warn("A sort definition is missing 'sortFields':", def);
            continue;
        }
        for (const field of def.sortFields) {
            combined.sortFields.push({
                sortColumn: field.sortColumn,
                path: field.path,
                sortDirection: field.sortDirection,
            });
        }
    }
    return combined;
}

/**
 * Build a map of path -> array of indices.
 * If a path is repeated multiple times, each occurrence
 * gets its own slot in the final offsets array.
 */
function buildPathIndexMap(sortDefinition: any) {
    const map: { [path: string]: number[] } = {};
    sortDefinition.sortFields.forEach((field: any, i: number) => {
        if (!map[field.path]) {
            map[field.path] = [];
        }
        map[field.path].push(i);
    });
    return map;
}

function serializeValueWithOffsets(
    value: any,
    currentPath: string,
    pathIndexMap: { [path: string]: number[] },
    offsets: number[],
    nestingLevel: number,
    currentOffset: number
): { json: string; offset: number } {
    if (value === null) {
        const chunk = "null";
        if (pathIndexMap[currentPath]) {
            for (const fieldIndex of pathIndexMap[currentPath]) {
                const idx = fieldIndex * 2;
                offsets[idx] = currentOffset;
                offsets[idx + 1] = currentOffset + chunk.length;
            }
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }

    const valueType = typeof value;
    if (valueType === "boolean") {
        const chunk = value ? "true" : "false";
        if (pathIndexMap[currentPath]) {
            for (const fieldIndex of pathIndexMap[currentPath]) {
                const idx = fieldIndex * 2;
                offsets[idx] = currentOffset;
                offsets[idx + 1] = currentOffset + chunk.length;
            }
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }

    if (valueType === "number") {
        const chunk = Number.isFinite(value) ? String(value) : "null";
        if (pathIndexMap[currentPath]) {
            for (const fieldIndex of pathIndexMap[currentPath]) {
                const idx = fieldIndex * 2;
                offsets[idx] = currentOffset;
                offsets[idx + 1] = currentOffset + chunk.length;
            }
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }

    if (valueType === "string") {
        const chunk = JSON.stringify(value);
        if (pathIndexMap[currentPath]) {
            for (const fieldIndex of pathIndexMap[currentPath]) {
                const idx = fieldIndex * 2;
                // If the chunk is wrapped in quotes, skip them for the offsets
                if (chunk.length >= 2 && chunk.startsWith('"') && chunk.endsWith('"')) {
                    offsets[idx] = currentOffset + 1;
                    offsets[idx + 1] = currentOffset + chunk.length - 1;
                } else {
                    offsets[idx] = currentOffset;
                    offsets[idx + 1] = currentOffset + chunk.length;
                }
            }
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }

    // Handle arrays
    if (Array.isArray(value)) {
        let result = "[";
        let localOffset = currentOffset + 1;
        for (let i = 0; i < value.length; i++) {
            if (i > 0) {
                result += ",";
                localOffset += 1;
            }
            const nextPath = currentPath ? `${currentPath}.${i}` : String(i);
            const { json: childJson, offset: updatedOffset } = serializeValueWithOffsets(
                value[i],
                nextPath,
                pathIndexMap,
                offsets,
                nestingLevel + 1,
                localOffset
            );
            result += childJson;
            localOffset = updatedOffset;
        }
        result += "]";
        localOffset += 1;
        return { json: result, offset: localOffset };
    }

    // Handle objects
    let result = "{";
    let localOffset = currentOffset + 1;
    const keys = Object.keys(value);
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        const propPath = currentPath ? `${currentPath}.${key}` : key;

        if (i > 0) {
            result += ",";
            localOffset += 1;
        }
        const keyJson = JSON.stringify(key);
        result += keyJson;
        localOffset += keyJson.length;

        result += ":";
        localOffset += 1;

        const { json: childJson, offset: updatedOffset } = serializeValueWithOffsets(
            value[key],
            propPath,
            pathIndexMap,
            offsets,
            nestingLevel + 1,
            localOffset
        );
        result += childJson;
        localOffset = updatedOffset;
    }
    result += "}";
    localOffset += 1;
    return { json: result, offset: localOffset };
}

function serializeObjectWithOffsets(obj: any, sortDefinition: any) {
    // Build the path map so we know which indices each path corresponds to
    const pathIndexMap = buildPathIndexMap(sortDefinition);
    // 2 offsets per field:
    const offsets = new Array(sortDefinition.sortFields.length * 2).fill(0);

    const { json: jsonString } = serializeValueWithOffsets(obj, "", pathIndexMap, offsets, 0, 0);

    return {
        jsonString,
        offsets: new Uint32Array(offsets),
    };
}

function computeOffsetsForSingleDefinition(dataArray: any[], sortDefinition: any) {
    const startTime = performance.now ? performance.now() : Date.now();

    const results = dataArray.map((obj) => {
        const { offsets } = serializeObjectWithOffsets(obj, sortDefinition);
        // Wrap in array for the flattening step
        return [offsets];
    });

    const endTime = performance.now ? performance.now() : Date.now();
    const elapsedTime = endTime - startTime;

    //console.log("\n=== Webworker Performance Metrics ===");
    //console.log(`Number of objects processed: ${dataArray.length}`);
    //console.log(`Sort Definition: `, sortDefinition);
    //console.log(`Time taken: ${elapsedTime.toFixed(3)} ms`);

    return results;
}

/** Flatten all fields from multiple definitions into one big Uint32Array. */
function getJsonFieldOffsetsFlattened(dataArray: any[], sortDefinitionOrDefinitions: any): Uint32Array {
    // Normalize to array
    const definitions = Array.isArray(sortDefinitionOrDefinitions)
        ? sortDefinitionOrDefinitions
        : [sortDefinitionOrDefinitions];

    // Combine them
    const combinedDefinition = combineSortDefinitions(definitions);

    // Single pass
    const rowResults = computeOffsetsForSingleDefinition(dataArray, combinedDefinition);

    // Flatten
    const totalFields = combinedDefinition.sortFields.length;
    const n = dataArray.length;
    const finalOffsets = new Uint32Array(n * totalFields * 2);

    for (let i = 0; i < n; i++) {
        // rowResults[i] is something like [ Uint32Array(...) ]
        const offsetsForRow = rowResults[i][0];
        finalOffsets.set(offsetsForRow, i * (2 * totalFields));
    }
    return finalOffsets;
}

// === Worker Listener ===
self.onmessage = (e: MessageEvent) => {
    try {
        if (!e.data) return;

        if (e.data.cmd === "getJsonFieldOffsets") {
            const { arrayBuffers, sortDefinition } = e.data;

            // Convert each ArrayBuffer into a JSON object
            const dataArray = arrayBuffers.map((ab: ArrayBuffer) => {
                const text = new TextDecoder().decode(new Uint8Array(ab));
                return JSON.parse(text);
            });

            // Compute one flat Uint32Array
            const flattenedOffsets = getJsonFieldOffsetsFlattened(dataArray, sortDefinition);

            // Send result (transfer the underlying buffer to avoid copying large data)
            self.postMessage(
                { cmd: "getJsonFieldOffsets_result", result: flattenedOffsets },
                [flattenedOffsets.buffer]
            );
        }
    } catch (err) {
        // Catch any runtime errors that occur in the message handler
        console.error("Worker error in onmessage:", err);
    }
};
