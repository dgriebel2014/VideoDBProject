"use strict";
/// <reference lib="webworker" />
// --- Global error / rejection handlers in the worker ---
self.onerror = (evt) => {
    console.error("Worker top-level error:", evt);
};
self.onunhandledrejection = (evt) => {
    console.error("Worker unhandled promise rejection:", evt.reason);
};
// Combine multiple definitions into one
function combineSortDefinitions(definitions) {
    const combined = { name: "combined", sortFields: [] };
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
 * Build a map of path -> array of indices in the final offset array.
 * If a path is repeated multiple times, each occurrence gets its own slot.
 * E.g. if path "title" appears as field #0 and #5 in sortFields, pathIndexMap["title"] = [0, 5].
 */
function buildPathIndexMap(sortDefinition) {
    const map = {};
    sortDefinition.sortFields.forEach((field, i) => {
        if (!map[field.path]) {
            map[field.path] = [];
        }
        map[field.path].push(i);
    });
    return map;
}
/**
 * Return the length of JSON.stringify(value) *without* building the string,
 * plus set the offset range in `offsets` if the current path is relevant.
 *
 * In JSON:
 * - `null` has length 4
 * - `true` length 4
 * - `false` length 5
 * - numbers => length = String(value).length (for finite) or 4 if infinite (i.e. "null")
 * - strings => length = 2 + length of the escaped string (we can do `JSON.stringify(value).length`)
 * - arrays/objects => accounted for with braces/brackets, keys, colons, commas, etc.
 */
function measureValueWithOffsets(value, currentPath, pathIndexMap, offsets, offsetBaseIndex, // The base index in 'offsets' for this row. 2*N fields total.
currentOffset) {
    // If this path is relevant, we need to record [startOffset, endOffset].
    // We'll fill them below if we have the mapping.
    const relevantFieldIndices = pathIndexMap[currentPath];
    // Handle null
    if (value === null) {
        // "null" => length 4
        if (relevantFieldIndices) {
            for (const fieldIndex of relevantFieldIndices) {
                const outIndex = offsetBaseIndex + fieldIndex * 2;
                offsets[outIndex] = currentOffset;
                offsets[outIndex + 1] = currentOffset + 4;
            }
        }
        return currentOffset + 4;
    }
    const valueType = typeof value;
    // Boolean
    if (valueType === "boolean") {
        // "true" => length 4, "false" => length 5
        const boolLen = value ? 4 : 5;
        if (relevantFieldIndices) {
            for (const fieldIndex of relevantFieldIndices) {
                const outIndex = offsetBaseIndex + fieldIndex * 2;
                offsets[outIndex] = currentOffset;
                offsets[outIndex + 1] = currentOffset + boolLen;
            }
        }
        return currentOffset + boolLen;
    }
    // Number
    if (valueType === "number") {
        // finite => length = stringified length, else "null" => 4
        if (!Number.isFinite(value)) {
            // "null"
            if (relevantFieldIndices) {
                for (const fieldIndex of relevantFieldIndices) {
                    const outIndex = offsetBaseIndex + fieldIndex * 2;
                    offsets[outIndex] = currentOffset;
                    offsets[outIndex + 1] = currentOffset + 4;
                }
            }
            return currentOffset + 4;
        }
        else {
            // e.g. 1234 => length 4
            // Use fast method: string conversion
            const strVal = String(value);
            const len = strVal.length;
            if (relevantFieldIndices) {
                for (const fieldIndex of relevantFieldIndices) {
                    const outIndex = offsetBaseIndex + fieldIndex * 2;
                    offsets[outIndex] = currentOffset;
                    offsets[outIndex + 1] = currentOffset + len;
                }
            }
            return currentOffset + len;
        }
    }
    // String
    if (valueType === "string") {
        // For correct JSON escaping length, you generally want JSON.stringify(...)
        // Then measure how long the result is. That includes quotes and any escaped chars.
        // Example: "hello" => 7 chars => "hello" (with quotes)
        // Example: "he\"llo" => 8 chars => "he\"llo"
        // You can do: 
        const strJson = JSON.stringify(value);
        const len = strJson.length;
        if (relevantFieldIndices) {
            // The user wants the substring *without* surrounding quotes in the offset:
            // If chunk = "some text", offset range is [currentOffset+1, currentOffset+len-1].
            for (const fieldIndex of relevantFieldIndices) {
                const outIndex = offsetBaseIndex + fieldIndex * 2;
                offsets[outIndex] = currentOffset + 1; // skip leading quote
                offsets[outIndex + 1] = currentOffset + len - 1; // skip trailing quote
            }
        }
        return currentOffset + len;
    }
    // Array
    if (Array.isArray(value)) {
        // We add 1 char for '[', 1 char for ']', plus commas.
        // Then measure children recursively.
        let localOffset = currentOffset;
        localOffset += 1; // '['
        for (let i = 0; i < value.length; i++) {
            if (i > 0) {
                localOffset += 1; // comma
            }
            const nextPath = currentPath ? `${currentPath}.${i}` : String(i);
            localOffset = measureValueWithOffsets(value[i], nextPath, pathIndexMap, offsets, offsetBaseIndex, localOffset);
        }
        localOffset += 1; // ']'
        return localOffset;
    }
    // Object
    let localOffset = currentOffset;
    localOffset += 1; // '{'
    const keys = Object.keys(value);
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        const propPath = currentPath ? `${currentPath}.${key}` : key;
        if (i > 0) {
            localOffset += 1; // comma
        }
        // Key in quotes:
        // length of JSON.stringify(key) => includes quotes
        const keyJson = JSON.stringify(key);
        const keyLen = keyJson.length;
        localOffset += keyLen;
        // colon
        localOffset += 1;
        // measure the value
        localOffset = measureValueWithOffsets(value[key], propPath, pathIndexMap, offsets, offsetBaseIndex, localOffset);
    }
    localOffset += 1; // '}'
    return localOffset;
}
/**
 * For a single combined definition, compute all offsets for each object in dataArray.
 * We store the results directly into a preallocated Uint32Array to skip intermediate steps.
 *
 * @param dataArray Array of parsed objects
 * @param sortDefinition The combined sort definition
 * @param finalOffsets  A preallocated Uint32Array of length = dataArray.length * (2 * numberOfFields)
 */
function computeOffsetsInPlace(dataArray, sortDefinition, finalOffsets) {
    const startTime = performance.now ? performance.now() : Date.now();
    const pathIndexMap = buildPathIndexMap(sortDefinition);
    const fieldsCount = sortDefinition.sortFields.length;
    for (let i = 0; i < dataArray.length; i++) {
        const obj = dataArray[i];
        // The base index in finalOffsets for the i-th row:
        // each row has (fieldsCount * 2) entries
        const offsetBaseIndex = i * (fieldsCount * 2);
        // measure the entire object as if we were JSON-stringifying
        measureValueWithOffsets(obj, "", pathIndexMap, finalOffsets, offsetBaseIndex, 0);
    }
    const endTime = performance.now ? performance.now() : Date.now();
    const elapsedTime = endTime - startTime;
    console.log("\n=== Webworker Performance Metrics ===");
    console.log(`Number of objects processed: ${dataArray.length}`);
    console.log(`Sort Definition: `, sortDefinition);
    console.log(`Time taken: ${elapsedTime.toFixed(3)} ms`);
}
function getJsonFieldOffsetsFlattened(dataArray, sortDefinitionOrDefinitions) {
    // Normalize to array
    const definitions = Array.isArray(sortDefinitionOrDefinitions)
        ? sortDefinitionOrDefinitions
        : [sortDefinitionOrDefinitions];
    // Combine them
    const combinedDefinition = combineSortDefinitions(definitions);
    const totalFields = combinedDefinition.sortFields.length;
    // Allocate one large buffer for offsets:
    // dataArray.length * totalFields * 2  (2 offsets per field)
    const finalOffsets = new Uint32Array(dataArray.length * totalFields * 2);
    // Populate it in a single pass:
    computeOffsetsInPlace(dataArray, combinedDefinition, finalOffsets);
    return finalOffsets;
}
// === Worker Listener ===
self.onmessage = (e) => {
    try {
        if (!e.data)
            return;
        if (e.data.cmd === "getJsonFieldOffsets") {
            const { arrayBuffers, sortDefinition } = e.data;
            // Convert each ArrayBuffer into a JSON object
            const dataArray = arrayBuffers.map((ab) => {
                const text = new TextDecoder().decode(new Uint8Array(ab));
                return JSON.parse(text);
            });
            // Compute one flat Uint32Array
            const flattenedOffsets = getJsonFieldOffsetsFlattened(dataArray, sortDefinition);
            // Send result (transfer the underlying buffer)
            self.postMessage({ cmd: "getJsonFieldOffsets_result", result: flattenedOffsets }, [flattenedOffsets.buffer]);
        }
    }
    catch (err) {
        console.error("Worker error in onmessage:", err);
    }
};
//# sourceMappingURL=offsetsWorker.js.map