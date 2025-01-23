/// <reference lib="webworker" />

/**
 * Global top-level error event handler for the worker.
 * Logs any uncaught errors to the console.
 *
 * @param {ErrorEvent} evt The error event object.
 */
self.onerror = (evt) => {
    console.error("Worker top-level error:", evt);
};

/**
 * Global top-level unhandled promise rejection event handler for the worker.
 * Logs any promise rejections to the console.
 *
 * @param {PromiseRejectionEvent} evt The unhandled rejection event object.
 */
self.onunhandledrejection = (evt) => {
    console.error("Worker unhandled promise rejection:", evt.reason);
};

interface SortField {
    sortColumn: string;   // e.g. "foo"
    path: string;         // e.g. "bar[2].baz"
    sortDirection: string;// e.g. "asc" or "desc"
}

interface SortDefinition {
    name: string;
    sortFields: SortField[];
}

/**
 * Combine multiple sort definitions into one.
 * Each definition has an array of sortFields; this merges them all into a single list.
 *
 * @param {SortDefinition[]} definitions Array of individual sort definitions.
 * @returns {SortDefinition} A new SortDefinition with all sortFields combined.
 */
function combineSortDefinitions(definitions: SortDefinition[]): SortDefinition {
    const combined: SortDefinition = { name: "combined", sortFields: [] };
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
 * Build a map of paths to array of field indices in the final offset array.
 * If a path appears multiple times in sortFields, it accumulates multiple indices.
 * Example: If "title" is in sortFields[0] and sortFields[5], then pathIndexMap["title"] = [0, 5].
 *
 * @param {SortDefinition} sortDefinition The sort definition to process.
 * @returns {Record<string, number[]>} An object mapping each path to an array of offset indices.
 */
function buildPathIndexMap(sortDefinition: SortDefinition) {
    const map: Record<string, number[]> = {};
    sortDefinition.sortFields.forEach((field, i) => {
        if (!map[field.path]) {
            map[field.path] = [];
        }
        map[field.path].push(i);
    });
    return map;
}

/**
 * Return the new offset after measuring how many characters a value would occupy in JSON,
 * without actually building the string. Also sets offset ranges in `offsets` if `currentPath`
 * matches any field in `pathIndexMap`.
 *
 * In JSON:
 * - `null` => length 4
 * - `true` => length 4
 * - `false` => length 5
 * - finite number => length of String(value)
 * - non-finite number => "null" (length 4)
 * - string => 2 quotes + length of escaped string
 * - arrays/objects => account for brackets/braces, commas, colons, etc.
 *
 * @param {*} value The current value to measure.
 * @param {string} currentPath The dot-delimited path for this value (e.g. "foo.bar.0").
 * @param {Record<string, number[]>} pathIndexMap Mapping of paths to field indices.
 * @param {Uint32Array} offsets Preallocated array for offsets (2 indices per field).
 * @param {number} offsetBaseIndex The base index in `offsets` for this particular row/object.
 * @param {number} currentOffset The current position in the hypothetical JSON string.
 * @returns {number} The next offset position after measuring this value.
 */
function measureValueWithOffsets(
    value: any,
    currentPath: string,
    pathIndexMap: Record<string, number[]>,
    offsets: Uint32Array,
    offsetBaseIndex: number,
    currentOffset: number
): number {
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
        // "true" => 4, "false" => 5
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
        if (!Number.isFinite(value)) {
            // "null" => length 4
            if (relevantFieldIndices) {
                for (const fieldIndex of relevantFieldIndices) {
                    const outIndex = offsetBaseIndex + fieldIndex * 2;
                    offsets[outIndex] = currentOffset;
                    offsets[outIndex + 1] = currentOffset + 4;
                }
            }
            return currentOffset + 4;
        } else {
            // e.g. 1234 => length 4 (stringified)
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
        // Need correct JSON escaping => measure length of JSON.stringify(value)
        const strJson = JSON.stringify(value);
        const len = strJson.length;
        if (relevantFieldIndices) {
            // Exclude surrounding quotes from the offset range:
            for (const fieldIndex of relevantFieldIndices) {
                const outIndex = offsetBaseIndex + fieldIndex * 2;
                offsets[outIndex] = currentOffset + 1;          // skip leading quote
                offsets[outIndex + 1] = currentOffset + len - 1; // skip trailing quote
            }
        }
        return currentOffset + len;
    }

    // Array
    if (Array.isArray(value)) {
        // '[' + ']' + commas + children
        let localOffset = currentOffset + 1; // account for '['
        for (let i = 0; i < value.length; i++) {
            if (i > 0) {
                localOffset += 1; // comma
            }
            const nextPath = currentPath ? `${currentPath}.${i}` : String(i);
            localOffset = measureValueWithOffsets(
                value[i],
                nextPath,
                pathIndexMap,
                offsets,
                offsetBaseIndex,
                localOffset
            );
        }
        return localOffset + 1; // account for ']'
    }

    // Object
    let localOffset = currentOffset + 1; // account for '{'
    const keys = Object.keys(value);
    for (let i = 0; i < keys.length; i++) {
        const key = keys[i];
        const propPath = currentPath ? `${currentPath}.${key}` : key;

        if (i > 0) {
            localOffset += 1; // comma
        }

        // Key in quotes
        const keyJson = JSON.stringify(key);
        const keyLen = keyJson.length;
        localOffset += keyLen; // length of the serialized key

        // colon
        localOffset += 1;

        // measure the child value
        localOffset = measureValueWithOffsets(
            value[key],
            propPath,
            pathIndexMap,
            offsets,
            offsetBaseIndex,
            localOffset
        );
    }
    return localOffset + 1; // account for '}'
}

/**
 * For a single combined definition, compute all offsets for each object in `dataArray`.
 * Stores results directly into a preallocated `Uint32Array` to avoid intermediate allocations.
 *
 * @param {any[]} dataArray The array of parsed JSON objects.
 * @param {SortDefinition} sortDefinition The combined sort definition to apply.
 * @param {Uint32Array} finalOffsets The preallocated offsets array to fill (2 offsets per field).
 */
function computeOffsetsInPlace(
    dataArray: any[],
    sortDefinition: SortDefinition,
    finalOffsets: Uint32Array
) {
    const startTime = performance.now ? performance.now() : Date.now();

    const pathIndexMap = buildPathIndexMap(sortDefinition);
    const fieldsCount = sortDefinition.sortFields.length;

    for (let i = 0; i < dataArray.length; i++) {
        const obj = dataArray[i];
        // Each row consumes (fieldsCount * 2) entries in finalOffsets
        const offsetBaseIndex = i * (fieldsCount * 2);
        measureValueWithOffsets(obj, "", pathIndexMap, finalOffsets, offsetBaseIndex, 0);
    }

    const endTime = performance.now ? performance.now() : Date.now();
    const elapsedTime = endTime - startTime;

    console.log("\n=== Webworker Performance Metrics ===");
    console.log(`Number of objects processed: ${dataArray.length}`);
    console.log(`Sort Definition: `, sortDefinition);
    console.log(`Time taken: ${elapsedTime.toFixed(3)} ms`);
}

/**
 * Computes a single flat `Uint32Array` of offsets for every object in `dataArray`,
 * across all fields in one or more sort definitions.
 *
 * @param {any[]} dataArray The array of parsed JSON objects to process.
 * @param {SortDefinition | SortDefinition[]} sortDefinitionOrDefinitions One or multiple definitions.
 * @returns {Uint32Array} A flat array of offsets (2 per field) for all rows.
 */
function getJsonFieldOffsetsFlattened(
    dataArray: any[],
    sortDefinitionOrDefinitions: SortDefinition | SortDefinition[]
): Uint32Array {
    // Normalize to an array of definitions
    const definitions = Array.isArray(sortDefinitionOrDefinitions)
        ? sortDefinitionOrDefinitions
        : [sortDefinitionOrDefinitions];

    // Combine them
    const combinedDefinition = combineSortDefinitions(definitions);
    const totalFields = combinedDefinition.sortFields.length;

    // Allocate one large buffer for offsets (2 offsets per field, per row)
    const finalOffsets = new Uint32Array(dataArray.length * totalFields * 2);

    // Populate offsets in a single pass
    computeOffsetsInPlace(dataArray, combinedDefinition, finalOffsets);

    return finalOffsets;
}

/**
 * Main worker message handler.
 * When receiving a "getJsonFieldOffsets" message, it decodes each ArrayBuffer as JSON,
 * computes the offsets via `getJsonFieldOffsetsFlattened`, and posts the result back.
 *
 * @param {MessageEvent} e The incoming message event.
 */
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

            // Compute one flat Uint32Array of offsets
            const flattenedOffsets = getJsonFieldOffsetsFlattened(dataArray, sortDefinition);

            // Transfer the underlying buffer back to main thread
            self.postMessage(
                { cmd: "getJsonFieldOffsets_result", result: flattenedOffsets },
                [flattenedOffsets.buffer]
            );
        }
    } catch (err) {
        console.error("Worker error in onmessage:", err);
    }
};
