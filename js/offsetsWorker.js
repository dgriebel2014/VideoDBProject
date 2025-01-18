"use strict";
// offsetsWorker.ts
// Define all the helper functions
function buildPathIndexMap(sortDefinition) {
    const map = {};
    sortDefinition.sortFields.forEach((field, i) => {
        map[field.path] = i;
    });
    return map;
}
function serializeValueWithOffsets(value, currentPath, pathIndexMap, offsets, nestingLevel, currentOffset) {
    if (value === null) {
        const chunk = "null";
        if (currentPath in pathIndexMap) {
            const idx = pathIndexMap[currentPath] * 2;
            offsets[idx] = currentOffset;
            offsets[idx + 1] = currentOffset + chunk.length;
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }
    const valueType = typeof value;
    if (valueType === "boolean") {
        const chunk = value ? "true" : "false";
        if (currentPath in pathIndexMap) {
            const idx = pathIndexMap[currentPath] * 2;
            offsets[idx] = currentOffset;
            offsets[idx + 1] = currentOffset + chunk.length;
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }
    if (valueType === "number") {
        const chunk = Number.isFinite(value) ? String(value) : "null";
        if (currentPath in pathIndexMap) {
            const idx = pathIndexMap[currentPath] * 2;
            offsets[idx] = currentOffset;
            offsets[idx + 1] = currentOffset + chunk.length;
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }
    if (valueType === "string") {
        const chunk = JSON.stringify(value);
        if (currentPath in pathIndexMap) {
            const idx = pathIndexMap[currentPath] * 2;
            if (chunk.length >= 2 && chunk.startsWith('"') && chunk.endsWith('"')) {
                offsets[idx] = currentOffset + 1;
                offsets[idx + 1] = currentOffset + chunk.length - 1;
            }
            else {
                offsets[idx] = currentOffset;
                offsets[idx + 1] = currentOffset + chunk.length;
            }
        }
        return { json: chunk, offset: currentOffset + chunk.length };
    }
    if (Array.isArray(value)) {
        let result = "[";
        let localOffset = currentOffset + 1;
        for (let i = 0; i < value.length; i++) {
            if (i > 0) {
                result += ",";
                localOffset += 1;
            }
            const nextPath = currentPath ? `${currentPath}.${i}` : String(i);
            const { json: childJson, offset: updatedOffset } = serializeValueWithOffsets(value[i], nextPath, pathIndexMap, offsets, nestingLevel + 1, localOffset);
            result += childJson;
            localOffset = updatedOffset;
        }
        result += "]";
        localOffset += 1;
        return { json: result, offset: localOffset };
    }
    // It's an object
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
        const { json: childJson, offset: updatedOffset } = serializeValueWithOffsets(value[key], propPath, pathIndexMap, offsets, nestingLevel + 1, localOffset);
        result += childJson;
        localOffset = updatedOffset;
    }
    result += "}";
    localOffset += 1;
    return { json: result, offset: localOffset };
}
function serializeObjectWithOffsets(obj, sortDefinition) {
    const pathIndexMap = buildPathIndexMap(sortDefinition);
    const offsets = new Array(sortDefinition.sortFields.length * 2).fill(0);
    const { json: jsonString } = serializeValueWithOffsets(obj, "", pathIndexMap, offsets, 0, 0);
    return {
        jsonString,
        offsets: new Uint32Array(offsets),
    };
}
function getJsonFieldOffsets(dataArray, sortDefinition) {
    const startTime = performance.now ? performance.now() : Date.now();
    const results = dataArray.map((obj) => {
        const { jsonString, offsets } = serializeObjectWithOffsets(obj, sortDefinition);
        // Build array of substrings for each tracked field
        const substrings = [];
        for (let i = 0; i < offsets.length; i += 2) {
            const start = offsets[i];
            const end = offsets[i + 1];
            substrings.push(jsonString.substring(start, end));
        }
        return [jsonString, offsets, substrings];
    });
    const endTime = performance.now ? performance.now() : Date.now();
    const elapsedTime = endTime - startTime;
    console.log("\n=== Performance Metrics ===");
    console.log(`Number of objects processed: ${dataArray.length}`);
    console.log(`Time taken: ${elapsedTime.toFixed(3)} ms`);
    return results;
}
// Worker "onmessage" listener
self.onmessage = (e) => {
    if (!e.data)
        return;
    if (e.data.cmd === "getJsonFieldOffsets") {
        const { data, sortDefinition } = e.data;
        const result = getJsonFieldOffsets(data, sortDefinition);
        // Send the response back to the main thread
        self.postMessage({
            cmd: "getJsonFieldOffsets_result",
            result,
        });
    }
};
//# sourceMappingURL=offsetsWorker.js.map