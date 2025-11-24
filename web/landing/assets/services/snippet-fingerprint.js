export async function createFingerprint(code, files = []) {
    const normalizedCode = code.replace(/[\s\t\n\r]/g, '');

    const encoder = new TextEncoder();
    const parts = [encoder.encode(normalizedCode)];

    for (const file of files) {
        const fileContent = await file.arrayBuffer();
        parts.push(new Uint8Array(fileContent));
    }

    const totalLength = parts.reduce((sum, part) => sum + part.length, 0);
    const combined = new Uint8Array(totalLength);

    let offset = 0;
    for (const part of parts) {
        combined.set(part, offset);
        offset += part.length;
    }

    const hashBuffer = await crypto.subtle.digest('SHA-256', combined);
    const hashArray = Array.from(new Uint8Array(hashBuffer));
    return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}
