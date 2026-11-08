'use strict';

// Streams the packed int16 neutron dump OFF the main thread: reads the response
// body chunk by chunk, decodes each int16 triple (x,y,z, 6 bytes/point) to
// Float32 light-years, and posts fixed-size batches back with the buffer
// TRANSFERRED (zero-copy). The page appends batches to the point cloud as they
// arrive, so the galaxy renders progressively while it downloads -- the trick
// spansh's map uses. Our fixed-width binary is simpler to stream than their
// NDJSON (no line parsing, just whole 6-byte records).

self.onmessage = async (e) => {
  const { url, scale, batchPoints } = e.data || {};
  const inv = 1 / (scale || 1);
  const BATCH_FLOATS = (batchPoints || 100000) * 3;

  try {
    const res = await fetch(url, { cache: 'force-cache' });
    if (!res.ok || !res.body) throw new Error('fetch failed (' + res.status + ')');

    let stream = res.body;
    // Transparently gunzip a .gz dump (streaming, native, no pako). nginx may
    // also Content-Encoding-gzip the plain .bin, which fetch decodes for us.
    if (/\.gz($|\?)/.test(url) && typeof DecompressionStream !== 'undefined') {
      stream = stream.pipeThrough(new DecompressionStream('gzip'));
    }

    const reader = stream.getReader();
    let leftover = null;                  // bytes carried across a chunk boundary
    let batch = new Float32Array(BATCH_FLOATS);
    let bi = 0;                           // next float slot in `batch`
    let totalPoints = 0;
    let bytesDown = 0;
    let lastProgress = 0;

    const flush = () => {
      if (bi === 0) return;
      const out = new Float32Array(batch.subarray(0, bi));  // exact-size, transferable
      self.postMessage({ type: 'batch', positions: out, points: bi / 3 }, [out.buffer]);
      bi = 0;
    };

    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      bytesDown += value.byteLength;

      // Merge any carried partial point, then process only whole 6-byte points.
      let bytes = value;
      if (leftover) {
        const merged = new Uint8Array(leftover.byteLength + value.byteLength);
        merged.set(leftover, 0);
        merged.set(value, leftover.byteLength);
        bytes = merged;
        leftover = null;
      }
      const wholeBytes = (bytes.byteLength - (bytes.byteLength % 6));
      if (wholeBytes > 0) {
        // Copy to a fresh (2-byte aligned) buffer so the Int16Array view is valid
        // regardless of the chunk's byteOffset.
        const aligned = new Int16Array(new Uint8Array(bytes.subarray(0, wholeBytes)).buffer);
        for (let k = 0; k < aligned.length; k++) {
          batch[bi++] = aligned[k] * inv;
          if (bi >= BATCH_FLOATS) flush();
        }
        totalPoints += aligned.length / 3;
      }
      if (wholeBytes < bytes.byteLength) {
        leftover = new Uint8Array(bytes.subarray(wholeBytes));  // partial point
      }

      // Throttle progress posts to ~every 200 ms.
      const now = (self.performance && self.performance.now) ? self.performance.now() : Date.now();
      if (now - lastProgress >= 200) {
        self.postMessage({ type: 'progress', bytes: bytesDown, points: totalPoints });
        lastProgress = now;
      }
    }

    flush();
    self.postMessage({ type: 'done', points: totalPoints, bytes: bytesDown });
  } catch (err) {
    self.postMessage({ type: 'error', message: String((err && err.message) || err) });
  }
};
