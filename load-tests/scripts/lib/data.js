// Pure helpers shared by k6 runtime and dependency-free Node unit tests.
export function assertRunId(runId) {
  if (!/^[a-f0-9]{8}$/i.test(runId || '') || /^0{8}$/.test(runId)) {
    throw new Error('RUN_ID must be 8 hexadecimal characters, not 00000000');
  }
  return runId.toLowerCase();
}

// The first eight UUID hex characters identify one test run without changing the API contract.
export function customerIdForRun(runId, random = Math.random) {
  const prefix = assertRunId(runId);
  const hex = (count) => Array.from({length: count}, () => Math.floor(random() * 16).toString(16)).join('');
  const variant = (8 + Math.floor(random() * 4)).toString(16);
  return `${prefix}-${hex(4)}-4${hex(3)}-${variant}${hex(3)}-${hex(12)}`;
}

// Seed only the numeric test-data distribution. UUIDs retain high-entropy randomness.
export function seededFieldsRandom(seed, vu, iteration) {
  const text = `${seed}:${vu}:${iteration}`;
  let state = 2166136261;
  for (let i=0; i<text.length; i++) {
    state = Math.imul(state ^ text.charCodeAt(i), 16777619) >>> 0;
  }
  return () => {
    state = (state + 0x6D2B79F5) >>> 0;
    let t = state;
    t = Math.imul(t ^ (t >>> 15), t | 1);
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
  };
}

export function applicationPayload(runId, random = Math.random, numericRandom = random) {
  return {
    customerId: customerIdForRun(runId, random),
    amount: Number((5000 + Math.floor(numericRandom() * 45000)).toFixed(2)),
    term: [6, 12, 24, 36, 48][Math.floor(numericRandom() * 5)],
    currency: 'USD'
  };
}
