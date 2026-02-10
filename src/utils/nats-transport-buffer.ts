export type BufferedEntry = Readonly<{
  readonly subject: string;
  readonly payload: Uint8Array;
  readonly size: number;
}>;

export type RingBufferState = Readonly<{
  readonly entries: readonly BufferedEntry[];
  readonly totalBytes: number;
  readonly maxBytes: number;
}>;

export type BufferUpdate = Readonly<{
  readonly state: RingBufferState;
  readonly dropped: number;
}>;

export const createRingBuffer = (maxBytes: number): RingBufferState =>
  Object.freeze({
    entries: Object.freeze([]) as readonly BufferedEntry[],
    totalBytes: 0,
    maxBytes,
  });

const sumEntryBytes = (entries: readonly BufferedEntry[]): number => {
  let total = 0;
  for (const entry of entries) {
    total += entry.size;
  }
  return total;
};

export const appendEntry = (state: RingBufferState, entry: BufferedEntry): BufferUpdate => {
  if (entry.size > state.maxBytes) {
    return { state, dropped: 1 };
  }

  const combined = [...state.entries, entry];
  let totalBytes = state.totalBytes + entry.size;
  let dropCount = 0;
  let startIndex = 0;

  while (totalBytes > state.maxBytes && startIndex < combined.length) {
    totalBytes -= combined[startIndex].size;
    startIndex += 1;
    dropCount += 1;
  }

  const entries = startIndex > 0 ? combined.slice(startIndex) : combined;
  const nextState: RingBufferState = Object.freeze({
    entries: Object.freeze(entries) as readonly BufferedEntry[],
    totalBytes,
    maxBytes: state.maxBytes,
  });

  return { state: nextState, dropped: dropCount };
};

export const takeBatch = (
  state: RingBufferState,
  size: number,
): { batch: readonly BufferedEntry[]; state: RingBufferState } => {
  if (state.entries.length === 0 || size <= 0) {
    return { batch: Object.freeze([]) as readonly BufferedEntry[], state };
  }

  const batch = state.entries.slice(0, size);
  const remaining = state.entries.slice(batch.length);
  const remainingBytes = state.totalBytes - sumEntryBytes(batch);
  const nextState: RingBufferState = Object.freeze({
    entries: Object.freeze(remaining) as readonly BufferedEntry[],
    totalBytes: remainingBytes,
    maxBytes: state.maxBytes,
  });

  return { batch: Object.freeze(batch) as readonly BufferedEntry[], state: nextState };
};

export const prependBatch = (
  state: RingBufferState,
  batch: readonly BufferedEntry[],
): RingBufferState => {
  if (batch.length === 0) {
    return state;
  }

  const entries = [...batch, ...state.entries];
  const totalBytes = state.totalBytes + sumEntryBytes(batch);
  return Object.freeze({
    entries: Object.freeze(entries) as readonly BufferedEntry[],
    totalBytes,
    maxBytes: state.maxBytes,
  });
};

export const createEntry = (subject: string, payload: Uint8Array): BufferedEntry =>
  Object.freeze({ subject, payload, size: payload.byteLength });
