type Labels = Record<string, string>;

type CounterStore = Map<string, number>;

type HistogramState = {
  count: number;
  sum: number;
  bucketCounts: number[];
};

type HistogramStore = Map<string, HistogramState>;

type DbQueryMetricInput = {
  collection: string;
  operation: string;
  status: 'ok' | 'error';
  durationMs: number;
};

type DbTransactionMetricInput = {
  status: 'success' | 'failed';
  durationMs: number;
};

const QUERY_DURATION_BUCKETS_MS = [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000];
const TRANSACTION_DURATION_BUCKETS_MS = [5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

const dbQueryCounter: CounterStore = new Map();
const dbQueryDurationHistogram: HistogramStore = new Map();

const dbTransactionCounter: CounterStore = new Map();
const dbTransactionDurationHistogram: HistogramStore = new Map();

const normalizeDurationMs = (durationMs: number): number =>
  Number.isFinite(durationMs) && durationMs >= 0 ? durationMs : 0;

const serializeLabelKey = (labels: Labels): string =>
  JSON.stringify(
    Object.entries(labels).sort(([left], [right]) => left.localeCompare(right)),
  );

const deserializeLabelKey = (key: string): Labels =>
  Object.fromEntries(JSON.parse(key) as Array<[string, string]>) as Labels;

const incrementCounter = (
  store: CounterStore,
  labels: Labels,
  value = 1,
): void => {
  const key = serializeLabelKey(labels);
  const current = store.get(key) ?? 0;
  store.set(key, current + value);
};

const getOrCreateHistogramState = (
  store: HistogramStore,
  labels: Labels,
  bucketCount: number,
): HistogramState => {
  const key = serializeLabelKey(labels);
  const existing = store.get(key);
  if (existing) {
    return existing;
  }
  const created: HistogramState = {
    count: 0,
    sum: 0,
    bucketCounts: Array.from({ length: bucketCount }, () => 0),
  };
  store.set(key, created);
  return created;
};

const observeHistogram = (
  store: HistogramStore,
  labels: Labels,
  buckets: readonly number[],
  value: number,
): void => {
  const state = getOrCreateHistogramState(store, labels, buckets.length);
  state.count += 1;
  state.sum += value;

  for (let index = 0; index < buckets.length; index += 1) {
    if (value <= buckets[index]) {
      state.bucketCounts[index] += 1;
    }
  }
};

const formatLabels = (labels: Labels): string => {
  const entries = Object.entries(labels);
  if (entries.length === 0) {
    return '';
  }
  const serialized = entries
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([key, value]) => `${key}="${value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`)
    .join(',');
  return `{${serialized}}`;
};

const formatMetricLine = (
  name: string,
  labels: Labels,
  value: number,
): string => `${name}${formatLabels(labels)} ${value}`;

export const recordDbQueryMetric = (input: DbQueryMetricInput): void => {
  const labels = {
    collection: input.collection,
    operation: input.operation,
    status: input.status,
  };
  const durationMs = normalizeDurationMs(input.durationMs);
  incrementCounter(dbQueryCounter, labels);
  observeHistogram(dbQueryDurationHistogram, labels, QUERY_DURATION_BUCKETS_MS, durationMs);
};

export const recordDbTransactionMetric = (
  input: DbTransactionMetricInput,
): void => {
  const labels = {
    status: input.status,
  };
  const durationMs = normalizeDurationMs(input.durationMs);
  incrementCounter(dbTransactionCounter, labels);
  observeHistogram(
    dbTransactionDurationHistogram,
    labels,
    TRANSACTION_DURATION_BUCKETS_MS,
    durationMs,
  );
};

const renderCounterMetric = (
  name: string,
  help: string,
  store: CounterStore,
): string[] => {
  const lines = [
    `# HELP ${name} ${help}`,
    `# TYPE ${name} counter`,
  ];

  for (const [key, value] of store.entries()) {
    lines.push(formatMetricLine(name, deserializeLabelKey(key), value));
  }

  return lines;
};

const renderHistogramMetric = (
  name: string,
  help: string,
  buckets: readonly number[],
  store: HistogramStore,
): string[] => {
  const lines = [
    `# HELP ${name} ${help}`,
    `# TYPE ${name} histogram`,
  ];

  for (const [key, state] of store.entries()) {
    const labels = deserializeLabelKey(key);
    for (let index = 0; index < buckets.length; index += 1) {
      lines.push(
        formatMetricLine(
          `${name}_bucket`,
          {
            ...labels,
            le: `${buckets[index]}`,
          },
          state.bucketCounts[index],
        ),
      );
    }

    lines.push(
      formatMetricLine(
        `${name}_bucket`,
        {
          ...labels,
          le: '+Inf',
        },
        state.count,
      ),
    );
    lines.push(formatMetricLine(`${name}_sum`, labels, state.sum));
    lines.push(formatMetricLine(`${name}_count`, labels, state.count));
  }

  return lines;
};

export const renderDbMetricsPrometheus = (): string => {
  const sections = [
    ...renderCounterMetric(
      'meristem_db_queries_total',
      'Total MongoDB repository operations by collection, operation and status.',
      dbQueryCounter,
    ),
    ...renderHistogramMetric(
      'meristem_db_query_duration_ms',
      'MongoDB repository operation latency in milliseconds.',
      QUERY_DURATION_BUCKETS_MS,
      dbQueryDurationHistogram,
    ),
    ...renderCounterMetric(
      'meristem_db_transactions_total',
      'Total MongoDB transaction attempts by final status.',
      dbTransactionCounter,
    ),
    ...renderHistogramMetric(
      'meristem_db_transaction_duration_ms',
      'MongoDB transaction attempt latency in milliseconds.',
      TRANSACTION_DURATION_BUCKETS_MS,
      dbTransactionDurationHistogram,
    ),
  ];

  return `${sections.join('\n')}\n`;
};

export const resetDbObservability = (): void => {
  dbQueryCounter.clear();
  dbQueryDurationHistogram.clear();
  dbTransactionCounter.clear();
  dbTransactionDurationHistogram.clear();
};
