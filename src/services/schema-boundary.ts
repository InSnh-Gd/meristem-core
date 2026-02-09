import { Effect, Exit, Schema } from 'effect';
import { DomainError } from '../errors/domain-error';
import {
  isPulseSnapshotPayload,
  type PulseSnapshotPayload,
} from './log-triad';

export const HeartbeatMessageSchema = Schema.Struct({
  node_id: Schema.String,
  ts: Schema.Number,
  v: Schema.Number,
  claimed_ip: Schema.optional(Schema.String),
});

export type HeartbeatMessage = Schema.Schema.Type<typeof HeartbeatMessageSchema>;
const JSON_DECODER = new TextDecoder();

const PulseSnapshotSchema = Schema.Struct({
  node_id: Schema.String,
  ts: Schema.Number,
  core: Schema.Struct({
    cpu_load: Schema.Number,
    ram_usage: Schema.Number,
    net_io: Schema.optional(
      Schema.Struct({
        in: Schema.Number,
        out: Schema.Number,
      }),
    ),
  }),
  plugins: Schema.optional(
    Schema.Record({
      key: Schema.String,
      value: Schema.Unknown,
    }),
  ),
});

const toDomainDecodeError = (scope: string, cause: unknown): DomainError =>
  new DomainError('INTERNAL_ERROR', {
    cause,
    meta: {
      reason: 'SCHEMA_DECODE_FAILED',
      scope,
    },
  });

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const isFiniteNumber = (value: unknown): value is number =>
  typeof value === 'number' && Number.isFinite(value);

const isHeartbeatFastPathPayload = (
  value: unknown,
): value is HeartbeatMessage => {
  if (!isRecord(value)) {
    return false;
  }

  if (typeof value.node_id !== 'string' || !isFiniteNumber(value.ts)) {
    return false;
  }

  if (!isFiniteNumber(value.v)) {
    return false;
  }

  if (value.claimed_ip !== undefined && typeof value.claimed_ip !== 'string') {
    return false;
  }

  return true;
};

const decodeHeartbeatWithSchema = (
  payload: unknown,
): Effect.Effect<HeartbeatMessage, DomainError> =>
  Schema.decodeUnknown(HeartbeatMessageSchema)(payload).pipe(
    Effect.mapError((error) => toDomainDecodeError('heartbeat', error)),
  );

const decodePulseWithSchema = (
  payload: unknown,
): Effect.Effect<PulseSnapshotPayload, DomainError> =>
  Schema.decodeUnknown(PulseSnapshotSchema)(payload).pipe(
    Effect.mapError((error) => toDomainDecodeError('pulse', error)),
  );

export const decodeHeartbeatBoundary = (
  payload: unknown,
  fastPathEnabled: boolean,
): Effect.Effect<HeartbeatMessage, DomainError> =>
  Effect.gen(function* () {
    if (fastPathEnabled && isHeartbeatFastPathPayload(payload)) {
      return payload;
    }

    return yield* decodeHeartbeatWithSchema(payload);
  });

export const decodePulseBoundary = (
  payload: unknown,
  fastPathEnabled: boolean,
): Effect.Effect<PulseSnapshotPayload, DomainError> =>
  Effect.gen(function* () {
    if (fastPathEnabled && isPulseSnapshotPayload(payload)) {
      return payload;
    }

    return yield* decodePulseWithSchema(payload);
  });

export const decodeJsonBoundary = (
  bytes: Uint8Array,
  scope: string,
): Effect.Effect<unknown, DomainError> =>
  Effect.try({
    try: () => JSON.parse(JSON_DECODER.decode(bytes)) as unknown,
    catch: (error) =>
      new DomainError('INTERNAL_ERROR', {
        cause: error,
        meta: {
          reason: 'JSON_DECODE_FAILED',
          scope,
        },
      }),
  });

export const runBoundarySync = <A>(
  program: Effect.Effect<A, DomainError>,
): { ok: true; value: A } | { ok: false; error: DomainError } => {
  const exit = Effect.runSyncExit(program);
  if (Exit.isSuccess(exit)) {
    return {
      ok: true,
      value: exit.value,
    };
  }

  return {
    ok: false,
    error: new DomainError('INTERNAL_ERROR', {
      cause: exit.cause,
      meta: {
        reason: 'BOUNDARY_PROGRAM_FAILED',
      },
    }),
  };
};
