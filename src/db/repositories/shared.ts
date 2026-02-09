import type { ClientSession, Filter } from 'mongodb';
import type { DbSession } from '../transactions';
import type { CreatedAtCursor } from '../query-policy';
import { recordDbQueryMetric } from '../observability';

export const toSessionOption = (
  session: DbSession,
): { session?: ClientSession } => (session ? { session } : {});

const hasObjectKeys = (value: Record<string, unknown>): boolean =>
  Object.keys(value).length > 0;

export const applyCreatedAtCursorFilter = <TDoc>(
  baseFilter: Filter<TDoc>,
  cursor: CreatedAtCursor | null,
  tieBreakerField: string,
): Filter<TDoc> => {
  if (!cursor) {
    return baseFilter;
  }

  const cursorFilter: Record<string, unknown> = {
    $or: [
      {
        created_at: { $gt: cursor.createdAt },
      },
      {
        created_at: cursor.createdAt,
        [tieBreakerField]: { $gt: cursor.tieBreaker },
      },
    ],
  };

  const baseFilterRecord = baseFilter as Record<string, unknown>;
  if (!hasObjectKeys(baseFilterRecord)) {
    return cursorFilter as Filter<TDoc>;
  }

  return {
    $and: [baseFilterRecord, cursorFilter],
  } as Filter<TDoc>;
};

export const executeRepositoryOperation = async <T>(
  collection: string,
  operation: string,
  work: () => Promise<T>,
): Promise<T> => {
  const startedAt = Date.now();
  try {
    const result = await work();
    recordDbQueryMetric({
      collection,
      operation,
      status: 'ok',
      durationMs: Date.now() - startedAt,
    });
    return result;
  } catch (error) {
    recordDbQueryMetric({
      collection,
      operation,
      status: 'error',
      durationMs: Date.now() - startedAt,
    });
    throw error;
  }
};
