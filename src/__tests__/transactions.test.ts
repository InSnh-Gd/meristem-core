import { expect, test } from 'bun:test';
import type { Db } from 'mongodb';
import { runInTransaction } from '../db/transactions';

type LabeledError = Error & {
  hasErrorLabel: (label: string) => boolean;
};

const createLabeledError = (message: string, label: string): LabeledError => {
  const error = new Error(message) as LabeledError;
  error.hasErrorLabel = (candidate: string): boolean => candidate === label;
  return error;
};

test('runInTransaction does not rerun work when commit result is unknown', async (): Promise<void> => {
  let workCalls = 0;
  let sessionCreates = 0;
  let endSessionCalls = 0;
  const unknownCommitError = createLabeledError(
    'commit result is unknown',
    'UnknownTransactionCommitResult',
  );

  const db = {
    client: {
      startSession: () => {
        sessionCreates += 1;
        return {
          withTransaction: async (
            work: () => Promise<void>,
            _options?: unknown,
          ): Promise<void> => {
            await work();
            throw unknownCommitError;
          },
          endSession: async (): Promise<void> => {
            endSessionCalls += 1;
          },
        };
      },
    },
  } as unknown as Db;

  let caught: unknown = null;
  try {
    await runInTransaction(db, async () => {
      workCalls += 1;
      return 'ok';
    });
  } catch (error) {
    caught = error;
  }

  expect(caught).toMatchObject({
    code: 'TRANSACTION_ABORTED',
  });

  expect(workCalls).toBe(1);
  expect(sessionCreates).toBe(1);
  expect(endSessionCalls).toBe(1);
});

test('runInTransaction falls back to non-session execution when session factory is absent', async (): Promise<void> => {
  const db = {} as Db;

  const value = await runInTransaction(db, async (session) =>
    session === null ? 'without-session' : 'with-session',
  );

  expect(value).toBe('without-session');
});

test('runInTransaction binds startSession context from db client', async (): Promise<void> => {
  let endSessionCalls = 0;
  let withTransactionCalls = 0;

  const db = {
    client: {
      s: { connected: true },
      startSession(this: { s?: unknown }) {
        if (!this.s) {
          throw new Error('missing client state');
        }
        return {
          withTransaction: async (
            work: () => Promise<void>,
            _options?: unknown,
          ): Promise<void> => {
            withTransactionCalls += 1;
            await work();
          },
          endSession: async (): Promise<void> => {
            endSessionCalls += 1;
          },
        };
      },
    },
  } as unknown as Db;

  const result = await runInTransaction(db, async (session) =>
    session ? 'with-session' : 'without-session',
  );

  expect(result).toBe('with-session');
  expect(withTransactionCalls).toBe(1);
  expect(endSessionCalls).toBe(1);
});

test('runInTransaction fails fast when transactions are unsupported', async (): Promise<void> => {
  let withTransactionCalls = 0;
  let workCalls = 0;

  const db = {
    client: {
      startSession: () => ({
        withTransaction: async (
          _work: () => Promise<void>,
          _options?: unknown,
        ): Promise<void> => {
          withTransactionCalls += 1;
          throw Object.assign(
            new Error('Transaction numbers are only allowed on a replica set member or mongos'),
            { code: 20, codeName: 'IllegalOperation' },
          );
        },
        endSession: async (): Promise<void> => {},
      }),
    },
  } as unknown as Db;

  await expect(runInTransaction(db, async () => {
    workCalls += 1;
    return 'with-session';
  })).rejects.toMatchObject({
    code: 'TRANSACTION_ABORTED',
  });
  expect(withTransactionCalls).toBe(1);
  expect(workCalls).toBe(0);
});
