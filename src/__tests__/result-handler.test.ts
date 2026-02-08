import { expect, test } from 'bun:test';
import type { Collection, Db, Document } from 'mongodb';

import type { TaskDocument } from '../db/collections';
import { submitResult } from '../services/result-handler';

type TaskDocumentWithResultInfo = TaskDocument & {
  result_error?: string;
};

const createTask = (taskId: string): TaskDocumentWithResultInfo => ({
  task_id: taskId,
  owner_id: 'owner-1',
  org_id: 'org-default',
  trace_id: 'trace-1',
  target_node_id: 'node-1',
  type: 'COMMAND',
  status: { type: 'RUNNING' },
  availability: 'READY',
  payload: {
    plugin_id: 'plugin-1',
    action: 'run',
    params: {},
    volatile: false,
  },
  lease: {
    expire_at: new Date('2026-02-07T00:00:00.000Z'),
    heartbeat_interval: 15000,
  },
  progress: {
    percent: 50,
    last_log_snippet: 'in-progress',
    updated_at: new Date('2026-02-07T00:00:00.000Z'),
  },
  result_uri: '',
  handshake: {
    result_sent: false,
    core_acked: false,
  },
  created_at: new Date('2026-02-07T00:00:00.000Z'),
});

type UpdatePayload = {
  $set: Record<string, unknown>;
  $unset?: Record<string, ''>;
};

const applyResultUpdate = (
  task: TaskDocumentWithResultInfo,
  update: UpdatePayload,
): TaskDocumentWithResultInfo => {
  const nextTask: TaskDocumentWithResultInfo = {
    ...task,
    status: {
      ...task.status,
      type: update.$set['status.type'] as TaskDocument['status']['type'],
    },
    result_uri:
      typeof update.$set.result_uri === 'string'
        ? update.$set.result_uri
        : task.result_uri,
  };

  if (typeof update.$set.result_error === 'string') {
    nextTask.result_error = update.$set.result_error;
  }

  if (update.$unset?.result_error === '') {
    delete nextTask.result_error;
  }

  return nextTask;
};

const createMockDb = (
  task: TaskDocumentWithResultInfo | null,
): Db => {
  const tasksCollection = {
    findOneAndUpdate: async (
      query: Record<string, unknown>,
      update: UpdatePayload,
    ): Promise<TaskDocumentWithResultInfo | null> => {
      if (!task) {
        return null;
      }

      if (query.task_id !== task.task_id) {
        return null;
      }

      return applyResultUpdate(task, update);
    },
  };

  const db = {
    collection: <TSchema extends Document>(_name: string): Collection<TSchema> =>
      tasksCollection as unknown as Collection<TSchema>,
  };

  return db as unknown as Db;
};

test('submitResult returns updated task document when task exists', async (): Promise<void> => {
  const db = createMockDb(createTask('task-existing'));

  const result = await submitResult(db, 'task-existing', {
    status: 'completed',
    output: 'mfs://result/output.json',
  });

  expect(result).not.toBeNull();
  if (!result) {
    throw new Error('result should not be null');
  }
  expect(result.status.type).toBe('FINISHED');
  expect(result.result_uri).toBe('mfs://result/output.json');
  expect(result.result_error).toBeUndefined();
});

test('submitResult returns null when task does not exist', async (): Promise<void> => {
  const db = createMockDb(null);

  const result = await submitResult(db, 'task-missing', {
    status: 'failed',
    error: 'network timeout',
  });

  expect(result).toBeNull();
});
