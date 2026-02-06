import { Db } from 'mongodb';

import {
  TASKS_COLLECTION,
  TaskDocument,
  TaskStatusType,
} from '../db/collections';

export type TaskResultStatus = 'completed' | 'failed';

export type TaskResultPayload = {
  status: TaskResultStatus;
  output?: string;
  error?: string;
};

type TaskDocumentWithResultInfo = TaskDocument & {
  result_error?: string;
};

const STATUS_MAP: Record<TaskResultStatus, TaskStatusType> = {
  completed: 'FINISHED',
  failed: 'FAILED',
};

const UNKNOWN_ERROR_MESSAGE = 'UNSPECIFIED_ERROR';

export const submitResult = async (
  db: Db,
  taskId: string,
  result: TaskResultPayload
): Promise<TaskDocumentWithResultInfo | null> => {
  const collection = db.collection<TaskDocumentWithResultInfo>(TASKS_COLLECTION);
  const statusType = STATUS_MAP[result.status];

  const baseSet: Record<string, unknown> = {
    'status.type': statusType,
  };

  const update: {
    $set: Record<string, unknown>;
    $unset?: Record<string, ''>;
  } = {
    $set: baseSet,
  };

  if (result.status === 'completed') {
    baseSet.result_uri = result.output ?? '';
    update.$unset = { result_error: '' };
  } else {
    baseSet.result_uri = '';
    baseSet.result_error = result.error ?? UNKNOWN_ERROR_MESSAGE;
  }

  const updated = await collection.findOneAndUpdate(
    { task_id: taskId },
    update,
    { returnDocument: 'after' }
  );

  return updated.value ?? null;
};
