import type {
  Db,
  Filter,
  FindOneAndUpdateOptions,
  UpdateFilter,
} from 'mongodb';
import { TASKS_COLLECTION, type TaskDocument } from '../collections';
import type { DbSession } from '../transactions';
import type { CreatedAtCursor } from '../query-policy';
import { resolveQueryMaxTimeMs } from '../query-policy';
import {
  applyCreatedAtCursorFilter,
  executeRepositoryOperation,
  toSessionOption,
} from './shared';

type TaskListInput = {
  filter: Filter<TaskDocument>;
  limit: number;
  cursor: CreatedAtCursor | null;
  session: DbSession;
};

const tasksCollection = (db: Db) => db.collection<TaskDocument>(TASKS_COLLECTION);
const QUERY_MAX_TIME_MS = resolveQueryMaxTimeMs();

export const countTasks = async (
  db: Db,
  filter: Filter<TaskDocument>,
  session: DbSession = null,
): Promise<number> =>
  executeRepositoryOperation(
    TASKS_COLLECTION,
    'count_tasks',
    () => tasksCollection(db).countDocuments(filter, {
      ...toSessionOption(session),
      maxTimeMS: QUERY_MAX_TIME_MS,
    }),
  );

export const listTasks = async (
  db: Db,
  input: TaskListInput,
): Promise<TaskDocument[]> =>
  executeRepositoryOperation(
    TASKS_COLLECTION,
    'list_tasks',
    () => tasksCollection(db)
      .find(
        applyCreatedAtCursorFilter(
          input.filter,
          input.cursor,
          'task_id',
        ),
        toSessionOption(input.session),
      )
      .sort({ created_at: 1, task_id: 1 })
      .limit(input.limit + 1)
      .maxTimeMS(QUERY_MAX_TIME_MS)
      .toArray(),
  );

export const insertTask = async (
  db: Db,
  task: TaskDocument,
  session: DbSession = null,
): Promise<void> => {
  await executeRepositoryOperation(
    TASKS_COLLECTION,
    'insert_task',
    () => tasksCollection(db).insertOne(task, toSessionOption(session)),
  );
};

export const updateTaskById = async (
  db: Db,
  taskId: string,
  update: UpdateFilter<TaskDocument>,
  session: DbSession = null,
): Promise<TaskDocument | null> => {
  const options: FindOneAndUpdateOptions = {
    returnDocument: 'after',
    ...toSessionOption(session),
  };
  return executeRepositoryOperation(
    TASKS_COLLECTION,
    'update_task_by_id',
    () => tasksCollection(db).findOneAndUpdate(
      { task_id: taskId },
      update,
      options,
    ),
  );
};
