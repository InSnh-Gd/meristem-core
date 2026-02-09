import type {
  Db,
  Filter,
  FindOneAndUpdateOptions,
  UpdateFilter,
} from 'mongodb';
import { TASKS_COLLECTION, type TaskDocument } from '../collections';
import type { DbSession } from '../transactions';
import { resolveQueryMaxTimeMs } from '../query-policy';
import { toSessionOption } from './shared';

type TaskListInput = {
  filter: Filter<TaskDocument>;
  limit: number;
  offset: number;
  session: DbSession;
};

const tasksCollection = (db: Db) => db.collection<TaskDocument>(TASKS_COLLECTION);
const QUERY_MAX_TIME_MS = resolveQueryMaxTimeMs();

export const countTasks = async (
  db: Db,
  filter: Filter<TaskDocument>,
  session: DbSession = null,
): Promise<number> =>
  tasksCollection(db).countDocuments(filter, {
    ...toSessionOption(session),
    maxTimeMS: QUERY_MAX_TIME_MS,
  });

export const listTasks = async (
  db: Db,
  input: TaskListInput,
): Promise<TaskDocument[]> =>
  tasksCollection(db)
    .find(input.filter, toSessionOption(input.session))
    .sort({ created_at: 1 })
    .skip(input.offset)
    .limit(input.limit)
    .maxTimeMS(QUERY_MAX_TIME_MS)
    .toArray();

export const insertTask = async (
  db: Db,
  task: TaskDocument,
  session: DbSession = null,
): Promise<void> => {
  await tasksCollection(db).insertOne(task, toSessionOption(session));
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
  return tasksCollection(db).findOneAndUpdate(
    { task_id: taskId },
    update,
    options,
  );
};
