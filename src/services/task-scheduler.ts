import type { Db, Filter } from 'mongodb';
import {
  TaskAvailability,
  TaskDocument,
  TaskStatusType,
} from '../db/collections';
import {
  countTasks,
  insertTask,
  listTasks,
  updateTaskById,
} from '../db/repositories/tasks';

/**
 * 任务调度服务
 *
 * 负责将任务记录写入 Mongo，并在需要时将任务分配到指定节点。
 */
export type CreateTaskInput = Omit<TaskDocument, 'created_at'>;

export type ListTasksInput = {
  filter: Filter<TaskDocument>;
  limit: number;
  offset: number;
};

const DEFAULT_ASSIGNMENT_STATUS: TaskStatusType = 'RUNNING';
const DEFAULT_ASSIGNMENT_AVAILABILITY: TaskAvailability = 'READY';

/**
 * 在数据库中创建一份任务文档。
 *
 * @param db - Mongo 实例
 * @param taskData - 除 created_at 外的任务字段
 */
export const createTask = async (
  db: Db,
  taskData: CreateTaskInput,
): Promise<TaskDocument> => {
  const document: TaskDocument = {
    ...taskData,
    created_at: new Date(),
  };

  await insertTask(db, document);
  return document;
};

export const listTaskDocuments = async (
  db: Db,
  input: ListTasksInput,
): Promise<{ data: TaskDocument[]; total: number }> => {
  const [total, data] = await Promise.all([
    countTasks(db, input.filter),
    listTasks(db, {
      filter: input.filter,
      limit: input.limit,
      offset: input.offset,
      session: null,
    }),
  ]);

  return {
    data,
    total,
  };
};

/**
 * 将任务分配到指定的节点，并更新状态/可用性字段。
 *
 * @param db - Mongo 实例
 * @param taskId - 任务 ID
 * @param nodeId - 节点 ID
 */
export const assignTask = async (
  db: Db,
  taskId: string,
  nodeId: string,
): Promise<TaskDocument | null> =>
  updateTaskById(db, taskId, {
    $set: {
      target_node_id: nodeId,
      'status.type': DEFAULT_ASSIGNMENT_STATUS,
      availability: DEFAULT_ASSIGNMENT_AVAILABILITY,
    },
  });
