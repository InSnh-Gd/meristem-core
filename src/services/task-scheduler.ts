import { Db } from 'mongodb';
import {
  TASKS_COLLECTION,
  TaskAvailability,
  TaskDocument,
  TaskStatusType,
} from '../db/collections';

/**
 * 任务调度服务
 *
 * 负责将任务记录写入 Mongo，并在需要时将任务分配到指定节点。
 */
export type CreateTaskInput = Omit<TaskDocument, 'created_at'>;

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
  taskData: CreateTaskInput
): Promise<TaskDocument> => {
  const collection = db.collection<TaskDocument>(TASKS_COLLECTION);
  const document: TaskDocument = {
    ...taskData,
    created_at: new Date(),
  };

  await collection.insertOne(document);
  return document;
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
  nodeId: string
): Promise<TaskDocument | null> => {
  const collection = db.collection<TaskDocument>(TASKS_COLLECTION);

  const updated = await collection.findOneAndUpdate(
    { task_id: taskId },
    {
      $set: {
        target_node_id: nodeId,
        'status.type': DEFAULT_ASSIGNMENT_STATUS,
        availability: DEFAULT_ASSIGNMENT_AVAILABILITY,
      },
    },
    { returnDocument: 'after' }
  );

  return updated;
};
