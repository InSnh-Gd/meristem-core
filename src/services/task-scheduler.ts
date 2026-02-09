import type { Db, Filter } from 'mongodb';
import {
  TaskAvailability,
  TaskDocument,
  TaskStatusType,
} from '../db/collections';
import {
  decodeCreatedAtCursor,
  encodeCreatedAtCursor,
  normalizeCursorPagination,
} from '../db/query-policy';
import {
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
  cursor?: string;
};

type CursorPageInfo = {
  has_next: boolean;
  next_cursor: string | null;
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
): Promise<{ data: TaskDocument[]; page_info: CursorPageInfo }> => {
  const pagination = normalizeCursorPagination(
    {
      limit: input.limit,
      cursor: input.cursor,
    },
    {
      defaultLimit: 100,
      maxLimit: 500,
    },
  );
  const cursor = pagination.cursor
    ? decodeCreatedAtCursor(pagination.cursor)
    : null;
  const rows = await listTasks(db, {
    filter: input.filter,
    limit: pagination.limit,
    cursor,
    session: null,
  });
  const hasNext = rows.length > pagination.limit;
  const data = hasNext ? rows.slice(0, pagination.limit) : rows;
  const last = data.at(-1);

  return {
    data,
    page_info: {
      has_next: hasNext,
      next_cursor:
        hasNext && last
          ? encodeCreatedAtCursor({
              createdAt: last.created_at,
              tieBreaker: last.task_id,
            })
          : null,
    },
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
