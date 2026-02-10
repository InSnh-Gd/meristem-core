import type { Db } from 'mongodb';
import type { AuditEventInput } from '../services/audit';
import { recordAuditEvent } from '../services/audit-pipeline';

type InvalidCallDepthRejectionInput = Readonly<{
  db: Db;
  routeTag: string;
  source: string;
  nodeId: string;
  traceId: string;
  reason: string;
  rawCallDepth: string;
  content: string;
}>;

export const recordInvalidCallDepthRejection = async (
  input: InvalidCallDepthRejectionInput,
): Promise<void> => {
  const auditEvent: AuditEventInput = {
    ts: Date.now(),
    level: 'WARN',
    node_id: input.nodeId,
    source: input.source,
    trace_id: input.traceId,
    content: input.content,
    meta: {
      reason: input.reason,
      raw_call_depth: input.rawCallDepth,
    },
  };

  /**
   * 逻辑块：无效 call_depth 的审计写入采用“失败不阻断主响应”策略。
   * 业务目标是优先返回一致的 INVALID_CALL_DEPTH 错误码，
   * 即便审计链路短时抖动，也不让路由行为变成 500 或超时。
   */
  try {
    await recordAuditEvent(input.db, auditEvent, { routeTag: input.routeTag });
  } catch (auditError) {
    console.error('[Audit] failed to log invalid call_depth rejection', auditError);
  }
};
