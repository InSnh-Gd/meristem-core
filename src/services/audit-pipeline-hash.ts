import { createHash, createHmac } from 'crypto';
import type { AuditEventInput } from './audit';

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const canonicalizeValue = (value: unknown): unknown => {
  if (Array.isArray(value)) {
    return value.map((item) => canonicalizeValue(item));
  }
  if (isRecord(value)) {
    const normalized: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      const candidate = value[key];
      if (candidate !== undefined) {
        normalized[key] = canonicalizeValue(candidate);
      }
    }
    return normalized;
  }
  return value;
};

export const stableStringifyUnknown = (value: unknown): string =>
  JSON.stringify(canonicalizeValue(value));

export const calculatePayloadDigest = (payload: AuditEventInput): string =>
  createHash('sha256').update(stableStringifyUnknown(payload)).digest('hex');

export const calculatePayloadHmac = (
  digest: string,
  secret: string,
): string => createHmac('sha256', secret).update(digest).digest('hex');

export const calculatePartitionHash = (
  payload: AuditEventInput,
  partitionSequence: number,
  partitionPreviousHash: string,
): string => {
  /**
   * 逻辑块：分区链 hash 计算输入固定为 payload + 分区序号 + 前驱 hash。
   * 该组合保证“同 payload 但不同链位置”得到不同结果，
   * 从而阻断跨分区/跨序号重放污染分区链的问题。
   */
  const input = {
    ...payload,
    partition_sequence: partitionSequence,
    partition_previous_hash: partitionPreviousHash,
  };
  return createHash('sha256').update(stableStringifyUnknown(input)).digest('hex');
};

export const calculatePartitionId = (
  payload: AuditEventInput,
  partitionCount: number,
): number => {
  /**
   * 逻辑块：分区路由只依赖稳定业务键（node_id/trace_id/source）。
   * 这样即使事件重试，也会落到同一分区，维持分区链顺序一致性。
   */
  const seed = `${payload.node_id}|${payload.trace_id}|${payload.source}`;
  const digest = createHash('sha256').update(seed).digest();
  return digest.readUInt32BE(0) % partitionCount;
};
