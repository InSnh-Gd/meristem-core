export type AuditPipelineStartOptions = {
  enableBackgroundLoops?: boolean;
  partitionCount?: number;
  batchSize?: number;
  flushIntervalMs?: number;
  anchorIntervalMs?: number;
  backlogSoftLimit?: number;
  backlogHardLimit?: number;
  leaseDurationMs?: number;
  maxRetryAttempts?: number;
  hmacSecret?: string;
  hmacKeyId?: string;
};

export type AuditPipelineOptions = {
  enableBackgroundLoops: boolean;
  partitionCount: number;
  batchSize: number;
  flushIntervalMs: number;
  anchorIntervalMs: number;
  backlogSoftLimit: number;
  backlogHardLimit: number;
  leaseDurationMs: number;
  maxRetryAttempts: number;
  hmacSecret: string;
  hmacKeyId: string;
};

export const DEFAULT_OPTIONS: AuditPipelineOptions = {
  enableBackgroundLoops: true,
  partitionCount: 16,
  batchSize: 32,
  flushIntervalMs: 20,
  anchorIntervalMs: 1_000,
  backlogSoftLimit: 3_000,
  backlogHardLimit: 8_000,
  leaseDurationMs: 10_000,
  maxRetryAttempts: 5,
  hmacSecret: process.env.MERISTEM_AUDIT_HMAC_SECRET ?? 'meristem-audit-default-secret',
  hmacKeyId: process.env.MERISTEM_AUDIT_HMAC_KEY_ID ?? 'audit-hmac-v1',
};

const toPositiveInteger = (value: number, fallback: number): number =>
  Number.isFinite(value) && value > 0 ? Math.floor(value) : fallback;

export const normalizeOptions = (
  options: AuditPipelineStartOptions = {},
): AuditPipelineOptions => ({
  /**
   * 逻辑块：启动参数归一化策略。
   * 所有数值配置统一收敛为正整数，非法值自动回退默认值，
   * 这样可以避免运行期出现负数/NaN 配置把调度器推入不可预测状态。
   */
  enableBackgroundLoops: options.enableBackgroundLoops ?? DEFAULT_OPTIONS.enableBackgroundLoops,
  partitionCount: toPositiveInteger(options.partitionCount ?? DEFAULT_OPTIONS.partitionCount, DEFAULT_OPTIONS.partitionCount),
  batchSize: toPositiveInteger(options.batchSize ?? DEFAULT_OPTIONS.batchSize, DEFAULT_OPTIONS.batchSize),
  flushIntervalMs: toPositiveInteger(options.flushIntervalMs ?? DEFAULT_OPTIONS.flushIntervalMs, DEFAULT_OPTIONS.flushIntervalMs),
  anchorIntervalMs: toPositiveInteger(options.anchorIntervalMs ?? DEFAULT_OPTIONS.anchorIntervalMs, DEFAULT_OPTIONS.anchorIntervalMs),
  backlogSoftLimit: toPositiveInteger(options.backlogSoftLimit ?? DEFAULT_OPTIONS.backlogSoftLimit, DEFAULT_OPTIONS.backlogSoftLimit),
  backlogHardLimit: toPositiveInteger(options.backlogHardLimit ?? DEFAULT_OPTIONS.backlogHardLimit, DEFAULT_OPTIONS.backlogHardLimit),
  leaseDurationMs: toPositiveInteger(options.leaseDurationMs ?? DEFAULT_OPTIONS.leaseDurationMs, DEFAULT_OPTIONS.leaseDurationMs),
  maxRetryAttempts: toPositiveInteger(options.maxRetryAttempts ?? DEFAULT_OPTIONS.maxRetryAttempts, DEFAULT_OPTIONS.maxRetryAttempts),
  hmacSecret: options.hmacSecret ?? DEFAULT_OPTIONS.hmacSecret,
  hmacKeyId: options.hmacKeyId ?? DEFAULT_OPTIONS.hmacKeyId,
});
