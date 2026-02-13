const DEFAULT_QUOTA_BYTES = 100 * 1024 * 1024;

const usageMap = new Map<string, number>();

class QuotaExceededError extends Error {
  constructor(pluginId: string) {
    super(`Storage quota exceeded for plugin ${pluginId}`);
    this.name = 'QuotaExceededError';
  }
}

class QuotaManager {
  private readonly quota: number;

  constructor(quota = DEFAULT_QUOTA_BYTES) {
    this.quota = quota;
  }

  trackUsage(pluginId: string, bytes: number): void {
    const currentUsage = this.getUsage(pluginId);
    usageMap.set(pluginId, currentUsage + bytes);
  }

  getUsage(pluginId: string): number {
    return usageMap.get(pluginId) ?? 0;
  }

  checkQuota(pluginId: string, requestedBytes: number): boolean {
    const currentUsage = this.getUsage(pluginId);
    return currentUsage + requestedBytes <= this.quota;
  }

  resetUsage(pluginId: string): void {
    usageMap.delete(pluginId);
  }
}

const enforceQuota = (
  pluginId: string,
  requestedBytes: number,
  quota = DEFAULT_QUOTA_BYTES,
): void => {
  const quotaManager = new QuotaManager(quota);

  if (!quotaManager.checkQuota(pluginId, requestedBytes)) {
    throw new QuotaExceededError(pluginId);
  }

  quotaManager.trackUsage(pluginId, requestedBytes);
};

export { QuotaManager, QuotaExceededError, enforceQuota };
