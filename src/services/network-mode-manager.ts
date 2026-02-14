import { publish } from '../nats/connection';
import {
  getPluginInstance,
  getPluginRegistry,
  invokePluginMethod,
  isPluginResponsive,
} from './plugin-lifecycle';
import { createTraceContext } from '../utils/trace-context';
import { createLogger } from '../utils/logger';
import { broadcastWsPush } from '../routes/ws';

export type NetworkMode = 'DIRECT' | 'M-NET';

export type NetworkModeSwitchReason =
  | 'plugin_enabled'
  | 'plugin_disabled'
  | 'plugin_failure'
  | 'plugin_proposal'
  | 'manual_override';

export type NetworkModeChangedEvent = Readonly<{
  from: NetworkMode;
  to: NetworkMode;
  reason: NetworkModeSwitchReason;
  ts: string;
  plugin_id: string;
  health: 'healthy' | 'unhealthy';
}>;

type PluginStateSnapshot = Readonly<{
  pluginId: string | null;
  exists: boolean;
  running: boolean;
  healthy: boolean;
}>;

type NetworkModeProviderSnapshot = Readonly<{
  pluginId: string;
  exportName: string;
  running: boolean;
  healthy: boolean;
}>;

type PluginModeProposal = Readonly<{
  mode: NetworkMode;
}>;

type NetworkModeManagerOptions = Readonly<{
  capabilityExport?: string;
  pollIntervalMs?: number;
  fallbackToDirect?: boolean;
  inspectProviders?: () => NetworkModeProviderSnapshot[];
  inspectProposal?: (
    provider: NetworkModeProviderSnapshot,
  ) => Promise<PluginModeProposal | null> | PluginModeProposal | null;
  publishEvent?: (subject: string, payload: NetworkModeChangedEvent) => Promise<void>;
  broadcastEvent?: (topic: string, payload: NetworkModeChangedEvent) => void;
  now?: () => Date;
}>;

const NETWORK_MODE_EVENT_SUBJECT = 'meristem.v1.sys.network.mode';
const NETWORK_MODE_EVENT_TOPIC = 'sys.network.mode';
const DEFAULT_POLL_INTERVAL_MS = 2_000;
const DEFAULT_CAPABILITY_EXPORT = 'network-mode-status';

const createProviderSnapshot = (
  capabilityExport: string,
): NetworkModeProviderSnapshot[] => {
  const providers: NetworkModeProviderSnapshot[] = [];
  for (const [pluginId, lifecycle] of getPluginRegistry()) {
    if (!lifecycle.manifest.exports.includes(capabilityExport)) {
      continue;
    }

    providers.push({
      pluginId,
      exportName: capabilityExport,
      running: lifecycle.state === 'RUNNING',
      healthy: isPluginResponsive(pluginId),
    });
  }

  providers.sort((a, b) => a.pluginId.localeCompare(b.pluginId));
  return providers;
};

const createPluginSnapshot = (
  providers: readonly NetworkModeProviderSnapshot[],
): PluginStateSnapshot => {
  if (providers.length === 0) {
    return {
      pluginId: null,
      exists: false,
      running: false,
      healthy: false,
    };
  }

  const provider = providers[0];
  return {
    pluginId: provider.pluginId,
    exists: true,
    running: provider.running,
    healthy: provider.healthy,
  };
};

const isRecord = (value: unknown): value is Record<string, unknown> =>
  typeof value === 'object' && value !== null && !Array.isArray(value);

const parseProposedMode = (value: unknown): NetworkMode | null => {
  if (value === 'DIRECT' || value === 'M-NET') {
    return value;
  }

  return null;
};

const parseInvokeProposalData = (value: unknown): PluginModeProposal | null => {
  if (!isRecord(value)) {
    return null;
  }

  const mode = parseProposedMode(value.mode ?? value.desired_mode);
  if (!mode) {
    return null;
  }

  return Object.freeze({ mode });
};

const createPluginProposalFromInvoke = async (
  provider: NetworkModeProviderSnapshot,
): Promise<PluginModeProposal | null> => {
  const invoked = await invokePluginMethod(
    provider.pluginId,
    provider.exportName,
    {
      probe: 'network-mode-manager',
    },
    2_000,
  );

  if (!invoked.success) {
    return null;
  }

  const parsed = parseInvokeProposalData(invoked.data);
  if (parsed) {
    return parsed;
  }

  const instance = getPluginInstance(provider.pluginId);
  const config = instance?.config;
  if (!config) {
    return null;
  }

  const fromRoot = parseProposedMode(config.network_mode_proposal);
  if (fromRoot) {
    return Object.freeze({ mode: fromRoot });
  }

  const candidate = config.network_mode_proposal;
  if (!isRecord(candidate)) {
    return null;
  }

  const fromObject = parseProposedMode(candidate.mode ?? candidate.desired_mode);
  if (!fromObject) {
    return null;
  }

  return Object.freeze({ mode: fromObject });
};

const defaultPublishEvent = async (
  subject: string,
  payload: NetworkModeChangedEvent,
): Promise<void> => {
  const traceContext = createTraceContext({
    traceId: `network-mode-${Date.now()}`,
    nodeId: 'core',
    source: 'network-mode-manager',
  });

  await publish(traceContext, subject, JSON.stringify(payload));
};

const defaultBroadcastEvent = (topic: string, payload: NetworkModeChangedEvent): void => {
  broadcastWsPush(topic, payload);
};

const resolveTargetMode = (
  snapshot: PluginStateSnapshot,
  fallbackToDirect: boolean,
  proposal: PluginModeProposal | null,
): NetworkMode => {
  if (proposal) {
    if (proposal.mode === 'DIRECT') {
      return 'DIRECT';
    }

    if (snapshot.exists && snapshot.running && snapshot.healthy) {
      return 'M-NET';
    }
  }

  if (snapshot.exists && snapshot.running && snapshot.healthy) {
    return 'M-NET';
  }

  if (!fallbackToDirect && snapshot.exists) {
    return 'M-NET';
  }

  return 'DIRECT';
};

const resolveSwitchReason = (
  previous: NetworkMode,
  snapshot: PluginStateSnapshot,
  proposal: PluginModeProposal | null,
): NetworkModeSwitchReason => {
  if (proposal) {
    return 'plugin_proposal';
  }

  if (previous === 'DIRECT') {
    return 'plugin_enabled';
  }

  if (!snapshot.exists) {
    return 'plugin_disabled';
  }

  if (!snapshot.running || !snapshot.healthy) {
    return 'plugin_failure';
  }

  return 'manual_override';
};

export type NetworkModeManager = Readonly<{
  start: () => Promise<void>;
  stop: () => void;
  getMode: () => NetworkMode;
}>;

/**
 * 逻辑块：网络模式管理器采用“周期观测 + 事件广播”策略。
 * - 目的：在不侵入现有插件生命周期实现的前提下，提供 Direct 与 M-Net 的自动切换闭环。
 * - 原因：当前插件状态/健康信息已具备读取能力，但尚无统一模式管理入口。
 * - 失败路径：消息发布失败时保持当前模式不回滚，仅记录日志并等待下一周期重试。
 */
export const createNetworkModeManager = (
  options: NetworkModeManagerOptions = {},
): NetworkModeManager => {
  const capabilityExport = options.capabilityExport ?? DEFAULT_CAPABILITY_EXPORT;
  const pollIntervalMs = options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS;
  const fallbackToDirect = options.fallbackToDirect ?? true;
  const inspectProviders =
    options.inspectProviders ?? (() => createProviderSnapshot(capabilityExport));
  const inspectProposal =
    options.inspectProposal ?? ((provider) => createPluginProposalFromInvoke(provider));
  const publishEvent = options.publishEvent ?? defaultPublishEvent;
  const broadcastEvent = options.broadcastEvent ?? defaultBroadcastEvent;
  const now = options.now ?? (() => new Date());
  const logger = createLogger(
    createTraceContext({
      traceId: 'network-mode-manager',
      nodeId: 'core',
      source: 'network-mode-manager',
    }),
  );

  let currentMode: NetworkMode = 'DIRECT';
  let lastProviderId = 'unknown';
  let timer: ReturnType<typeof setInterval> | null = null;
  let checking = false;

  const evaluateAndSwitch = async (): Promise<void> => {
    if (checking) {
      return;
    }

    checking = true;
    try {
      const providers = inspectProviders();
      const snapshot = createPluginSnapshot(providers);
      const provider = providers[0] ?? null;
      const proposal = provider ? await Promise.resolve(inspectProposal(provider)) : null;
      const targetMode = resolveTargetMode(snapshot, fallbackToDirect, proposal);
      if (targetMode === currentMode) {
        return;
      }

      if (snapshot.pluginId) {
        lastProviderId = snapshot.pluginId;
      }

      const payload: NetworkModeChangedEvent = Object.freeze({
        from: currentMode,
        to: targetMode,
        reason: resolveSwitchReason(currentMode, snapshot, proposal),
        ts: now().toISOString(),
        plugin_id: snapshot.pluginId ?? lastProviderId,
        health: snapshot.healthy ? 'healthy' : 'unhealthy',
      });

      currentMode = targetMode;
      await publishEvent(NETWORK_MODE_EVENT_SUBJECT, payload);
      broadcastEvent(NETWORK_MODE_EVENT_TOPIC, payload);
      logger.info('network mode changed', payload);
    } catch (error) {
      logger.warn('network mode switch evaluation failed', {
        error: error instanceof Error ? error.message : String(error),
      });
    } finally {
      checking = false;
    }
  };

  const start = async (): Promise<void> => {
    if (timer) {
      return;
    }

    await evaluateAndSwitch();
    timer = setInterval(() => {
      void evaluateAndSwitch();
    }, pollIntervalMs);
  };

  const stop = (): void => {
    if (!timer) {
      return;
    }

    clearInterval(timer);
    timer = null;
  };

  return Object.freeze({
    start,
    stop,
    getMode: (): NetworkMode => currentMode,
  });
};
