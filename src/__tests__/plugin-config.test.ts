import { describe, expect, test } from 'bun:test';
import { decryptConfig, encryptConfig } from '../services/plugin-config-crypto';
import { QuotaExceededError, QuotaManager, enforceQuota } from '../services/storage-quota';

type PluginConfigApi = Readonly<{
  getConfig: (pluginId: string) => Record<string, unknown>;
  setConfig: (pluginId: string, config: Record<string, unknown>) => void;
  getStoredEncryptedConfig: (pluginId: string) => Buffer | undefined;
  reset: (pluginId: string) => void;
}>;

const createPluginConfigApi = (secret: string, quotaBytes: number): PluginConfigApi => {
  const encryptedStore = new Map<string, Buffer>();
  const quotaManager = new QuotaManager(quotaBytes);

  return {
    getConfig: pluginId => {
      const encryptedConfig = encryptedStore.get(pluginId);
      if (!encryptedConfig) {
        return {};
      }

      return decryptConfig(encryptedConfig, secret);
    },
    setConfig: (pluginId, config) => {
      const encryptedConfig = encryptConfig(config, secret);
      enforceQuota(pluginId, encryptedConfig.byteLength, quotaBytes);
      encryptedStore.set(pluginId, encryptedConfig);
    },
    getStoredEncryptedConfig: pluginId => encryptedStore.get(pluginId),
    reset: pluginId => {
      quotaManager.resetUsage(pluginId);
      encryptedStore.delete(pluginId);
    },
  };
};

describe('Plugin Config', () => {
  describe('Encryption/Decryption', () => {
    test('encrypts config data and decrypts back to original data', () => {
      const config = {
        retries: 3,
        endpoint: 'https://api.meristem.local',
        enabled: true,
      };
      const secret = 'plugin-config-secret';

      const encrypted = encryptConfig(config, secret);
      const decrypted = decryptConfig(encrypted, secret);

      expect(encrypted.byteLength).toBeGreaterThan(0);
      expect(decrypted).toEqual(config);
    });

    test('produces different ciphertexts when using different secrets', () => {
      const config = { tokenTtl: 30 };
      const encryptedWithFirstSecret = encryptConfig(config, 'secret-a');
      const encryptedWithSecondSecret = encryptConfig(config, 'secret-b');

      expect(encryptedWithFirstSecret.equals(encryptedWithSecondSecret)).toBe(false);
      expect(decryptConfig(encryptedWithFirstSecret, 'secret-a')).toEqual(config);
      expect(decryptConfig(encryptedWithSecondSecret, 'secret-b')).toEqual(config);
    });
  });

  describe('Quota Management', () => {
    test('tracks usage correctly', () => {
      const pluginId = 'plugin-config.quota.track';
      const quotaManager = new QuotaManager(64);

      quotaManager.resetUsage(pluginId);
      quotaManager.trackUsage(pluginId, 20);
      quotaManager.trackUsage(pluginId, 10);

      expect(quotaManager.getUsage(pluginId)).toBe(30);
    });

    test('checkQuota returns true when under limit', () => {
      const pluginId = 'plugin-config.quota.under';
      const quotaManager = new QuotaManager(64);

      quotaManager.resetUsage(pluginId);
      quotaManager.trackUsage(pluginId, 20);

      expect(quotaManager.checkQuota(pluginId, 10)).toBe(true);
    });

    test('checkQuota returns false when over limit', () => {
      const pluginId = 'plugin-config.quota.over';
      const quotaManager = new QuotaManager(30);

      quotaManager.resetUsage(pluginId);
      quotaManager.trackUsage(pluginId, 20);

      expect(quotaManager.checkQuota(pluginId, 15)).toBe(false);
    });

    test('throws QuotaExceededError when quota is exceeded', () => {
      const pluginId = 'plugin-config.quota.enforce';
      const quotaBytes = 48;
      const quotaManager = new QuotaManager(quotaBytes);

      quotaManager.resetUsage(pluginId);
      enforceQuota(pluginId, 32, quotaBytes);

      expect(() => enforceQuota(pluginId, 17, quotaBytes)).toThrow(QuotaExceededError);
    });
  });

  describe('Context Config API', () => {
    test('getConfig returns decrypted config', () => {
      const pluginId = 'plugin-config.context.get';
      const configApi = createPluginConfigApi('context-secret', 4096);
      const config = { mode: 'strict', retries: 2 };

      configApi.reset(pluginId);
      configApi.setConfig(pluginId, config);

      expect(configApi.getConfig(pluginId)).toEqual(config);
    });

    test('setConfig encrypts and stores config', () => {
      const pluginId = 'plugin-config.context.store';
      const configApi = createPluginConfigApi('context-secret', 4096);
      const config = { schedule: 'hourly', compression: 'gzip' };

      configApi.reset(pluginId);
      configApi.setConfig(pluginId, config);

      const encryptedConfig = configApi.getStoredEncryptedConfig(pluginId);
      const plaintextBuffer = Buffer.from(JSON.stringify(config), 'utf8');

      expect(encryptedConfig).toBeDefined();
      if (!encryptedConfig) {
        throw new Error('Expected encrypted config to be stored');
      }

      expect(encryptedConfig.equals(plaintextBuffer)).toBe(false);
    });

    test('setConfig enforces quota', () => {
      const pluginId = 'plugin-config.context.quota';
      const configApi = createPluginConfigApi('context-secret', 96);

      configApi.reset(pluginId);

      expect(() => {
        configApi.setConfig(pluginId, { payload: 'x'.repeat(256) });
      }).toThrow(QuotaExceededError);

      expect(configApi.getStoredEncryptedConfig(pluginId)).toBeUndefined();
    });
  });
});
