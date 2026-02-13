/**
 * 配置加载模块
 * 
 * 从 TOML 文件加载配置，支持环境变量覆盖
 */
import { parse } from '@iarna/toml';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';
import { randomBytes } from 'crypto';
import { readJwtRotationStateSync } from './jwt-rotation-store';

export interface CoreConfig {
    server: {
        host: string;
        port: number;
        ws_path: string;
    };
    database: {
        mongo_uri: string;
    };
    security: {
        jwt_algorithm: string;
        jwt_sign_secret: string;
        jwt_verify_secrets: string[];
        jwt_rotation_grace_seconds: number;
        jwt_rotation_store_path: string;
        access_token_ttl: number;
        refresh_token_ttl: number;
        bootstrap_token_ttl: number;
        plugin_secret: string;
    };
    logging: {
        level: string;
        format: string;
    };
    nats?: {
        stream_replicas?: number;
        stream_max_bytes?: number;
    };
}

const CONFIG_PATHS = [
    './config.toml',
    '/etc/meristem/config.toml',
];

const parseSecretList = (value: string | undefined): string[] => {
    if (!value) {
        return [];
    }
    return value
        .split(',')
        .map((item) => item.trim())
        .filter((item) => item.length > 0);
};

const parseNumber = (value: string | undefined, fallback: number): number => {
    if (!value) {
        return fallback;
    }
    const parsed = Number.parseInt(value, 10);
    return Number.isFinite(parsed) && parsed > 0 ? parsed : fallback;
};

const normalizeVerifySecrets = (
    signSecret: string,
    configuredVerifySecrets: readonly string[],
): string[] => {
    const seen = new Set<string>();
    const normalized: string[] = [];

    const pushUnique = (secret: string): void => {
        if (!secret || seen.has(secret)) {
            return;
        }
        seen.add(secret);
        normalized.push(secret);
    };

    pushUnique(signSecret);
    for (const secret of configuredVerifySecrets) {
        pushUnique(secret);
    }

    return normalized;
};

/**
 * 加载配置文件
 * 优先级: 环境变量 > 配置文件 > 默认值
 */
export function loadConfig(): CoreConfig {
    let configPath: string | null = null;

    // 查找配置文件
    for (const path of CONFIG_PATHS) {
        if (existsSync(path)) {
            configPath = path;
            break;
        }
    }

    let fileConfig: Partial<CoreConfig> = {};
    if (configPath) {
        const content = readFileSync(configPath, 'utf-8');
        fileConfig = parse(content) as unknown as Partial<CoreConfig>;
    }

    const fileSecurity = fileConfig.security;
    const rotationStorePath =
        process.env.MERISTEM_SECURITY_JWT_ROTATION_STORE_PATH ??
        fileSecurity?.jwt_rotation_store_path ??
        join(process.cwd(), 'data', 'core', 'jwt-rotation.json');
    const rotationState = readJwtRotationStateSync(rotationStorePath);
    const fileSignSecret = fileSecurity?.jwt_sign_secret ?? '';
    const signSecret =
        process.env.MERISTEM_SECURITY_JWT_SIGN_SECRET ??
        rotationState?.current_sign_secret ??
        fileSignSecret;
    const envVerifySecrets = parseSecretList(process.env.MERISTEM_SECURITY_JWT_VERIFY_SECRETS);
    const fileVerifySecrets = Array.isArray(fileSecurity?.jwt_verify_secrets)
        ? fileSecurity.jwt_verify_secrets.filter((item): item is string => typeof item === 'string' && item.length > 0)
        : [];
    const rotationVerifySecrets = rotationState?.verify_secrets ?? [];
    const verifySecrets = normalizeVerifySecrets(
        signSecret,
        envVerifySecrets.length > 0
            ? envVerifySecrets
            : rotationVerifySecrets.length > 0
              ? rotationVerifySecrets
              : fileVerifySecrets,
    );

    // 合并默认值和文件配置
    const config: CoreConfig = {
        server: {
            host: process.env.MERISTEM_SERVER_HOST ?? fileConfig.server?.host ?? '0.0.0.0',
            port: Number(process.env.MERISTEM_SERVER_PORT ?? fileConfig.server?.port ?? 8080),
            ws_path: process.env.MERISTEM_SERVER_WS_PATH ?? fileConfig.server?.ws_path ?? '/ws',
        },
        database: {
            mongo_uri:
                process.env.MERISTEM_DATABASE_MONGO_URI ??
                fileConfig.database?.mongo_uri ??
                'mongodb://localhost:27017/meristem',
        },
        security: {
            jwt_algorithm: fileSecurity?.jwt_algorithm ?? 'HS256',
            jwt_sign_secret: signSecret,
            jwt_verify_secrets: verifySecrets,
            jwt_rotation_store_path: rotationStorePath,
            jwt_rotation_grace_seconds: parseNumber(
                process.env.MERISTEM_SECURITY_JWT_ROTATION_GRACE_SECONDS,
                rotationState?.grace_seconds ?? fileSecurity?.jwt_rotation_grace_seconds ?? 86400,
            ),
            access_token_ttl: fileSecurity?.access_token_ttl ?? 3600,
            refresh_token_ttl: fileSecurity?.refresh_token_ttl ?? 604800,
            bootstrap_token_ttl: fileSecurity?.bootstrap_token_ttl ?? 1800,
            plugin_secret: process.env.MERISTEM_PLUGIN_SECRET ?? fileSecurity?.plugin_secret ?? randomBytes(32).toString('hex'),
        },
        logging: {
            level: process.env.MERISTEM_LOGGING_LEVEL ?? fileConfig.logging?.level ?? 'info',
            format: fileConfig.logging?.format ?? 'json',
        },
    };

    return config;
}

/**
 * Get MongoDB connection URI from loaded configuration
 */
export function getMongoUri(): string {
    const config = loadConfig();
    return config.database.mongo_uri;
}

/**
 * Get NATS connection URL from environment or default value
 */
export function getNatsUrl(): string {
    return process.env.MERISTEM_NATS_URL ?? 'nats://localhost:4222';
}

export function getJwtSignSecret(): string {
    const config = loadConfig();
    return config.security.jwt_sign_secret;
}

export function getJwtVerifySecrets(): readonly string[] {
    const config = loadConfig();
    return config.security.jwt_verify_secrets;
}

export function getJwtRotationGraceSeconds(): number {
    const config = loadConfig();
    return config.security.jwt_rotation_grace_seconds;
}

/**
 * Get NATS JetStream replicas from environment or config
 * Priority: NATS_STREAM_REPLICAS env var > config.toml [nats].stream_replicas > default 1
 */
export function getStreamReplicas(): number {
    const envReplicas = process.env.NATS_STREAM_REPLICAS;
    if (envReplicas !== undefined) {
        const parsed = parseInt(envReplicas, 10);
        if (!isNaN(parsed) && parsed >= 1) {
            return parsed;
        }
    }

    const config = loadConfig();
    const configReplicas = config.nats?.stream_replicas;
    if (configReplicas !== undefined && configReplicas >= 1) {
        return configReplicas;
    }

    return 1;
}

const ONE_GIGABYTE = 1073741824;

/**
 * Get NATS JetStream stream max bytes.
 * Priority: NATS_STREAM_MAX_BYTES env var > config.toml [nats].stream_max_bytes > default 1GiB
 */
export function getStreamMaxBytes(): number {
    const envMaxBytes = process.env.NATS_STREAM_MAX_BYTES;
    if (envMaxBytes !== undefined) {
        const parsed = parseInt(envMaxBytes, 10);
        if (!isNaN(parsed) && parsed > 0) {
            return parsed;
        }
    }

    const config = loadConfig();
    const configMaxBytes = config.nats?.stream_max_bytes;
    if (
        configMaxBytes !== undefined &&
        Number.isFinite(configMaxBytes) &&
        configMaxBytes > 0
    ) {
        return Math.floor(configMaxBytes);
    }

    return ONE_GIGABYTE;
}
