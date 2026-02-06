/**
 * 配置加载模块
 * 
 * 从 TOML 文件加载配置，支持环境变量覆盖
 */
import { parse } from '@iarna/toml';
import { readFileSync, existsSync } from 'fs';
import { join } from 'path';

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
        jwt_secret: string;
        access_token_ttl: number;
        refresh_token_ttl: number;
        bootstrap_token_ttl: number;
    };
    logging: {
        level: string;
        format: string;
    };
    nats?: {
        stream_replicas?: number;
    };
}

const CONFIG_PATHS = [
    './config.toml',
    '/etc/meristem/config.toml',
];

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

    // 合并默认值和文件配置
    const config: CoreConfig = {
        server: {
            host: process.env.MERISTEM_SERVER_HOST ?? fileConfig.server?.host ?? '0.0.0.0',
            port: Number(process.env.MERISTEM_SERVER_PORT ?? fileConfig.server?.port ?? 8080),
            ws_path: process.env.MERISTEM_SERVER_WS_PATH ?? fileConfig.server?.ws_path ?? '/ws',
        },
        database: {
            mongo_uri: process.env.MERISTEM_DATABASE_MONGO_URI ?? fileConfig.database?.mongo_uri ?? 'mongodb://localhost:27017/meristem',
        },
        security: {
            jwt_algorithm: fileConfig.security?.jwt_algorithm ?? 'HS256',
            jwt_secret: process.env.MERISTEM_SECURITY_JWT_SECRET ?? fileConfig.security?.jwt_secret ?? '',
            access_token_ttl: fileConfig.security?.access_token_ttl ?? 3600,
            refresh_token_ttl: fileConfig.security?.refresh_token_ttl ?? 604800,
            bootstrap_token_ttl: fileConfig.security?.bootstrap_token_ttl ?? 1800,
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

/**
 * Get JWT secret from loaded configuration
 */
export function getJwtSecret(): string {
    const config = loadConfig();
    return config.security.jwt_secret;
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
