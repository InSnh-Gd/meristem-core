import type { NodePersona } from '@insnh-gd/meristem-shared';

export const NODES_COLLECTION = 'nodes';
export const TASKS_COLLECTION = 'tasks';
export const PLUGINS_COLLECTION = 'plugins';
export const USERS_COLLECTION = 'users';
export const ROLES_COLLECTION = 'roles';
export const ORGS_COLLECTION = 'orgs';
export const INVITATIONS_COLLECTION = 'user_invitations';

export type SduiVersion = `${number}.${number}`;

/**
 * Node 相关类型定义，确保所有节点数据遵循统一字段约束
 */
export type NodeRoleFlags = {
  is_relay: boolean;
  is_storage: boolean;
  is_compute: boolean;
};

export type NodeNetworkManualOverride = {
  extreme_mode: boolean;
  forced_tcp: boolean;
};

export type NodeNetworkIpShadowLease = {
  reclaim_status: 'ACTIVE' | 'PENDING_RECLAIM' | 'RECLAIMED';
  reclaim_at: Date;
  reclaim_generation?: number;
};

export type NodeNetwork = {
  virtual_ip: string;
  current_relay_id?: string;
  mode: 'DIRECT' | 'RELAY';
  v: number;
  manual_override?: NodeNetworkManualOverride;
  ip_shadow_lease?: NodeNetworkIpShadowLease;
};

export type NodeInventory = {
  cpu_model: string;
  cores: number;
  ram_total: number;
  os: string;
  arch: 'x86_64' | 'arm64';
};

export type NodeHardwareProfile = {
  cpu?: {
    model: string;
    cores: number;
    threads?: number;
  };
  memory?: {
    total: number;
    available?: number;
    type?: string;
  };
  storage?: Array<{
    type?: string;
    size?: number;
    total?: number;
    available?: number;
  }>;
  gpu?: Array<{
    model: string;
    vram?: number;
    memory?: number;
  }>;
  os?: string;
  arch?: 'x86_64' | 'arm64' | 'unknown';
};

export type NodeHardwareProfileDrift = {
  detected: boolean;
  baseline_hash?: string;
  incoming_hash?: string;
  detected_at?: Date;
};

export type NodeGpuInfo = {
  model: string;
  vram_total: number;
  usage: number;
};

export type NodeStatus = {
  online: boolean;
  connection_status: 'online' | 'offline' | 'expired_credentials' | 'pending_approval';
  last_seen: Date;
  cpu_usage: number;
  ram_free: number;
  gpu_info: NodeGpuInfo[];
};

export type NodeDocument = {
  node_id: string;
  org_id: string;
  hwid: string;
  hostname: string;
  persona: NodePersona;
  hardware_profile?: NodeHardwareProfile;
  hardware_profile_hash?: string;
  hardware_profile_drift?: NodeHardwareProfileDrift;
  role_flags: NodeRoleFlags;
  network: NodeNetwork;
  inventory: NodeInventory;
  status: NodeStatus;
  created_at: Date;
};

/**
 * Task 相关类型定义，操作跨插件的 payload 是动态的，故用 Record 表示
 */
export type TaskStatusType = 'PENDING' | 'RUNNING' | 'PAUSED' | 'FINISHED' | 'FAILED' | 'ORPHANED';
export type TaskType = 'COMMAND' | 'GIG_JOB';
export type TaskAvailability = 'READY' | 'SOURCE_OFFLINE' | 'EXPIRED';

export type TaskPayload = {
  plugin_id: string;
  action: string;
  params: Record<string, unknown>;
  volatile: boolean;
};

export type TaskLease = {
  expire_at: Date;
  heartbeat_interval: number;
};

export type TaskProgress = {
  percent: number;
  last_log_snippet: string;
  updated_at: Date;
};

export type TaskHandshake = {
  result_sent: boolean;
  core_acked: boolean;
};

export type TaskDocument = {
  task_id: string;
  owner_id: string;
  org_id: string;
  trace_id: string;
  target_node_id: string;
  type: TaskType;
  status: { type: TaskStatusType };
  availability: TaskAvailability;
  payload: TaskPayload;
  lease: TaskLease;
  progress: TaskProgress;
  result_uri: string;
  handshake: TaskHandshake;
  created_at: Date;
};

/**
 * Plugin 元数据与乐观锁约定
 */
export type PluginUI = {
  entry?: string;
  mode: 'SDUI' | 'ESM';
  icon?: string;
  sdui_version?: SduiVersion;
};

export type PluginConfig = {
  encrypted: Buffer;
  schema: Record<string, unknown>;
  v?: number;
};

export type PluginDocument = {
  plugin_id: string;
  name: string;
  version: string;
  entry: string;
  ui: PluginUI;
  permissions: string[];
  events: string[];
  exports: string[];
  config: PluginConfig;
  status: 'ACTIVE' | 'DISABLED' | 'ERROR';
  installed_at: Date;
  updated_at: Date;
};

export type RoleDocument = {
  role_id: string;
  name: string;
  description: string;
  permissions: string[];
  is_builtin: boolean;
  org_id: string;
  created_at: Date;
  updated_at: Date;
};

export type OrgDocument = {
  org_id: string;
  name: string;
  slug: string;
  owner_user_id: string;
  settings: Record<string, unknown>;
  created_at: Date;
  updated_at: Date;
};

/**
 * User 数据类型与 Token 管理
 */
export type UserToken = {
  token_id: string;
  refresh_token_hash: string;
  device_info?: string;
  created_at: Date;
  last_active: Date;
};

export type UserDocument = {
  user_id: string;
  username: string;
  password_hash: string;
  role_ids: string[];
  org_id: string;
  permissions: string[];
  permissions_v: number;
  tokens: UserToken[];
  created_at: Date;
  updated_at: Date;
};

export type InvitationDocument = {
  invitation_id: string;
  invitation_token: string;
  username: string;
  org_id: string;
  role_ids: string[];
  created_by: string;
  status: 'pending' | 'accepted' | 'expired';
  expires_at: Date;
  accepted_at?: Date;
  created_at: Date;
  updated_at: Date;
};
