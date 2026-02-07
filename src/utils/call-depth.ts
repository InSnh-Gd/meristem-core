export const DEFAULT_CALL_DEPTH = 0;
export const MAX_CALL_DEPTH = 16;
export const CALL_DEPTH_HEADER = 'x-call-depth';

type CallDepthValidation =
  | { ok: true; depth: number }
  | { ok: false; reason: string; raw?: string };

const parseDepth = (raw: string): number | null => {
  if (!/^\d+$/.test(raw)) {
    return null;
  }
  const parsed = Number.parseInt(raw, 10);
  if (!Number.isFinite(parsed)) {
    return null;
  }
  return parsed;
};

export const validateCallDepth = (rawValue: string | null): CallDepthValidation => {
  if (rawValue === null || rawValue.trim().length === 0) {
    return { ok: true, depth: DEFAULT_CALL_DEPTH };
  }

  const parsed = parseDepth(rawValue.trim());
  if (parsed === null) {
    return { ok: false, reason: 'CALL_DEPTH_NOT_INTEGER', raw: rawValue };
  }

  if (parsed > MAX_CALL_DEPTH) {
    return { ok: false, reason: 'CALL_DEPTH_EXCEEDED', raw: rawValue };
  }

  return { ok: true, depth: parsed };
};

export const validateCallDepthFromHeaders = (headers: Headers): CallDepthValidation => {
  const rawValue = headers.get(CALL_DEPTH_HEADER);
  return validateCallDepth(rawValue);
};
