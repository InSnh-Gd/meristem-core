const PING_MESSAGE = 'compatibility-ping' as const;
const PONG_MESSAGE = 'compatibility-pong' as const;

const formatResponse = (input: MessageEvent<unknown>): string => {
  const payload = input.data;
  if (typeof payload === 'string') {
    return payload === PING_MESSAGE ? PONG_MESSAGE : `unexpected:${payload}`;
  }

  return `unexpected:${String(payload)}`;
};

const respondToWorker = (event: MessageEvent<unknown>): void => {
  const response = formatResponse(event);
  globalThis.postMessage(response);
};

self.addEventListener('message', respondToWorker);

export {};
