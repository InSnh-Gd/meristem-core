import { setupJetstreamLogs } from './src/services/jetstream-setup.js';
import { createTraceContext } from './src/utils/trace-context.js';

const ctx = createTraceContext({ 
  traceId: 'setup', 
  nodeId: 'core', 
  source: 'bootstrap' 
});

const result = await setupJetstreamLogs(ctx);
console.log('JetStream setup result:', result);
process.exit(0);
