import { collectRuntimeCheckReport } from '../runtime/runtime-check';

const report = collectRuntimeCheckReport();
const payload = JSON.stringify(report, null, 2);

console.log(payload);

if (!report.ok) {
  process.exit(1);
}

