import {
  Activity,
  ArrowRight,
  CheckCircle2,
  Gauge,
  TriangleAlert,
} from 'lucide-react';
import { recentRuns } from './data';

const metrics = [
  { label: 'Rows processed · 24h', value: '136.4M', icon: Gauge },
  { label: 'Successful runs', value: '98.7%', icon: CheckCircle2 },
  { label: 'Active runs', value: '2', icon: Activity },
  { label: 'Rejected rows', value: '0.04%', icon: TriangleAlert },
];

export function RunActivity() {
  return (
    <section className="studio-run-panel">
      <div className="studio-section-title">
        <div>
          <h2>Run activity</h2>
          <p>Sample telemetry showing the intended operational surface.</p>
        </div>
        <button type="button">View all runs <ArrowRight size={14} /></button>
      </div>

      <div className="studio-metrics">
        {metrics.map(({ label, value, icon: Icon }) => (
          <article key={label}>
            <span className="studio-metric-icon"><Icon size={15} /></span>
            <div><span>{label}</span><strong>{value}</strong></div>
            <small>sample</small>
          </article>
        ))}
      </div>

      <div className="studio-table-wrap">
        <table>
          <thead>
            <tr>
              <th>Status</th>
              <th>Run</th>
              <th>Pipeline</th>
              <th>Rows</th>
              <th>Duration</th>
              <th>Started</th>
              <th><span className="sr-only">Actions</span></th>
            </tr>
          </thead>
          <tbody>
            {recentRuns.map((run) => (
              <tr key={run.id}>
                <td>
                  <span className={`studio-run-status status-${run.status.toLowerCase()}`}>
                    {run.status}
                  </span>
                </td>
                <td><code>{run.id}</code></td>
                <td>{run.pipeline}</td>
                <td>{run.rows}</td>
                <td>{run.duration}</td>
                <td className="studio-muted-cell">{run.started}</td>
                <td>
                  <button className="studio-table-action" type="button" aria-label={`Open ${run.id}`}>
                    <ArrowRight size={15} />
                  </button>
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </section>
  );
}
