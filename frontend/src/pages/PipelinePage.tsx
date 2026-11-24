import { Activity, CheckCircle, XCircle, HelpCircle } from 'lucide-react'
import { usePipelineHealth } from '../hooks/useApi'
import StatusBadge from '../components/StatusBadge'
import { ChartSkeleton } from '../components/LoadingSkeleton'

const statusIcon = (status: string) => {
  if (status === 'online' || status === 'fresh') return <CheckCircle className="w-4 h-4 text-green-400" />
  if (status === 'offline' || status === 'old') return <XCircle className="w-4 h-4 text-red-400" />
  return <HelpCircle className="w-4 h-4 text-slate-400" />
}

export default function PipelinePage() {
  const pipeline = usePipelineHealth()
  const d = pipeline.data

  const formattedLast = d?.last_ingestion_time
    ? new Date(d.last_ingestion_time).toLocaleString()
    : 'Never'

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div>
        <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
          <Activity className="w-6 h-6 text-cyan-400" /> Pipeline & System Health
        </h1>
        <p className="text-sm text-slate-400 mt-1">Monitor live GDELT ingestion freshness, Spark processing, and cluster service status</p>
      </div>

      {pipeline.isLoading ? <ChartSkeleton height={200} /> : !d ? (
        <div className="card text-center py-12 text-slate-400">
          <p>Unable to reach the API health endpoint.</p>
          <p className="text-xs mt-1">Ensure the backend is running on {import.meta.env.VITE_API_URL || 'http://localhost:8000'}</p>
        </div>
      ) : (
        <>
          {/* Freshness */}
          <div className="card">
            <h3 className="section-title mb-4">Data Freshness</h3>
            <div className="flex items-center gap-4 flex-wrap">
              <StatusBadge status={d.freshness_status || 'unknown'} label={d.freshness_status === 'fresh' ? 'Fresh (<20min)' : d.freshness_status === 'stale' ? 'Stale (20-60min)' : d.freshness_status === 'old' ? 'Old (>1h)' : 'Unknown'} />
              <span className="text-sm text-slate-300">
                Last ingestion: {formattedLast}
              </span>
              {d.pipeline_lag_minutes != null && (
                <span className="text-xs text-slate-500">
                  Lag: {d.pipeline_lag_minutes} min
                </span>
              )}
              {d.spark_workers != null && (
                <span className="text-xs text-slate-400">
                  Spark workers alive: {d.spark_workers}
                </span>
              )}
            </div>
          </div>

          {/* Services */}
          <div className="grid grid-cols-2 lg:grid-cols-5 gap-4">
            {([
              { label: 'HBase', status: d.hbase_connected ? 'online' : 'offline' },
              { label: 'Kafka', status: d.kafka_status || 'unknown' },
              { label: 'Spark', status: d.spark_status || 'unknown' },
              { label: 'HDFS', status: d.hdfs_status || 'unknown' },
              { label: 'API', status: 'online' },
            ]).map(({ label, status }) => (
              <div key={label} className="card flex items-center gap-3">
                {statusIcon(status)}
                <div>
                  <p className="text-sm font-medium text-slate-200">{label}</p>
                  <StatusBadge status={status} />
                </div>
              </div>
            ))}
          </div>

          {/* Ingestion Stats */}
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <div className="card">
              <p className="metric-label">Files Processed Today</p>
              <p className="metric-value text-blue-400 mt-1">{d.files_processed_today?.toLocaleString() ?? 0}</p>
            </div>
            <div className="card">
              <p className="metric-label">Records Accepted Today</p>
              <p className="metric-value text-green-400 mt-1">{d.records_accepted_today?.toLocaleString() ?? 0}</p>
            </div>
            <div className="card">
              <p className="metric-label">Records Rejected Today</p>
              <p className="metric-value text-orange-400 mt-1">{d.records_rejected_today?.toLocaleString() ?? 0}</p>
            </div>
            <div className="card">
              <p className="metric-label">Total Errors</p>
              <p className="metric-value text-red-400 mt-1">{d.total_errors ?? 0}</p>
            </div>
          </div>

          {/* Record Breakdown by Dataset */}
          {d.breakdown && (
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
              <div className="card">
                <p className="metric-label">Events Records Today</p>
                <p className="text-xl font-bold text-slate-100 mt-1">{(d.breakdown.events_accepted ?? 0).toLocaleString()}</p>
              </div>
              <div className="card">
                <p className="metric-label">Mentions Records Today</p>
                <p className="text-xl font-bold text-slate-100 mt-1">{(d.breakdown.mentions_accepted ?? 0).toLocaleString()}</p>
              </div>
              <div className="card">
                <p className="metric-label">GKG Records Today</p>
                <p className="text-xl font-bold text-slate-100 mt-1">{(d.breakdown.gkg_accepted ?? 0).toLocaleString()}</p>
              </div>
            </div>
          )}

          {/* Info */}
          <div className="card bg-slate-800/50 text-xs text-slate-500">
            <p>All telemetry reflects real cluster probes (Kafka, Spark Master, HDFS WebHDFS, HBase Thrift) and SQLite/GDELT ingestion checkpoint counts — zero hardcoded metrics.</p>
          </div>
        </>
      )}
    </div>
  )
}
