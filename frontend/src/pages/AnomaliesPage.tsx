import { useState } from 'react'
import { subDays } from 'date-fns'
import { AlertTriangle, TrendingUp, Zap } from 'lucide-react'
import { useAnomalies } from '../hooks/useApi'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import LoadingSkeleton from '../components/LoadingSkeleton'
import type { DateRange } from '../types'

const severityConfig: Record<string, { color: string; bg: string; badge: string }> = {
  critical: { color: 'text-red-400', bg: 'bg-red-900/20 border-red-700/40', badge: 'badge-red' },
  high: { color: 'text-orange-400', bg: 'bg-orange-900/20 border-orange-700/40', badge: 'badge-yellow' },
  medium: { color: 'text-yellow-400', bg: 'bg-yellow-900/20 border-yellow-700/40', badge: 'badge-blue' },
}

export default function AnomaliesPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const anomalies = useAnomalies(range)

  const items = anomalies.data?.data || []

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <AlertTriangle className="w-6 h-6 text-amber-400" /> Anomalies & Breaking Spikes
          </h1>
          <p className="text-sm text-slate-400 mt-1">Unusual increases in coverage detected by z-score analysis</p>
        </div>
        <DateRangePicker value={range} onChange={setRange} />
      </div>

      {/* Explanation */}
      <div className="card bg-slate-800/50">
        <p className="text-xs text-slate-400">
          Anomalies are detected by comparing the last 2-hour event window against a 24-hour baseline. Events are flagged when z-score &gt; 2.5 AND event count &gt; 10. Method: rolling z-score = (current - baseline_mean) / baseline_std.
        </p>
      </div>

      {anomalies.isLoading ? <LoadingSkeleton rows={5} /> : items.length === 0 ? (
        <EmptyState message="No anomalies detected" description="No unusual spikes in the selected period — this is normal." />
      ) : (
        <div className="space-y-4">
          {items.map((a: any, i: number) => {
            const cfg = severityConfig[a.severity] || severityConfig.medium
            return (
              <div key={i} className={`card border ${cfg.bg} flex items-start gap-4`}>
                <div className="flex-shrink-0 mt-1">
                  <Zap className={`w-5 h-5 ${cfg.color}`} />
                </div>
                <div className="flex-1 min-w-0">
                  <div className="flex items-center gap-2 flex-wrap">
                    <p className="text-slate-100 font-semibold">{a.label || a.topic || a.country_code}</p>
                    <span className={cfg.badge}>{a.severity}</span>
                    <span className="badge-purple">{a.anomaly_type}</span>
                  </div>
                  <div className="flex gap-6 mt-2 text-sm text-slate-300 flex-wrap">
                    <div>
                      <span className="text-slate-500">Baseline:</span> {a.baseline_avg?.toFixed(1)}
                    </div>
                    <div>
                      <span className="text-slate-500">Current:</span> <span className="font-medium">{a.current_count?.toFixed(0)}</span>
                    </div>
                    <div>
                      <span className="text-slate-500">Change:</span>
                      <span className={`ml-1 font-mono ${a.pct_change > 0 ? 'text-red-400' : 'text-green-400'}`}>
                        {a.pct_change > 0 ? '+' : ''}{a.pct_change?.toFixed(1)}%
                      </span>
                    </div>
                    <div>
                      <span className="text-slate-500">Z-score:</span>
                      <span className={`ml-1 font-mono font-bold ${cfg.color}`}>{a.z_score?.toFixed(2)}</span>
                    </div>
                  </div>
                  {a.detected_at && <p className="text-xs text-slate-500 mt-1">Detected: {a.detected_at}</p>}
                </div>
                <TrendingUp className={`w-6 h-6 ${cfg.color} flex-shrink-0`} />
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}

