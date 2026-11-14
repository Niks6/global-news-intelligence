interface MetricCardProps {
  label: string
  value: string | number | null | undefined
  unit?: string
  trend?: number
  trendLabel?: string
  icon?: React.ReactNode
  colorClass?: string
  loading?: boolean
}

export default function MetricCard({ label, value, unit, trend, trendLabel, icon, colorClass = 'text-blue-400', loading }: MetricCardProps) {
  if (loading) {
    return (
      <div className="card animate-pulse">
        <div className="h-3 bg-slate-700 rounded w-2/3 mb-3" />
        <div className="h-7 bg-slate-700 rounded w-1/2 mb-2" />
        <div className="h-3 bg-slate-700 rounded w-1/3" />
      </div>
    )
  }

  const displayValue = value === null || value === undefined ? '—' : value

  return (
    <div className="card">
      <div className="flex items-start justify-between">
        <p className="metric-label">{label}</p>
        {icon && <span className={`${colorClass} opacity-70`}>{icon}</span>}
      </div>
      <p className={`metric-value mt-2 ${colorClass}`}>
        {typeof displayValue === 'number' ? displayValue.toLocaleString() : displayValue}
        {unit && <span className="text-sm font-normal text-slate-400 ml-1">{unit}</span>}
      </p>
      {trend !== undefined && trend !== null && (
        <p className={`text-xs mt-1 ${trend >= 0 ? 'text-green-400' : 'text-red-400'}`}>
          {trend >= 0 ? '▲' : '▼'} {Math.abs(trend).toFixed(1)}% {trendLabel || 'vs prior period'}
        </p>
      )}
    </div>
  )
}

