interface StatusBadgeProps {
  status: 'online' | 'offline' | 'unknown' | 'fresh' | 'stale' | 'old' | string
  label?: string
}

const statusColors: Record<string, string> = {
  online: 'bg-green-500',
  fresh: 'bg-green-500',
  offline: 'bg-red-500',
  old: 'bg-red-500',
  stale: 'bg-yellow-500',
  unknown: 'bg-slate-500',
}

export default function StatusBadge({ status, label }: StatusBadgeProps) {
  const color = statusColors[status] || 'bg-slate-500'
  return (
    <span className="inline-flex items-center gap-1.5 text-xs font-medium text-slate-300">
      <span className={`w-2 h-2 rounded-full ${color} animate-pulse`} />
      {label || status}
    </span>
  )
}

