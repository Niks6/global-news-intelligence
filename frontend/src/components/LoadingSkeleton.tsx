export default function LoadingSkeleton({ rows = 4 }: { rows?: number }) {
  return (
    <div className="space-y-3 animate-pulse">
      {Array.from({ length: rows }).map((_, i) => (
        <div key={i} className="card">
          <div className="flex gap-3 items-center">
            <div className="w-8 h-8 bg-slate-700 rounded-lg flex-shrink-0" />
            <div className="flex-1 space-y-2">
              <div className="h-3 bg-slate-700 rounded w-3/4" />
              <div className="h-3 bg-slate-700 rounded w-1/2" />
            </div>
            <div className="h-5 bg-slate-700 rounded w-16" />
          </div>
        </div>
      ))}
    </div>
  )
}

export function GridSkeleton({ count = 4 }: { count?: number }) {
  return (
    <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 animate-pulse">
      {Array.from({ length: count }).map((_, i) => (
        <div key={i} className="card">
          <div className="h-3 bg-slate-700 rounded w-2/3 mb-3" />
          <div className="h-7 bg-slate-700 rounded w-1/2 mb-2" />
          <div className="h-3 bg-slate-700 rounded w-1/3" />
        </div>
      ))}
    </div>
  )
}

export function ChartSkeleton({ height = 200 }: { height?: number }) {
  return (
    <div className="card animate-pulse" style={{ height }}>
      <div className="h-3 bg-slate-700 rounded w-1/3 mb-4" />
      <div className="flex items-end gap-2 h-3/4">
        {Array.from({ length: 12 }).map((_, i) => (
          <div key={i} className="bg-slate-700 rounded-t flex-1" style={{ height: `${30 + Math.random() * 60}%` }} />
        ))}
      </div>
    </div>
  )
}

