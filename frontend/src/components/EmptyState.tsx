import { AlertCircle, RefreshCw } from 'lucide-react'

interface EmptyStateProps {
  message?: string
  description?: string
  onRetry?: () => void
  isError?: boolean
}

export default function EmptyState({ message, description, onRetry, isError }: EmptyStateProps) {
  return (
    <div className="flex flex-col items-center justify-center py-16 px-6 text-center">
      <div className={`w-12 h-12 rounded-full flex items-center justify-center mb-4 ${isError ? 'bg-red-900/40' : 'bg-slate-700'}`}>
        <AlertCircle className={`w-6 h-6 ${isError ? 'text-red-400' : 'text-slate-400'}`} />
      </div>
      <p className="text-slate-300 font-medium">{message || 'No data available'}</p>
      {description && <p className="text-slate-500 text-sm mt-1 max-w-sm">{description}</p>}
      {onRetry && (
        <button onClick={onRetry} className="btn-secondary mt-4 flex items-center gap-2">
          <RefreshCw className="w-4 h-4" /> Retry
        </button>
      )}
    </div>
  )
}

