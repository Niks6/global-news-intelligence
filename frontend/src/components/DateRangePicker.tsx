import { useState } from 'react'
import { Calendar, ChevronDown } from 'lucide-react'
import type { DateRange, TimePreset } from '../types'
import { subDays, format, parseISO } from 'date-fns'

interface DateRangePickerProps {
  value: DateRange
  onChange: (range: DateRange) => void
}

const presets: { label: string; value: TimePreset }[] = [
  { label: 'Last 24h', value: '24h' },
  { label: 'Last 7 days', value: '7d' },
  { label: 'Last 30 days', value: '30d' },
  { label: 'Custom', value: 'custom' },
]

export default function DateRangePicker({ value, onChange }: DateRangePickerProps) {
  const [open, setOpen] = useState(false)
  const [preset, setPreset] = useState<TimePreset>('7d')

  const selectPreset = (p: TimePreset) => {
    setPreset(p)
    if (p !== 'custom') {
      const to = new Date()
      const from = p === '24h' ? subDays(to, 1) : p === '7d' ? subDays(to, 7) : subDays(to, 30)
      onChange({ from, to })
      setOpen(false)
    }
  }

  return (
    <div className="relative">
      <button
        onClick={() => setOpen(!open)}
        className="flex items-center gap-2 btn-secondary"
      >
        <Calendar className="w-4 h-4 text-slate-400" />
        <span>
          {format(value.from, 'MMM d')} – {format(value.to, 'MMM d, yyyy')} UTC
        </span>
        <ChevronDown className="w-4 h-4 text-slate-400" />
      </button>

      {open && (
        <div className="absolute right-0 top-full mt-1 z-50 bg-slate-800 border border-slate-600 rounded-xl shadow-xl p-4 w-72">
          <p className="text-xs text-slate-400 uppercase tracking-wide mb-3">Quick select</p>
          <div className="grid grid-cols-2 gap-2 mb-4">
            {presets.map((p) => (
              <button
                key={p.value}
                onClick={() => selectPreset(p.value)}
                className={`text-sm py-2 rounded-lg border transition-colors ${
                  preset === p.value
                    ? 'bg-blue-600/20 border-blue-500/50 text-blue-400'
                    : 'border-slate-600 text-slate-300 hover:border-slate-500'
                }`}
              >
                {p.label}
              </button>
            ))}
          </div>
          {preset === 'custom' && (
            <div className="space-y-2">
              <div>
                <label className="text-xs text-slate-400 block mb-1">From (UTC)</label>
                <input
                  type="date"
                  className="input-base w-full"
                  value={format(value.from, 'yyyy-MM-dd')}
                  onChange={(e) => onChange({ ...value, from: parseISO(e.target.value) })}
                />
              </div>
              <div>
                <label className="text-xs text-slate-400 block mb-1">To (UTC)</label>
                <input
                  type="date"
                  className="input-base w-full"
                  value={format(value.to, 'yyyy-MM-dd')}
                  onChange={(e) => onChange({ ...value, to: parseISO(e.target.value) })}
                />
              </div>
              <button className="btn-primary w-full mt-2" onClick={() => setOpen(false)}>
                Apply
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  )
}

