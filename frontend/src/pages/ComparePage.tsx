import { useState } from 'react'
import { subDays } from 'date-fns'
import { BarChart2 } from 'lucide-react'
import { useCompareCountries } from '../hooks/useApi'
import MetricCard from '../components/MetricCard'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import { GridSkeleton } from '../components/LoadingSkeleton'
import type { DateRange } from '../types'
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, Cell } from 'recharts'
import { useSearchParams } from 'react-router-dom'

const BAR_COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7', '#ec4899']

export default function ComparePage() {
  const [searchParams] = useSearchParams()
  const initial = searchParams.get('codes')?.split(',').filter(Boolean) || ['US', 'UK']
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const [codes, setCodes] = useState<string[]>(initial)
  const [inputVal, setInputVal] = useState(codes.join(', '))

  const compare = useCompareCountries(codes, range)
  const countries = compare.data?.countries || []

  const handleApply = () => {
    const parsed = inputVal.split(/[,\s]+/).map(s => s.trim().toUpperCase()).filter(Boolean)
    if (parsed.length >= 2) setCodes(parsed.slice(0, 6))
  }

  // Build chart data
  const volumeData = countries.map((c: any) => ({
    country: c.country_code,
    events: c.total_events || 0,
    mentions: c.total_mentions || 0,
  }))
  const toneData = countries.map((c: any) => ({
    country: c.country_code,
    avg_tone: c.avg_tone || 0,
  }))

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <BarChart2 className="w-6 h-6 text-cyan-400" /> Country Comparison
          </h1>
          <p className="text-sm text-slate-400 mt-1">Compare metrics across countries side by side</p>
        </div>
        <DateRangePicker value={range} onChange={setRange} />
      </div>

      {/* Country input */}
      <div className="flex gap-3 items-center">
        <input className="input-base flex-1 max-w-md" placeholder="Country codes (comma-separated, e.g. US, UK, FR)" value={inputVal} onChange={e => setInputVal(e.target.value)} onKeyDown={e => { if (e.key === 'Enter') handleApply() }} />
        <button className="btn-primary" onClick={handleApply}>Compare</button>
      </div>

      {compare.isLoading ? <GridSkeleton count={4} /> : countries.length === 0 ? (
        <EmptyState message="Enter at least 2 country codes to compare" />
      ) : (
        <>
          {/* KPI row per country */}
          <div className="grid grid-cols-2 lg:grid-cols-3 xl:grid-cols-6 gap-4">
            {countries.map((c: any, i: number) => (
              <div key={c.country_code} className="card border-l-4" style={{ borderLeftColor: BAR_COLORS[i % BAR_COLORS.length] }}>
                <p className="text-lg font-bold text-slate-100">{c.country_code}</p>
                <p className="text-xs text-slate-400 mt-1">{c.total_events?.toLocaleString()} events</p>
                <p className={`text-sm font-mono mt-1 ${(c.avg_tone ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                  Tone: {c.avg_tone?.toFixed(2) ?? '—'}
                </p>
              </div>
            ))}
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Volume comparison */}
            <div className="card">
              <h3 className="section-title mb-4">Event & Mention Volume</h3>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={volumeData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                  <XAxis dataKey="country" tick={{ fill: '#94a3b8', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                  <Legend />
                  <Bar dataKey="events" fill="#3b82f6" radius={[4, 4, 0, 0]} name="Events" />
                  <Bar dataKey="mentions" fill="#22c55e" radius={[4, 4, 0, 0]} name="Mentions" />
                </BarChart>
              </ResponsiveContainer>
            </div>

            {/* Tone comparison */}
            <div className="card">
              <h3 className="section-title mb-4">Average Tone</h3>
              <ResponsiveContainer width="100%" height={300}>
                <BarChart data={toneData}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                  <XAxis dataKey="country" tick={{ fill: '#94a3b8', fontSize: 12 }} />
                  <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                  <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                  <Bar dataKey="avg_tone" radius={[4, 4, 0, 0]} name="Avg Tone">
                    {toneData.map((_: any, i: number) => (
                      <Cell key={i} fill={toneData[i]?.avg_tone >= 0 ? '#22c55e' : '#ef4444'} />
                    ))}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

