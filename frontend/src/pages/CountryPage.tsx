import { useState } from 'react'
import { useParams, useNavigate } from 'react-router-dom'
import { subDays } from 'date-fns'
import { MapPin, BarChart2 } from 'lucide-react'
import { useCountryOverview, useTopics } from '../hooks/useApi'
import MetricCard from '../components/MetricCard'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import { GridSkeleton, ChartSkeleton } from '../components/LoadingSkeleton'
import type { DateRange } from '../types'
import {
  AreaChart, Area, BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer
} from 'recharts'

const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7', '#ec4899', '#14b8a6', '#f97316']

export default function CountryPage() {
  const { code } = useParams<{ code: string }>()
  const navigate = useNavigate()
  const countryCode = (code || 'US').toUpperCase()
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })

  const overview = useCountryOverview(countryCode, range)
  const topics = useTopics(range, countryCode)
  const d = overview.data

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <MapPin className="w-6 h-6 text-emerald-400" /> Country Intelligence — {countryCode}
          </h1>
          <p className="text-sm text-slate-400 mt-1">Detailed analysis for a single country</p>
        </div>
        <div className="flex gap-3 items-center">
          <input placeholder="Country code" className="input-base w-24" defaultValue={countryCode}
            onKeyDown={(e) => { if (e.key === 'Enter') navigate(`/country/${(e.target as HTMLInputElement).value.toUpperCase()}`) }} />
          <button className="btn-secondary flex items-center gap-1.5" onClick={() => navigate(`/compare?codes=${countryCode}`)}>
            <BarChart2 className="w-4 h-4" /> Compare
          </button>
          <DateRangePicker value={range} onChange={setRange} />
        </div>
      </div>

      {overview.isLoading ? <GridSkeleton count={4} /> : !d?.data_available ? (
        <EmptyState message={`No data for ${countryCode}`} description="Try a different country code or expand the date range" onRetry={() => overview.refetch()} />
      ) : (
        <>
          <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
            <MetricCard label="Total Events" value={d.total_events} colorClass="text-blue-400" />
            <MetricCard label="Mentions" value={d.total_mentions} colorClass="text-green-400" />
            <MetricCard label="Avg Tone" value={d.avg_tone?.toFixed(2)} colorClass={d.avg_tone >= 0 ? 'text-green-400' : 'text-red-400'} />
            <MetricCard label="Avg Goldstein" value={d.avg_goldstein?.toFixed(2)} colorClass={d.avg_goldstein >= 0 ? 'text-emerald-400' : 'text-orange-400'} />
          </div>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Event Timeline */}
            <div className="card">
              <h3 className="section-title mb-4">Event Timeline</h3>
              {d.event_timeline?.length > 0 ? (
                <ResponsiveContainer width="100%" height={280}>
                  <AreaChart data={d.event_timeline}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="date" tick={{ fill: '#94a3b8', fontSize: 10 }} />
                    <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                    <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                    <Area type="monotone" dataKey="event_count" stroke="#3b82f6" fill="#3b82f6" fillOpacity={0.15} strokeWidth={2} name="Events" />
                  </AreaChart>
                </ResponsiveContainer>
              ) : <EmptyState message="No timeline data" />}
            </div>

            {/* Top Topics */}
            <div className="card">
              <h3 className="section-title mb-4">Top Topics in {countryCode}</h3>
              {topics.data?.data?.length > 0 ? (
                <ResponsiveContainer width="100%" height={280}>
                  <BarChart data={topics.data.data.slice(0, 10)}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="topic" tick={{ fill: '#94a3b8', fontSize: 9 }} angle={-25} textAnchor="end" height={60} />
                    <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                    <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                    <Bar dataKey="count" fill="#a855f7" radius={[4, 4, 0, 0]} name="Count" />
                  </BarChart>
                </ResponsiveContainer>
              ) : <EmptyState message="No topic data" />}
            </div>
          </div>

          {/* Cooperation/Conflict */}
          <div className="card">
            <h3 className="section-title mb-4">Cooperation vs Conflict</h3>
            <div className="flex gap-6 items-center justify-center py-6">
              <div className="text-center">
                <p className="text-2xl font-bold text-green-400">{d.cooperation_count?.toLocaleString()}</p>
                <p className="text-sm text-slate-400">Cooperation</p>
              </div>
              <div className="w-px h-12 bg-slate-600" />
              <div className="text-center">
                <p className="text-2xl font-bold text-red-400">{d.conflict_count?.toLocaleString()}</p>
                <p className="text-sm text-slate-400">Conflict</p>
              </div>
            </div>
          </div>
        </>
      )}
    </div>
  )
}

