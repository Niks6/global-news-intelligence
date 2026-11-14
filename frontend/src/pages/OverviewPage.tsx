import { useState } from 'react'
import { subDays } from 'date-fns'
import { Globe2, Newspaper, MessageSquare, MapPin, TrendingUp, TrendingDown } from 'lucide-react'
import { useOverview, useCategories, useCooperationConflict, useTopics } from '../hooks/useApi'
import MetricCard from '../components/MetricCard'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import { GridSkeleton, ChartSkeleton } from '../components/LoadingSkeleton'
import StatusBadge from '../components/StatusBadge'
import type { DateRange } from '../types'
import {
  BarChart, Bar, LineChart, Line, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, AreaChart, Area
} from 'recharts'

const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7', '#ec4899', '#14b8a6', '#f97316', '#6366f1', '#84cc16']

export default function OverviewPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const overview = useOverview(range)
  const categories = useCategories(range)
  const coopConflict = useCooperationConflict(range)
  const topics = useTopics(range)

  const d = overview.data
  const isLoading = overview.isLoading

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      {/* Header */}
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <Globe2 className="w-6 h-6 text-blue-400" /> Global Overview
          </h1>
          <p className="text-sm text-slate-400 mt-1">Real-time global news intelligence powered by GDELT</p>
        </div>
        <DateRangePicker value={range} onChange={setRange} />
      </div>

      {/* KPI Cards */}
      {isLoading ? <GridSkeleton count={4} /> : (
        <div className="grid grid-cols-2 lg:grid-cols-4 gap-4">
          <MetricCard label="Total Events" value={d?.total_events} icon={<Newspaper className="w-5 h-5" />} colorClass="text-blue-400" />
          <MetricCard label="Total Mentions" value={d?.total_mentions} icon={<MessageSquare className="w-5 h-5" />} colorClass="text-green-400" />
          <MetricCard label="Active Countries" value={d?.active_countries} icon={<MapPin className="w-5 h-5" />} colorClass="text-purple-400" />
          <MetricCard label="Avg Tone" value={d?.avg_tone != null ? d.avg_tone.toFixed(2) : null} icon={d?.avg_tone != null && d.avg_tone >= 0 ? <TrendingUp className="w-5 h-5" /> : <TrendingDown className="w-5 h-5" />} colorClass={d?.avg_tone != null && d.avg_tone >= 0 ? 'text-green-400' : 'text-red-400'} />
        </div>
      )}

      {!isLoading && !d?.data_available && (
        <EmptyState message="No data available for the selected time range" description="Try expanding the date range or check that the ingestion pipeline is running." onRetry={() => overview.refetch()} />
      )}

      {d?.data_available && (
        <>
          {/* Row: Top Countries + Category Distribution */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Top Countries */}
            <div className="card">
              <h3 className="section-title mb-4">Top Countries by Events</h3>
              {d.top_countries?.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={d.top_countries.slice(0, 10)} layout="vertical" margin={{ left: 40 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                    <YAxis type="category" dataKey="country_code" tick={{ fill: '#94a3b8', fontSize: 11 }} width={40} />
                    <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                    <Bar dataKey="event_count" fill="#3b82f6" radius={[0, 4, 4, 0]} name="Events" />
                  </BarChart>
                </ResponsiveContainer>
              ) : <EmptyState message="No country data" />}
            </div>

            {/* Category Pie */}
            <div className="card">
              <h3 className="section-title mb-4">Event Category Distribution</h3>
              {d.category_distribution?.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <PieChart>
                    <Pie data={d.category_distribution} dataKey="count" nameKey="category_name" cx="50%" cy="50%" outerRadius={100} label={({ category_name, pct }: any) => `${(category_name || '').slice(0, 12)} ${pct}%`} labelLine={false}>
                      {d.category_distribution.map((_: any, i: number) => (
                        <Cell key={i} fill={COLORS[i % COLORS.length]} />
                      ))}
                    </Pie>
                    <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                  </PieChart>
                </ResponsiveContainer>
              ) : <EmptyState message="No category data" />}
            </div>
          </div>

          {/* Row: Cooperation/Conflict + Top Topics */}
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Coop/Conflict */}
            <div className="card">
              <h3 className="section-title mb-4">Cooperation vs Conflict</h3>
              <div className="flex gap-6 items-center justify-center py-8">
                <div className="text-center">
                  <p className="text-3xl font-bold text-green-400">{d.cooperation_pct?.toFixed(1)}%</p>
                  <p className="text-sm text-slate-400 mt-1">Cooperation</p>
                  <p className="text-xs text-slate-500">{d.cooperation_count?.toLocaleString()} events</p>
                </div>
                <div className="w-px h-16 bg-slate-600" />
                <div className="text-center">
                  <p className="text-3xl font-bold text-red-400">{d.conflict_pct?.toFixed(1)}%</p>
                  <p className="text-sm text-slate-400 mt-1">Conflict</p>
                  <p className="text-xs text-slate-500">{d.conflict_count?.toLocaleString()} events</p>
                </div>
              </div>
              {/* Visual bar */}
              <div className="flex h-3 rounded-full overflow-hidden bg-slate-700">
                <div className="bg-green-500 transition-all" style={{ width: `${d.cooperation_pct || 50}%` }} />
                <div className="bg-red-500 transition-all" style={{ width: `${d.conflict_pct || 50}%` }} />
              </div>
            </div>

            {/* Top Topics */}
            <div className="card">
              <h3 className="section-title mb-4">Trending Topics</h3>
              {topics.data?.data?.length > 0 ? (
                <ResponsiveContainer width="100%" height={300}>
                  <BarChart data={topics.data.data.slice(0, 10)} margin={{ left: 10 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                    <XAxis dataKey="topic" tick={{ fill: '#94a3b8', fontSize: 9 }} interval={0} angle={-30} textAnchor="end" height={60} />
                    <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                    <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                    <Bar dataKey="count" fill="#a855f7" radius={[4, 4, 0, 0]} name="Count" />
                  </BarChart>
                </ResponsiveContainer>
              ) : <EmptyState message="No topic data" />}
            </div>
          </div>
        </>
      )}
    </div>
  )
}

