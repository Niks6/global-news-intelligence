import { useState } from 'react'
import { subDays } from 'date-fns'
import { TrendingUp } from 'lucide-react'
import { useTopics, useTopicTrend } from '../hooks/useApi'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import { ChartSkeleton } from '../components/LoadingSkeleton'
import type { DateRange } from '../types'
import {
  BarChart, Bar, LineChart, Line,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'

const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7']

export default function TopicsPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const [selected, setSelected] = useState<string | null>(null)
  const topics = useTopics(range)
  const trend = useTopicTrend(selected || '', range)

  const topicData = topics.data?.data || []

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <TrendingUp className="w-6 h-6 text-purple-400" /> Topics & Trends
          </h1>
          <p className="text-sm text-slate-400 mt-1">Discover what topics are driving global news coverage</p>
        </div>
        <DateRangePicker value={range} onChange={setRange} />
      </div>

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Top Topics Bar */}
        <div className="card">
          <h3 className="section-title mb-4">Top Topics</h3>
          {topics.isLoading ? <ChartSkeleton /> : topicData.length === 0 ? (
            <EmptyState message="No topics found" />
          ) : (
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={topicData.slice(0, 15)} layout="vertical" margin={{ left: 80 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <YAxis type="category" dataKey="topic" tick={{ fill: '#94a3b8', fontSize: 10 }} width={80} />
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                <Bar dataKey="count" fill="#a855f7" radius={[0, 4, 4, 0]} name="Frequency" cursor="pointer"
                  onClick={(data: any) => setSelected(data.topic)} />
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Topic Trend */}
        <div className="card">
          <h3 className="section-title mb-4">
            {selected ? `Trend: ${selected}` : 'Select a topic to see its trend'}
          </h3>
          {!selected ? (
            <div className="flex items-center justify-center h-80 text-slate-500">Click a topic bar to view its trend over time</div>
          ) : trend.isLoading ? <ChartSkeleton /> : (
            <ResponsiveContainer width="100%" height={400}>
              <LineChart data={trend.data?.trend || []}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis dataKey="date" tick={{ fill: '#94a3b8', fontSize: 10 }} />
                <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                <Line type="monotone" dataKey="count" stroke="#3b82f6" strokeWidth={2} dot={false} name="Count" />
              </LineChart>
            </ResponsiveContainer>
          )}
        </div>
      </div>

      {/* Topics Table */}
      {topicData.length > 0 && (
        <div className="card">
          <h3 className="section-title mb-4">All Topics</h3>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-700 text-xs text-slate-400 uppercase">
                  <th className="text-left py-2 px-3">#</th>
                  <th className="text-left py-2 px-3">Topic</th>
                  <th className="text-right py-2 px-3">Count</th>
                  <th className="text-right py-2 px-3">Avg Tone</th>
                </tr>
              </thead>
              <tbody>
                {topicData.map((t: any, i: number) => (
                  <tr key={t.topic} className="border-b border-slate-800 hover:bg-slate-800/60 cursor-pointer" onClick={() => setSelected(t.topic)}>
                    <td className="py-2 px-3 text-slate-500">{i + 1}</td>
                    <td className="py-2 px-3 text-slate-200 font-medium">{t.topic}</td>
                    <td className="py-2 px-3 text-right text-slate-300">{t.count?.toLocaleString()}</td>
                    <td className={`py-2 px-3 text-right font-mono ${(t.avg_tone ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                      {t.avg_tone != null ? t.avg_tone.toFixed(2) : '—'}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  )
}

