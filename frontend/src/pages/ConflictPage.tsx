import { useState } from 'react'
import { subDays } from 'date-fns'
import { Swords } from 'lucide-react'
import { useCategories, useCooperationConflict } from '../hooks/useApi'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import { ChartSkeleton } from '../components/LoadingSkeleton'
import type { DateRange } from '../types'
import {
  BarChart, Bar, PieChart, Pie, Cell,
  XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer
} from 'recharts'

const COOP_COLOR = '#22c55e'
const CONF_COLOR = '#ef4444'
const COLORS = ['#3b82f6', '#22c55e', '#f59e0b', '#ef4444', '#a855f7', '#ec4899', '#14b8a6', '#f97316', '#6366f1', '#84cc16',
  '#0ea5e9', '#d946ef', '#fb7185', '#2dd4bf', '#fbbf24', '#a3e635', '#38bdf8', '#c084fc', '#f43f5e', '#4ade80']

export default function ConflictPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const cc = useCooperationConflict(range)
  const categories = useCategories(range)

  const catData = categories.data?.data || []
  const ccData = cc.data

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <Swords className="w-6 h-6 text-orange-400" /> Conflict & Cooperation
          </h1>
          <p className="text-sm text-slate-400 mt-1">CAMEO event taxonomy and Goldstein scale analysis</p>
        </div>
        <DateRangePicker value={range} onChange={setRange} />
      </div>

      {/* Cooperation vs Conflict Summary */}
      {cc.isLoading ? <ChartSkeleton height={140} /> : !ccData ? (
        <EmptyState message="No conflict data" />
      ) : (
        <div className="card">
          <h3 className="section-title mb-4">Overall Cooperation vs Conflict</h3>
          <div className="flex gap-8 items-center justify-center py-6">
            <div className="text-center">
              <p className="text-4xl font-bold text-green-400">{ccData.cooperation_pct?.toFixed(1)}%</p>
              <p className="text-sm text-slate-400 mt-1">Cooperation</p>
              <p className="text-xs text-slate-500">{ccData.cooperation_count?.toLocaleString()} events</p>
            </div>
            <div className="w-48 h-5 rounded-full overflow-hidden bg-slate-700 flex">
              <div className="bg-green-500" style={{ width: `${ccData.cooperation_pct || 50}%` }} />
              <div className="bg-red-500" style={{ width: `${ccData.conflict_pct || 50}%` }} />
            </div>
            <div className="text-center">
              <p className="text-4xl font-bold text-red-400">{ccData.conflict_pct?.toFixed(1)}%</p>
              <p className="text-sm text-slate-400 mt-1">Conflict</p>
              <p className="text-xs text-slate-500">{ccData.conflict_count?.toLocaleString()} events</p>
            </div>
          </div>
        </div>
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Root Code Breakdown */}
        <div className="card">
          <h3 className="section-title mb-4">CAMEO Root Code Distribution</h3>
          {categories.isLoading ? <ChartSkeleton /> : catData.length === 0 ? (
            <EmptyState message="No category data" />
          ) : (
            <ResponsiveContainer width="100%" height={400}>
              <BarChart data={catData} layout="vertical" margin={{ left: 120 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
                <XAxis type="number" tick={{ fill: '#94a3b8', fontSize: 11 }} />
                <YAxis type="category" dataKey="category_name" tick={{ fill: '#94a3b8', fontSize: 10 }} width={120} />
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
                <Bar dataKey="count" radius={[0, 4, 4, 0]} name="Events">
                  {catData.map((entry: any, i: number) => (
                    <Cell key={i} fill={entry.is_conflict ? CONF_COLOR : COOP_COLOR} />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          )}
        </div>

        {/* Pie */}
        <div className="card">
          <h3 className="section-title mb-4">Category Share</h3>
          {catData.length > 0 ? (
            <ResponsiveContainer width="100%" height={400}>
              <PieChart>
                <Pie data={catData} dataKey="count" nameKey="category_name" cx="50%" cy="50%" outerRadius={130}
                  label={({ category_name, pct }: any) => `${(category_name || '').slice(0, 15)} ${pct}%`} labelLine={false}>
                  {catData.map((_: any, i: number) => <Cell key={i} fill={COLORS[i % COLORS.length]} />)}
                </Pie>
                <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
              </PieChart>
            </ResponsiveContainer>
          ) : <EmptyState message="No data" />}
        </div>
      </div>

      {/* Category Table */}
      {catData.length > 0 && (
        <div className="card">
          <h3 className="section-title mb-4">Detailed Category Breakdown</h3>
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-slate-700 text-xs text-slate-400 uppercase">
                <th className="text-left py-2 px-3">Code</th>
                <th className="text-left py-2 px-3">Category</th>
                <th className="text-right py-2 px-3">Count</th>
                <th className="text-right py-2 px-3">Share</th>
                <th className="text-right py-2 px-3">Avg Goldstein</th>
                <th className="text-right py-2 px-3">Avg Tone</th>
                <th className="text-center py-2 px-3">Type</th>
              </tr>
            </thead>
            <tbody>
              {catData.map((c: any) => (
                <tr key={c.root_code} className="border-b border-slate-800 hover:bg-slate-800/60">
                  <td className="py-2 px-3 text-slate-300 font-mono">{c.root_code}</td>
                  <td className="py-2 px-3 text-slate-200">{c.category_name}</td>
                  <td className="py-2 px-3 text-right text-slate-300">{c.count?.toLocaleString()}</td>
                  <td className="py-2 px-3 text-right text-slate-300">{c.pct?.toFixed(1)}%</td>
                  <td className={`py-2 px-3 text-right font-mono ${(c.avg_goldstein ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {c.avg_goldstein?.toFixed(2) ?? '—'}
                  </td>
                  <td className={`py-2 px-3 text-right font-mono ${(c.avg_tone ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                    {c.avg_tone?.toFixed(2) ?? '—'}
                  </td>
                  <td className="py-2 px-3 text-center">
                    <span className={c.is_conflict ? 'badge-red' : 'badge-green'}>{c.is_conflict ? 'Conflict' : 'Cooperation'}</span>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

