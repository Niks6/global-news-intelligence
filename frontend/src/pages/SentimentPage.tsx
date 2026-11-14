import { useState } from 'react'
import { subDays } from 'date-fns'
import { Heart, AlertCircle } from 'lucide-react'
import { useTone } from '../hooks/useApi'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import { ChartSkeleton } from '../components/LoadingSkeleton'
import type { DateRange } from '../types'
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, ReferenceLine } from 'recharts'

export default function SentimentPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const [country, setCountry] = useState('')
  const tone = useTone(range, country || undefined)

  const trendData = tone.data?.trend || []
  const overallTone = tone.data?.overall_avg_tone

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <Heart className="w-6 h-6 text-pink-400" /> Sentiment & Tone Analysis
          </h1>
          <p className="text-sm text-slate-400 mt-1">GDELT average tone trends and distributions</p>
        </div>
        <div className="flex gap-3 items-center">
          <input className="input-base w-24" placeholder="Country" value={country} onChange={e => setCountry(e.target.value.toUpperCase())} />
          <DateRangePicker value={range} onChange={setRange} />
        </div>
      </div>

      {/* Disclaimer */}
      <div className="flex items-start gap-3 bg-amber-900/20 border border-amber-700/40 rounded-xl p-4 text-sm text-amber-200">
        <AlertCircle className="w-5 h-5 text-amber-400 flex-shrink-0 mt-0.5" />
        <div>
          <p className="font-medium">Important Note on Tone Data</p>
          <p className="text-amber-300/80 mt-1">GDELT's AvgTone reflects the <em>textual reporting tone</em> of news articles, not a definitive measure of public opinion, factual truth, or real-world sentiment. Positive tone means articles use more positive language; negative tone means more negative language. Interpret with caution.</p>
        </div>
      </div>

      {/* Overall metric */}
      {overallTone != null && (
        <div className="card inline-block">
          <p className="metric-label">Overall Average Tone</p>
          <p className={`text-3xl font-bold mt-1 ${overallTone >= 0 ? 'text-green-400' : 'text-red-400'}`}>
            {overallTone.toFixed(3)}
          </p>
          <p className="text-xs text-slate-500 mt-0.5">{overallTone >= 0 ? 'Net positive reporting tone' : 'Net negative reporting tone'}</p>
        </div>
      )}

      {/* Tone over time */}
      <div className="card">
        <h3 className="section-title mb-4">Tone Trend Over Time</h3>
        {tone.isLoading ? <ChartSkeleton /> : trendData.length === 0 ? (
          <EmptyState message="No tone data" />
        ) : (
          <ResponsiveContainer width="100%" height={350}>
            <LineChart data={trendData}>
              <CartesianGrid strokeDasharray="3 3" stroke="#334155" />
              <XAxis dataKey="date" tick={{ fill: '#94a3b8', fontSize: 10 }} />
              <YAxis tick={{ fill: '#94a3b8', fontSize: 11 }} />
              <Tooltip contentStyle={{ background: '#1e293b', border: '1px solid #334155', borderRadius: 8, color: '#f1f5f9' }} />
              <ReferenceLine y={0} stroke="#475569" strokeDasharray="3 3" />
              <Line type="monotone" dataKey="avg_tone" stroke="#ec4899" strokeWidth={2} dot={false} name="Avg Tone" />
            </LineChart>
          </ResponsiveContainer>
        )}
      </div>

      {/* Interpretation guide */}
      <div className="card">
        <h3 className="section-title mb-4">Tone Interpretation Guide</h3>
        <div className="grid grid-cols-1 sm:grid-cols-3 gap-4 text-sm">
          <div className="bg-green-900/20 border border-green-700/40 rounded-lg p-4">
            <p className="font-medium text-green-400">Positive Tone (&gt; 0)</p>
            <p className="text-slate-400 mt-1">Articles use predominantly positive language. Does NOT mean events are good — it means reporting uses positive words.</p>
          </div>
          <div className="bg-slate-700/40 border border-slate-600 rounded-lg p-4">
            <p className="font-medium text-slate-300">Neutral (~0)</p>
            <p className="text-slate-400 mt-1">Balanced reporting tone with no strong positive or negative language bias.</p>
          </div>
          <div className="bg-red-900/20 border border-red-700/40 rounded-lg p-4">
            <p className="font-medium text-red-400">Negative Tone (&lt; 0)</p>
            <p className="text-slate-400 mt-1">Articles use predominantly negative language. More negative ≠ more severe — it reflects article word choice.</p>
          </div>
        </div>
      </div>
    </div>
  )
}

