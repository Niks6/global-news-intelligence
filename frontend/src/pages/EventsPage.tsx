import { useState } from 'react'
import { subDays, formatISO } from 'date-fns'
import { Zap, ExternalLink, Search } from 'lucide-react'
import { useEvents, useSearchEvents } from '../hooks/useApi'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import LoadingSkeleton from '../components/LoadingSkeleton'
import type { DateRange } from '../types'

const quadClassColors: Record<number, string> = { 1: 'text-green-400', 2: 'text-emerald-400', 3: 'text-orange-400', 4: 'text-red-400' }
const quadClassNames: Record<number, string> = { 1: 'Verbal Coop', 2: 'Material Coop', 3: 'Verbal Conflict', 4: 'Material Conflict' }

export default function EventsPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const [page, setPage] = useState(1)
  const [country, setCountry] = useState('')
  const [searchQ, setSearchQ] = useState('')

  const isSearching = searchQ.trim().length >= 2
  const params: Record<string, any> = {
    from_dt: formatISO(range.from), to_dt: formatISO(range.to),
    page, limit: 50,
  }
  if (country && !isSearching) params.country = country

  const events = useEvents(params, !isSearching)
  const searchResults = useSearchEvents(searchQ.trim(), params)
  const activeQuery = isSearching ? searchResults : events

  const items = activeQuery.data?.items || []
  const total = activeQuery.data?.total || 0
  const totalPages = activeQuery.data?.total_pages || 1

  return (
    <div className="p-6 space-y-6 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <Zap className="w-6 h-6 text-yellow-400" /> Live Events Feed
          </h1>
          <p className="text-sm text-slate-400 mt-1">Browse, search, and filter global events</p>
        </div>
        <DateRangePicker value={range} onChange={(r) => { setRange(r); setPage(1) }} />
      </div>

      {/* Filters */}
      <div className="flex gap-3 flex-wrap">
        <div className="relative flex-1 min-w-[200px] max-w-md">
          <Search className="w-4 h-4 absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
          <input
            placeholder="Search actors, locations, or URLs (min 2 chars)..."
            className="input-base w-full pl-9"
            value={searchQ}
            onChange={e => { setSearchQ(e.target.value); setPage(1) }}
          />
        </div>
        <input
          placeholder="Country (e.g. US)"
          className="input-base w-28"
          value={country}
          disabled={isSearching}
          onChange={e => { setCountry(e.target.value.toUpperCase()); setPage(1) }}
        />
      </div>

      {/* Table */}
      {activeQuery.isLoading ? <LoadingSkeleton rows={8} /> : items.length === 0 ? (
        <EmptyState message="No events found" description="Adjust filters or expand the date range" onRetry={() => activeQuery.refetch()} />
      ) : (
        <>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-700 text-slate-400 text-xs uppercase tracking-wider">
                  <th className="text-left py-3 px-3">Date</th>
                  <th className="text-left py-3 px-3">Actor 1</th>
                  <th className="text-left py-3 px-3">Actor 2</th>
                  <th className="text-left py-3 px-3">Event</th>
                  <th className="text-left py-3 px-3">Country</th>
                  <th className="text-right py-3 px-3">Goldstein</th>
                  <th className="text-right py-3 px-3">Tone</th>
                  <th className="text-right py-3 px-3">Mentions</th>
                  <th className="text-center py-3 px-3">Source</th>
                </tr>
              </thead>
              <tbody>
                {items.map((e: any, i: number) => {
                  const sqlStr = String(e.SQLDATE || '')
                  const dateLabel = sqlStr.length === 8 ? `${sqlStr.slice(0,4)}-${sqlStr.slice(4,6)}-${sqlStr.slice(6,8)}` : sqlStr
                  const qc = e.QuadClass || 0
                  return (
                    <tr key={e.GlobalEventID || i} className="border-b border-slate-800 hover:bg-slate-800/60 transition-colors">
                      <td className="py-2.5 px-3 text-slate-300 whitespace-nowrap">{dateLabel}</td>
                      <td className="py-2.5 px-3">
                        <span className="text-slate-200">{e.Actor1Name || '—'}</span>
                        {e.Actor1CountryCode && <span className="text-xs text-slate-500 ml-1">({e.Actor1CountryCode})</span>}
                      </td>
                      <td className="py-2.5 px-3">
                        <span className="text-slate-200">{e.Actor2Name || '—'}</span>
                        {e.Actor2CountryCode && <span className="text-xs text-slate-500 ml-1">({e.Actor2CountryCode})</span>}
                      </td>
                      <td className="py-2.5 px-3">
                        <span className={`text-xs font-medium ${quadClassColors[qc] || 'text-slate-400'}`}>{e.EventCategoryName || e.EventCode}</span>
                        <span className="text-slate-500 text-xs ml-1">{quadClassNames[qc] || ''}</span>
                      </td>
                      <td className="py-2.5 px-3 text-slate-300">{e.ActionGeo_CountryCode || '—'}</td>
                      <td className={`py-2.5 px-3 text-right font-mono text-xs ${(e.GoldsteinScale ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {e.GoldsteinScale != null ? e.GoldsteinScale.toFixed(1) : '—'}
                      </td>
                      <td className={`py-2.5 px-3 text-right font-mono text-xs ${(e.AvgTone ?? 0) >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                        {e.AvgTone != null ? e.AvgTone.toFixed(1) : '—'}
                      </td>
                      <td className="py-2.5 px-3 text-right text-slate-300">{e.NumMentions ?? '—'}</td>
                      <td className="py-2.5 px-3 text-center">
                        {e.SOURCEURL && <a href={e.SOURCEURL} target="_blank" rel="noopener noreferrer" className="text-blue-400 hover:text-blue-300"><ExternalLink className="w-3.5 h-3.5 inline" /></a>}
                      </td>
                    </tr>
                  )
                })}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          <div className="flex items-center justify-between text-sm text-slate-400">
            <span>Showing page {page} of {totalPages} ({total} results)</span>
            <div className="flex gap-2">
              <button disabled={page <= 1} onClick={() => setPage(p => p - 1)} className="btn-secondary disabled:opacity-40">Previous</button>
              <button disabled={page >= totalPages} onClick={() => setPage(p => p + 1)} className="btn-secondary disabled:opacity-40">Next</button>
            </div>
          </div>
        </>
      )}
    </div>
  )
}
