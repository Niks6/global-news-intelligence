import { useState, useMemo } from 'react'
import { subDays } from 'date-fns'
import { Map as MapIcon, Layers } from 'lucide-react'
import { useMapEvents, useMapCountries } from '../hooks/useApi'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import type { DateRange } from '../types'
import { MapContainer, TileLayer, CircleMarker, Popup, useMap } from 'react-leaflet'

export default function MapPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const [mode, setMode] = useState<'markers' | 'choropleth'>('markers')
  const mapEvents = useMapEvents(range)
  const mapCountries = useMapCountries(range)

  const markers = mapEvents.data?.markers || []
  const noCoords = mapEvents.data?.no_coordinates_count || 0

  const toneColor = (tone: number | null) => {
    if (tone == null) return '#64748b'
    if (tone >= 2) return '#22c55e'
    if (tone >= 0) return '#84cc16'
    if (tone >= -2) return '#f59e0b'
    return '#ef4444'
  }

  return (
    <div className="p-6 space-y-4 max-w-[1600px] mx-auto">
      <div className="flex items-center justify-between flex-wrap gap-4">
        <div>
          <h1 className="text-2xl font-bold text-slate-100 flex items-center gap-2">
            <MapIcon className="w-6 h-6 text-teal-400" /> Geographic Explorer
          </h1>
          <p className="text-sm text-slate-400 mt-1">Event distribution across the globe</p>
        </div>
        <div className="flex gap-3 items-center">
          <button onClick={() => setMode(mode === 'markers' ? 'choropleth' : 'markers')} className="btn-secondary flex items-center gap-1.5">
            <Layers className="w-4 h-4" /> {mode === 'markers' ? 'Switch to Choropleth' : 'Switch to Markers'}
          </button>
          <DateRangePicker value={range} onChange={setRange} />
        </div>
      </div>

      {noCoords > 0 && (
        <p className="text-xs text-slate-500 bg-slate-800 rounded-lg px-3 py-1.5 inline-block">
          ⚠ {noCoords.toLocaleString()} events have no coordinates and are not shown on the map.
        </p>
      )}

      {/* Map */}
      <div className="card p-0 overflow-hidden rounded-xl" style={{ height: 'calc(100vh - 220px)', minHeight: 400 }}>
        {mapEvents.isLoading ? (
          <div className="flex items-center justify-center h-full text-slate-400">Loading map data…</div>
        ) : markers.length === 0 ? (
          <div className="flex items-center justify-center h-full">
            <EmptyState message="No geo-located events" description="Try expanding the date range" />
          </div>
        ) : (
          <MapContainer center={[20, 0]} zoom={2} className="h-full w-full" scrollWheelZoom={true}>
            <TileLayer
              attribution='&copy; <a href="https://carto.com">CARTO</a>'
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            />
            {markers.slice(0, 2000).map((m: any, i: number) => (
              <CircleMarker
                key={`${m.event_id}-${i}`}
                center={[m.lat, m.lng]}
                radius={4}
                pathOptions={{ fillColor: toneColor(m.avg_tone), color: '#0f172a', weight: 1, fillOpacity: 0.7 }}
              >
                <Popup>
                  <div className="text-xs space-y-1" style={{ minWidth: 160 }}>
                    <p className="font-semibold text-slate-800">{m.event_name || m.event_code}</p>
                    <p>Country: {m.country_code || '—'}</p>
                    <p>Tone: <span style={{ color: toneColor(m.avg_tone) }}>{m.avg_tone?.toFixed(2) ?? '—'}</span></p>
                    <p>Goldstein: {m.goldstein?.toFixed(1) ?? '—'}</p>
                    {m.source_url && <a href={m.source_url} target="_blank" rel="noopener noreferrer" className="text-blue-600 underline">Source ↗</a>}
                  </div>
                </Popup>
              </CircleMarker>
            ))}
          </MapContainer>
        )}
      </div>

      {/* Legend */}
      <div className="flex gap-4 items-center text-xs text-slate-400">
        <span>Tone color:</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-green-500 inline-block" /> Positive (≥2)</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-lime-500 inline-block" /> Mild Positive</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-yellow-500 inline-block" /> Mild Negative</span>
        <span className="flex items-center gap-1"><span className="w-3 h-3 rounded-full bg-red-500 inline-block" /> Negative (≤-2)</span>
      </div>
    </div>
  )
}

