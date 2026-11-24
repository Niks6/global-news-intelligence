import { useState, useMemo } from 'react'
import { subDays } from 'date-fns'
import { Map as MapIcon, Layers } from 'lucide-react'
import { useMapEvents, useMapCountries } from '../hooks/useApi'
import DateRangePicker from '../components/DateRangePicker'
import EmptyState from '../components/EmptyState'
import type { DateRange } from '../types'
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet'

const COUNTRY_CENTROIDS: Record<string, [number, number]> = {
  US: [38.9, -77.0], UK: [51.5, -0.1], FR: [48.9, 2.3], GM: [52.5, 13.4],
  RS: [55.8, 37.6], CH: [35.9, 104.2], JA: [36.2, 138.3], IN: [20.6, 78.9],
  BR: [-14.2, -51.9], AU: [-25.3, 133.8], CA: [56.1, -106.3], SF: [-30.6, 22.9],
  EG: [26.8, 30.8], TU: [38.9, 35.2], SA: [23.9, 45.1], NI: [9.1, 8.7],
  IS: [31.0, 34.9], KE: [-0.0, 37.9], MX: [23.6, -102.6], ID: [-0.8, 113.9],
  UP: [48.4, 31.2], IR: [32.4, 53.7], IZ: [33.2, 43.7], PK: [30.4, 69.3],
  IT: [41.9, 12.6], SP: [40.5, -3.7], PL: [51.9, 19.1], TH: [15.9, 100.9],
  VM: [14.1, 108.3], KS: [35.9, 127.8], SY: [34.8, 38.9], AF: [33.9, 67.7],
}

export default function MapPage() {
  const [range, setRange] = useState<DateRange>({ from: subDays(new Date(), 7), to: new Date() })
  const [mode, setMode] = useState<'markers' | 'choropleth'>('markers')
  const mapEvents = useMapEvents(range)
  const mapCountries = useMapCountries(range)

  const markers = useMemo(
    () => (mapEvents.data?.markers || []).filter((m: any) => typeof m.lat === 'number' && typeof m.lng === 'number'),
    [mapEvents.data]
  )
  const countryBubbles = useMemo(() => {
    const raw = mapCountries.data?.data || []
    return raw
      .map((c: any) => {
        const coords = COUNTRY_CENTROIDS[c.country_code]
        if (!coords) return null
        return { ...c, lat: coords[0], lng: coords[1] }
      })
      .filter(Boolean)
  }, [mapCountries.data])

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
            <Layers className="w-4 h-4" /> {mode === 'markers' ? 'Switch to Country Volume' : 'Switch to Event Markers'}
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
        ) : (mode === 'markers' ? markers.length === 0 : countryBubbles.length === 0) ? (
          <div className="flex items-center justify-center h-full">
            <EmptyState message="No geo-located events" description="Try expanding the date range" />
          </div>
        ) : (
          <MapContainer center={[20, 0]} zoom={2} className="h-full w-full" scrollWheelZoom={true}>
            <TileLayer
              attribution='&copy; <a href="https://carto.com">CARTO</a>'
              url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
            />
            {mode === 'markers'
              ? markers.slice(0, 2000).map((m: any, i: number) => (
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
                ))
              : countryBubbles.map((c: any) => (
                  <CircleMarker
                    key={c.country_code}
                    center={[c.lat, c.lng]}
                    radius={Math.max(6, Math.min(28, 6 + (c.normalized || 0) * 22))}
                    pathOptions={{ fillColor: '#14b8a6', color: '#0f172a', weight: 1.5, fillOpacity: 0.65 }}
                  >
                    <Popup>
                      <div className="text-xs space-y-1">
                        <p className="font-semibold text-slate-800">{c.country_name || c.country_code}</p>
                        <p>Events: {(c.event_count ?? c.value ?? 0).toLocaleString()}</p>
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
