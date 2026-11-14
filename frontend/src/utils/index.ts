import { clsx, type ClassValue } from 'clsx'
import { twMerge } from 'tailwind-merge'
import { format, parseISO, formatDistanceToNow } from 'date-fns'

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

export function formatDate(dateStr: string | number | Date, fmt = 'MMM d, yyyy HH:mm') {
  if (!dateStr) return '—'
  const d = typeof dateStr === 'string' ? parseISO(dateStr) : new Date(dateStr)
  return format(d, fmt) + ' UTC'
}

export function formatDateShort(dateStr: string | number | Date) {
  return formatDate(dateStr, 'MMM d')
}

export function timeAgo(dateStr: string | Date) {
  if (!dateStr) return '—'
  const d = typeof dateStr === 'string' ? parseISO(dateStr) : dateStr
  return formatDistanceToNow(d, { addSuffix: true })
}

export function formatNumber(n: number | null | undefined, decimals = 0) {
  if (n == null) return '—'
  if (Math.abs(n) >= 1_000_000) return (n / 1_000_000).toFixed(1) + 'M'
  if (Math.abs(n) >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return n.toFixed(decimals)
}

export function formatTone(tone: number | null | undefined) {
  if (tone == null) return '—'
  return tone > 0 ? `+${tone.toFixed(2)}` : tone.toFixed(2)
}

export function toneColor(tone: number): string {
  if (tone > 2) return '#22c55e'
  if (tone > 0) return '#86efac'
  if (tone > -2) return '#fca5a5'
  return '#ef4444'
}

export function goldsteinColor(val: number): string {
  if (val >= 5) return '#22c55e'
  if (val >= 0) return '#86efac'
  if (val >= -5) return '#fca5a5'
  return '#ef4444'
}

export function getDateRangePreset(preset: '24h' | '7d' | '30d'): { from: Date; to: Date } {
  const to = new Date()
  const from = new Date()
  if (preset === '24h') from.setDate(from.getDate() - 1)
  else if (preset === '7d') from.setDate(from.getDate() - 7)
  else if (preset === '30d') from.setDate(from.getDate() - 30)
  return { from, to }
}

export function isoDate(d: Date) {
  return d.toISOString().split('T')[0]
}

export function gdeltEventDate(day: number): string {
  const s = day.toString()
  return `${s.slice(0, 4)}-${s.slice(4, 6)}-${s.slice(6, 8)}`
}

export function cameoRootName(rootCode: string): string {
  const map: Record<string, string> = {
    '01': 'Make Public Statement',
    '02': 'Appeal',
    '03': 'Express Intent',
    '04': 'Consult',
    '05': 'Engage in Diplomatic Cooperation',
    '06': 'Engage in Material Cooperation',
    '07': 'Provide Aid',
    '08': 'Yield',
    '09': 'Investigate',
    '10': 'Demand',
    '11': 'Disapprove',
    '12': 'Reject',
    '13': 'Threaten',
    '14': 'Protest',
    '15': 'Exhibit Force Posture',
    '16': 'Reduce Relations',
    '17': 'Coerce',
    '18': 'Assault',
    '19': 'Fight',
    '20': 'Use Unconventional Mass Violence',
  }
  return map[rootCode] || `Code ${rootCode}`
}

export function quadClassName(quadClass: number): string {
  const map: Record<number, string> = {
    1: 'Verbal Cooperation',
    2: 'Material Cooperation',
    3: 'Verbal Conflict',
    4: 'Material Conflict',
  }
  return map[quadClass] || 'Unknown'
}

export function countryFlag(countryCode: string): string {
  if (!countryCode || countryCode.length !== 2) return '🌐'
  const codePoints = [...countryCode.toUpperCase()].map(
    (char) => 127397 + char.charCodeAt(0)
  )
  return String.fromCodePoint(...codePoints)
}

export function severityColor(severity: 'critical' | 'high' | 'medium'): string {
  if (severity === 'critical') return '#ef4444'
  if (severity === 'high') return '#f59e0b'
  return '#3b82f6'
}

export function downloadCSV(data: Record<string, unknown>[], filename: string) {
  if (!data.length) return
  const headers = Object.keys(data[0])
  const rows = data.map((row) =>
    headers.map((h) => JSON.stringify(row[h] ?? '')).join(',')
  )
  const csv = [headers.join(','), ...rows].join('\n')
  const blob = new Blob([csv], { type: 'text/csv' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = filename
  a.click()
  URL.revokeObjectURL(url)
}
