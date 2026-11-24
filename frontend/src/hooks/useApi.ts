import { useQuery } from '@tanstack/react-query'
import { subDays, formatISO } from 'date-fns'
import type { DateRange } from '../types'
import * as api from '../api/endpoints'

function dtStr(d: Date): string { return formatISO(d) }

export function useDateRange(range: DateRange) {
  return { from: dtStr(range.from), to: dtStr(range.to) }
}

export function useOverview(range: DateRange) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['overview', from, to], queryFn: () => api.fetchOverview(from, to) })
}

export function useEvents(params: Record<string, any>, enabled: boolean = true) {
  return useQuery({ queryKey: ['events', params], queryFn: () => api.fetchEvents(params), enabled })
}

export function useSearchEvents(q: string, params: Record<string, any>) {
  return useQuery({
    queryKey: ['search', q, params],
    queryFn: () => api.fetchSearch(q, params),
    enabled: q.trim().length >= 2,
  })
}

export function useEventDetail(id: number | null) {
  return useQuery({ queryKey: ['event', id], queryFn: () => api.fetchEventById(id!), enabled: id !== null })
}

export function useTopics(range: DateRange, country?: string) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['topics', from, to, country], queryFn: () => api.fetchTopics({ from_dt: from, to_dt: to, country }) })
}

export function useTopicTrend(topic: string, range: DateRange) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['topicTrend', topic, from, to], queryFn: () => api.fetchTopicTrend(topic, from, to), enabled: !!topic })
}

export function useCountryList() {
  return useQuery({ queryKey: ['countryList'], queryFn: api.fetchCountryList, staleTime: 300_000 })
}

export function useCountryOverview(code: string, range: DateRange) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['countryOverview', code, from, to], queryFn: () => api.fetchCountryOverview(code, from, to), enabled: !!code })
}

export function useCompareCountries(codes: string[], range: DateRange) {
  const { from, to } = useDateRange(range)
  return useQuery({
    queryKey: ['compare', codes, from, to],
    queryFn: () => api.fetchCompareCountries(codes, from, to),
    enabled: codes.length >= 2,
  })
}

export function useCategories(range: DateRange, country?: string) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['categories', from, to, country], queryFn: () => api.fetchCategories({ from_dt: from, to_dt: to, country }) })
}

export function useTone(range: DateRange, country?: string) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['tone', from, to, country], queryFn: () => api.fetchTone({ from_dt: from, to_dt: to, country }) })
}

export function useCooperationConflict(range: DateRange, country?: string) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['coopConflict', from, to, country], queryFn: () => api.fetchCooperationConflict({ from_dt: from, to_dt: to, country }) })
}

export function useAnomalies(range: DateRange) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['anomalies', from, to], queryFn: () => api.fetchAnomalies({ from_dt: from, to_dt: to }) })
}

export function useMapEvents(range: DateRange, params?: Record<string, any>) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['mapEvents', from, to, params], queryFn: () => api.fetchMapEvents({ from_dt: from, to_dt: to, ...params }) })
}

export function useMapCountries(range: DateRange) {
  const { from, to } = useDateRange(range)
  return useQuery({ queryKey: ['mapCountries', from, to], queryFn: () => api.fetchMapCountries(from, to) })
}

export function usePipelineHealth() {
  return useQuery({ queryKey: ['pipelineHealth'], queryFn: api.fetchPipelineHealth, refetchInterval: 30_000 })
}

