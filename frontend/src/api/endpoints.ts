import client from './client'
import type {
  KPIMetrics, EventTimeSeries, TopicCount, CountryMetric,
  CategoryDistribution, GDELTEvent, PaginatedResponse,
  AnomalyAlert, PipelineStatus, MapEventMarker, ChoroplethData,
  ToneTrend, ConflictCooperationSummary, TopicTimeSeries
} from '../types'

// ---------- Overview ----------
export const fetchOverview = async (from: string, to: string) => {
  const { data } = await client.get('/api/overview', { params: { from_dt: from, to_dt: to } })
  return data
}

// ---------- Events ----------
export const fetchEvents = async (params: Record<string, any>) => {
  const { data } = await client.get('/api/events', { params })
  return data
}

export const fetchEventById = async (id: number) => {
  const { data } = await client.get(`/api/events/${id}`)
  return data
}

export const fetchEventMentions = async (id: number) => {
  const { data } = await client.get(`/api/events/${id}/mentions`)
  return data
}

// ---------- Topics ----------
export const fetchTopics = async (params: Record<string, any>) => {
  const { data } = await client.get('/api/topics', { params })
  return data
}

export const fetchTopicTrend = async (topic: string, from: string, to: string) => {
  const { data } = await client.get(`/api/topics/${encodeURIComponent(topic)}/trend`, { params: { from_dt: from, to_dt: to } })
  return data
}

export const fetchTopicCountries = async (topic: string, from: string, to: string) => {
  const { data } = await client.get(`/api/topics/${encodeURIComponent(topic)}/countries`, { params: { from_dt: from, to_dt: to } })
  return data
}

// ---------- Countries ----------
export const fetchCountryList = async () => {
  const { data } = await client.get('/api/countries')
  return data
}

export const fetchCountryOverview = async (code: string, from: string, to: string) => {
  const { data } = await client.get(`/api/countries/${code}/overview`, { params: { from_dt: from, to_dt: to } })
  return data
}

export const fetchCountryEvents = async (code: string, params: Record<string, any>) => {
  const { data } = await client.get(`/api/countries/${code}/events`, { params })
  return data
}

export const fetchCountryTopics = async (code: string, from: string, to: string) => {
  const { data } = await client.get(`/api/countries/${code}/topics`, { params: { from_dt: from, to_dt: to } })
  return data
}

// ---------- Comparison ----------
export const fetchCompareCountries = async (codes: string[], from: string, to: string) => {
  const { data } = await client.get('/api/compare/countries', { params: { codes: codes.join(','), from_dt: from, to_dt: to } })
  return data
}

// ---------- Analytics ----------
export const fetchCategories = async (params: Record<string, any>) => {
  const { data } = await client.get('/api/analytics/categories', { params })
  return data
}

export const fetchTone = async (params: Record<string, any>) => {
  const { data } = await client.get('/api/analytics/tone', { params })
  return data
}

export const fetchCooperationConflict = async (params: Record<string, any>) => {
  const { data } = await client.get('/api/analytics/cooperation-conflict', { params })
  return data
}

export const fetchAnomalies = async (params: Record<string, any>) => {
  const { data } = await client.get('/api/analytics/anomalies', { params })
  return data
}

// ---------- Map ----------
export const fetchMapEvents = async (params: Record<string, any>) => {
  const { data } = await client.get('/api/map/events', { params })
  return data
}

export const fetchMapCountries = async (from: string, to: string) => {
  const { data } = await client.get('/api/map/countries', { params: { from_dt: from, to_dt: to } })
  return data
}

// ---------- Search ----------
export const fetchSearch = async (q: string, params: Record<string, any>) => {
  const { data } = await client.get('/api/search', { params: { q, ...params } })
  return data
}

// ---------- Health ----------
export const fetchHealth = async () => {
  const { data } = await client.get('/api/health')
  return data
}

export const fetchPipelineHealth = async () => {
  const { data } = await client.get('/api/health/pipeline')
  return data
}

// ---------- Regions ----------
export const fetchRegionOverview = async (regionId: string, from: string, to: string) => {
  const { data } = await client.get(`/api/regions/${regionId}/overview`, { params: { from_dt: from, to_dt: to } })
  return data
}

