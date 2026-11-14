// Core GDELT event types
export interface GDELTEvent {
  GlobalEventID: number
  Day: number
  MonthYear: number
  Year: number
  FractionDate: number
  Actor1Code: string | null
  Actor1Name: string | null
  Actor1CountryCode: string | null
  Actor1Type1Code: string | null
  Actor2Code: string | null
  Actor2Name: string | null
  Actor2CountryCode: string | null
  Actor2Type1Code: string | null
  IsRootEvent: number
  EventCode: string
  EventBaseCode: string
  EventRootCode: string
  QuadClass: number
  GoldsteinScale: number
  NumMentions: number
  NumSources: number
  NumArticles: number
  AvgTone: number
  Actor1Geo_Type: number | null
  Actor1Geo_FullName: string | null
  Actor1Geo_CountryCode: string | null
  Actor2Geo_Type: number | null
  Actor2Geo_FullName: string | null
  Actor2Geo_CountryCode: string | null
  ActionGeo_Type: number | null
  ActionGeo_FullName: string | null
  ActionGeo_CountryCode: string | null
  ActionGeo_Lat: number | null
  ActionGeo_Long: number | null
  DATEADDED: number
  SOURCEURL: string
  // Derived
  EventCategoryName?: string
  EventDate?: string
}

export interface KPIMetrics {
  total_events: number
  total_articles: number
  total_mentions: number
  active_countries: number
  avg_tone: number
  avg_goldstein: number
}

export interface EventTimeSeries {
  date: string
  count: number
  avg_tone: number
  avg_goldstein: number
}

export interface TopicCount {
  topic: string
  count: number
  avg_tone: number
}

export interface CountryMetric {
  country_code: string
  country_name: string
  event_count: number
  mention_count: number
  avg_tone: number
  avg_goldstein: number
  lat?: number
  lng?: number
}

export interface CategoryDistribution {
  category: string
  category_name: string
  count: number
  pct: number
  is_conflict: boolean
}

export interface AnomalyAlert {
  id: string
  type: 'topic' | 'country' | 'category'
  label: string
  country_code?: string
  topic?: string
  baseline: number
  current: number
  pct_change: number
  z_score: number
  severity: 'critical' | 'high' | 'medium'
  spike_started: string
  event_count: number
}

export interface PipelineStatus {
  last_ingestion_time: string
  freshness_status: 'fresh' | 'stale' | 'old'
  files_today: number
  files_week: number
  records_accepted_today: number
  records_rejected_today: number
  kafka_status: 'online' | 'offline' | 'unknown'
  spark_status: 'online' | 'offline' | 'unknown'
  hdfs_status: 'online' | 'offline' | 'unknown'
  hbase_status: 'online' | 'offline' | 'unknown'
  api_status: 'online' | 'offline' | 'unknown'
  error_logs: ErrorLogEntry[]
  ingestion_history: IngestionRecord[]
}

export interface ErrorLogEntry {
  timestamp: string
  level: 'ERROR' | 'WARN' | 'INFO'
  message: string
  source: string
}

export interface IngestionRecord {
  date: string
  accepted: number
  rejected: number
}

export interface CountryDetail extends CountryMetric {
  event_timeline: EventTimeSeries[]
  top_topics: TopicCount[]
  category_distribution: CategoryDistribution[]
  conflict_timeline: ConflictTimeline[]
  recent_events: GDELTEvent[]
}

export interface ConflictTimeline {
  date: string
  cooperation: number
  conflict: number
}

export interface MapEventMarker {
  lat: number
  lng: number
  event_id: number
  event_code: string
  event_name: string
  country_code: string
  avg_tone: number
  goldstein: number
  date: string
  source_url: string
}

export interface ChoroplethData {
  country_code: string
  country_name: string
  value: number
  normalized: number
}

export interface DateRange {
  from: Date
  to: Date
}

export interface FilterState {
  dateRange: DateRange
  country?: string
  category?: string
  toneMin?: number
  toneMax?: number
  topic?: string
  actor?: string
}

export type TimePreset = '24h' | '7d' | '30d' | 'custom'

export interface PaginatedResponse<T> {
  items: T[]
  total: number
  page: number
  page_size: number
  total_pages: number
}

export interface ToneTrend {
  date: string
  avg_tone: number
  min_tone: number
  max_tone: number
  count: number
}

export interface GoldsteinTrend {
  date: string
  avg_goldstein: number
  conflict_pct: number
  cooperation_pct: number
}

export interface TopicTimeSeries {
  date: string
  [topic: string]: number | string
}

export interface ConflictCooperationSummary {
  cooperation_count: number
  conflict_count: number
  neutral_count: number
  cooperation_pct: number
  conflict_pct: number
  avg_goldstein: number
}
