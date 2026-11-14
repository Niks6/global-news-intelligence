import { BrowserRouter, Routes, Route, Navigate } from 'react-router-dom'
import Layout from './components/Layout'
import OverviewPage from './pages/OverviewPage'
import EventsPage from './pages/EventsPage'
import TopicsPage from './pages/TopicsPage'
import CountryPage from './pages/CountryPage'
import ComparePage from './pages/ComparePage'
import MapPage from './pages/MapPage'
import ConflictPage from './pages/ConflictPage'
import SentimentPage from './pages/SentimentPage'
import AnomaliesPage from './pages/AnomaliesPage'
import PipelinePage from './pages/PipelinePage'

export default function App() {
  return (
    <BrowserRouter>
      <Routes>
        <Route path="/" element={<Layout />}>
          <Route index element={<Navigate to="/overview" replace />} />
          <Route path="overview" element={<OverviewPage />} />
          <Route path="events" element={<EventsPage />} />
          <Route path="topics" element={<TopicsPage />} />
          <Route path="country/:code" element={<CountryPage />} />
          <Route path="compare" element={<ComparePage />} />
          <Route path="map" element={<MapPage />} />
          <Route path="conflict" element={<ConflictPage />} />
          <Route path="sentiment" element={<SentimentPage />} />
          <Route path="anomalies" element={<AnomaliesPage />} />
          <Route path="pipeline" element={<PipelinePage />} />
        </Route>
      </Routes>
    </BrowserRouter>
  )
}

