import { Outlet, NavLink, useLocation } from 'react-router-dom'
import {
  Globe2, Zap, TrendingUp, MapPin, BarChart2, Map,
  Swords, Heart, AlertTriangle, Activity, ChevronRight
} from 'lucide-react'

const navItems = [
  { to: '/overview', icon: Globe2, label: 'Global Overview' },
  { to: '/events', icon: Zap, label: 'Live Events' },
  { to: '/topics', icon: TrendingUp, label: 'Topics & Trends' },
  { to: '/country/US', icon: MapPin, label: 'Country Intel' },
  { to: '/compare', icon: BarChart2, label: 'Country Compare' },
  { to: '/map', icon: Map, label: 'Geographic Map' },
  { to: '/conflict', icon: Swords, label: 'Conflict & Coop' },
  { to: '/sentiment', icon: Heart, label: 'Sentiment & Tone' },
  { to: '/anomalies', icon: AlertTriangle, label: 'Anomalies' },
  { to: '/pipeline', icon: Activity, label: 'Pipeline Health' },
]

export default function Layout() {
  return (
    <div className="flex h-screen overflow-hidden bg-slate-900">
      {/* Sidebar */}
      <aside className="w-60 flex-shrink-0 bg-slate-900 border-r border-slate-700/60 flex flex-col">
        {/* Logo */}
        <div className="px-4 py-5 border-b border-slate-700/60">
          <div className="flex items-center gap-2.5">
            <div className="w-8 h-8 rounded-lg bg-blue-600 flex items-center justify-center">
              <Globe2 className="w-5 h-5 text-white" />
            </div>
            <div>
              <p className="text-slate-100 font-semibold text-sm leading-tight">Global News</p>
              <p className="text-slate-400 text-xs">Intelligence Platform</p>
            </div>
          </div>
        </div>

        {/* Nav */}
        <nav className="flex-1 py-3 overflow-y-auto">
          {navItems.map(({ to, icon: Icon, label }) => (
            <NavLink
              key={to}
              to={to}
              className={({ isActive }) =>
                `flex items-center gap-3 px-4 py-2.5 mx-2 rounded-lg text-sm font-medium transition-colors group ${
                  isActive
                    ? 'bg-blue-600/20 text-blue-400 border border-blue-500/30'
                    : 'text-slate-400 hover:text-slate-100 hover:bg-slate-800'
                }`
              }
            >
              <Icon className="w-4 h-4 flex-shrink-0" />
              <span className="flex-1 truncate">{label}</span>
              <ChevronRight className="w-3 h-3 opacity-0 group-hover:opacity-50 transition-opacity" />
            </NavLink>
          ))}
        </nav>

        {/* Footer */}
        <div className="px-4 py-3 border-t border-slate-700/60">
          <p className="text-xs text-slate-500">Powered by GDELT Project</p>
          <p className="text-xs text-slate-600 mt-0.5">Academic Research Platform</p>
        </div>
      </aside>

      {/* Main content */}
      <main className="flex-1 overflow-y-auto">
        <Outlet />
      </main>
    </div>
  )
}

