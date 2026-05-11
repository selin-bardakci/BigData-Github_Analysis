import { Routes, Route } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import ErrorBoundary from "./components/ErrorBoundary";
import Overview from "./pages/Overview";
import D1Trends from "./pages/D1Trends";
import D2Forecasting from "./pages/D2Forecasting";
import D3Migration from "./pages/D3Migration";
import D4PageRank from "./pages/D4PageRank";
import D5Communities from "./pages/D5Communities";

function Boundary({ children }: { children: React.ReactNode }) {
  return <ErrorBoundary>{children}</ErrorBoundary>;
}

export default function App() {
  return (
    <div className="flex min-h-screen">
      <Sidebar />
      <main className="flex-1 min-w-0 overflow-x-hidden">
        <Routes>
          <Route path="/"   element={<Boundary><Overview /></Boundary>} />
          <Route path="/d1" element={<Boundary><D1Trends /></Boundary>} />
          <Route path="/d2" element={<Boundary><D2Forecasting /></Boundary>} />
          <Route path="/d3" element={<Boundary><D3Migration /></Boundary>} />
          <Route path="/d4" element={<Boundary><D4PageRank /></Boundary>} />
          <Route path="/d5" element={<Boundary><D5Communities /></Boundary>} />
        </Routes>
      </main>
    </div>
  );
}
