import "./App.css";
import { BrowserRouter, Routes, Route } from "react-router-dom";
import Dashboard from "./pages/Dashboard";
import AgentOperations from "./pages/AgentOperations";
import ReviewQueue from "./pages/ReviewQueue";
import Vendors from "./pages/Vendors";
import Exceptions from "./pages/Exceptions";
import Payments from "./pages/Payments";
import { LiveRefreshProvider } from "./lib/liveRefreshContext";

function App() {
  return (
    <div className="App">
      <BrowserRouter>
        <LiveRefreshProvider>
          <Routes>
            <Route path="/" element={<Dashboard />} />
            <Route path="/agent-operations" element={<AgentOperations />} />
            <Route path="/review-queue" element={<ReviewQueue />} />
            <Route path="/vendors" element={<Vendors />} />
            <Route path="/exceptions" element={<Exceptions />} />
            <Route path="/payments" element={<Payments />} />
          </Routes>
        </LiveRefreshProvider>
      </BrowserRouter>
    </div>
  );
}

export default App;
