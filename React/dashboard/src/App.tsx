import { useState, useEffect, useMemo } from "react";

interface NetworkEvent {
  device: string;
  url: string;
  previous_status: string;
  status: string;
  status_code: number;
  latency_ms: number | null; //accommodate null values on failure
  error: string;
  timestamp: string;
}

function App() {
  const [historyLogs, setHistoryLogs] = useState<NetworkEvent[]>([]);
  const [activeDevices, setActiveDevices] = useState<
    Record<string, NetworkEvent>
  >({});
  const [isConnected, setIsConnected] = useState(true);

  useEffect(() => {
    // fetch timeline history
    fetch("http://127.0.0.1:8081/api/history")
      .then((response) => response.json())
      .then((data: NetworkEvent[]) => {
        if (data) setHistoryLogs(data);
      })
      .catch((error) =>
        console.error("Error fetching historical logs:", error),
      );

    // fetch active devices
    fetch("http://127.0.0.1:8081/api/devices")
      .then((response) => response.json())
      .then((data: NetworkEvent[]) => {
        if (data) {
          const deviceMap: Record<string, NetworkEvent> = {};
          data.forEach((d) => (deviceMap[d.device] = d));
          setActiveDevices(deviceMap);
        }
      })
      .catch((error) => console.error("Error fetching active devices:", error));

    // open persistent connection to the server for real-time updates
    const sseStream = new EventSource("http://127.0.0.1:8081/api/stream");
    sseStream.onopen = () => setIsConnected(true);

    // listen for incoming events
    sseStream.onmessage = (messageEvent) => {
      const liveEvent: NetworkEvent = JSON.parse(messageEvent.data);

      // always update the active devices map with the latest event
      setActiveDevices((prevDevices) => ({
        ...prevDevices,
        [liveEvent.device]: liveEvent,
      }));

      // only update state to the timeline table if a state transition has occurred
      if (liveEvent.status !== liveEvent.previous_status) {
        setHistoryLogs((prevHistory) => [liveEvent, ...prevHistory]);
      }
    };

    sseStream.onerror = (error) => {
      setIsConnected(false);
      console.error("SSE connection error:", error);
    };

    // cleanup function to close the SSE connection when the component unmounts
    return () => {
      sseStream.close();
      console.warn(
        "SSE connection interrupted. Browser will attempt to reconnect automatically.",
      );
    };
  }, []);

  // helper UI function for status color coding
  const getStatusColor = (status: string) => {
    switch (status) {
      case "UP":
        return "bg-green-500/10 text-green-400 border-green-500/20";
      case "DOWN":
        return "bg-red-500/10 text-red-400 border-red-500/20";
      case "TIMEOUT":
        return "bg-yellow-500/10 text-yellow-400 border-yellow-500/20";
      default:
        return "bg-gray-500/10 text-gray-400 border-gray-500/20";
    }
  };

  return (
    <>
      <div className="min-h-screen bg-gray-950 text-gray-100 p-8 font-sans">
        {/* Header */}
        <header className="mb-10 flex items-center justify-between border-b border-gray-800 pb-6">
          <div>
            <h1 className="text-3xl font-bold tracking-tight text-white flex items-center gap-3">
              <svg
                className="w-8 h-8 text-blue-500"
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
                xmlns="http://www.w3.org/2000/svg"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M13 10V3L4 14h7v7l9-11h-7z"
                />
              </svg>
              NetPulse Main Dashboard
            </h1>
          </div>
          <div className="flex items-center gap-2 px-4 py-2 bg-gray-900 rounded-full border border-gray-800">
            <div
              className={`w-3 h-3 rounded-full ${isConnected ? "bg-green-500 animate-pulse" : "bg-red-500"}`}
            ></div>
            <span className="text-sm font-medium">
              {isConnected ? "Live Telemetry Active" : "Connecting..."}
            </span>
          </div>
        </header>

        {/* Active Devices Grid */}
        <section className="mb-12">
          <h2 className="text-xl font-semibold mb-4 text-gray-200">
            Active Targets ({Object.keys(activeDevices).length})
          </h2>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {Object.values(activeDevices).map((device) => (
              <div
                key={device.device}
                className="bg-gray-900 border border-gray-800 rounded-xl p-5 shadow-lg relative overflow-hidden"
              >
                {/* Background Ping Flash Animation */}
                <div
                  key={device.timestamp}
                  className="absolute inset-0 bg-green-500/20 opacity-0 animate-[ping_1s_ease-out]"
                ></div>

                <div className="flex justify-between items-start mb-4 relative z-10">
                  <h3 className="font-medium text-lg truncate pr-2">
                    {device.device}
                  </h3>
                  <span
                    className={`px-2.5 py-1 rounded-md text-xs font-bold border ${getStatusColor(device.status)}`}
                  >
                    {device.status}
                  </span>
                </div>
                <div className="text-sm text-gray-400 mb-1 truncate">
                  {device.url}
                </div>
                <div className="flex justify-between items-end mt-4 relative z-10">
                  <div className="text-xs text-gray-500">
                    Last Ping:{" "}
                    <span className="text-gray-300 font-mono">
                      {new Date(device.timestamp).toLocaleTimeString()}
                    </span>
                  </div>
                  <div className="text-2xl font-light font-mono text-gray-200">
                    {device.latency_ms !== null
                      ? `${device.latency_ms}ms`
                      : "---"}
                  </div>
                </div>
              </div>
            ))}
          </div>
        </section>

        {/* Transition Log Table */}
        <section>
          <h2 className="text-xl font-semibold mb-4 text-gray-200">
            Transition Audit Log
          </h2>
          <div className="bg-gray-900 border border-gray-800 rounded-xl overflow-hidden shadow-lg">
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead className="bg-gray-800/50 text-gray-400">
                  <tr>
                    <th className="px-6 py-4 font-medium">Timestamp</th>
                    <th className="px-6 py-4 font-medium">Target Device</th>
                    <th className="px-6 py-4 font-medium">State Transition</th>
                  </tr>
                </thead>
                <tbody className="divide-y divide-gray-800">
                  {historyLogs.map((ev, idx) => (
                    <tr
                      key={`${ev.device}-${ev.timestamp}-${idx}`}
                      className="hover:bg-gray-800/20 transition-colors"
                    >
                      <td className="px-6 py-4 whitespace-nowrap text-gray-400 font-mono text-xs">
                        {new Date(ev.timestamp).toLocaleString()}
                      </td>
                      <td className="px-6 py-4 font-medium text-gray-200">
                        {ev.device}
                      </td>
                      <td className="px-6 py-4">
                        <div className="flex items-center gap-2 font-mono text-xs font-bold">
                          <span
                            className={`px-2 py-0.5 rounded border ${getStatusColor(ev.previous_status)}`}
                          >
                            {ev.previous_status || "UNKNOWN"}
                          </span>
                          <span className="text-gray-500">→</span>
                          <span
                            className={`px-2 py-0.5 rounded border ${getStatusColor(ev.status)}`}
                          >
                            {ev.status}
                          </span>
                        </div>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </section>
      </div>
    </>
  );
}

export default App;
