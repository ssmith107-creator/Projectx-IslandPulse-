# 🇯🇲 IslandPulse — Jamaica Emergency Resource Mesh

> *"Keep the island connected when traditional infrastructure goes offline."*

A decentralized resource management system simulating Jamaica's 14-parish logistics network as independent Docker-containerized nodes, solving the **split-brain problem** in distributed systems to ensure emergency supplies reach those who need them most during a crisis.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     IslandPulse Mesh                            │
│                                                                 │
│  ┌─────────────┐    Redis Pub/Sub    ┌──────────────────────┐  │
│  │  Kingston   │◄──────────────────►│  Parish Replica Nodes│  │
│  │  (Primary)  │                     │  St. Andrew          │  │
│  │  Leader     │   WebSocket         │  St. Catherine       │  │
│  │  Raft Term  │◄── Dashboard ──────►│  Manchester          │  │
│  └──────┬──────┘                     │  ... 10 more         │  │
│         │                            └──────────────────────┘  │
│         │  Quorum: 8/14 nodes required for write consensus      │
│         │                                                       │
│         ▼                                                       │
│  Vector Clock: { kingston:42, stAndrew:38, clarendon:29... }   │
└─────────────────────────────────────────────────────────────────┘
```

## Key Distributed Systems Concepts Implemented

### 1. Split-Brain Problem & Quorum
When network partitions occur, nodes on each side of the partition may believe they are the only surviving partition and accept writes independently, leading to divergent state. IslandPulse solves this with **majority quorum**:

- **14 total nodes** (one per Jamaica parish)
- **Quorum = 8 nodes** (majority) required for write operations
- If fewer than 8 nodes are reachable, the minority partition enters **read-only mode**
- Prevents two partitions from independently accepting conflicting emergency resource allocations

### 2. Vector Clocks (Lamport Timestamps)
Each node maintains a logical clock that ticks on every write. When nodes sync, they **merge** clocks by taking the maximum value per node:

```javascript
function mergeClocks(a, b) {
  const merged = { ...a };
  Object.keys(b).forEach(k => {
    merged[k] = Math.max(merged[k] || 0, b[k] || 0);
  });
  return merged;
}
```

This provides **causal consistency** — we know that event A happened-before event B if `A.clock[node] < B.clock[node]` for all nodes. Critical for ordering emergency resource transfers.

### 3. Raft-Style Leader Election
When the primary node (Kingston) goes offline, surviving nodes elect a new leader:

1. Nodes detect leader failure via missed heartbeats
2. Candidates rank themselves by `uptime + (term × 1000)` — longest-running node wins
3. Winner increments the **term number** (prevents split votes across reboots)
4. New primary begins accepting writes and broadcasting state

### 4. Auto-Rebalancing (Eager Anti-Entropy)
Every 5 seconds, the coordinator scans resource levels across all parishes. If any node drops below its **critical threshold**, the system automatically pulls from the richest online node — prioritizing population-weighted demand.

### 5. Redis as Distributed Backbone
Redis pub/sub acts as the message bus even when the primary coordinator is unreachable. Each parish node subscribes to:
- `mesh:heartbeat` — liveness detection
- `mesh:resources:{parishId}` — resource state updates
- `mesh:events` — crisis and sync notifications
- `mesh:vclock` — vector clock propagation

---

## Resource Types

| Resource | Unit | Critical Threshold | Nominal |
|----------|------|--------------------|---------|
| Water    | Gallons | 5,000 | 50,000 |
| Food     | Rations | 200 | 2,000 |
| Medical  | Kits | 50 | 500 |
| Fuel     | Liters | 1,000 | 15,000 |
| Power    | kWh | 500 | 8,000 |
| Comms    | Units | 10 | 100 |

---

## Crisis Scenarios

| Type | Resources Affected | Severity |
|------|--------------------|----------|
| Hurricane | Water −35%, Food −25%, Fuel −45% | High |
| Earthquake | Medical −55%, Comms −65% | High |
| Flash Flooding | Water −15%, Power −55% | Medium |
| Grid Failure | Power −85%, Comms −35% | Critical |
| Supply Disruption | Food −45%, Medical −35% | Medium |

---

## Quick Start

### Prerequisites
- Docker & Docker Compose v2
- 4GB RAM minimum (16 containers)

### Run the full mesh:

```bash
# Clone and launch all 14 parish nodes + Redis + UI
git clone <repo>
cd islandpulse

docker compose -f docker/docker-compose.yml up -d

# View all running containers
docker compose ps

# Open the dashboard
open http://localhost:8080

# Watch live logs from all parish nodes
docker compose logs -f --tail=50

# Kill a specific parish to trigger leader election
docker stop islandpulse-kingston-primary

# Watch St. Andrew become the new primary!
docker compose logs islandpulse-st-andrew --follow

# Restore Kingston
docker start islandpulse-kingston-primary
```

### API Endpoints (Backend on :3001)

```bash
# Full mesh state
GET  /api/state

# All parish nodes
GET  /api/nodes

# Specific node + resources
GET  /api/nodes/:id

# Toggle node status
POST /api/nodes/:id/status        { "status": "offline" | "online" | "degraded" }

# Manual resource transfer
POST /api/transfer                { "from": "kingston", "to": "portland", "resource": "water", "amount": 5000 }

# Trigger crisis event
POST /api/crisis                  { "parish": "stThomas", "type": "HURRICANE" }

# Resolve alert
POST /api/crisis/:alertId/resolve

# Force global rebalance
POST /api/rebalance

# Mesh analytics
GET  /api/analytics

# WebSocket (real-time)
WS   ws://localhost:3001          Streams: init | state | event | sync | alert
```

---

## Dashboard Features

| Feature | Description |
|---------|-------------|
| **Live Map** | Jamaica parish map showing node status, connections, crisis zones |
| **Node List** | All 14 parishes with latency, role, status controls |
| **Resource Bars** | Per-parish and aggregate resource levels with trend indicators |
| **Sync Log** | Real-time inter-node resource transfer feed |
| **Vector Clock** | Live Lamport timestamp display per node |
| **Event Stream** | System events: heartbeats, elections, crises, rebalances |
| **Crisis Simulation** | Trigger hurricanes, earthquakes, grid failures on any parish |
| **Manual Transfer** | Direct resource allocation between any two nodes |
| **Node Controls** | Kill/degrade/restore any parish node — watch leader election happen |

---

## Project Structure

```
islandpulse/
├── backend/
│   ├── server.js          # Express + WebSocket coordinator
│   ├── package.json
│   └── Dockerfile
├── frontend/
│   └── index.html         # Full dashboard (self-contained)
├── docker/
│   ├── docker-compose.yml # 16-container mesh
│   └── nginx.conf         # Frontend + API proxy
└── README.md
```

---

## Why This Matters

During Hurricane Beryl (2024), Jamaica experienced widespread infrastructure failures that demonstrated the exact problems IslandPulse addresses:

- **Grid failures** cutting communications between parishes
- **Supply chain disruptions** preventing resource redistribution
- **Coordination failures** between relief organizations without shared state

IslandPulse proves that a distributed, offline-first architecture can maintain regional coordination even when 40–50% of nodes are unreachable — keeping water, food, medical supplies, and communications flowing to where they're needed most.

---

## Technologies

- **Node.js** — Coordinator runtime
- **Express** — REST API
- **WebSocket (ws)** — Real-time mesh state broadcast
- **Redis** — Pub/sub backbone, distributed cache
- **Docker** — Parish node isolation
- **Nginx** — Frontend serving + API proxy
- **Vector Clocks** — Causal consistency protocol
- **Raft** — Leader election algorithm
- **Quorum Consensus** — Split-brain prevention

---

*Built for Jamaica. Designed for resilience. Engineered for crisis.*
