const express = require('express');
const cors = require('cors');
const { createServer } = require('http');
const { WebSocketServer } = require('ws');
const { v4: uuidv4 } = require('uuid');

const app = express();
app.use(cors());
app.use(express.json());

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer });

// ─────────────────────────────────────────────
// JAMAICA PARISH NODE DEFINITIONS
// Real parishes with coordinates and population data
// ─────────────────────────────────────────────
const PARISHES = {
  kingston: {
    id: 'kingston', name: 'Kingston', region: 'southeast',
    population: 96052, lat: 17.9714, lng: -76.7936,
    isCapital: true, elevation: 5, coastalRisk: 'high'
  },
  stAndrew: {
    id: 'stAndrew', name: 'St. Andrew', region: 'southeast',
    population: 573369, lat: 18.0747, lng: -76.7983,
    isCapital: false, elevation: 180, coastalRisk: 'medium'
  },
  stThomas: {
    id: 'stThomas', name: 'St. Thomas', region: 'east',
    population: 94108, lat: 17.9840, lng: -76.3405,
    isCapital: false, elevation: 40, coastalRisk: 'high'
  },
  portland: {
    id: 'portland', name: 'Portland', region: 'northeast',
    population: 81553, lat: 18.1759, lng: -76.4545,
    isCapital: false, elevation: 350, coastalRisk: 'medium'
  },
  stMary: {
    id: 'stMary', name: 'St. Mary', region: 'northeast',
    population: 116099, lat: 18.3636, lng: -76.9235,
    isCapital: false, elevation: 120, coastalRisk: 'medium'
  },
  stAnn: {
    id: 'stAnn', name: "St. Ann", region: 'north',
    population: 173512, lat: 18.4367, lng: -77.2022,
    isCapital: false, elevation: 200, coastalRisk: 'low'
  },
  trelawny: {
    id: 'trelawny', name: 'Trelawny', region: 'north',
    population: 75853, lat: 18.3512, lng: -77.5977,
    isCapital: false, elevation: 150, coastalRisk: 'low'
  },
  stJames: {
    id: 'stJames', name: 'St. James', region: 'northwest',
    population: 183811, lat: 18.4762, lng: -77.9088,
    isCapital: false, elevation: 80, coastalRisk: 'medium'
  },
  hanover: {
    id: 'hanover', name: 'Hanover', region: 'west',
    population: 70085, lat: 18.4033, lng: -78.1323,
    isCapital: false, elevation: 50, coastalRisk: 'high'
  },
  westmoreland: {
    id: 'westmoreland', name: 'Westmoreland', region: 'west',
    population: 144103, lat: 18.2181, lng: -78.1315,
    isCapital: false, elevation: 30, coastalRisk: 'high'
  },
  stElizabeth: {
    id: 'stElizabeth', name: 'St. Elizabeth', region: 'southwest',
    population: 151466, lat: 18.0303, lng: -77.7403,
    isCapital: false, elevation: 300, coastalRisk: 'low'
  },
  manchester: {
    id: 'manchester', name: 'Manchester', region: 'central',
    population: 191513, lat: 18.0419, lng: -77.5135,
    isCapital: false, elevation: 600, coastalRisk: 'none'
  },
  clarendon: {
    id: 'clarendon', name: 'Clarendon', region: 'central',
    population: 246171, lat: 17.9614, lng: -77.2380,
    isCapital: false, elevation: 100, coastalRisk: 'medium'
  },
  stCatherine: {
    id: 'stCatherine', name: 'St. Catherine', region: 'southeast',
    population: 519953, lat: 17.9993, lng: -77.0301,
    isCapital: false, elevation: 60, coastalRisk: 'medium'
  }
};

// ─────────────────────────────────────────────
// IN-MEMORY DISTRIBUTED STATE
// Simulates Redis pub/sub with vector clocks
// ─────────────────────────────────────────────
const state = {
  nodes: {},           // parish node states
  resources: {},       // resource inventories per parish
  events: [],          // system event log (last 200)
  syncLog: [],         // inter-node sync operations
  vectorClock: {},     // logical timestamps per node
  splitBrainPartitions: [], // active partition events
  alerts: [],          // active emergency alerts
  consensusVotes: {},  // Raft-style leader election
};

// ─────────────────────────────────────────────
// RESOURCE TYPES WITH REALISTIC UNITS
// ─────────────────────────────────────────────
const RESOURCE_TYPES = {
  water: { unit: 'gallons', critical: 5000, nominal: 50000, max: 100000 },
  food: { unit: 'rations', critical: 200, nominal: 2000, max: 5000 },
  medical: { unit: 'kits', critical: 50, nominal: 500, max: 1200 },
  fuel: { unit: 'liters', critical: 1000, nominal: 15000, max: 40000 },
  power: { unit: 'kWh', critical: 500, nominal: 8000, max: 20000 },
  comms: { unit: 'units', critical: 10, nominal: 100, max: 300 },
};

// ─────────────────────────────────────────────
// INITIALIZE NODE STATES
// ─────────────────────────────────────────────
function initializeNodes() {
  Object.keys(PARISHES).forEach(id => {
    const parish = PARISHES[id];

    state.vectorClock[id] = 0;

    state.nodes[id] = {
      id,
      name: parish.name,
      status: 'online',
      role: id === 'kingston' ? 'primary' : 'replica',
      lastHeartbeat: Date.now(),
      heartbeatCount: 0,
      syncedWith: [],
      pendingWrites: 0,
      networkLatency: Math.floor(Math.random() * 40) + 5,
      isLeader: id === 'kingston',
      term: 1,
      uptime: Math.floor(Math.random() * 86400 * 30),
    };

    state.resources[id] = {};
    Object.keys(RESOURCE_TYPES).forEach(rt => {
      const cfg = RESOURCE_TYPES[rt];
      const ratio = 0.3 + Math.random() * 0.6;
      state.resources[id][rt] = {
        current: Math.floor(cfg.nominal * ratio),
        capacity: cfg.max,
        unit: cfg.unit,
        inbound: 0,
        outbound: 0,
        lastUpdated: Date.now(),
        trend: 'stable',
      };
    });
  });

  // Kingston starts fully stocked as primary hub
  Object.keys(RESOURCE_TYPES).forEach(rt => {
    const cfg = RESOURCE_TYPES[rt];
    state.resources.kingston[rt].current = Math.floor(cfg.max * 0.85);
  });
}

// ─────────────────────────────────────────────
// EVENT SYSTEM
// ─────────────────────────────────────────────
function logEvent(type, message, payload = {}) {
  const event = {
    id: uuidv4(),
    timestamp: Date.now(),
    type,
    message,
    payload
  };
  state.events.unshift(event);
  if (state.events.length > 200) state.events.pop();
  broadcastToClients({ type: 'event', data: event });
  return event;
}

// ─────────────────────────────────────────────
// VECTOR CLOCK — CAUSALITY TRACKING
// ─────────────────────────────────────────────
function tick(nodeId) {
  state.vectorClock[nodeId] = (state.vectorClock[nodeId] || 0) + 1;
  return { ...state.vectorClock };
}

function mergeClocks(a, b) {
  const merged = { ...a };
  Object.keys(b).forEach(k => {
    merged[k] = Math.max(merged[k] || 0, b[k] || 0);
  });
  return merged;
}

// ─────────────────────────────────────────────
// SPLIT-BRAIN DETECTION & RESOLUTION
// Using quorum-based approach (majority wins)
// ─────────────────────────────────────────────
function detectSplitBrain() {
  const onlineNodes = Object.values(state.nodes).filter(n => n.status === 'online');
  const totalNodes = Object.keys(state.nodes).length;
  const quorum = Math.floor(totalNodes / 2) + 1;

  if (onlineNodes.length < quorum) {
    const existing = state.splitBrainPartitions.find(p => p.active);
    if (!existing) {
      const partition = {
        id: uuidv4(),
        detectedAt: Date.now(),
        onlineCount: onlineNodes.length,
        offlineCount: totalNodes - onlineNodes.length,
        quorumRequired: quorum,
        active: true,
        resolution: null
      };
      state.splitBrainPartitions.push(partition);
      logEvent('SPLIT_BRAIN', `⚠️ Split-brain detected: ${onlineNodes.length}/${totalNodes} nodes online (need ${quorum} for quorum)`, partition);

      // Auto-resolve after quorum lost - primary node retains writes, isolates minority
      setTimeout(() => {
        partition.active = false;
        partition.resolution = 'quorum_maintained';
        logEvent('PARTITION_RESOLVED', `✓ Partition resolved via quorum — minority nodes entering read-only mode`, { partition });
      }, 15000);
    }
  }
}

// ─────────────────────────────────────────────
// RAFT-STYLE LEADER ELECTION
// ─────────────────────────────────────────────
function triggerLeaderElection(failedNodeId) {
  const candidates = Object.values(state.nodes).filter(n =>
    n.status === 'online' && n.id !== failedNodeId
  );
  if (candidates.length === 0) return;

  // Sort by uptime + term, pick highest
  candidates.sort((a, b) => (b.uptime + b.term * 1000) - (a.uptime + a.term * 1000));
  const newLeader = candidates[0];

  Object.values(state.nodes).forEach(n => n.isLeader = false);
  newLeader.isLeader = true;
  newLeader.role = 'primary';
  newLeader.term += 1;

  logEvent('LEADER_ELECTION', `🗳 Leader election: ${newLeader.name} elected as primary (term ${newLeader.term})`, {
    newLeader: newLeader.id,
    failedNode: failedNodeId,
    candidates: candidates.map(c => c.id)
  });
}

// ─────────────────────────────────────────────
// RESOURCE TRANSFER BETWEEN NODES
// ─────────────────────────────────────────────
function transferResource(fromId, toId, resourceType, amount) {
  const from = state.resources[fromId];
  const to = state.resources[toId];
  if (!from || !to) return { success: false, error: 'Node not found' };

  const available = from[resourceType]?.current || 0;
  const actualAmount = Math.min(amount, available);
  if (actualAmount <= 0) return { success: false, error: 'Insufficient resources' };

  from[resourceType].current -= actualAmount;
  from[resourceType].outbound += actualAmount;
  to[resourceType].current += actualAmount;
  to[resourceType].inbound += actualAmount;

  const vclock = tick(fromId);
  state.vectorClock = mergeClocks(state.vectorClock, vclock);

  const syncEntry = {
    id: uuidv4(),
    timestamp: Date.now(),
    from: fromId,
    to: toId,
    resource: resourceType,
    amount: actualAmount,
    vectorClock: { ...state.vectorClock }
  };
  state.syncLog.unshift(syncEntry);
  if (state.syncLog.length > 100) state.syncLog.pop();

  broadcastToClients({ type: 'sync', data: syncEntry });

  return { success: true, amount: actualAmount, vectorClock: state.vectorClock };
}

// ─────────────────────────────────────────────
// CRISIS EVENT SIMULATION
// ─────────────────────────────────────────────
const CRISIS_TYPES = [
  { type: 'HURRICANE', name: 'Hurricane Warning', drain: { water: 0.3, food: 0.2, fuel: 0.4 }, affectsCoastal: true },
  { type: 'EARTHQUAKE', name: 'Seismic Event', drain: { medical: 0.5, comms: 0.6 }, affectsAll: true },
  { type: 'FLOODING', name: 'Flash Flooding', drain: { water: 0.1, power: 0.5 }, affectsLow: true },
  { type: 'GRID_FAILURE', name: 'Power Grid Failure', drain: { power: 0.8, comms: 0.3 }, affectsAll: true },
  { type: 'SUPPLY_DISRUPTION', name: 'Supply Chain Disruption', drain: { food: 0.4, medical: 0.3 }, affectsAll: true },
];

function triggerCrisisEvent(parishId, crisisType) {
  const crisis = CRISIS_TYPES.find(c => c.type === crisisType) || CRISIS_TYPES[Math.floor(Math.random() * CRISIS_TYPES.length)];
  const parish = PARISHES[parishId];

  // Apply resource drain
  Object.entries(crisis.drain).forEach(([resource, ratio]) => {
    if (state.resources[parishId][resource]) {
      const current = state.resources[parishId][resource].current;
      state.resources[parishId][resource].current = Math.max(0, Math.floor(current * (1 - ratio)));
      state.resources[parishId][resource].trend = 'critical';
    }
  });

  const alert = {
    id: uuidv4(),
    timestamp: Date.now(),
    parish: parishId,
    parishName: parish.name,
    type: crisis.type,
    name: crisis.name,
    severity: 'high',
    active: true
  };
  state.alerts.unshift(alert);
  if (state.alerts.length > 50) state.alerts.pop();

  logEvent('CRISIS', `🚨 ${crisis.name} at ${parish.name} — initiating emergency resource reallocation`, alert);
  broadcastToClients({ type: 'alert', data: alert });

  return alert;
}

// ─────────────────────────────────────────────
// SIMULATION LOOP — Heartbeats, Resource Drift, Sync
// ─────────────────────────────────────────────
function simulationTick() {
  const now = Date.now();

  Object.keys(state.nodes).forEach(id => {
    const node = state.nodes[id];
    const resources = state.resources[id];
    const parish = PARISHES[id];

    if (node.status !== 'online') {
      node.lastHeartbeat = now - (Math.random() * 60000 + 30000); // stale
      return;
    }

    // Heartbeat
    node.lastHeartbeat = now;
    node.heartbeatCount++;
    node.uptime += 5;
    node.networkLatency = Math.max(5, node.networkLatency + (Math.random() - 0.5) * 8);

    // Consume resources based on population demand
    const demandFactor = parish.population / 500000;
    Object.keys(RESOURCE_TYPES).forEach(rt => {
      const res = resources[rt];
      const drain = Math.floor(Math.random() * 20 * demandFactor) + 2;
      res.current = Math.max(0, res.current - drain);
      res.lastUpdated = now;

      // Update trend
      const cfg = RESOURCE_TYPES[rt];
      if (res.current < cfg.critical) res.trend = 'critical';
      else if (res.current < cfg.nominal * 0.4) res.trend = 'low';
      else if (res.current > cfg.nominal * 0.8) res.trend = 'stable';
      else res.trend = 'stable';
    });

    // Propagate sync metadata
    const partners = Object.keys(state.nodes).filter(k => k !== id && state.nodes[k].status === 'online');
    node.syncedWith = partners.slice(0, 3);
    node.pendingWrites = Math.max(0, node.pendingWrites - Math.floor(Math.random() * 3));

    // Small random incoming replenishment
    if (Math.random() < 0.1) {
      const rt = Object.keys(RESOURCE_TYPES)[Math.floor(Math.random() * 6)];
      const cfg = RESOURCE_TYPES[rt];
      const restock = Math.floor(Math.random() * 500) + 100;
      resources[rt].current = Math.min(cfg.max, resources[rt].current + restock);
      resources[rt].inbound += restock;
    }
  });

  // Auto-rebalance: pull resources from wealthy to critical nodes
  Object.keys(state.resources).forEach(toId => {
    if (state.nodes[toId].status !== 'online') return;
    Object.keys(RESOURCE_TYPES).forEach(rt => {
      const cfg = RESOURCE_TYPES[rt];
      const toRes = state.resources[toId][rt];
      if (toRes.current < cfg.critical) {
        // Find richest online node
        const donors = Object.keys(state.resources)
          .filter(fId => fId !== toId && state.nodes[fId].status === 'online')
          .sort((a, b) => state.resources[b][rt].current - state.resources[a][rt].current);
        if (donors.length > 0) {
          const fromId = donors[0];
          const fromRes = state.resources[fromId][rt];
          if (fromRes.current > cfg.nominal) {
            const amount = Math.min(Math.floor(cfg.nominal * 0.15), fromRes.current - cfg.nominal);
            if (amount > 0) {
              transferResource(fromId, toId, rt, amount);
              logEvent('AUTO_REBALANCE', `📦 Auto-transfer: ${amount} ${cfg.unit} of ${rt} from ${PARISHES[fromId].name} → ${PARISHES[toId].name}`, { fromId, toId, rt, amount });
            }
          }
        }
      }
    });
  });

  detectSplitBrain();
}

// Run simulation every 5 seconds
setInterval(simulationTick, 5000);

// ─────────────────────────────────────────────
// WEBSOCKET BROADCAST
// ─────────────────────────────────────────────
const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
  // Send full state on connect
  ws.send(JSON.stringify({ type: 'init', data: getFullState() }));
  ws.on('close', () => clients.delete(ws));
  ws.on('error', () => clients.delete(ws));
});

function broadcastToClients(msg) {
  const payload = JSON.stringify(msg);
  clients.forEach(ws => {
    if (ws.readyState === 1) ws.send(payload);
  });
}

// Broadcast full state every 5s
setInterval(() => {
  broadcastToClients({ type: 'state', data: getFullState() });
}, 5000);

function getFullState() {
  return {
    nodes: state.nodes,
    resources: state.resources,
    events: state.events.slice(0, 50),
    syncLog: state.syncLog.slice(0, 30),
    vectorClock: state.vectorClock,
    splitBrainPartitions: state.splitBrainPartitions.slice(-10),
    alerts: state.alerts.filter(a => a.active).slice(0, 20),
    parishes: PARISHES,
    resourceTypes: RESOURCE_TYPES,
    timestamp: Date.now()
  };
}

// ─────────────────────────────────────────────
// REST API ROUTES
// ─────────────────────────────────────────────

app.get('/api/state', (req, res) => res.json(getFullState()));

app.get('/api/nodes', (req, res) => res.json(Object.values(state.nodes)));

app.get('/api/nodes/:id', (req, res) => {
  const node = state.nodes[req.params.id];
  if (!node) return res.status(404).json({ error: 'Node not found' });
  res.json({
    node,
    resources: state.resources[req.params.id],
    parish: PARISHES[req.params.id],
    vectorClock: state.vectorClock
  });
});

app.post('/api/nodes/:id/status', (req, res) => {
  const { status } = req.body;
  const node = state.nodes[req.params.id];
  if (!node) return res.status(404).json({ error: 'Node not found' });

  const prev = node.status;
  node.status = status;

  if (status === 'offline' && prev === 'online') {
    if (node.isLeader) triggerLeaderElection(req.params.id);
    logEvent('NODE_DOWN', `🔴 Node ${node.name} went offline — failover initiated`, { nodeId: req.params.id });
  } else if (status === 'online' && prev === 'offline') {
    node.lastHeartbeat = Date.now();
    node.pendingWrites = Math.floor(Math.random() * 20);
    logEvent('NODE_UP', `🟢 Node ${node.name} rejoined mesh — syncing ${node.pendingWrites} pending writes`, { nodeId: req.params.id });
  } else if (status === 'degraded') {
    logEvent('NODE_DEGRADED', `🟡 Node ${node.name} entered degraded mode — partial connectivity`, { nodeId: req.params.id });
  }

  broadcastToClients({ type: 'state', data: getFullState() });
  res.json({ success: true, node });
});

app.post('/api/transfer', (req, res) => {
  const { from, to, resource, amount } = req.body;
  if (!from || !to || !resource || !amount) {
    return res.status(400).json({ error: 'Missing required fields: from, to, resource, amount' });
  }

  const result = transferResource(from, to, resource, parseInt(amount));
  if (result.success) {
    logEvent('MANUAL_TRANSFER', `📦 Manual transfer: ${result.amount} ${RESOURCE_TYPES[resource]?.unit} of ${resource} from ${PARISHES[from]?.name} → ${PARISHES[to]?.name}`, { from, to, resource, amount: result.amount });
  }
  res.json(result);
});

app.post('/api/crisis', (req, res) => {
  const { parish, type } = req.body;
  if (!parish) return res.status(400).json({ error: 'Parish required' });
  const alert = triggerCrisisEvent(parish, type);
  res.json({ success: true, alert });
});

app.post('/api/crisis/:alertId/resolve', (req, res) => {
  const alert = state.alerts.find(a => a.id === req.params.alertId);
  if (!alert) return res.status(404).json({ error: 'Alert not found' });
  alert.active = false;
  alert.resolvedAt = Date.now();
  logEvent('CRISIS_RESOLVED', `✅ Crisis resolved at ${alert.parishName}: ${alert.name}`, { alertId: alert.id });
  broadcastToClients({ type: 'state', data: getFullState() });
  res.json({ success: true });
});

app.post('/api/rebalance', (req, res) => {
  const transfers = [];
  Object.keys(state.resources).forEach(toId => {
    if (state.nodes[toId].status !== 'online') return;
    Object.keys(RESOURCE_TYPES).forEach(rt => {
      const cfg = RESOURCE_TYPES[rt];
      const toRes = state.resources[toId][rt];
      if (toRes.current < cfg.nominal * 0.5) {
        const donors = Object.keys(state.resources)
          .filter(fId => fId !== toId && state.nodes[fId].status === 'online')
          .sort((a, b) => state.resources[b][rt].current - state.resources[a][rt].current);
        if (donors.length > 0) {
          const fromId = donors[0];
          const fromRes = state.resources[fromId][rt];
          if (fromRes.current > cfg.nominal * 1.2) {
            const amount = Math.floor((fromRes.current - cfg.nominal) * 0.3);
            if (amount > 0) {
              const result = transferResource(fromId, toId, rt, amount);
              if (result.success) transfers.push({ from: fromId, to: toId, resource: rt, amount });
            }
          }
        }
      }
    });
  });
  logEvent('GLOBAL_REBALANCE', `⚖️ Global rebalance executed: ${transfers.length} transfers completed`, { transfers });
  broadcastToClients({ type: 'state', data: getFullState() });
  res.json({ success: true, transfers });
});

app.get('/api/analytics', (req, res) => {
  const onlineNodes = Object.values(state.nodes).filter(n => n.status === 'online').length;
  const totalNodes = Object.keys(state.nodes).length;

  const resourceSummary = {};
  Object.keys(RESOURCE_TYPES).forEach(rt => {
    const cfg = RESOURCE_TYPES[rt];
    const totals = Object.values(state.resources).map(r => r[rt].current);
    const sum = totals.reduce((a, b) => a + b, 0);
    const criticalNodes = totals.filter(t => t < cfg.critical).length;
    resourceSummary[rt] = {
      total: sum,
      average: Math.floor(sum / totals.length),
      criticalCount: criticalNodes,
      unit: cfg.unit
    };
  });

  res.json({
    meshHealth: Math.round((onlineNodes / totalNodes) * 100),
    onlineNodes,
    totalNodes,
    activeAlerts: state.alerts.filter(a => a.active).length,
    totalSyncs: state.syncLog.length,
    eventCount: state.events.length,
    splitBrainEvents: state.splitBrainPartitions.length,
    resourceSummary,
    vectorClock: state.vectorClock,
    timestamp: Date.now()
  });
});

app.get('/health', (req, res) => res.json({ status: 'ok', uptime: process.uptime() }));

// ─────────────────────────────────────────────
// BOOT
// ─────────────────────────────────────────────
initializeNodes();

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
  console.log(`\n🇯🇲 IslandPulse Mesh Coordinator online — port ${PORT}`);
  console.log(`📡 ${Object.keys(PARISHES).length} parish nodes initialized`);
  console.log(`🔄 Simulation loop active (5s tick)\n`);
});

module.exports = { app, state, PARISHES, RESOURCE_TYPES };
