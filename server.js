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
// REALISTIC JAMAICAN GEOGRAPHY & POLITICS
// Grouped by the 3 Counties: Cornwall, Middlesex, Surrey
// Controlled by local municipal majorities: JLP or PNP
// ─────────────────────────────────────────────
const PARISHES = {
  // --- SURREY COUNTY (East) ---
  kingston: {
    id: 'kingston', name: 'Kingston', county: 'surrey', region: 'southeast',
    population: 96052, lat: 17.9714, lng: -76.7936,
    isCapital: true, elevation: 5, coastalRisk: 'high', politicalControl: 'PNP'
  },
  stAndrew: {
    id: 'stAndrew', name: 'St. Andrew', county: 'surrey', region: 'southeast',
    population: 573369, lat: 18.0747, lng: -76.7983,
    isCapital: false, elevation: 180, coastalRisk: 'medium', politicalControl: 'JLP'
  },
  stThomas: {
    id: 'stThomas', name: 'St. Thomas', county: 'surrey', region: 'east',
    population: 94108, lat: 17.9840, lng: -76.3405,
    isCapital: false, elevation: 40, coastalRisk: 'high', politicalControl: 'JLP'
  },
  portland: {
    id: 'portland', name: 'Portland', county: 'surrey', region: 'northeast',
    population: 81553, lat: 18.1759, lng: -76.4545,
    isCapital: false, elevation: 350, coastalRisk: 'medium', politicalControl: 'PNP'
  },

  // --- MIDDLESEX COUNTY (Central) ---
  stMary: {
    id: 'stMary', name: 'St. Mary', county: 'middlesex', region: 'northeast',
    population: 116099, lat: 18.3636, lng: -76.9235,
    isCapital: false, elevation: 120, coastalRisk: 'medium', politicalControl: 'JLP'
  },
  stAnn: {
    id: 'stAnn', name: "St. Ann", county: 'middlesex', region: 'north',
    population: 173512, lat: 18.4367, lng: -77.2022,
    isCapital: false, elevation: 200, coastalRisk: 'low', politicalControl: 'PNP'
  },
  manchester: {
    id: 'manchester', name: 'Manchester', county: 'middlesex', region: 'central',
    population: 191513, lat: 18.0419, lng: -77.5135,
    isCapital: false, elevation: 600, coastalRisk: 'none', politicalControl: 'PNP'
  },
  clarendon: {
    id: 'clarendon', name: 'Clarendon', county: 'middlesex', region: 'central',
    population: 246171, lat: 17.9614, lng: -77.2380,
    isCapital: false, elevation: 100, coastalRisk: 'medium', politicalControl: 'JLP'
  },
  stCatherine: {
    id: 'stCatherine', name: 'St. Catherine', county: 'middlesex', region: 'southeast',
    population: 519953, lat: 17.9993, lng: -77.0301,
    isCapital: false, elevation: 60, coastalRisk: 'medium', politicalControl: 'PNP'
  },

  // --- CORNWALL COUNTY (West) ---
  trelawny: {
    id: 'trelawny', name: 'Trelawny (Falmouth)', county: 'cornwall', region: 'north',
    population: 75853, lat: 18.3512, lng: -77.5977,
    isCapital: false, elevation: 150, coastalRisk: 'low', politicalControl: 'JLP'
  },
  stJames: {
    id: 'stJames', name: 'St. James', county: 'cornwall', region: 'northwest',
    population: 183811, lat: 18.4762, lng: -77.9088,
    isCapital: false, elevation: 80, coastalRisk: 'medium', politicalControl: 'JLP'
  },
  hanover: {
    id: 'hanover', name: 'Hanover', county: 'cornwall', region: 'west',
    population: 70085, lat: 18.4033, lng: -78.1323,
    isCapital: false, elevation: 50, coastalRisk: 'high', politicalControl: 'PNP'
  },
  westmoreland: {
    id: 'westmoreland', name: 'Westmoreland', county: 'cornwall', region: 'west',
    population: 144103, lat: 18.2181, lng: -78.1315,
    isCapital: false, elevation: 30, coastalRisk: 'high', politicalControl: 'PNP'
  },
  stElizabeth: {
    id: 'stElizabeth', name: 'St. Elizabeth', county: 'cornwall', region: 'southwest',
    population: 151466, lat: 18.0303, lng: -77.7403,
    isCapital: false, elevation: 300, coastalRisk: 'low', politicalControl: 'JLP'
  }
};

const RESOURCE_TYPES = {
  water: { unit: 'gallons', critical: 5000, nominal: 50000, max: 100000 },
  food: { unit: 'rations', critical: 200, nominal: 2000, max: 5000 },
  medical: { unit: 'kits', critical: 50, nominal: 500, max: 1200 },
  fuel: { unit: 'liters', critical: 1000, nominal: 15000, max: 40000 },
  power: { unit: 'kWh', critical: 500, nominal: 8000, max: 20000 },
  comms: { unit: 'units', critical: 10, nominal: 100, max: 300 },
};

// ─────────────────────────────────────────────
// EXPANDED SYSTEM STATE (Step A & B Implementation)
// ─────────────────────────────────────────────
const state = {
  nodes: {},           
  resources: {},       
  events: [],          
  syncLog: [],         
  vectorClock: {},     
  splitBrainPartitions: [], 
  alerts: [],          
  consensusVotes: {},  
  
  // Real-world infrastructure and response states
  infrastructure: {},  // Tracks local power grid and municipal details
  dispatchTeams: {},   // Tracks locations of active JPS, JCF, and JDF teams
  activeTasks: [],     // Ongoing emergency incidents waiting for relief dispatch
  electionCycle: {
    yearsUntilNext: 4,
    lastElectionWinner: 'JLP',
    isElectionYear: false
  }
};

// ─────────────────────────────────────────────
// INITIALIZE STATE WITH JAMAICAN INFRASTRUCTURE
// ─────────────────────────────────────────────
function initializeNodes() {
  Object.keys(PARISHES).forEach(id => {
    const parish = PARISHES[id];

    state.vectorClock[id] = 0;

    // Node state includes political affiliation
    state.nodes[id] = {
      id,
      name: parish.name,
      county: parish.county,
      politicalControl: parish.politicalControl,
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

    // Initialize Parish Infrastructures
    state.infrastructure[id] = {
      jpsGridStatus: 100,       // percentage of power grid integrity
      roadsOpen: true,          // track blockages (e.g. mudslides, downed trees)
      shelterCapacity: Math.floor(parish.population * 0.05), // 5% capacity max
      shelterOccupancy: 0
    };

    // Initialize Emergency Resource/Utility Dispatch Teams
    state.dispatchTeams[id] = {
      jpsCrews: id === 'kingston' || id === 'stAndrew' ? 5 : 2, 
      emergencyOfficers: id === 'kingston' || id === 'stCatherine' ? 8 : 3, // JCF/JDF responders
      waterTrucks: 2
    };

    state.resources[id] = {};
    Object.keys(RESOURCE_TYPES).forEach(rt => {
      const cfg = RESOURCE_TYPES[rt];
      const ratio = 0.6 + Math.random() * 0.3; // Higher starting stocks for baseline stability
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

  // Kingston starts heavily stockpiled as the central response hub
  Object.keys(RESOURCE_TYPES).forEach(rt => {
    const cfg = RESOURCE_TYPES[rt];
    state.resources.kingston[rt].current = Math.floor(cfg.max * 0.9);
  });
}

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
      logEvent('SPLIT_BRAIN', `⚠️ Mesh Partition: ${onlineNodes.length}/${totalNodes} nodes connected. Quorum lost! Parishes split along county borders.`, partition);

      setTimeout(() => {
        partition.active = false;
        partition.resolution = 'quorum_maintained';
        logEvent('PARTITION_RESOLVED', `✓ Network topology repaired. Central ODPEM node re-synchronized clocks.`, { partition });
      }, 15000);
    }
  }
}

function triggerLeaderElection(failedNodeId) {
  const candidates = Object.values(state.nodes).filter(n =>
    n.status === 'online' && n.id !== failedNodeId
  );
  if (candidates.length === 0) return;

  candidates.sort((a, b) => (b.uptime + b.term * 1000) - (a.uptime + a.term * 1000));
  const newLeader = candidates[0];

  Object.values(state.nodes).forEach(n => n.isLeader = false);
  newLeader.isLeader = true;
  newLeader.role = 'primary';
  newLeader.term += 1;

  logEvent('LEADER_ELECTION', `🗳 ODPEM Coordinator Failover: Central authority shifted to ${newLeader.name} Corporation Node (Term ${newLeader.term})`, {
    newLeader: newLeader.id,
    failedNode: failedNodeId
  });
}

// ─────────────────────────────────────────────
// RESOURCE AND TEAM DISPATCH TRANSFERS
// ─────────────────────────────────────────────
function transferResource(fromId, toId, resourceType, amount) {
  const from = state.resources[fromId];
  const to = state.resources[toId];
  const fromNode = state.nodes[fromId];
  const toNode = state.nodes[toId];
  
  if (!from || !to) return { success: false, error: 'Node not found' };

  // Bureaucracy Multiplier: Political friction calculation if crossing rival parish lines
  let politicalDelay = 0;
  if (fromNode && toNode && fromNode.politicalControl !== toNode.politicalControl) {
    politicalDelay = 2000; // Simulates 2 seconds administrative gridlock delay
  }

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
    timestamp: Date.now() + politicalDelay,
    from: fromId,
    to: toId,
    resource: resourceType,
    amount: actualAmount,
    politicalFriction: politicalDelay > 0,
    vectorClock: { ...state.vectorClock }
  };
  state.syncLog.unshift(syncEntry);
  if (state.syncLog.length > 100) state.syncLog.pop();

  broadcastToClients({ type: 'sync', data: syncEntry });
  return { success: true, amount: actualAmount, vectorClock: state.vectorClock };
}

// Dispatch a resource physical crew (JPS / Emergency Officers) across nodes
function dispatchCrew(fromId, toId, crewType) {
  if (!state.dispatchTeams[fromId] || !state.dispatchTeams[toId]) return false;
  if (state.dispatchTeams[fromId][crewType] <= 0) return false;

  state.dispatchTeams[fromId][crewType]--;
  state.dispatchTeams[toId][crewType]++;
  
  logEvent('TEAM_DISPATCH', `🚒 Cross-Parish Support: ${PARISHES[fromId].name} dispatched 1 ${crewType} crew to ${PARISHES[toId].name}.`, { fromId, toId, crewType });
  return true;
}

// ─────────────────────────────────────────────
// NATURAL WEATHER ENGINE — HURRICANE PATHS
// Simulating dynamic destruction realistically
// ─────────────────────────────────────────────
const HURRICANE_SCENARIOS = {
  WESTERN_HIT: {
    name: "Hurricane Melissa",
    description: "Category 4 storm slamming directly through Cornwall county.",
    targets: ['westmoreland', 'hanover', 'trelawny', 'stJames', 'stElizabeth', 'manchester'],
    gridDamage: 0.85, // 85% grid destruction
    roadBlockChance: 0.8,
    drain: { water: 0.4, food: 0.3, power: 0.9, fuel: 0.5 }
  },
  SOUTH_COAST_SWEEP: {
    name: "Hurricane Ivan Clone",
    description: "Slamming southern coastal areas and low-lying plains.",
    targets: ['stThomas', 'kingston', 'stCatherine', 'clarendon', 'stElizabeth', 'westmoreland'],
    gridDamage: 0.70,
    roadBlockChance: 0.6,
    drain: { water: 0.5, medical: 0.4, power: 0.75, comms: 0.5 }
  }
};

function executeHurricaneSim(scenarioKey) {
  const scenario = HURRICANE_SCENARIOS[scenarioKey] || HURRICANE_SCENARIOS.WESTERN_HIT;
  
  logEvent('HURRICANE_LANDFALL', `🌀 ALERT: ${scenario.name} Landfall! ${scenario.description}`, scenario);

  scenario.targets.forEach(parishId => {
    if (!state.nodes[parishId]) return;

    // 1. Structural Collapse
    state.infrastructure[parishId].jpsGridStatus = Math.max(0, Math.floor(100 - (100 * scenario.gridDamage)));
    if (Math.random() < scenario.roadBlockChance) {
      state.infrastructure[parishId].roadsOpen = false;
    }

    // 2. Population displacement into local shelters
    const localParish = PARISHES[parishId];
    const displaced = Math.floor(localParish.population * (0.01 + Math.random() * 0.03));
    state.infrastructure[parishId].shelterOccupancy = Math.min(state.infrastructure[parishId].shelterCapacity, displaced);

    // 3. Severe Resource Drain
    Object.entries(scenario.drain).forEach(([resource, ratio]) => {
      if (state.resources[parishId][resource]) {
        const current = state.resources[parishId][resource].current;
        state.resources[parishId][resource].current = Math.max(0, Math.floor(current * (1 - ratio)));
        state.resources[parishId][resource].trend = 'critical';
      }
    });

    // 4. Append specific urgent task tickets to the queue
    createActionTask(parishId, scenario.name);

    // Turn node state degraded/offline due to storm force
    state.nodes[parishId].status = Math.random() > 0.5 ? 'offline' : 'degraded';
    if (state.nodes[parishId].status === 'offline' && state.nodes[parishId].isLeader) {
      triggerLeaderElection(parishId);
    }
  });

  // Create global alert entry
  state.alerts.unshift({
    id: uuidv4(),
    timestamp: Date.now(),
    type: 'HURRICANE',
    name: scenario.name,
    severity: 'critical',
    active: true,
    affectedCounties: [...new Set(scenario.targets.map(id => PARISHES[id].county))]
  });

  broadcastToClients({ type: 'state', data: getFullState() });
}

function createActionTask(parishId, crisisName) {
  const pName = PARISHES[parishId].name;
  const issues = [
    { type: 'GRID', msg: `Downed high-voltage lines in ${pName} blocking main corridor. Requires JPS crews.`, cost: 'jpsCrews' },
    { type: 'RESCUE', msg: `Flooding reported in low lying zones of ${pName}. Displaced residents stranded. Deploy JCF/JDF officers.`, cost: 'emergencyOfficers' },
    { type: 'SUPPLY', msg: `Critical health facility in ${pName} short on power generators and water. Dispatch supply trucks.`, cost: 'waterTrucks' }
  ];

  const pick = issues[Math.floor(Math.random() * issues.length)];
  state.activeTasks.push({
    id: uuidv4(),
    parishId,
    parishName: pName,
    origin: crisisName,
    type: pick.type,
    description: pick.msg,
    requiredCrew: pick.cost,
    status: 'pending',
    timestamp: Date.now()
  });
}

// ─────────────────────────────────────────────
// SIMULATION ENGINE INTERVAL TICK (Every 5s)
// ─────────────────────────────────────────────
function simulationTick() {
  const now = Date.now();

  Object.keys(state.nodes).forEach(id => {
    const node = state.nodes[id];
    const infra = state.infrastructure[id];
    const teams = state.dispatchTeams[id];
    const resources = state.resources[id];
    const parish = PARISHES[id];

    if (node.status === 'offline') return;

    node.lastHeartbeat = now;
    node.heartbeatCount++;
    node.uptime += 5;

    // Power Grid Recovery Engine: If a grid is hit, JPS crews slowly fix it over time
    if (infra.jpsGridStatus < 100 && teams.jpsCrews > 0) {
      const recoveryAmount = teams.jpsCrews * 4; // Each crew yields +4% power infrastructure repair per tick
      infra.jpsGridStatus = Math.min(100, infra.jpsGridStatus + recoveryAmount);
      if (infra.jpsGridStatus === 100) {
        infra.roadsOpen = true; // Clearing the grid unlocks paths
        if (node.status === 'degraded') node.status = 'online';
        logEvent('INFRA_REPAIRED', `⚡ JPS Crews restored sub-station links in ${parish.name}. Infrastructure stabilized.`);
      }
    }

    // Normal baseline consumption model
    const demandFactor = parish.population / 400000;
    Object.keys(RESOURCE_TYPES).forEach(rt => {
      const res = resources[rt];
      // Power issues increase drain rates on fuel due to back-up diesel generators running
      const generatorFactor = infra.jpsGridStatus < 50 && rt === 'fuel' ? 2.5 : 1;
      const drain = Math.floor((Math.random() * 15 * demandFactor) + 2) * generatorFactor;
      
      res.current = Math.max(0, res.current - drain);
      res.lastUpdated = now;

      const cfg = RESOURCE_TYPES[rt];
      if (res.current < cfg.critical) res.trend = 'critical';
      else if (res.current < cfg.nominal * 0.4) res.trend = 'low';
      else res.trend = 'stable';
    });

    // Task Automator: Self-resolve tasks if teams exist on site
    state.activeTasks.forEach(task => {
      if (task.parishId === id && task.status === 'pending') {
        if (teams[task.requiredCrew] > 0) {
          task.status = 'processing';
          setTimeout(() => {
            task.status = 'resolved';
            logEvent('TASK_RESOLVED', `✓ Resolved: ${task.description}`);
            // Remove completed tasks
            state.activeTasks = state.activeTasks.filter(t => t.id !== task.id);
          }, 4000);
        }
      }
    });
  });

  // Cross-Parish Network Assistance (Auto-routing relief)
  Object.keys(state.resources).forEach(toId => {
    if (state.nodes[toId].status !== 'online') return;
    Object.keys(RESOURCE_TYPES).forEach(rt => {
      const cfg = RESOURCE_TYPES[rt];
      if (state.resources[toId][rt].current < cfg.critical) {
        // Look within the same historic County first to save logistical resources
        const regionalDonors = Object.keys(state.resources)
          .filter(fId => fId !== toId && state.nodes[fId].status === 'online' && PARISHES[fId].county === PARISHES[toId].county)
          .sort((a, b) => state.resources[b][rt].current - state.resources[a][rt].current);

        const donorId = regionalDonors[0] || Object.keys(state.resources)
          .filter(fId => fId !== toId && state.nodes[fId].status === 'online')
          .sort((a, b) => state.resources[b][rt].current - state.resources[a][rt].current)[0];

        if (donorId && state.resources[donorId][rt].current > cfg.nominal) {
          const amount = Math.min(Math.floor(cfg.nominal * 0.2), state.resources[donorId][rt].current - cfg.nominal);
          if (amount > 0) {
            transferResource(donorId, toId, rt, amount);
            logEvent('AUTO_ROUTE', `📦 Automated Mesh Route: ${amount} ${cfg.unit} of ${rt} sent from ${PARISHES[donorId].name} (${PARISHES[donorId].county.toUpperCase()}) → ${PARISHES[toId].name}`);
          }
        }
      }
    });
  });

  // Handle local government election calendar tracking
  if (Math.random() < 0.02) {
    state.electionCycle.yearsUntilNext--;
    if (state.electionCycle.yearsUntilNext <= 0) {
      state.electionCycle.yearsUntilNext = 4;
      // Cycle leadership randomly across parishes to simulate a changing political landscape
      Object.keys(state.nodes).forEach(id => {
        state.nodes[id].politicalControl = Math.random() > 0.5 ? 'JLP' : 'PNP';
      });
      logEvent('ELECTION_NIGHT', `🗳 Local Government Elections Completed! Municipal corporation control redistributed nationwide.`);
    }
  }

  detectSplitBrain();
}

setInterval(simulationTick, 5000);

// ─────────────────────────────────────────────
// WEBSOCKET COMMUNICATOR
// ─────────────────────────────────────────────
const clients = new Set();

wss.on('connection', (ws) => {
  clients.add(ws);
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

setInterval(() => {
  broadcastToClients({ type: 'state', data: getFullState() });
}, 5000);

function getFullState() {
  return {
    nodes: state.nodes,
    resources: state.resources,
    infrastructure: state.infrastructure,
    dispatchTeams: state.dispatchTeams,
    activeTasks: state.activeTasks,
    electionCycle: state.electionCycle,
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
// REST API ENDPOINTS FOR CRISIS SIMULATION
// ─────────────────────────────────────────────
app.get('/api/state', (req, res) => res.json(getFullState()));

// Trigger realistic comprehensive hurricane tracks
app.post('/api/hurricane', (req, res) => {
  const { track } = req.body; // 'WESTERN_HIT' or 'SOUTH_COAST_SWEEP'
  executeHurricaneSim(track);
  res.json({ success: true, message: `Hurricane trajectory executed successfully.` });
});

// Manual support dispatch operations
app.post('/api/dispatch', (req, res) => {
  const { from, to, crewType } = req.body; // crewType: 'jpsCrews' | 'emergencyOfficers' | 'waterTrucks'
  const success = dispatchCrew(from, to, crewType);
  if (success) {
    res.json({ success: true });
  } else {
    res.status(400).json({ error: 'Failed to dispatch crew. Verify availability.' });
  }
});

app.get('/health', (req, res) => res.json({ status: 'ok', uptime: process.uptime() }));

initializeNodes();

const PORT = process.env.PORT || 3001;
httpServer.listen(PORT, () => {
  console.log(`\n🇯🇲 IslandPulse Disaster Network Coordination Node online — port ${PORT}`);
  console.log(`📡 Parsed 14 regional parish clusters across Surrey, Middlesex, & Cornwall`);
  console.log(`🌀 Hurricane Simulation Engine & Grid Infrastructure loops operational.\n`);
});

module.exports = { app, state, PARISHES, RESOURCE_TYPES };