const express = require('express');
const cors = require('cors');
const http = require('http');
const os = require('os');
const path = require('path');
const fs = require('fs');
const dgram = require('dgram');
const multer = require('multer');
const { Server } = require('socket.io');
const { v4: uuidv4 } = require('uuid');

const DATA_DIR = process.env.DATA_DIR || path.join(__dirname, 'data');
const PORT = Number(process.env.PORT || 4100);
const DISCOVERY_PORT = Number(process.env.DISCOVERY_PORT || 41234);
const ADMIN_NAME = process.env.ADMIN_NAME || 'Cremería El Güero';

const UPLOADS_DIR = path.join(DATA_DIR, 'uploads');
const DB_FILE = path.join(DATA_DIR, 'db.json');

fs.mkdirSync(DATA_DIR, { recursive: true });
fs.mkdirSync(UPLOADS_DIR, { recursive: true });

function round2(n) {
  return Math.round((Number(n || 0) + Number.EPSILON) * 100) / 100;
}

function calcRetailPrice(costPrice, retailMarginPct) {
  return round2(Number(costPrice || 0) * (1 + (Number(retailMarginPct || 0) / 100)));
}

function safeNumber(n, fallback = 0) {
  const parsed = Number(n);
  return Number.isFinite(parsed) ? parsed : fallback;
}

function seed() {
  const now = new Date().toISOString();
  return {
    users: [
      { id: 'u-admin', username: 'admin', password: 'admin123', role: 'ADMIN', name: 'Administrador General', active: true, employeeNumber: 'ADMIN-01' },
      { id: 'u-caja1', username: 'caja1', password: '1234', role: 'POS', name: 'Caja 1', active: true, employeeNumber: '1001' },
      { id: 'u-caja2', username: 'caja2', password: '1234', role: 'POS', name: 'Caja 2', active: true, employeeNumber: '1002' },
      { id: 'u-caja3', username: 'caja3', password: '1234', role: 'POS', name: 'Caja 3', active: true, employeeNumber: '1003' }
    ],
    security: {
      requireAdminPasswordForEdits: true,
      updatedAt: now,
      remoteAccess: {
        apiKey: `REMOTE-${uuidv4().replace(/-/g, '').slice(0, 24).toUpperCase()}`,
        allowAdminCredentials: true,
        permissions: {
          readSummary: true,
          readInventory: true,
          writeInventory: true
        }
      }
    },
    fiscalConfig: {
      emitterRfc: '',
      emitterName: '',
      emitterRegimen: '',
      expeditionZip: '',
      pacProvider: 'SW',
      pacMode: 'MOCK'
    },
    products: [
      {
        id: 'p-queso',
        name: 'Queso Oaxaca',
        category: 'Lácteos',
        sku: 'QOA-001',
        unit: 'KG',
        allowsFraction: true,
        costPrice: 96,
        retailMarginPct: 25,
        retailPrice: 120,
        wholesalePrice: 110,
        price: 120,
        stock: 200,
        lowStockThreshold: 15,
        imageUrl: '',
        active: true,
        createdAt: now,
        updatedAt: now
      },
      {
        id: 'p-coca',
        name: 'Coca Cola 600ml',
        category: 'Bebidas',
        sku: 'COC-600',
        unit: 'PZA',
        allowsFraction: false,
        costPrice: 12,
        retailMarginPct: 50,
        retailPrice: 18,
        wholesalePrice: 15,
        price: 18,
        stock: 150,
        lowStockThreshold: 24,
        imageUrl: '',
        active: true,
        createdAt: now,
        updatedAt: now
      }
    ],
    openTickets: [],
    sales: [],
    saleConflicts: [],
    fiscalCustomers: [],
    invoices: [],
    branches: [],
    inventoryAdjustments: [],
    counters: {
      salesSequence: 0
    },
    terminals: [
      {
        id: 't-admin',
        terminalCode: 'ADMIN-01',
        terminalType: 'ADMIN',
        terminalName: ADMIN_NAME,
        lastSeenAt: now
      }
    ]
  };
}

function validateSalePrices({ name, costPrice, retailPrice, wholesalePrice }) {
  const safeCost = safeNumber(costPrice);
  const safeRetail = safeNumber(retailPrice);
  const safeWholesale = safeNumber(wholesalePrice);

  if (safeCost < 0) {
    throw new Error(`El costo no puede ser negativo en ${name || 'el producto'}`);
  }
  if (safeRetail < safeCost) {
    throw new Error(`El precio menudeo no puede ser menor al costo en ${name || 'el producto'}`);
  }
  if (safeWholesale < safeCost) {
    throw new Error(`El precio mayoreo no puede ser menor al costo en ${name || 'el producto'}`);
  }
}

function normalizeProductPayload(body, imageUrl = '') {
  const costPrice = round2(body.costPrice);
  const retailMarginPct = round2(body.retailMarginPct);
  const retailPrice = round2(
    body.retailPrice !== undefined && body.retailPrice !== ''
      ? body.retailPrice
      : calcRetailPrice(costPrice, retailMarginPct)
  );
  const wholesalePrice = round2(
    body.wholesalePrice !== undefined && body.wholesalePrice !== ''
      ? body.wholesalePrice
      : retailPrice
  );
  const lowStockThreshold = safeNumber(body.lowStockThreshold);
  const now = new Date().toISOString();

  const product = {
    id: body.id || uuidv4(),
    name: String(body.name || '').trim(),
    category: String(body.category || '').trim(),
    sku: String(body.sku || '').trim(),
    unit: body.unit || 'PZA',
    allowsFraction:
      String(body.allowsFraction) === 'true' ||
      body.allowsFraction === true ||
      body.allowsFraction === 'on',
    costPrice,
    retailMarginPct,
    retailPrice,
    wholesalePrice,
    price: retailPrice,
    stock: safeNumber(body.stock),
    lowStockThreshold,
    imageUrl: imageUrl || body.imageUrl || '',
    active: body.active !== false,
    createdAt: body.createdAt || now,
    updatedAt: now
  };

  if (!product.name) {
    throw new Error('El nombre del producto es obligatorio');
  }
  if (!product.category) {
    throw new Error('La categoría del producto es obligatoria');
  }

  validateSalePrices(product);

  return product;
}


function normalizeEmployeePayload(body = {}) {
  const name = String(body.name || '').trim();
  const employeeNumber = String(body.employeeNumber || '').trim();
  if (!name) throw new Error('El nombre completo del empleado es obligatorio');
  if (!employeeNumber) throw new Error('El número de empleado es obligatorio');

  const usernameBase = String(body.username || `emp-${employeeNumber}`).trim().toLowerCase();
  return {
    id: body.id || uuidv4(),
    role: 'POS',
    name,
    employeeNumber,
    username: usernameBase,
    password: String(body.password || employeeNumber || '1234').trim(),
    active: body.active !== false,
    createdAt: body.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };
}

function getActiveEmployees(db) {
  return (db.users || [])
    .filter((u) => u.role === 'POS' && u.active !== false)
    .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'es'));
}

function parseDateInput(value, fallback = new Date()) {
  if (!value) return fallback;
  const parsed = new Date(value);
  return Number.isNaN(parsed.getTime()) ? fallback : parsed;
}

function getRangeBounds(range = 'day', anchorDate) {
  const base = parseDateInput(anchorDate, new Date());
  const start = new Date(base);
  const end = new Date(base);

  if (range === 'month') {
    start.setDate(1);
    start.setHours(0, 0, 0, 0);
    end.setMonth(start.getMonth() + 1, 1);
    end.setHours(0, 0, 0, 0);
  } else if (range === 'week') {
    const day = start.getDay();
    const diff = day === 0 ? -6 : 1 - day;
    start.setDate(start.getDate() + diff);
    start.setHours(0, 0, 0, 0);
    end.setTime(start.getTime());
    end.setDate(start.getDate() + 7);
  } else {
    start.setHours(0, 0, 0, 0);
    end.setTime(start.getTime());
    end.setDate(start.getDate() + 1);
  }

  return { start, end };
}

function getBranchSummarySkeleton(branch) {
  return {
    id: branch.id,
    branchId: branch.id,
    name: branch.name,
    code: branch.code || '',
    remoteUrl: branch.remoteUrl,
    authType: branch.authType || 'credentials',
    apiKey: branch.authType === 'token' ? (branch.apiKey || '') : '',
    remoteAdminUsername: branch.remoteAdminUsername || '',
    mode: branch.mode || 'render',
    status: branch.lastStatus || 'PENDING',
    lastCheckedAt: branch.lastCheckedAt || '',
    lastError: branch.lastError || '',
    lastRemoteSummary: branch.lastRemoteSummary || null
  };
}

function normalizeTicketItems(items = []) {
  return (Array.isArray(items) ? items : []).map((item) => ({
    productId: item.productId,
    name: item.name,
    unit: item.unit || 'PZA',
    quantity: safeNumber(item.quantity),
    unitPrice: round2(item.unitPrice),
    saleMode: item.saleMode === 'WHOLESALE' ? 'WHOLESALE' : 'RETAIL'
  }));
}

function sumTicket(items = []) {
  return round2(items.reduce((acc, item) => acc + safeNumber(item.quantity) * safeNumber(item.unitPrice), 0));
}

function isLowStock(product) {
  return safeNumber(product.stock) <= safeNumber(product.lowStockThreshold);
}

function ensureDb() {
  if (!fs.existsSync(DB_FILE)) {
    fs.writeFileSync(DB_FILE, JSON.stringify(seed(), null, 2), 'utf8');
    return;
  }

  const db = JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
  let changed = false;

  db.users ||= seed().users;
  db.security ||= { requireAdminPasswordForEdits: true, updatedAt: new Date().toISOString(), remoteAccess: {} };
  db.security.remoteAccess ||= {};
  if (!db.security.remoteAccess.apiKey) {
    db.security.remoteAccess.apiKey = `REMOTE-${uuidv4().replace(/-/g, '').slice(0, 24).toUpperCase()}`;
    changed = true;
  }
  if (typeof db.security.remoteAccess.allowAdminCredentials !== 'boolean') {
    db.security.remoteAccess.allowAdminCredentials = true;
    changed = true;
  }
  db.security.remoteAccess.permissions ||= {};
  if (typeof db.security.remoteAccess.permissions.readSummary !== 'boolean') {
    db.security.remoteAccess.permissions.readSummary = true;
    changed = true;
  }
  if (typeof db.security.remoteAccess.permissions.readInventory !== 'boolean') {
    db.security.remoteAccess.permissions.readInventory = true;
    changed = true;
  }
  if (typeof db.security.remoteAccess.permissions.writeInventory !== 'boolean') {
    db.security.remoteAccess.permissions.writeInventory = true;
    changed = true;
  }
  db.fiscalConfig ||= seed().fiscalConfig;
  db.products ||= [];
  db.openTickets ||= [];
  db.sales ||= [];
  db.saleConflicts ||= [];
  db.fiscalCustomers ||= [];
  db.invoices ||= [];
  db.branches ||= [];
  db.inventoryAdjustments ||= [];
  db.counters ||= { salesSequence: 0 };
  db.terminals ||= seed().terminals;


  db.users = db.users.map((u) => {
    const migrated = {
      active: u.active !== false,
      employeeNumber: u.employeeNumber || (u.role === 'ADMIN' ? 'ADMIN-01' : ''),
      createdAt: u.createdAt || new Date().toISOString(),
      updatedAt: u.updatedAt || new Date().toISOString(),
      ...u
    };
    if (
      migrated.active !== u.active ||
      migrated.employeeNumber !== u.employeeNumber ||
      migrated.createdAt !== u.createdAt ||
      migrated.updatedAt !== u.updatedAt
    ) {
      changed = true;
    }
    return migrated;
  });

  db.branches = db.branches.map((branch) => {
    const migrated = {
      id: branch.id || uuidv4(),
      name: String(branch.name || '').trim(),
      code: String(branch.code || '').trim(),
      remoteUrl: String(branch.remoteUrl || '').trim(),
      authType: branch.authType || (String(branch.apiKey || '').trim() ? 'token' : 'credentials'),
      apiKey: String(branch.apiKey || '').trim(),
      remoteAdminUsername: String(branch.remoteAdminUsername || '').trim(),
      remoteAdminPassword: String(branch.remoteAdminPassword || '').trim(),
      mode: branch.mode || 'render',
      active: branch.active !== false,
      lastStatus: branch.lastStatus || 'PENDING',
      lastCheckedAt: branch.lastCheckedAt || '',
      lastError: branch.lastError || '',
      lastRemoteSummary: branch.lastRemoteSummary || null,
      createdAt: branch.createdAt || new Date().toISOString(),
      updatedAt: branch.updatedAt || new Date().toISOString()
    };
    if (
      migrated.id !== branch.id ||
      migrated.name !== branch.name ||
      migrated.code !== branch.code ||
      migrated.remoteUrl !== branch.remoteUrl ||
      migrated.authType !== branch.authType ||
      migrated.apiKey !== branch.apiKey ||
      migrated.remoteAdminUsername !== branch.remoteAdminUsername ||
      migrated.remoteAdminPassword !== branch.remoteAdminPassword ||
      migrated.mode !== branch.mode ||
      migrated.active !== branch.active ||
      migrated.lastStatus !== branch.lastStatus ||
      migrated.lastCheckedAt !== branch.lastCheckedAt ||
      migrated.lastError !== branch.lastError ||
      JSON.stringify(migrated.lastRemoteSummary || null) !== JSON.stringify(branch.lastRemoteSummary || null) ||
      migrated.createdAt !== branch.createdAt ||
      migrated.updatedAt !== branch.updatedAt
    ) {
      changed = true;
    }
    return migrated;
  });


  db.products = db.products.map((p) => {
    const migrated = {
      ...p,
      category: String(p.category || 'General').trim() || 'General',
      costPrice: safeNumber(p.costPrice, 0),
      retailMarginPct: safeNumber(p.retailMarginPct, 0),
      retailPrice: round2(p.retailPrice ?? p.price ?? 0),
      wholesalePrice: round2(p.wholesalePrice ?? p.retailPrice ?? p.price ?? 0),
      price: round2(p.retailPrice ?? p.price ?? 0),
      lowStockThreshold: safeNumber(p.lowStockThreshold, 0),
      active: p.active !== false,
      createdAt: p.createdAt || new Date().toISOString(),
      updatedAt: p.updatedAt || new Date().toISOString()
    };

    if (
      migrated.category !== p.category ||
      migrated.costPrice !== p.costPrice ||
      migrated.retailMarginPct !== p.retailMarginPct ||
      migrated.retailPrice !== p.retailPrice ||
      migrated.wholesalePrice !== p.wholesalePrice ||
      migrated.lowStockThreshold !== p.lowStockThreshold ||
      migrated.price !== p.price
    ) {
      changed = true;
    }

    return migrated;
  });

  db.openTickets = db.openTickets.map((t, index) => {
    const migrated = {
      id: t.id || uuidv4(),
      folio: t.folio || `TK-${String(index + 1).padStart(4, '0')}`,
      terminalCode: t.terminalCode || 'POS-01',
      operatorName: t.operatorName || 'Operador',
      customerName: String(t.customerName || '').trim(),
      customerReference: String(t.customerReference || '').trim(),
      takenBy: t.takenBy || '',
      status: t.status || 'OPEN',
      items: normalizeTicketItems(t.items || []),
      subtotal: round2(t.subtotal ?? sumTicket(t.items || [])),
      createdAt: t.createdAt || new Date().toISOString(),
      updatedAt: t.updatedAt || new Date().toISOString()
    };
    if (
      migrated.id !== t.id ||
      migrated.folio !== t.folio ||
      migrated.terminalCode !== t.terminalCode ||
      migrated.operatorName !== t.operatorName ||
      migrated.customerName !== t.customerName ||
      migrated.customerReference !== t.customerReference ||
      migrated.takenBy !== t.takenBy ||
      migrated.status !== t.status ||
      migrated.subtotal !== t.subtotal
    ) {
      changed = true;
    }
    return migrated;
  });

  db.sales = db.sales.map((sale, index) => {
    const migrated = {
      id: sale.id || uuidv4(),
      folio: sale.folio || `VTA-${String(index + 1).padStart(6, '0')}`,
      terminalCode: sale.terminalCode || 'POS-01',
      createdAt: sale.createdAt || new Date().toISOString(),
      clientTxnId: sale.clientTxnId || uuidv4(),
      items: normalizeTicketItems(sale.items || []),
      total: round2(sale.total ?? sumTicket(sale.items || [])),
      operatorId: sale.operatorId || '',
      operatorName: sale.operatorName || '',
      employeeNumber: sale.employeeNumber || '',
      customerName: String(sale.customerName || '').trim(),
      customerReference: String(sale.customerReference || '').trim(),
      ticketId: sale.ticketId || ''
    };
    if (
      migrated.id !== sale.id ||
      migrated.folio !== sale.folio ||
      migrated.terminalCode !== sale.terminalCode ||
      migrated.clientTxnId !== sale.clientTxnId ||
      migrated.total !== sale.total ||
      migrated.operatorId !== sale.operatorId ||
      migrated.operatorName !== sale.operatorName ||
      migrated.employeeNumber !== sale.employeeNumber ||
      migrated.customerName !== sale.customerName ||
      migrated.customerReference !== sale.customerReference ||
      migrated.ticketId !== sale.ticketId
    ) {
      changed = true;
    }
    return migrated;
  });


  db.inventoryAdjustments = (db.inventoryAdjustments || []).map((entry) => ({
    id: entry.id || uuidv4(),
    productId: entry.productId || '',
    productName: entry.productName || '',
    sku: entry.sku || '',
    previousStock: round2(safeNumber(entry.previousStock)),
    newStock: round2(safeNumber(entry.newStock)),
    delta: round2(safeNumber(entry.delta)),
    unit: entry.unit || 'PZA',
    reason: String(entry.reason || '').trim(),
    sourceType: entry.sourceType || 'LOCAL_ADMIN',
    actorName: entry.actorName || '',
    actorUsername: entry.actorUsername || '',
    actorBranch: entry.actorBranch || '',
    createdAt: entry.createdAt || new Date().toISOString()
  }));

  const derivedSalesSequence = db.sales.reduce((max, sale, index) => {
    const match = String(sale.folio || '').match(/(\d+)$/);
    const seq = match ? Number(match[1]) : index + 1;
    return Math.max(max, seq);
  }, 0);

  if (safeNumber(db.counters.salesSequence, 0) !== derivedSalesSequence) {
    db.counters.salesSequence = Math.max(safeNumber(db.counters.salesSequence, 0), derivedSalesSequence);
    changed = true;
  }

  if (changed) {
    fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2), 'utf8');
  }
}

function readDb() {
  ensureDb();
  return JSON.parse(fs.readFileSync(DB_FILE, 'utf8'));
}

function writeDb(db) {
  fs.writeFileSync(DB_FILE, JSON.stringify(db, null, 2), 'utf8');
}

function verifyAdminPassword(db, password) {
  const admin = (db.users || []).find((u) => u.role === 'ADMIN');
  return !!(admin && password && admin.password === password);
}

function getRemotePermissions(db) {
  return {
    readSummary: db.security?.remoteAccess?.permissions?.readSummary !== false,
    readInventory: db.security?.remoteAccess?.permissions?.readInventory !== false,
    writeInventory: db.security?.remoteAccess?.permissions?.writeInventory !== false
  };
}

function hasValidRemoteToken(db, token) {
  const configured = String(db.security?.remoteAccess?.apiKey || '').trim();
  return !!(configured && token && String(token).trim() === configured);
}

function hasValidRemoteAdminCredentials(db, username, password) {
  if (db.security?.remoteAccess?.allowAdminCredentials === false) return false;
  const admin = (db.users || []).find((u) => u.role === 'ADMIN');
  if (!admin) return false;
  return !!(username && password && String(username).trim() === String(admin.username || '').trim() && String(password).trim() === String(admin.password || '').trim());
}

function authorizeRemoteRequest(req, res, db, permissionKey = 'readSummary') {
  const token = req.headers['x-remote-key'];
  const username = req.headers['x-remote-username'];
  const password = req.headers['x-remote-password'];
  const hasToken = hasValidRemoteToken(db, token);
  const hasCredentials = hasValidRemoteAdminCredentials(db, username, password);

  if (!hasToken && !hasCredentials) {
    res.status(401).json({ ok: false, message: 'Acceso remoto no autorizado' });
    return null;
  }

  const permissions = getRemotePermissions(db);
  if (permissions[permissionKey] === false) {
    res.status(403).json({ ok: false, message: 'La sucursal remota no permite esta operación' });
    return null;
  }

  return {
    authMode: hasToken ? 'token' : 'credentials',
    permissions,
    actorName: hasToken ? 'Integración remota' : String(username || '').trim(),
    actorUsername: hasToken ? 'remote-token' : String(username || '').trim()
  };
}

function sanitizeInventoryProduct(product) {
  return {
    id: product.id,
    name: product.name,
    category: product.category || 'General',
    sku: product.sku || '',
    stock: round2(safeNumber(product.stock)),
    unit: product.unit || 'PZA',
    allowsFraction: !!product.allowsFraction,
    lowStockThreshold: round2(safeNumber(product.lowStockThreshold)),
    retailPrice: round2(product.retailPrice ?? product.price ?? 0),
    wholesalePrice: round2(product.wholesalePrice ?? product.retailPrice ?? product.price ?? 0),
    costPrice: round2(safeNumber(product.costPrice)),
    imageUrl: product.imageUrl || '',
    active: product.active !== false,
    updatedAt: product.updatedAt || ''
  };
}

function buildRemoteInventory(db, filters = {}) {
  const query = String(filters.query || '').trim().toLowerCase();
  const category = String(filters.category || '').trim().toLowerCase();
  const lowOnly = String(filters.lowOnly || '').trim() === '1';

  const allProducts = (db.products || []).filter((product) => product.active !== false).map(sanitizeInventoryProduct);
  const allCategories = Array.from(new Set(allProducts.map((product) => String(product.category || '').trim()).filter(Boolean))).sort((a, b) => a.localeCompare(b, 'es'));
  let rows = allProducts.slice();

  if (query) {
    rows = rows.filter((product) => {
      return [product.name, product.sku, product.category].some((value) => String(value || '').toLowerCase().includes(query));
    });
  }

  if (category) {
    rows = rows.filter((product) => String(product.category || '').trim().toLowerCase() === category);
  }

  if (lowOnly) {
    rows = rows.filter((product) => safeNumber(product.stock) <= safeNumber(product.lowStockThreshold));
  }

  rows.sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'es'));

  return {
    ok: true,
    products: rows,
    categories: allCategories,
    totals: {
      products: rows.length,
      stockUnits: round2(rows.reduce((acc, product) => acc + safeNumber(product.stock), 0)),
      lowStockCount: rows.filter((product) => safeNumber(product.stock) <= safeNumber(product.lowStockThreshold)).length
    },
    generatedAt: new Date().toISOString()
  };
}

function applyInventoryAdjustment(db, payload = {}) {
  const product = (db.products || []).find((row) => row.id === payload.productId);
  if (!product) {
    throw new Error('Producto no encontrado');
  }

  const operation = payload.operation === 'set' ? 'set' : (payload.operation === 'subtract' ? 'subtract' : 'add');
  const quantity = round2(safeNumber(payload.quantity));
  const previousStock = round2(safeNumber(product.stock));
  let newStock = previousStock;

  if (operation === 'set') {
    newStock = round2(safeNumber(payload.targetStock, previousStock));
  } else if (operation === 'subtract') {
    if (quantity <= 0) throw new Error('La cantidad a descontar debe ser mayor a 0');
    newStock = round2(previousStock - quantity);
  } else {
    if (quantity <= 0) throw new Error('La cantidad a agregar debe ser mayor a 0');
    newStock = round2(previousStock + quantity);
  }

  if (newStock < 0) {
    throw new Error(`El ajuste dejaría stock negativo en ${product.name}`);
  }

  product.stock = newStock;
  product.updatedAt = new Date().toISOString();

  const movement = {
    id: uuidv4(),
    productId: product.id,
    productName: product.name,
    sku: product.sku || '',
    previousStock,
    newStock,
    delta: round2(newStock - previousStock),
    unit: product.unit || 'PZA',
    reason: String(payload.reason || '').trim() || 'Ajuste manual de inventario',
    sourceType: payload.sourceType || 'LOCAL_ADMIN',
    actorName: payload.actorName || '',
    actorUsername: payload.actorUsername || '',
    actorBranch: payload.actorBranch || '',
    createdAt: new Date().toISOString()
  };

  db.inventoryAdjustments ||= [];
  db.inventoryAdjustments.unshift(movement);
  db.inventoryAdjustments = db.inventoryAdjustments.slice(0, 5000);

  return { product, movement };
}

function buildRemoteSummary(db) {
  const totalSales = round2((db.sales || []).reduce((a, s) => a + safeNumber(s.total), 0));
  const lowStockProducts = (db.products || []).filter(isLowStock).slice(0, 8).map((product) => ({
    id: product.id,
    name: product.name,
    sku: product.sku || '',
    stock: safeNumber(product.stock),
    unit: product.unit || 'PZA',
    threshold: safeNumber(product.lowStockThreshold)
  }));
  const recentSales = (db.sales || []).slice().sort((a, b) => new Date(b.createdAt || 0).getTime() - new Date(a.createdAt || 0).getTime()).slice(0, 8).map((sale) => ({
    id: sale.id,
    folio: sale.folio,
    total: round2(safeNumber(sale.total)),
    operatorName: sale.operatorName || '-',
    customerName: sale.customerName || '',
    createdAt: sale.createdAt || '',
    itemsCount: Array.isArray(sale.items) ? sale.items.length : 0
  }));
  const recentInventoryAdjustments = (db.inventoryAdjustments || []).slice(0, 8).map((entry) => ({
    id: entry.id,
    productName: entry.productName || '-',
    delta: round2(safeNumber(entry.delta)),
    newStock: round2(safeNumber(entry.newStock)),
    reason: entry.reason || '',
    actorName: entry.actorName || entry.actorUsername || '-',
    createdAt: entry.createdAt || ''
  }));

  return {
    ok: true,
    remote: {
      adminName: ADMIN_NAME,
      hostname: os.hostname(),
      port: PORT,
      service: 'embedded-admin-backend',
      permissions: getRemotePermissions(db)
    },
    metrics: {
      products: (db.products || []).length,
      stockUnits: round2((db.products || []).reduce((a, p) => a + safeNumber(p.stock), 0)),
      salesCount: (db.sales || []).length,
      totalSales,
      employees: getActiveEmployees(db).length,
      lowStockCount: (db.products || []).filter(isLowStock).length,
      openTickets: (db.openTickets || []).length,
      invoices: (db.invoices || []).length,
      inventoryAdjustments: (db.inventoryAdjustments || []).length
    },
    recentSales,
    lowStockProducts,
    recentInventoryAdjustments,
    generatedAt: new Date().toISOString()
  };
}

function buildRemoteHeaders(branch) {
  const headers = {};
  if ((branch.authType || 'credentials') === 'token') {
    if (!String(branch.apiKey || '').trim()) {
      throw new Error('Falta el token/API key de la sucursal remota');
    }
    headers['x-remote-key'] = String(branch.apiKey || '').trim();
  } else {
    if (!String(branch.remoteAdminUsername || '').trim() || !String(branch.remoteAdminPassword || '').trim()) {
      throw new Error('Faltan las credenciales del administrador remoto');
    }
    headers['x-remote-username'] = String(branch.remoteAdminUsername || '').trim();
    headers['x-remote-password'] = String(branch.remoteAdminPassword || '').trim();
  }
  return headers;
}

async function fetchRemoteBranch(branch, endpointPath, options = {}) {
  const base = String(branch.remoteUrl || '').trim().replace(/\/$/, '');
  if (!base) {
    throw new Error('La URL remota de la sucursal está vacía');
  }

  const headers = {
    ...buildRemoteHeaders(branch),
    ...(options.headers || {})
  };

  if (options.body && !(options.body instanceof Buffer)) {
    headers['Content-Type'] = 'application/json';
  }

  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 10000);

  try {
    const response = await fetch(`${base}${endpointPath}`, {
      method: options.method || 'GET',
      headers,
      body: options.body,
      signal: controller.signal
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.message || `La sucursal respondió con HTTP ${response.status}`);
    }
    return data;
  } finally {
    clearTimeout(timeout);
  }
}

async function fetchRemoteBranchSummary(branch) {
  return fetchRemoteBranch(branch, '/api/remote/summary');
}

function touchTerminal(db, payload = {}) {
  const terminalCode = payload.terminalCode || 'UNKNOWN';
  const existing = (db.terminals || []).find((t) => t.terminalCode === terminalCode);
  const now = new Date().toISOString();

  if (existing) {
    existing.lastSeenAt = now;
    existing.terminalType = payload.terminalType || existing.terminalType;
    existing.terminalName = payload.terminalName || existing.terminalName;
  } else {
    db.terminals.push({
      id: uuidv4(),
      terminalCode,
      terminalType: payload.terminalType || 'POS',
      terminalName: payload.terminalName || terminalCode,
      lastSeenAt: now
    });
  }
}

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: '*' } });

const upload = multer({ dest: UPLOADS_DIR });

app.use(cors());
app.use(express.json({ limit: '12mb' }));
app.use('/uploads', express.static(UPLOADS_DIR));

function auth(req, _res, next) {
  const username = req.headers['x-username'];
  const db = readDb();
  req.user = (db.users || []).find((u) => u.username === username) || null;
  next();
}

function requireAdmin(req, res, next) {
  if (!req.user || req.user.role !== 'ADMIN') {
    return res.status(403).json({ ok: false, message: 'Solo administrador' });
  }
  next();
}

function broadcastInventory(db, reason = 'SYNC', extra = {}) {
  io.emit('inventory:update', {
    products: db.products,
    lowStock: db.products.filter(isLowStock).map((p) => p.id),
    reason,
    ...extra
  });
}

function emitTickets(db, reason = 'TICKETS_SYNC', extra = {}) {
  io.emit('tickets:update', {
    ok: true,
    reason,
    tickets: (db.openTickets || []).slice().sort((a, b) => {
      return new Date(b.updatedAt).getTime() - new Date(a.updatedAt).getTime();
    }),
    ...extra
  });
}

app.get('/health', (_req, res) => {
  res.json({ ok: true, service: 'embedded-lan-backend', port: PORT, adminName: ADMIN_NAME, hostname: os.hostname() });
});

app.get('/api/remote/summary', (req, res) => {
  const db = readDb();
  const remoteAuth = authorizeRemoteRequest(req, res, db, 'readSummary');
  if (!remoteAuth) return;

  const payload = buildRemoteSummary(db);
  payload.authMode = remoteAuth.authMode;
  return res.json(payload);
});

app.get('/api/remote/inventory', (req, res) => {
  const db = readDb();
  const remoteAuth = authorizeRemoteRequest(req, res, db, 'readInventory');
  if (!remoteAuth) return;

  const payload = buildRemoteInventory(db, req.query || {});
  payload.authMode = remoteAuth.authMode;
  payload.permissions = remoteAuth.permissions;
  return res.json(payload);
});

app.post('/api/remote/inventory/adjust', (req, res) => {
  const db = readDb();
  const remoteAuth = authorizeRemoteRequest(req, res, db, 'writeInventory');
  if (!remoteAuth) return;

  try {
    const result = applyInventoryAdjustment(db, {
      ...req.body,
      sourceType: 'REMOTE_ADMIN',
      actorName: req.body?.actorName || remoteAuth.actorName,
      actorUsername: req.body?.actorUsername || remoteAuth.actorUsername,
      actorBranch: String(req.body?.actorBranch || '').trim()
    });
    writeDb(db);
    broadcastInventory(db, 'REMOTE_INVENTORY_ADJUSTED', { productId: result.product.id });
    return res.json({ ok: true, product: sanitizeInventoryProduct(result.product), movement: result.movement });
  } catch (err) {
    return res.status(400).json({ ok: false, message: err.message });
  }
});

app.post('/auth/login', (req, res) => {
  const { username, password, terminalCode, terminalType, terminalName } = req.body || {};
  const db = readDb();
  const user = db.users.find((u) => u.username === username && u.password === password);

  if (!user) {
    return res.status(401).json({ ok: false, message: 'Credenciales inválidas' });
  }

  if (user.role !== 'ADMIN' && user.active === false) {
    return res.status(403).json({ ok: false, message: 'Empleado inactivo' });
  }

  touchTerminal(db, { terminalCode, terminalType, terminalName });
  writeDb(db);

  res.json({
    ok: true,
    user: { id: user.id, username: user.username, role: user.role, name: user.name, employeeNumber: user.employeeNumber || '' }
  });
});

app.post('/auth/admin/verify', auth, (req, res) => {
  const { password } = req.body || {};
  const db = readDb();

  if (!verifyAdminPassword(db, password)) {
    return res.status(401).json({ ok: false, message: 'Contraseña de administrador inválida' });
  }

  res.json({ ok: true });
});

app.get('/api/dashboard', auth, (_req, res) => {
  const db = readDb();
  const totalSales = db.sales.reduce((a, s) => a + safeNumber(s.total), 0);
  const today = new Date().toISOString().slice(0, 10);
  const todaySales = db.sales
    .filter((s) => String(s.createdAt || '').startsWith(today))
    .reduce((a, s) => a + safeNumber(s.total), 0);

  res.json({
    ok: true,
    metrics: {
      products: db.products.length,
      stockUnits: round2(db.products.reduce((a, p) => a + safeNumber(p.stock), 0)),
      salesCount: db.sales.length,
      totalSales,
      todaySales,
      invoices: db.invoices.length,
      terminals: db.terminals.length,
      lowStockCount: db.products.filter(isLowStock).length,
      openTickets: db.openTickets.length,
      employees: getActiveEmployees(db).length,
      branches: (db.branches || []).filter((b) => b.active !== false).length
    }
  });
});

app.get('/api/products', auth, (_req, res) => {
  const db = readDb();
  res.json({ ok: true, products: db.products });
});

app.get('/api/inventory', auth, (_req, res) => {
  const db = readDb();
  res.json({
    ok: true,
    inventory: db.products.map((p) => ({
      id: p.id,
      name: p.name,
      sku: p.sku,
      stock: p.stock,
      unit: p.unit,
      price: p.price,
      retailPrice: p.retailPrice,
      wholesalePrice: p.wholesalePrice,
      costPrice: p.costPrice,
      lowStockThreshold: p.lowStockThreshold,
      imageUrl: p.imageUrl,
      allowsFraction: p.allowsFraction
    }))
  });
});

app.post('/api/products', auth, requireAdmin, upload.single('image'), (req, res) => {
  try {
    const db = readDb();
    const imageUrl = req.file ? `/uploads/${req.file.filename}` : '';
    const product = normalizeProductPayload(req.body || {}, imageUrl);

    db.products.push(product);
    writeDb(db);
    broadcastInventory(db, 'NEW_PRODUCT', { productId: product.id });

    res.json({ ok: true, product });
  } catch (err) {
    res.status(400).json({ ok: false, message: err.message });
  }
});

app.put('/api/products/:id', auth, requireAdmin, upload.single('image'), (req, res) => {
  try {
    const db = readDb();
    const current = db.products.find((p) => p.id === req.params.id);

    if (!current) {
      return res.status(404).json({ ok: false, message: 'Producto no encontrado' });
    }

    const imageUrl = req.file ? `/uploads/${req.file.filename}` : current.imageUrl;
    const updated = normalizeProductPayload({ ...current, ...(req.body || {}), id: current.id, createdAt: current.createdAt }, imageUrl);

    Object.assign(current, updated);
    writeDb(db);
    broadcastInventory(db, 'PRODUCT_UPDATED', { productId: current.id });

    res.json({ ok: true, product: current });
  } catch (err) {
    res.status(400).json({ ok: false, message: err.message });
  }
});

app.delete('/api/products/:id', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const before = db.products.length;
  db.products = db.products.filter((p) => p.id !== req.params.id);

  if (db.products.length === before) {
    return res.status(404).json({ ok: false, message: 'Producto no encontrado' });
  }

  db.openTickets = db.openTickets.map((ticket) => ({
    ...ticket,
    items: normalizeTicketItems(ticket.items).filter((item) => item.productId !== req.params.id),
    subtotal: sumTicket(normalizeTicketItems(ticket.items).filter((item) => item.productId !== req.params.id)),
    updatedAt: new Date().toISOString()
  }));

  writeDb(db);
  broadcastInventory(db, 'PRODUCT_DELETED', { productId: req.params.id });
  emitTickets(db, 'PRODUCT_DELETED', { productId: req.params.id });

  res.json({ ok: true });
});

app.post('/api/products/restock', auth, requireAdmin, (req, res) => {
  const { productId, quantity } = req.body || {};
  const db = readDb();
  const product = db.products.find((x) => x.id === productId);

  if (!product) {
    return res.status(404).json({ ok: false, message: 'Producto no encontrado' });
  }

  product.stock = round2(safeNumber(product.stock) + safeNumber(quantity));
  product.updatedAt = new Date().toISOString();
  writeDb(db);
  broadcastInventory(db, 'RESTOCK', { productId });

  res.json({ ok: true, product });
});


app.post('/api/inventory/adjust', auth, requireAdmin, (req, res) => {
  const db = readDb();

  try {
    const result = applyInventoryAdjustment(db, {
      ...req.body,
      sourceType: 'LOCAL_ADMIN',
      actorName: req.user?.name || 'Administrador local',
      actorUsername: req.user?.username || 'admin',
      actorBranch: ADMIN_NAME
    });
    writeDb(db);
    broadcastInventory(db, 'LOCAL_INVENTORY_ADJUSTED', { productId: result.product.id });
    res.json({ ok: true, product: sanitizeInventoryProduct(result.product), movement: result.movement });
  } catch (err) {
    res.status(400).json({ ok: false, message: err.message });
  }
});

app.get('/api/tickets/open', auth, (_req, res) => {
  const db = readDb();
  res.json({ ok: true, tickets: db.openTickets || [] });
});

app.post('/api/tickets/open', auth, (req, res) => {
  const db = readDb();
  const body = req.body || {};
  const items = normalizeTicketItems(body.items || []);
  const subtotal = sumTicket(items);
  let ticket = (db.openTickets || []).find((t) => t.id === body.id);

  if (ticket) {
    Object.assign(ticket, {
      items,
      subtotal,
      operatorName: body.operatorName || ticket.operatorName,
      terminalCode: body.terminalCode || ticket.terminalCode,
      customerName: String(body.customerName || ticket.customerName || '').trim(),
      customerReference: String(body.customerReference || ticket.customerReference || '').trim(),
      updatedAt: new Date().toISOString(),
      status: 'OPEN'
    });
  } else {
    const seq = (db.openTickets || []).length + 1;
    ticket = {
      id: uuidv4(),
      folio: `TK-${String(seq).padStart(4, '0')}`,
      terminalCode: body.terminalCode || 'POS-01',
      operatorName: body.operatorName || 'Operador',
      customerName: String(body.customerName || '').trim(),
      customerReference: String(body.customerReference || '').trim(),
      takenBy: '',
      status: 'OPEN',
      items,
      subtotal,
      createdAt: new Date().toISOString(),
      updatedAt: new Date().toISOString()
    };
    db.openTickets.push(ticket);
  }

  touchTerminal(db, {
    terminalCode: body.terminalCode || 'POS-01',
    terminalType: 'POS',
    terminalName: body.terminalName || body.terminalCode || 'Punto de Venta'
  });

  writeDb(db);
  emitTickets(db, 'TICKET_UPSERT', { ticketId: ticket.id });

  res.json({ ok: true, ticket });
});

app.post('/api/tickets/take', auth, (req, res) => {
  const db = readDb();
  const { ticketId, terminalCode, operatorName } = req.body || {};
  const ticket = (db.openTickets || []).find((t) => t.id === ticketId);

  if (!ticket) {
    return res.status(404).json({ ok: false, message: 'Ticket no encontrado' });
  }

  ticket.terminalCode = terminalCode || ticket.terminalCode;
  ticket.takenBy = operatorName || ticket.takenBy;
  ticket.status = 'IN_PROGRESS';
  ticket.updatedAt = new Date().toISOString();

  touchTerminal(db, {
    terminalCode: terminalCode || 'POS-01',
    terminalType: 'POS',
    terminalName: terminalCode || 'Punto de Venta'
  });

  writeDb(db);
  emitTickets(db, 'TICKET_TAKEN', { ticketId: ticket.id });

  res.json({ ok: true, ticket });
});

app.post('/api/tickets/close', auth, (req, res) => {
  const db = readDb();
  const { ticketId } = req.body || {};

  db.openTickets = (db.openTickets || []).filter((t) => t.id !== ticketId);
  writeDb(db);
  emitTickets(db, 'TICKET_CLOSED', { ticketId });

  res.json({ ok: true });
});

app.post('/api/sales', auth, (req, res) => {
  const {
    items = [],
    terminalCode = 'UNKNOWN',
    clientTxnId,
    operatorId = '',
    operatorName = '',
    employeeNumber = '',
    ticketId = '',
    customerName = '',
    customerReference = ''
  } = req.body || {};
  const db = readDb();
  const existing = db.sales.find((s) => s.clientTxnId === clientTxnId);

  if (existing) {
    return res.json({ ok: true, sale: existing, deduped: true });
  }

  const normalizedItems = normalizeTicketItems(items);
  if (!normalizedItems.length) {
    return res.status(400).json({ ok: false, message: 'No hay productos para vender' });
  }

  if (!String(operatorName || '').trim()) {
    return res.status(400).json({ ok: false, message: 'Selecciona un operador válido antes de cobrar' });
  }

  const productMap = new Map(db.products.map((p) => [p.id, p]));

  for (const item of normalizedItems) {
    const product = productMap.get(item.productId);

    if (!product) {
      return res.status(400).json({ ok: false, message: `Producto inexistente: ${item.productId}` });
    }

    if (safeNumber(product.stock) < safeNumber(item.quantity)) {
      const conflict = {
        id: uuidv4(),
        terminalCode,
        clientTxnId,
        productId: item.productId,
        requested: item.quantity,
        available: safeNumber(product.stock),
        createdAt: new Date().toISOString()
      };

      db.saleConflicts.push(conflict);
      writeDb(db);

      return res.status(409).json({
        ok: false,
        message: `Stock insuficiente para ${product.name}`,
        conflict
      });
    }
  }

  for (const item of normalizedItems) {
    const product = productMap.get(item.productId);
    product.stock = round2(safeNumber(product.stock) - safeNumber(item.quantity));
    product.updatedAt = new Date().toISOString();
  }

  const nextSalesSequence = safeNumber(db.counters?.salesSequence, 0) + 1;
  db.counters = { ...(db.counters || {}), salesSequence: nextSalesSequence };

  const sale = {
    id: uuidv4(),
    folio: `VTA-${String(nextSalesSequence).padStart(6, '0')}`,
    terminalCode,
    operatorId: operatorId || '',
    operatorName,
    employeeNumber: employeeNumber || '',
    customerName: String(customerName || '').trim(),
    customerReference: String(customerReference || '').trim(),
    createdAt: new Date().toISOString(),
    clientTxnId: clientTxnId || uuidv4(),
    ticketId: ticketId || '',
    items: normalizedItems,
    total: sumTicket(normalizedItems)
  };

  db.sales.push(sale);

  if (ticketId) {
    db.openTickets = db.openTickets.filter((t) => t.id !== ticketId);
  }

  touchTerminal(db, {
    terminalCode,
    terminalType: 'POS',
    terminalName: terminalCode
  });

  writeDb(db);
  io.emit('sale:created', sale);
  broadcastInventory(db, 'SALE_COMMITTED', { saleId: sale.id, terminalCode });
  emitTickets(db, 'SALE_COMMITTED', { saleId: sale.id, ticketId });

  res.json({ ok: true, sale, inventory: db.products });
});

app.get('/api/sales', auth, (_req, res) => {
  res.json({ ok: true, sales: readDb().sales.slice().reverse() });
});

app.delete('/api/sales', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const removedCount = (db.sales || []).length;
  const clearedConflictsCount = (db.saleConflicts || []).length;

  db.sales = [];
  db.saleConflicts = [];
  writeDb(db);
  io.emit('sale:history-cleared', {
    ok: true,
    removedCount,
    clearedConflictsCount,
    clearedAt: new Date().toISOString()
  });

  res.json({ ok: true, removedCount, clearedConflictsCount });
});


app.get('/api/employees', auth, (req, res) => {
  const db = readDb();
  const allEmployees = (db.users || [])
    .filter((u) => u.role === 'POS')
    .sort((a, b) => String(a.name || '').localeCompare(String(b.name || ''), 'es'));

  const employees = req.user?.role === 'ADMIN'
    ? allEmployees
    : allEmployees.filter((u) => u.active !== false);

  res.json({
    ok: true,
    employees: employees.map((u) => ({
      id: u.id,
      name: u.name,
      employeeNumber: u.employeeNumber || '',
      username: u.username || '',
      active: u.active !== false,
      createdAt: u.createdAt || '',
      updatedAt: u.updatedAt || ''
    }))
  });
});

app.post('/api/employees', auth, requireAdmin, (req, res) => {
  try {
    const db = readDb();
    const payload = normalizeEmployeePayload(req.body || {});
    const duplicateNumber = (db.users || []).find((u) => u.role === 'POS' && u.employeeNumber === payload.employeeNumber && u.id !== payload.id);
    if (duplicateNumber) {
      return res.status(400).json({ ok: false, message: 'El número de empleado ya existe' });
    }

    const duplicateUsername = (db.users || []).find((u) => u.username === payload.username && u.id !== payload.id);
    if (duplicateUsername) {
      return res.status(400).json({ ok: false, message: 'El usuario generado para este empleado ya existe' });
    }

    const existing = (db.users || []).find((u) => u.id === payload.id && u.role === 'POS');
    if (existing) {
      Object.assign(existing, { ...payload, createdAt: existing.createdAt || payload.createdAt });
    } else {
      db.users.push(payload);
    }

    writeDb(db);
    res.json({ ok: true, employee: payload });
  } catch (err) {
    res.status(400).json({ ok: false, message: err.message });
  }
});

app.delete('/api/employees/:id', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const employee = (db.users || []).find((u) => u.id === req.params.id && u.role === 'POS');
  if (!employee) {
    return res.status(404).json({ ok: false, message: 'Empleado no encontrado' });
  }

  employee.active = false;
  employee.updatedAt = new Date().toISOString();
  writeDb(db);
  res.json({ ok: true });
});

app.get('/api/reports/employees', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const { range = 'day', anchorDate = '', from = '', to = '' } = req.query || {};

  let start;
  let end;
  if (from && to) {
    start = parseDateInput(from, new Date());
    end = parseDateInput(to, new Date());
    end.setDate(end.getDate() + 1);
  } else {
    ({ start, end } = getRangeBounds(range, anchorDate));
  }

  const salesInRange = (db.sales || []).filter((sale) => {
    const d = new Date(sale.createdAt);
    return d >= start && d < end;
  });

  const map = new Map();
  for (const sale of salesInRange) {
    const key = sale.operatorId || `${sale.employeeNumber || ''}-${sale.operatorName || 'SIN_OPERADOR'}`;
    if (!map.has(key)) {
      map.set(key, {
        operatorId: sale.operatorId || '',
        name: sale.operatorName || 'Sin asignar',
        employeeNumber: sale.employeeNumber || '',
        salesCount: 0,
        total: 0
      });
    }
    const row = map.get(key);
    row.salesCount += 1;
    row.total = round2(row.total + safeNumber(sale.total));
  }

  const rows = Array.from(map.values()).sort((a, b) => b.total - a.total);
  res.json({
    ok: true,
    range,
    from: start.toISOString(),
    to: end.toISOString(),
    rows,
    totals: {
      salesCount: salesInRange.length,
      total: round2(salesInRange.reduce((acc, sale) => acc + safeNumber(sale.total), 0))
    }
  });
});

app.delete('/api/sales/:id', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const sale = db.sales.find((s) => s.id === req.params.id);

  if (!sale) {
    return res.status(404).json({ ok: false, message: 'Venta no encontrada' });
  }

  const productMap = new Map(db.products.map((p) => [p.id, p]));
  for (const item of normalizeTicketItems(sale.items || [])) {
    const product = productMap.get(item.productId);
    if (product) {
      product.stock = round2(safeNumber(product.stock) + safeNumber(item.quantity));
      product.updatedAt = new Date().toISOString();
    }
  }

  db.sales = db.sales.filter((s) => s.id !== req.params.id);
  writeDb(db);
  broadcastInventory(db, 'SALE_DELETED', { saleId: req.params.id });

  res.json({ ok: true });
});

app.get('/api/terminals', auth, requireAdmin, (_req, res) => {
  res.json({ ok: true, terminals: readDb().terminals });
});

app.get('/api/conflicts', auth, requireAdmin, (_req, res) => {
  res.json({ ok: true, conflicts: readDb().saleConflicts.slice().reverse() });
});

app.get('/api/security/admin', auth, requireAdmin, (_req, res) => {
  const db = readDb();
  const admin = db.users.find((u) => u.role === 'ADMIN');
  res.json({
    ok: true,
    admin: {
      username: admin?.username || 'admin',
      name: admin?.name || 'Administrador General',
      requireAdminPasswordForEdits: db.security?.requireAdminPasswordForEdits !== false,
      remoteApiKey: db.security?.remoteAccess?.apiKey || '',
      allowRemoteAdminCredentials: db.security?.remoteAccess?.allowAdminCredentials !== false,
      allowRemoteInventoryWrite: db.security?.remoteAccess?.permissions?.writeInventory !== false
    }
  });
});

app.post('/api/security/admin', auth, requireAdmin, (req, res) => {
  const {
    currentPassword,
    username,
    newPassword,
    confirmPassword,
    name,
    requireAdminPasswordForEdits,
    remoteApiKey,
    allowRemoteAdminCredentials,
    allowRemoteInventoryWrite
  } = req.body || {};
  const db = readDb();
  const admin = db.users.find((u) => u.role === 'ADMIN');

  if (!admin) {
    return res.status(404).json({ ok: false, message: 'Administrador no encontrado' });
  }

  if (!verifyAdminPassword(db, currentPassword)) {
    return res.status(401).json({ ok: false, message: 'La contraseña actual de administrador no es válida' });
  }

  if (newPassword && newPassword !== confirmPassword) {
    return res.status(400).json({ ok: false, message: 'La confirmación de contraseña no coincide' });
  }

  if (username && String(username).trim()) {
    admin.username = String(username).trim();
  }
  if (name && String(name).trim()) {
    admin.name = String(name).trim();
  }
  if (newPassword && String(newPassword).trim()) {
    admin.password = String(newPassword).trim();
  }

  db.security = {
    ...(db.security || {}),
    requireAdminPasswordForEdits: requireAdminPasswordForEdits !== false,
    remoteAccess: {
      apiKey: String(remoteApiKey || db.security?.remoteAccess?.apiKey || '').trim() || `REMOTE-${uuidv4().replace(/-/g, '').slice(0, 24).toUpperCase()}`,
      allowAdminCredentials: allowRemoteAdminCredentials !== false,
      permissions: {
        readSummary: true,
        readInventory: true,
        writeInventory: allowRemoteInventoryWrite !== false
      }
    },
    updatedAt: new Date().toISOString()
  };

  writeDb(db);
  res.json({
    ok: true,
    admin: {
      username: admin.username,
      name: admin.name,
      requireAdminPasswordForEdits: db.security.requireAdminPasswordForEdits !== false,
      remoteApiKey: db.security?.remoteAccess?.apiKey || '',
      allowRemoteAdminCredentials: db.security?.remoteAccess?.allowAdminCredentials !== false,
      allowRemoteInventoryWrite: db.security?.remoteAccess?.permissions?.writeInventory !== false
    }
  });
});


app.get('/api/branches', auth, requireAdmin, (_req, res) => {
  const db = readDb();
  res.json({ ok: true, branches: (db.branches || []).map(getBranchSummarySkeleton) });
});

app.post('/api/branches', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const body = req.body || {};
  const name = String(body.name || '').trim();
  const code = String(body.code || '').trim();
  const remoteUrl = String(body.remoteUrl || '').trim().replace(/\/$/, '');
  const authType = body.authType === 'token' ? 'token' : 'credentials';
  const apiKey = String(body.apiKey || '').trim();
  const remoteAdminUsername = String(body.remoteAdminUsername || '').trim();
  const remoteAdminPassword = String(body.remoteAdminPassword || '').trim();

  if (!name) return res.status(400).json({ ok: false, message: 'El nombre de la sucursal es obligatorio' });
  if (!code) return res.status(400).json({ ok: false, message: 'El código de la sucursal es obligatorio' });
  if (!remoteUrl) return res.status(400).json({ ok: false, message: 'La URL remota es obligatoria' });
  if (authType === 'token' && !apiKey) return res.status(400).json({ ok: false, message: 'El token/API key remoto es obligatorio' });
  if (authType === 'credentials' && (!remoteAdminUsername || !(remoteAdminPassword || body.id))) {
    return res.status(400).json({ ok: false, message: 'Las credenciales del administrador remoto son obligatorias' });
  }

  const existing = (db.branches || []).find((b) => b.id === body.id);
  const branch = {
    id: existing?.id || uuidv4(),
    name,
    code,
    remoteUrl,
    authType,
    apiKey: authType === 'token' ? apiKey : '',
    remoteAdminUsername: authType === 'credentials' ? remoteAdminUsername : '',
    remoteAdminPassword: authType === 'credentials'
      ? (remoteAdminPassword || existing?.remoteAdminPassword || '')
      : '',
    mode: body.mode || existing?.mode || 'render',
    active: body.active !== false,
    lastStatus: existing?.lastStatus || 'PENDING',
    lastCheckedAt: existing?.lastCheckedAt || '',
    lastError: existing?.lastError || '',
    lastRemoteSummary: existing?.lastRemoteSummary || null,
    createdAt: existing?.createdAt || new Date().toISOString(),
    updatedAt: new Date().toISOString()
  };

  if (existing) {
    Object.assign(existing, branch);
  } else {
    db.branches.push(branch);
  }

  writeDb(db);
  res.json({ ok: true, branch: getBranchSummarySkeleton(branch) });
});

app.delete('/api/branches/:id', auth, requireAdmin, (req, res) => {
  const db = readDb();
  db.branches = (db.branches || []).filter((b) => b.id !== req.params.id);
  writeDb(db);
  res.json({ ok: true });
});

app.post('/api/branches/test', auth, requireAdmin, async (req, res) => {
  try {
    const candidate = {
      remoteUrl: req.body?.remoteUrl,
      authType: req.body?.authType,
      apiKey: req.body?.apiKey,
      remoteAdminUsername: req.body?.remoteAdminUsername,
      remoteAdminPassword: req.body?.remoteAdminPassword
    };
    const data = await fetchRemoteBranchSummary(candidate);
    res.json({ ok: true, remote: data });
  } catch (err) {
    res.status(502).json({ ok: false, message: `No se pudo conectar a la sucursal remota: ${err.message}` });
  }
});

app.get('/api/branches/:id/summary', auth, requireAdmin, async (req, res) => {
  const db = readDb();
  const branch = (db.branches || []).find((b) => b.id === req.params.id);
  if (!branch) return res.status(404).json({ ok: false, message: 'Sucursal no encontrada' });

  try {
    const data = await fetchRemoteBranchSummary(branch);
    branch.lastStatus = 'ONLINE';
    branch.lastCheckedAt = new Date().toISOString();
    branch.lastError = '';
    branch.lastRemoteSummary = data;
    writeDb(db);
    return res.json({ ok: true, branch: getBranchSummarySkeleton(branch), remote: data });
  } catch (err) {
    branch.lastStatus = 'OFFLINE';
    branch.lastCheckedAt = new Date().toISOString();
    branch.lastError = err.message;
    writeDb(db);
    return res.status(502).json({
      ok: false,
      message: `La sucursal no respondió: ${err.message}`,
      branch: getBranchSummarySkeleton(branch)
    });
  }
});


app.get('/api/branches/:id/inventory', auth, requireAdmin, async (req, res) => {
  const db = readDb();
  const branch = (db.branches || []).find((b) => b.id === req.params.id);
  if (!branch) return res.status(404).json({ ok: false, message: 'Sucursal no encontrada' });

  try {
    const query = new URLSearchParams();
    if (req.query?.query) query.set('query', String(req.query.query));
    if (req.query?.category) query.set('category', String(req.query.category));
    if (String(req.query?.lowOnly || '') === '1') query.set('lowOnly', '1');
    const data = await fetchRemoteBranch(branch, `/api/remote/inventory${query.toString() ? `?${query.toString()}` : ''}`);
    branch.lastStatus = 'ONLINE';
    branch.lastCheckedAt = new Date().toISOString();
    branch.lastError = '';
    writeDb(db);
    return res.json({ ok: true, branch: getBranchSummarySkeleton(branch), inventory: data });
  } catch (err) {
    branch.lastStatus = 'OFFLINE';
    branch.lastCheckedAt = new Date().toISOString();
    branch.lastError = err.message;
    writeDb(db);
    return res.status(502).json({ ok: false, message: `No se pudo consultar el inventario remoto: ${err.message}` });
  }
});

app.post('/api/branches/:id/inventory/adjust', auth, requireAdmin, async (req, res) => {
  const db = readDb();
  const branch = (db.branches || []).find((b) => b.id === req.params.id);
  if (!branch) return res.status(404).json({ ok: false, message: 'Sucursal no encontrada' });

  try {
    const data = await fetchRemoteBranch(branch, '/api/remote/inventory/adjust', {
      method: 'POST',
      body: JSON.stringify({
        productId: req.body?.productId,
        operation: req.body?.operation,
        quantity: req.body?.quantity,
        targetStock: req.body?.targetStock,
        reason: req.body?.reason,
        actorName: req.user?.name || 'Administrador maestro',
        actorUsername: req.user?.username || 'admin',
        actorBranch: ADMIN_NAME
      })
    });
    branch.lastStatus = 'ONLINE';
    branch.lastCheckedAt = new Date().toISOString();
    branch.lastError = '';
    writeDb(db);
    return res.json({ ok: true, branch: getBranchSummarySkeleton(branch), result: data });
  } catch (err) {
    branch.lastStatus = 'OFFLINE';
    branch.lastCheckedAt = new Date().toISOString();
    branch.lastError = err.message;
    writeDb(db);
    return res.status(502).json({ ok: false, message: `No se pudo ajustar el inventario remoto: ${err.message}` });
  }
});

app.post('/api/branches/refresh-all', auth, requireAdmin, async (_req, res) => {
  const db = readDb();
  const branches = db.branches || [];
  const results = [];

  for (const branch of branches) {
    try {
      const data = await fetchRemoteBranchSummary(branch);
      branch.lastStatus = 'ONLINE';
      branch.lastCheckedAt = new Date().toISOString();
      branch.lastError = '';
      branch.lastRemoteSummary = data;
      results.push({ id: branch.id, ok: true, name: branch.name, checkedAt: branch.lastCheckedAt });
    } catch (err) {
      branch.lastStatus = 'OFFLINE';
      branch.lastCheckedAt = new Date().toISOString();
      branch.lastError = err.message;
      results.push({ id: branch.id, ok: false, name: branch.name, message: err.message, checkedAt: branch.lastCheckedAt });
    }
  }

  writeDb(db);
  res.json({ ok: true, results, branches: branches.map(getBranchSummarySkeleton) });
});

app.get('/api/fiscal/config', auth, requireAdmin, (_req, res) => {
  res.json({ ok: true, config: readDb().fiscalConfig });
});

app.post('/api/fiscal/config', auth, requireAdmin, (req, res) => {
  const db = readDb();
  db.fiscalConfig = { ...db.fiscalConfig, ...(req.body || {}) };
  writeDb(db);
  res.json({ ok: true, config: db.fiscalConfig });
});

app.get('/api/fiscal/customers', auth, requireAdmin, (_req, res) => {
  res.json({ ok: true, customers: readDb().fiscalCustomers });
});

app.post('/api/fiscal/customers', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const customer = { id: uuidv4(), ...req.body };
  db.fiscalCustomers.push(customer);
  writeDb(db);
  res.json({ ok: true, customer });
});

app.post('/api/invoices/generate', auth, requireAdmin, (req, res) => {
  const db = readDb();
  const { saleId, customerId, useCfdi, paymentMethod, paymentForm } = req.body || {};

  const sale = db.sales.find((s) => s.id === saleId);
  if (!sale) {
    return res.status(404).json({ ok: false, message: 'Venta no encontrada' });
  }

  const customer = db.fiscalCustomers.find((c) => c.id === customerId);
  if (!customer) {
    return res.status(404).json({ ok: false, message: 'Cliente fiscal no encontrado' });
  }

  const invoice = {
    id: uuidv4(),
    uuid: `MOCK-${Date.now()}`,
    saleId,
    customerId,
    status: 'MOCK_READY',
    pacProvider: db.fiscalConfig.pacProvider || 'SW',
    createdAt: new Date().toISOString(),
    useCfdi: useCfdi || customer.useCfdi || 'S01',
    paymentMethod: paymentMethod || 'PUE',
    paymentForm: paymentForm || '01',
    xml: `<cfdi mock="true" saleId="${saleId}" customerId="${customerId}" total="${Number(sale.total || 0).toFixed(2)}"></cfdi>`,
    pdfText: `Factura mock ${sale.folio} - ${customer.name || customer.razonSocial}`
  };

  db.invoices.push(invoice);
  writeDb(db);

  res.json({ ok: true, invoice });
});

app.get('/api/invoices', auth, requireAdmin, (_req, res) => {
  res.json({ ok: true, invoices: readDb().invoices.slice().reverse() });
});

// Rutas reservadas para futura sincronización entre sucursales vía Render.
// app.get('/api/branches/:branchId/summary', auth, requireAdmin, (_req, res) => {
//   res.status(501).json({ ok: false, message: 'Módulo de sucursales aún no habilitado' });
// });

io.on('connection', (socket) => {
  socket.on('terminal:hello', (payload) => {
    const db = readDb();
    touchTerminal(db, payload || {});
    writeDb(db);

    socket.emit('inventory:update', {
      products: db.products,
      lowStock: db.products.filter(isLowStock).map((p) => p.id),
      reason: 'HELLO_SYNC'
    });

    socket.emit('tickets:update', {
      tickets: db.openTickets || [],
      reason: 'HELLO_SYNC'
    });
  });
});

server.listen(PORT, '0.0.0.0', () => {
  console.log(`LAN backend escuchando en 0.0.0.0:${PORT}`);
});

const udp = dgram.createSocket('udp4');

udp.on('message', (msg, rinfo) => {
  try {
    const data = JSON.parse(String(msg));
    if (data.type !== 'DISCOVER_LAN_ADMIN') return;

    const reply = Buffer.from(
      JSON.stringify({ type: 'LAN_ADMIN_HERE', adminName: ADMIN_NAME, port: PORT })
    );

    udp.send(reply, 0, reply.length, rinfo.port, rinfo.address);
  } catch {}
});

udp.bind(DISCOVERY_PORT, '0.0.0.0', () => {
  console.log(`Descubrimiento UDP en ${DISCOVERY_PORT}`);
});
