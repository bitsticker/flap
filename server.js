require('dotenv').config();
const express = require('express');
const http = require('http');
const { Server } = require('socket.io');
const { ethers } = require('ethers');
const path = require('path');

const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }
});

app.use(express.static(path.join(__dirname, 'public')));

// ==================== 配置 ====================
const PORTAL = '0xe2cE6ab80874Fa9Fa2aAE65D277Dd6B8e65C9De0';
const TAX_HELPER = '0x53841c73217735F37BC1775538b03b23feFD8346';
const WSS = process.env.BSC_WSS || 'wss://bsc-rpc.publicnode.com';
const IPFS = 'https://flap.mypinata.cloud/ipfs/';
const PORT = process.env.PORT || 3000;
const BNB_PRICE_USD = 600;

const MAX_TAX_RATE = 5;
const TARGET_DIVIDEND = 100;

const PORTAL_ABI = [
  'event TokenCreated(uint256 ts, address creator, uint256 nonce, address token, string name, string symbol, string meta)'
];

const HELPER_ABI = [
  `function getTaxTokenInfoV2(address) view returns (
    tuple(
      uint16 marketBps, uint16 deflationBps, uint16 lpBps, uint16 dividendBps,
      uint16 buyTaxRate, uint16 sellTaxRate,
      uint256, uint256, uint256, uint256, uint256,
      address dividendToken, address, uint256,
      tuple(address, address, uint8, bool, bool, bool)
    )
  )`
];

const ERC20_ABI = [
  'event Transfer(address indexed from, address indexed to, uint256 value)',
  'function decimals() view returns (uint8)',
  'function balanceOf(address account) view returns (uint256)',
  'function totalSupply() view returns (uint256)'
];

let provider, portal, helper;
let statsTotal = 0;
let statsFiltered = 0;
let statsTransactions = 0;

const monitoredTokens = new Map();
const tokenInfo = new Map();
const tokenTransactions = new Map();
const tokenMetrics = new Map();

// ==================== 初始化 ====================
async function startMonitor() {
  try {
    provider = new ethers.WebSocketProvider(WSS);
    portal = new ethers.Contract(PORTAL, PORTAL_ABI, provider);
    helper = new ethers.Contract(TAX_HELPER, HELPER_ABI, provider);

    console.log('\n✅ ========== 启动 Flap 监控 ==========');
    console.log(`🔥 过滤条件: 买税 ≤ ${MAX_TAX_RATE}% 且 卖税 ≤ ${MAX_TAX_RATE}% 且 分红 = ${TARGET_DIVIDEND}%`);
    console.log(`📡 监听方式: 基于区块事件的 Transfer 查询`);
    console.log(`🌐 前端地址: http://localhost:${PORT}`);
    console.log('=====================================\n');

    // 监听 TokenCreated 事件
    portal.on('TokenCreated', async (ts, creator, nonce, token, name, symbol, meta, event) => {
      const start = Date.now();
      statsTotal++;
      
      try {
        const taxInfo = await getTaxInfo(token);
        
        if (!meetsCriteria(taxInfo)) {
          return;
        }

        const data = await processToken({
          ts: Number(ts),
          creator: creator.toLowerCase(),
          token: token.toLowerCase(),
          name,
          symbol,
          metaCid: meta,
          taxInfo
        });

        const latency = Date.now() - start;
        data.latency = latency;
        statsFiltered++;
        
        // 初始化存储
        tokenInfo.set(token.toLowerCase(), {
          ...data,
          creator: creator.toLowerCase()
        });
        tokenTransactions.set(token.toLowerCase(), []);
        tokenMetrics.set(token.toLowerCase(), {
          totalTokenSupply: 0,
          totalBNBInvested: 0,
          currentPrice: 0,
          marketCapBNB: 0,
          marketCapUSD: 0,
          buyVolume: 0,
          sellVolume: 0
        });
        
        // 启动监听
        startTokenMonitoring(token.toLowerCase(), creator.toLowerCase());
        
        // 推送到前端
        io.emit('newToken', data);
        console.log(`✅ [${latency}ms] 发现符合条件代币 → $${symbol} (${token.slice(0, 10)}...)`);

        if (statsTotal % 50 === 0) {
          console.log(`\n📊 统计: 总发现=${statsTotal} 符合=${statsFiltered} 交易=${statsTransactions} 监听=${monitoredTokens.size}\n`);
        }
      } catch (err) {
        console.error('⚠️  处理失败:', err.message);
      }
    });

    provider.websocket.on('close', () => {
      console.log('⚠️ WebSocket 断开连接，3秒后重连...');
      setTimeout(startMonitor, 3000);
    });

    provider.on('error', (err) => {
      console.error('Provider 错误:', err.message);
    });
  } catch (e) {
    console.error('启动失败:', e.message);
    setTimeout(startMonitor, 5000);
  }
}

// ==================== 启动代币监听 ====================
async function startTokenMonitoring(tokenAddress, creator) {
  try {
    const contract = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
    
    let decimals = 18;
    try {
      decimals = await contract.decimals();
    } catch (err) {
      console.log(`⚠️  获取 decimals 失败，使用默认值 18`);
    }

    // 创建 Transfer 过滤器
    const transferFilter = contract.filters.Transfer();

    // 监听 Transfer 事件
    const transferHandler = async (from, to, amount, event) => {
      try {
        handleTransfer(tokenAddress, creator, from, to, amount, decimals);
      } catch (err) {
        console.error('处理 Transfer 错误:', err.message);
      }
    };

    contract.on(transferFilter, transferHandler);

    // 存储监听器信息
    monitoredTokens.set(tokenAddress, {
      contract,
      decimals,
      handler: transferHandler,
      filter: transferFilter
    });

    console.log(`   ✅ Transfer 监听已启动: ${tokenAddress.slice(0, 10)}...\n`);
  } catch (err) {
    console.error(`❌ 启动监听失败 ${tokenAddress}:`, err.message);
  }
}

// ==================== 处理 Transfer 事件 ====================
function handleTransfer(tokenAddress, creator, from, to, amount, decimals) {
  const fromLower = from.toLowerCase();
  const toLower = to.toLowerCase();
  const amountBig = BigInt(amount.toString());
  const amountFormatted = Number(amountBig * BigInt(10 ** (18 - decimals))) / (10 ** 18);

  const isMint = fromLower === '0x0000000000000000000000000000000000000000';
  const isBurn = toLower === '0x0000000000000000000000000000000000000000';

  // 构建交易数据
  const txData = {
    token: tokenAddress,
    from: fromLower,
    to: toLower,
    amount: amount.toString(), // 原始金额（BigInt 字符串）
    amountFormatted: amountFormatted.toFixed(8),
    amountDecimal: amountFormatted,
    timestamp: Date.now(),
    type: isMint ? 'mint' : isBurn ? 'burn' : 'transfer',
    isBuy: isMint,
    isSell: isBurn,
    isCreatorBuy: toLower === creator && isMint
  };

  statsTransactions++;
  console.log(`📝 [TX] ${tokenAddress.slice(0, 8)}... | ${txData.type.padEnd(8)} | ${amountFormatted.toFixed(6)}`);

  // 更新指标
  updateMetrics(tokenAddress, txData, decimals);

  // 存储交易
  const txList = tokenTransactions.get(tokenAddress) || [];
  txList.unshift(txData);
  if (txList.length > 1000) txList.pop();
  tokenTransactions.set(tokenAddress, txList);

  // 获取指标
  const metrics = tokenMetrics.get(tokenAddress) || {};

  // 推送到前端
  const emitData = {
    token: tokenAddress,
    from: fromLower,
    to: toLower,
    amount: amountFormatted.toFixed(8),
    timestamp: Date.now(),
    type: txData.type,
    isBuy: isMint,
    isSell: isBurn,
    metrics: {
      marketCapUSD: metrics.marketCapUSD || '0',
      currentPrice: metrics.currentPrice || '0',
      buyVolume: (metrics.buyVolume || 0).toFixed(2),
      sellVolume: (metrics.sellVolume || 0).toFixed(2),
      totalTokenSupply: (metrics.totalTokenSupply || 0).toFixed(2)
    }
  };

  io.emit('tokenTransaction', emitData);
}

// ==================== 更新指标 ====================
function updateMetrics(tokenAddress, txData, decimals) {
  const metrics = tokenMetrics.get(tokenAddress);
  if (!metrics) return;

  const amount = txData.amountDecimal;

  if (txData.isBuy) {
    metrics.totalTokenSupply += amount;
    metrics.buyVolume += amount;
  } else if (txData.isSell) {
    metrics.totalTokenSupply = Math.max(0, metrics.totalTokenSupply - amount);
    metrics.sellVolume += amount;
  }

  if (txData.isBuy) {
    const estimatedBNB = amount * 0.00001;
    metrics.totalBNBInvested += estimatedBNB;
  }

  if (metrics.totalTokenSupply > 0 && metrics.totalBNBInvested > 0) {
    metrics.currentPrice = (metrics.totalBNBInvested / metrics.totalTokenSupply).toFixed(10);
    metrics.marketCapBNB = (metrics.totalTokenSupply * parseFloat(metrics.currentPrice)).toFixed(4);
    metrics.marketCapUSD = (parseFloat(metrics.marketCapBNB) * BNB_PRICE_USD).toFixed(2);
  }
}

// ==================== 筛选条件 ====================
function meetsCriteria(taxInfo) {
  if (!taxInfo) return false;

  const buyTax = taxInfo.buyTaxRate / 100;
  const sellTax = taxInfo.sellTaxRate / 100;
  const dividend = taxInfo.dividendBps / 100;

  if (buyTax > MAX_TAX_RATE || sellTax > MAX_TAX_RATE) return false;
  if (dividend !== TARGET_DIVIDEND) return false;

  return true;
}

// ==================== 获取税务信息 ====================
async function getTaxInfo(token) {
  try {
    const info = await helper.getTaxTokenInfoV2(token);
    return {
      buyTaxRate: Number(info.buyTaxRate),
      sellTaxRate: Number(info.sellTaxRate),
      dividendBps: Number(info.dividendBps)
    };
  } catch {
    return null;
  }
}

// ==================== 处理代币信息 ====================
async function processToken(raw) {
  const [meta] = await Promise.all([fetchMeta(raw.metaCid)]);

  return {
    ...raw,
    image: meta?.image
      ? (meta.image.startsWith('http') ? meta.image : IPFS + meta.image)
      : null,
    twitter: meta?.twitter || null,
    telegram: meta?.telegram || null,
    website: meta?.website || null,
    description: meta?.description || null,
    buyTax: (raw.taxInfo.buyTaxRate / 100).toFixed(1),
    sellTax: (raw.taxInfo.sellTaxRate / 100).toFixed(1),
    dividend: (raw.taxInfo.dividendBps / 100).toFixed(0),
    creatorBuy: '0'
  };
}

async function fetchMeta(cid) {
  try {
    const res = await fetch(IPFS + cid, { signal: AbortSignal.timeout(2000) });
    if (res.ok) return await res.json();
  } catch {}
  return null;
}

// ==================== API 端点 ====================
app.get('/api/transactions/:token', (req, res) => {
  const token = req.params.token.toLowerCase();
  const transactions = tokenTransactions.get(token) || [];
  res.json({
    token,
    total: transactions.length,
    data: transactions.slice(0, 100)
  });
});

app.get('/api/metrics/:token', (req, res) => {
  const token = req.params.token.toLowerCase();
  const metrics = tokenMetrics.get(token) || {};
  res.json(metrics);
});

// ==================== Socket 连接 ====================
io.on('connection', (socket) => {
  console.log('✅ 前端连接:', socket.id);
  
  socket.on('disconnect', () => {
    console.log('❌ 前端断开:', socket.id);
  });
});

// ==================== 启动服务器 ====================
server.listen(PORT, () => {
  console.log(`\n🚀 服务器启动在 http://localhost:${PORT}\n`);
  startMonitor();
});
