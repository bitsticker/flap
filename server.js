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
const monitoredTokens = new Map(); // { tokenAddress => { contract, decimals, filter } }
const tokenTransactions = new Map();
const tokenCreatorInfo = new Map();
const tokenMetrics = new Map();

// ==================== 初始化监听 ====================
async function startMonitor() {
  try {
    provider = new ethers.WebSocketProvider(WSS);
    portal = new ethers.Contract(PORTAL, PORTAL_ABI, provider);
    helper = new ethers.Contract(TAX_HELPER, HELPER_ABI, provider);

    console.log('✅ BSC_WSS 监控已启动（毫秒级实时）');
    console.log(`🔥 过滤条件：买税 ≤ ${MAX_TAX_RATE}% 且 卖税 ≤ ${MAX_TAX_RATE}% 且 分红 = ${TARGET_DIVIDEND}%`);
    console.log('📡 只监听符合条件代币的实时 Transfer 事件\n');

    portal.on('TokenCreated', async (ts, creator, nonce, token, name, symbol, meta, event) => {
      const start = Date.now();
      statsTotal++;
      
      try {
        const taxInfo = await getTaxInfo(token);
        
        if (!meetsCriteria(taxInfo)) {
          const latency = Date.now() - start;
          console.log(`⏭️  [${latency}ms] 过滤 → ${symbol}`);
          return;
        }

        const data = await processToken({
          ts: Number(ts),
          creator: creator.toLowerCase(),
          token: token.toLowerCase(),
          name,
          symbol,
          metaCid: meta,
          txHash: event.log.transactionHash,
          blockNumber: event.log.blockNumber,
          taxInfo
        });

        const latency = Date.now() - start;
        data.latency = latency;
        statsFiltered++;
        
        tokenTransactions.set(token.toLowerCase(), []);
        tokenCreatorInfo.set(token.toLowerCase(), {
          creator: creator.toLowerCase(),
          buyAmount: '0',
          buyCount: 0,
          buyTxHash: null,
          decimals: 18
        });
        
        tokenMetrics.set(token.toLowerCase(), {
          totalTokenSupply: 0,
          totalBNBInvested: 0,
          currentPrice: 0,
          marketCapBNB: 0,
          marketCapUSD: 0,
          buyVolume: 0,
          sellVolume: 0,
          lastPrice: 0
        });
        
        // 启动实时 Transfer 监听
        await startTokenMonitoring(token.toLowerCase(), creator.toLowerCase(), data);
        
        io.emit('newToken', data);
        console.log(`✅ [${latency}ms] 发现符合条件的代币 → $${symbol}\n`);

        if (statsTotal % 50 === 0) {
          const ratio = ((statsFiltered / statsTotal) * 100).toFixed(2);
          console.log(`\n📊 ============ 统计报告 ============`);
          console.log(`   📡 链上监听总数：${statsTotal} 个`);
          console.log(`   ✅ 符合条件已推送：${statsFiltered} 个`);
          console.log(`   📝 总 Transfer 事件：${statsTransactions} 笔`);
          console.log(`   🔍 实时监听中的 CA 数：${monitoredTokens.size}`);
          console.log(`=====================================\n`);
        }
      } catch (err) {
        console.error('⚠️  处理失败:', err.message);
      }
    });

    provider.websocket.on('close', () => {
      console.log('⚠️ WebSocket 断开，3秒后重连...');
      setTimeout(startMonitor, 3000);
    });
  } catch (e) {
    console.error('启动失败:', e.message);
    setTimeout(startMonitor, 5000);
  }
}

// ==================== 启动实时 Transfer 监听 ====================
async function startTokenMonitoring(tokenAddress, creator, tokenData) {
  try {
    const contract = new ethers.Contract(tokenAddress, ERC20_ABI, provider);
    
    let decimals = 18;
    try {
      decimals = await contract.decimals();
    } catch (err) {
      console.log(`⚠️  无法获取 decimals，使用默认值 18`);
    }
    
    const creatorInfo = tokenCreatorInfo.get(tokenAddress);
    if (creatorInfo) {
      creatorInfo.decimals = decimals;
    }
    
    // 【关键】创建针对该代币的 Transfer 过滤器
    // 这样每个代币有自己独立的监听器，不会收到其他代币的事件
    const transferFilter = contract.filters.Transfer();
    
    // 实时监听该代币的所有 Transfer 事件
    contract.on(transferFilter, (from, to, amount, event) => {
      const startTime = Date.now();
      
      const fromLower = from.toLowerCase();
      const toLower = to.toLowerCase();
      const amountNum = parseInt(amount.toString());
      const amountFormatted = (amountNum / Math.pow(10, decimals)).toFixed(8);
      
      const isMint = fromLower === '0x0000000000000000000000000000000000000000';
      const isBurn = toLower === '0x0000000000000000000000000000000000000000';
      
      const txData = {
        token: tokenAddress,
        from: fromLower,
        to: toLower,
        amount: amount.toString(),
        amountFormatted: amountFormatted,
        transactionHash: event.log.transactionHash,
        blockNumber: event.log.blockNumber,
        timestamp: Date.now(),
        type: isMint ? 'mint' : isBurn ? 'burn' : 'transfer',
        isBuy: isMint,
        isSell: isBurn,
        isCreatorBuy: toLower === creator && isMint
      };
      
      const processingTime = Date.now() - startTime;
      console.log(`📝 [Transfer] ${tokenAddress.slice(0, 10)}... | ${txData.type.padEnd(8)} | ${amountFormatted} | ${processingTime}ms`);
      statsTransactions++;
      
      updateTokenMetrics(tokenAddress, txData, decimals);
      
      if (txData.isCreatorBuy) {
        const creatorInfo = tokenCreatorInfo.get(tokenAddress);
        if (creatorInfo) {
          const prevAmount = parseFloat(creatorInfo.buyAmount);
          creatorInfo.buyAmount = (prevAmount + parseFloat(amountFormatted)).toFixed(8);
          creatorInfo.buyCount++;
          creatorInfo.buyTxHash = event.log.transactionHash;
          
          io.emit('creatorBuyUpdate', {
            token: tokenAddress,
            buyAmount: creatorInfo.buyAmount,
            buyCount: creatorInfo.buyCount,
            buyTxHash: creatorInfo.buyTxHash
          });
          
          console.log(`💰 [创建者购买] ${tokenAddress.slice(0, 10)}... | 累计: ${creatorInfo.buyAmount}`);
        }
      }
      
      // 存储交易记录
      const transactions = tokenTransactions.get(tokenAddress) || [];
      transactions.unshift(txData);
      if (transactions.length > 1000) transactions.pop();
      tokenTransactions.set(tokenAddress, transactions);
      
      // 推送到前端
      const metrics = tokenMetrics.get(tokenAddress) || {};
      io.emit('tokenTransaction', {
        token: tokenAddress,
        from: fromLower,
        to: toLower,
        amount: amount.toString(),
        amountFormatted: amountFormatted,
        transactionHash: event.log.transactionHash,
        blockNumber: event.log.blockNumber,
        timestamp: Date.now(),
        type: txData.type,
        isBuy: isMint,
        isSell: isBurn,
        metrics: {
          marketCapUSD: metrics.marketCapUSD,
          currentPrice: metrics.currentPrice,
          totalBNBInvested: metrics.totalBNBInvested
        }
      });
    });
    
    monitoredTokens.set(tokenAddress, { contract, decimals, filter: transferFilter });
    console.log(`   ✅ Transfer 监听已启动: ${tokenAddress.slice(0, 10)}...\n`);
  } catch (err) {
    console.error(`❌ 监听失败 ${tokenAddress}:`, err.message);
  }
}

// ==================== 更新市值指标 ====================
function updateTokenMetrics(tokenAddress, txData, decimals) {
  const metrics = tokenMetrics.get(tokenAddress);
  if (!metrics) return;
  
  const amount = parseFloat(txData.amountFormatted);
  
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

// ==================== 链上数据筛选 ====================
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

// ==================== 处理单个代币 ====================
async function processToken(raw) {
  const [meta] = await Promise.all([
    fetchMeta(raw.metaCid)
  ]);

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

app.get('/api/creator/:token', (req, res) => {
  const token = req.params.token.toLowerCase();
  const creatorInfo = tokenCreatorInfo.get(token) || {};
  res.json(creatorInfo);
});

app.get('/api/metrics/:token', (req, res) => {
  const token = req.params.token.toLowerCase();
  const metrics = tokenMetrics.get(token) || {};
  res.json(metrics);
});

// ==================== Socket 连接 ====================
io.on('connection', (socket) => {
  console.log('✅ 前端已连接:', socket.id);
  
  socket.on('getTransactions', (token) => {
    const transactions = tokenTransactions.get(token.toLowerCase()) || [];
    socket.emit('transactionsUpdate', {
      token,
      data: transactions.slice(0, 100)
    });
  });
  
  socket.on('getCreatorInfo', (token) => {
    const creatorInfo = tokenCreatorInfo.get(token.toLowerCase()) || {};
    socket.emit('creatorInfoUpdate', {
      token,
      ...creatorInfo
    });
  });
  
  socket.on('getMetrics', (token) => {
    const metrics = tokenMetrics.get(token.toLowerCase()) || {};
    socket.emit('metricsUpdate', {
      token,
      ...metrics
    });
  });
  
  socket.on('disconnect', () => console.log('❌ 前端断开:', socket.id));
});

// 启动
server.listen(PORT, () => {
  console.log(`🚀 服务器运行在 http://localhost:${PORT}`);
  startMonitor();
});
