const path = require('path')
const fs = require('fs')
const cp = require('child_process')
const constants = require('./constants.js')
const defer = require('./utils.js').defer
const jsonrpc = require('./utils.js').jsonrpc
const express = require('express')
const cors = require('cors')
const bodyParser = require('body-parser')
const log = require('debug')('info:api-app')
const EthService = require('./eth-service.js')
const BN = require('web3').utils.BN
const models = require('plasma-utils').serialization.models
const SignedTransaction = models.SignedTransaction
const debug = require('debug')

if (process.env.DEBUG === undefined) {
  // If no logging is enabled, set these as defaults
  debug.enable('info:state,info:block-store,info:leveldb-sum-tree')
}

// Set up express
const app = express()
// Set up child processes
let stateManager
let blockManager
let started = false
const alreadyStartedError = new Error('Operator already started!')

app.use(bodyParser.json())
app.use(cors())

// Setup simple message queue
const messageQueue = {}
let messageCounter = 0

function sendMessage (process, message) {
  const deferred = defer()
  process.send({
    ipcID: messageCounter,
    message
  })
  messageQueue[messageCounter] = { resolve: deferred.resolve }
  messageCounter++
  return deferred.promise
}

function resolveMessage (m) {
  log('Resolving message with ipcID', m.ipcID)
  if (m.ipcID === -1) {
    // If this is a message directly from a child, it must be the root hash from the block-store
    log('Got new block root:', m.message.rootHash, '- submitting to Ethereum')
    EthService.submitRootHash(m.message.rootHash)
    return
  }
  messageQueue[m.ipcID].resolve(m)
  delete messageQueue[m.ipcID]
}

// Handle incoming transactions
app.post('/api', function (req, res) {
  log('INCOMING RPC request with method:', req.body.method, 'and rpcID:', req.body.id)
  if (req.body.method === constants.DEPOSIT_METHOD ||
      req.body.method === constants.ADD_TX_METHOD ||
      req.body.method === constants.NEW_BLOCK_METHOD ||
      req.body.method === constants.GET_BLOCK_NUMBER_METHOD ||
      req.body.method === constants.GET_TXS_METHOD ||
      req.body.method === constants.GET_RECENT_TXS_METHOD) {
    if (req.body.method === constants.ADD_TX_METHOD) {
      // For performance, check sigs here
      try {
        const tx = new SignedTransaction(req.body.params[0])
        if (tx.checkSigs() === false) {
          throw new Error('Invalid signature on tx!')
        }
      } catch (err) {}
    }
    sendMessage(stateManager, req.body).then((response) => {
      log('OUTGOING response to RPC request with method:', req.body.method, 'and rpcID:', req.body.id)
      if (req.body.method === constants.ADD_TX_METHOD) {
        // Have Operator sign transaction
        const tx = new SignedTransaction(req.body.params[0])
        response.message.signature = EthService.web3.eth.accounts.sign(tx.hash, EthService.web3.eth.accounts.wallet[0].privateKey)
      }
      res.send(response.message)
    })
  } else if (req.body.method === constants.GET_HISTORY_PROOF ||
             req.body.method === constants.GET_BLOCK_METADATA_METHOD ||
             req.body.method === constants.GET_TX_FROM_HASH_METHOD ||
             req.body.method === constants.GET_BLOCK_TXS_METHOD) {
    sendMessage(blockManager, req.body).then((response) => {
      log('OUTGOING response to RPC request with method:', req.body.method, 'and rpcID:', req.body.id)
      res.send(response.message)
    })
  } else if (req.body.method === constants.GET_ETH_INFO_METHOD) {
    res.send({ result: {
      operatorAddress: EthService.operatorAddress,
      plasmaRegistryAddress: EthService.ethDB.plasmaRegistryAddress,
      plasmaChainAddress: EthService.ethDB.plasmaChainAddress
    }})
  }
})

async function startup (config) {
  if (started) {
    throw alreadyStartedError
  }
  // Make a new db directory if it doesn't exist.
  if (!fs.existsSync(config.dbDir)) {
    log('Creating a new db directory because it does not exist')
    fs.mkdirSync(config.dbDir, { recursive: true })
    fs.mkdirSync(config.ethDBDir)
  }
  try {
    // Setup web3
    await EthService.startup(config)
    // Setup our child processes -- stateManager & blockManager
    stateManager = cp.fork(path.join(__dirname, '/state-manager/app.js'))
    blockManager = cp.fork(path.join(__dirname, '/block-manager/app.js'))
    stateManager.on('message', resolveMessage)
    blockManager.on('message', resolveMessage)
    // Now send an init message
    await sendMessage(stateManager, jsonrpc(constants.INIT_METHOD, {
      stateDBDir: config.stateDBDir,
      txLogDir: config.txLogDir
    }))
    await sendMessage(blockManager, jsonrpc(constants.INIT_METHOD, {
      blockDBDir: config.blockDBDir,
      txLogDir: config.txLogDir
    }))
    // Set up the eth event watchers
    log('Registering Ethereum event watcher for `DepositEvent`')
    EthService.eventWatchers['DepositEvent'].subscribe(_submitDeposits)
    // Set up auto new block creator
    if (config.blockTimeInSeconds !== undefined) {
      const blockTimeInMiliseconds = parseInt(config.blockTimeInSeconds) * 1000
      setTimeout(() => newBlockTrigger(blockTimeInMiliseconds), blockTimeInMiliseconds)
    }
  } catch (err) {
    throw err
  }
  log('Finished sub process startup')
  app.listen(config.port, '0.0.0.0', () => {
    console.log('\x1b[36m%s\x1b[0m', `Operator listening on port ${config.port}!`)
  })
  started = true
}

// Startup that will only run once
async function safeStartup (config) {
  try {
    await startup(config)
  } catch (err) {
    if (err !== alreadyStartedError) {
      // If this error is anything other than an already started error, throw it
      throw err
    }
    log('Startup has already been run... skipping...')
  }
}

async function _submitDeposits (err, depositEvents) {
  if (err) {
    throw err
  }
  for (const e of depositEvents) {
    // Decode the event...
    const depositEvent = e.returnValues
    log('Detected deposit event with start:', depositEvent.untypedStart, '- & end:', depositEvent.untypedEnd, 'and id:', e.id)
    const recipient = depositEvent.depositer
    const token = new BN(depositEvent.tokenType, 10).toString('hex')
    const start = new BN(depositEvent.untypedStart, 10).toString('hex')
    const end = new BN(depositEvent.untypedEnd, 10).toString('hex')
    // Send the deposit to the state manager
    await sendMessage(stateManager, jsonrpc(constants.DEPOSIT_METHOD, {
      id: e.id,
      recipient,
      token,
      start,
      end
    }))
    // Send the deposit to the block manager
    await sendMessage(blockManager, jsonrpc(constants.DEPOSIT_METHOD, {
      id: e.id,
      recipient,
      token,
      start,
      end
    }))
  }
}

async function newBlockTrigger (blockTime) {
  const newBlockReq = {
    method: constants.NEW_BLOCK_METHOD
  }
  const response = await sendMessage(stateManager, newBlockReq)
  log('New block created with blocknumber:', response.message.newBlockNumber)
  setTimeout(() => newBlockTrigger(blockTime), blockTime)
}

module.exports = {
  app,
  startup,
  safeStartup
}
