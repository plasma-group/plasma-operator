/* eslint-env mocha */

const fs = require('fs')
const chai = require('chai')
const log = require('debug')('test:info:test-block-store')
const levelup = require('levelup')
const leveldown = require('leveldown')
const BlockStore = require('../../src/block-manager/block-store.js')
const BN = require('web3').utils.BN
const dummyTxs = require('./dummy-tx-utils')
const EthService = require('../../src/eth-service.js')
const readConfigFile = require('../../src/utils.js').readConfigFile
const path = require('path')
const BLOCKNUMBER_BYTE_SIZE = require('../../src/constants.js').BLOCKNUMBER_BYTE_SIZE
// const constants = require('../../src/constants.js')
const models = require('plasma-utils').serialization.models
const PlasmaMerkleSumTree = require('plasma-utils').PlasmaMerkleSumTree
const UnsignedTransaction = models.UnsignedTransaction
const TransferProof = models.TransferProof
const getDepositTransaction = require('../../src/utils.js').getDepositTransaction

const expect = chai.expect

function getTxBundle (txs) {
  const txBundle = []
  for (const tx of txs) {
    txBundle.push([tx, Buffer.from(tx.encoded, 'hex')])
  }
  return txBundle
}

function getUnsignedTransaction (tx) {
  const unsignedTx = new UnsignedTransaction({block: tx.block, transfers: tx.transfers})
  return unsignedTx
}

function getHexStringProof (proof) { // TODO: Remove this and instead support buffers by default
  let inclusionProof = []
  for (const sibling of proof) {
    inclusionProof.push(sibling.toString('hex'))
  }
  return inclusionProof
}

describe('BlockStore', function () {
  let db
  let web3
  let plasmaChain
  let blockStore
  let config

  beforeEach(async () => {
    // Startup with test config file
    const configFile = path.join(__dirname, '..', 'config-test.json')
    config = readConfigFile(configFile, 'test')
    // Create dbDir and ethDB dir directory
    if (!fs.existsSync(config.dbDir)) {
      log('Creating a new db directory because it does not exist')
      fs.mkdirSync(config.dbDir)
      fs.mkdirSync(config.txLogDir)
    }
    // Copy a sample tx log to the dbDir
    const TEST_BLOCK_FILENAME = 'test-block-0001.bin'
    fs.copyFileSync(path.join(__dirname, 'tx-log', TEST_BLOCK_FILENAME), config.txLogDir + '0001')
    // Start up a new chain
    await EthService.startup(config)
    web3 = EthService.web3
    plasmaChain = EthService.plasmaChain
    db = levelup(leveldown(config.blockDBDir))
    blockStore = new BlockStore(db, config.txLogDir)
  })

  it('ingests a block without fail', async () => {
    await blockStore.addBlock('0001')
    // await blockStore.ingestBlock('00000000000000000000000000000002')
    expect(blockStore).to.not.equal(undefined)
  })

  // it('gets range correctly', async () => {
  //   const TXs = dummyTxs.genNSequentialTransactionsSpacedByOne(100)
  //   const txBundle = getTxBundle(TXs)
  //   const blockNumber = Buffer.from([0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
  //   blockStore.storeTransactions(blockNumber, txBundle)
  //   const res = await blockStore.getTransactionsAt(blockNumber, new BN(0), new BN(1), new BN(4))
  //   // Should print out the ranges 1, 2, 3
  //   for (let r of res) { log(r) }
  //   console.log('')
  // })

  it.skip('gets transaction leaves over a number of blocks correctly', async () => {
    // add some blocks
    for (let i = 1; i < 4; i++) {
      const TXs = dummyTxs.getSequentialTxs(100, 1)
      const txBundle = getTxBundle(TXs)
      const blockNumber = new BN(i).toArrayLike(Buffer, 'big', BLOCKNUMBER_BYTE_SIZE)
      blockStore.storeTransactions(blockNumber, txBundle)
      blockStore.blockNumberBN = blockStore.blockNumberBN.add(new BN(1))
    }
    // begin test
    const rangeSinceBlockZero = await blockStore.getTransactions(new BN(1), blockStore.blockNumberBN, new BN(0), new BN(1), new BN(2))
    for (const range of rangeSinceBlockZero) {
      for (const r of range) { log(r) }
    }
  })

  it('generates history proofs correctly', async () => {
    const numTxs = 50
    const txAmt = 2
    // add some blocks
    const roots = []
    for (let i = 1; i < 3; i++) {
      const TXs = dummyTxs.getSequentialTxs(numTxs, txAmt, i)
      const txBundle = getTxBundle(TXs)
      const blockNumber = new BN(i).toArrayLike(Buffer, 'big', BLOCKNUMBER_BYTE_SIZE)
      // Store the transactions
      blockStore.storeTransactions(blockNumber, txBundle)
      await Promise.all(blockStore.batchPromises)
      // Generate a new block using these transactions
      const rootHash = await blockStore.sumTree.generateTree(blockNumber)
      blockStore.blockNumberBN = blockStore.blockNumberBN.add(new BN(1))
      // Submit the block to the Plasma Chain contract
      const reciept = await plasmaChain.methods.submitBlock(rootHash).send({gas: 400000})
      expect(reciept.events.SubmitBlockEvent.returnValues['0']).to.equal(blockStore.blockNumberBN.toString())
      expect(reciept.events.SubmitBlockEvent.returnValues['1']).to.equal('0x' + Buffer.from(rootHash).toString('hex'))
      roots.push('0x' + Buffer.from(rootHash).toString('hex'))
    }
    log('Roots:', roots)
    // Check proofs for the first block
    const start = new BN(3)
    const end = new BN(50)
    // Get the tx proofs for the range
    const txsAndProofs = await blockStore.getTxsWithProofs(new BN(1), new BN(1), new BN(0), start, end)
    log(txsAndProofs)
    // await verifyTxsAndProofs(txsAndProofs, web3, plasmaChain, roots)
  })

  it.skip('generates history proofs correctly when given a particular transaction', async () => {
    const numTxs = 50
    const txAmt = 2
    // First add some deposits
    const depositTxs = dummyTxs.getSequentialTxs(numTxs, txAmt, 0)
    for (let i = 0; i < depositTxs.length; i++) {
      const tr = depositTxs[i].transfers[0]
      depositTxs[i] = getDepositTransaction(tr.recipient, tr.token, tr.start, tr.end, new BN(i))
      await blockStore.addDeposit(depositTxs[i])
    }
    // add some blocks
    const roots = []
    let TXs
    for (let i = 1; i < 100; i++) {
      TXs = dummyTxs.getSequentialTxs(i, 2, i)
      const txBundle = getTxBundle(TXs)
      const blockNumber = new BN(i).toArrayLike(Buffer, 'big', BLOCKNUMBER_BYTE_SIZE)
      // Store the transactions
      blockStore.storeTransactions(blockNumber, txBundle)
      await Promise.all(blockStore.batchPromises)
      // Generate a new block using these transactions
      const rootHash = await blockStore.sumTree.generateTree(blockNumber)
      blockStore.blockNumberBN = blockStore.blockNumberBN.add(new BN(1))
      // Submit the block to the Plasma Chain contract
      const reciept = await plasmaChain.methods.submitBlock(rootHash).send({gas: 400000})
      expect(reciept.events.SubmitBlockEvent.returnValues['0']).to.equal(blockStore.blockNumberBN.toString())
      expect(reciept.events.SubmitBlockEvent.returnValues['1']).to.equal('0x' + Buffer.from(rootHash).toString('hex'))
      roots.push('0x' + Buffer.from(rootHash).toString('hex'))
      // Check that the tree root from utils matches the tree root from the sumTree
      const plasmaUtilsTree = new PlasmaMerkleSumTree(txBundle.map((value) => value[0]))
      expect(plasmaUtilsTree.root().hash).to.equal(Buffer.from(rootHash).toString('hex'))
    }
    log('Roots:', roots)
    // Check proofs for our transaction. This tx will not be real but instead span over a bunch of txs
    let proofTx = dummyTxs.getSequentialTxs(1, 20, 99)[0]
    // Get the tx proofs for the range
    const txsAndProofs = await blockStore.getTxHistory(new BN(1), new BN(99), proofTx)
    await verifyTxsAndProofs(txsAndProofs.transactionHistory, web3, plasmaChain, roots)
  })
})

async function verifyTxsAndProofs (txsAndProofs, web3, plasmaChain, roots) {
  // For all blocks...
  for (const blockNumber of Object.keys(txsAndProofs)) {
    // For all transactions in each block...
    for (const txAndProof of txsAndProofs[blockNumber]) {
      const transaction = txAndProof.transaction
      const unsignedTx = getUnsignedTransaction(transaction)
      // For all transfers in each transaction in each block....
      for (const [i, trProofDecoded] of txAndProof.transactionProof.transferProofs.entries()) {
        // Actually check inclusion. LOL
        trProofDecoded.inclusionProof = getHexStringProof(trProofDecoded.inclusionProof)
        const transferProof = new TransferProof(trProofDecoded)
        for (const hash of transferProof.inclusionProof) {
          log('Inclusion Proof:', hash.toString('hex'))
        }
        const res = await plasmaChain.methods.checkTransferProofAndGetBounds(
          web3.utils.soliditySha3('0x' + unsignedTx.encoded),
          unsignedTx.block.toString(),
          '0x' + transferProof.encoded
        ).call({gas: 5000000}) // This should not revert!
        log('Result:', res)
        // Check the Plasma Utils transfer checker
        const result = PlasmaMerkleSumTree.checkTransferProof(unsignedTx, i, transferProof, roots[0].slice(2) + 'ffffffffffffffffffffffffffffffff')
        expect(result).to.not.equal(false)
      }
      // Check transaction proof in utils... TODO: Assert that it's true
      // PlasmaMerkleSumTree.checkTransactionProof(transaction, anotherTxProof, roots[0].slice(2) + 'ffffffffffffffffffffffffffffffff')
    }
  }
}
