const fs = require('fs-extra')
const Web3 = require('web3')
const BN = Web3.utils.BN
const log = require('debug')('info:state')
const config = require('./../config')
const models = require('plasma-utils').serialization.models
const UnsignedTransaction = models.UnsignedTransaction
const SignedTransaction = models.SignedTransaction
const itNext = require('../utils.js').itNext
const itEnd = require('../utils.js').itEnd
const getDepositTransaction = require('../utils.js').getDepositTransaction
const getCoinId = require('../utils.js').getCoinId
const colors = require('colors') // eslint-disable-line no-unused-vars

const COIN_ID_PREFIX = require('../constants.js').COIN_ID_PREFIX
const ADDRESS_PREFIX = require('../constants.js').ADDRESS_PREFIX
const DEPOSIT_PREFIX = require('../constants.js').DEPOSIT_PREFIX
const START_BYTE_SIZE = require('../constants.js').START_BYTE_SIZE
const TYPE_BYTE_SIZE = require('../constants.js').TYPE_BYTE_SIZE
const DEPOSIT_SENDER = require('../constants.js').DEPOSIT_SENDER
const BLOCKNUMBER_BYTE_SIZE = require('../constants.js').BLOCKNUMBER_BYTE_SIZE

const DEPOSIT_TX_LENGTH = 73
const RECENT_TX_CACHE_SIZE = 30

// ************* HELPER FUNCTIONS ************* //
const timeout = ms => new Promise(resolve => setTimeout(resolve, ms))
const timeoutAmt = () => 0
// const timeoutAmt = () => Math.floor(Math.random() * 2)

function decodeTransaction (encoding) {
  let tx
  if (encoding.length === DEPOSIT_TX_LENGTH) {
    tx = new UnsignedTransaction(encoding.toString('hex'))
  } else {
    tx = new SignedTransaction(encoding.toString('hex'))
  }
  tx.tr = tx.transfers[0]
  return tx
}

function getAddressToCoinKey (address, token, end) {
  const buffers = [
    ADDRESS_PREFIX,
    address,
    token.toArrayLike(Buffer, 'big', TYPE_BYTE_SIZE),
    end.toArrayLike(Buffer, 'big', START_BYTE_SIZE)
  ]
  return Buffer.concat(buffers)
}

function getCoinToTxKey (token, start) {
  const buffers = [
    COIN_ID_PREFIX,
    token.toArrayLike(Buffer, 'big', TYPE_BYTE_SIZE),
    start.toArrayLike(Buffer, 'big', START_BYTE_SIZE)
  ]
  return Buffer.concat(buffers)
}

class State {
  constructor (db, txLogDirectory) {
    this.db = db
    this.txLogDirectory = txLogDirectory
    this.tmpTxLogFile = this.txLogDirectory + 'tmp-tx-log.bin'
    this.lock = {}
    this.recentTransactions = []
    this.isCurrentBlockEmpty = true
  }

  async init () {
    // Get the current block number
    try {
      const blockNumberBuff = await this.db.get('blockNumber')
      this.blockNumber = new BN(blockNumberBuff)
      log('Block number found! Starting at: ' + this.blockNumber)
    } catch (err) {
      if (err.notFound) {
        log('No blockNumber found! Starting from block 1.')
        this.blockNumber = new BN(1)
        await this.db.put(Buffer.from('blockNumber'), this.blockNumber.toArrayLike(Buffer, 'big', BLOCKNUMBER_BYTE_SIZE))
      } else { throw err }
    }
    // Make a new tx-log directory if it doesn't exist.
    if (!fs.existsSync(this.txLogDirectory)) {
      log('Creating a new tx-log directory')
      fs.mkdirSync(this.txLogDirectory)
    }

    const txnBucketName = config.get('txn_bucket')

    // Open a write stream for our tx log
    if (fs.existsSync(this.tmpTxLogFile)) {
      console.log('WARNING:'.yellow, `Partially complete transaction log detected.
        Starting from where we left off but note that for extra security you may want to
        start from scratch & reingest only the finalized blocks in the transaction log.`)
    }
    this.writeStream = fs.createWriteStream(this.tmpTxLogFile, { flags: 'a' })
  }

  async startNewBlock () {
    // Check that this isn't an empty block
    if (this.isCurrentBlockEmpty) {
      throw new Error('Block is empty! Cannot start new block.')
    }
    if (this.lock.all === true) {
      throw new Error('Attempting to start a new block when a global lock is already active')
    }
    // Start a global lock as we increment the block number. Note that we will have to wait until all other locks are released
    this.lock.all = true
    // Wait until all other locks are released
    while (Object.keys(this.lock).length !== 1) {
      log('Waiting to acquire global lock')
      await timeout(timeoutAmt())
    }
    // Everything should be locked now that we have a `lock.all` activated. Time to increment the blockNumber
    this.blockNumber = this.blockNumber.add(new BN(1))
    // Create a new block
    await this.db.put(Buffer.from('blockNumber'), this.blockNumber.toArrayLike(Buffer, 'big', BLOCKNUMBER_BYTE_SIZE))
    // Start a new tx log
    this.writeStream.end()
    const txLogPath = this.txLogDirectory + this.blockNumber.subn(1).toString(10, BLOCKNUMBER_BYTE_SIZE * 2)
    await fs.rename(this.tmpTxLogFile, txLogPath)
    this.writeStream = fs.createWriteStream(this.tmpTxLogFile, { flags: 'a' })
    // Set empty block flag
    this.isCurrentBlockEmpty = true
    // Release our lock
    delete this.lock.all
    log('#### Started new Block #', this.blockNumber.toString())
    return this.blockNumber
  }

  attemptAcquireLocks (k) {
    const keywords = k.slice() // Make a copy of the array to make sure we don't pollute anything when we add the `all` keyword
    log('Attempting to acquire lock for:', keywords)
    keywords.push('all')
    if (keywords.some((val) => { return this.lock.hasOwnProperty(val) })) {
      log('Failed')
      // Failed to acquire locks
      return false
    }
    // Acquire locks
    for (let i = 0; i < keywords.length - 1; i++) {
      this.lock[keywords[i]] = true
    }
    return true
  }

  releaseLocks (keywords) {
    // Pop off our lock queue
    for (const keyword of keywords) {
      delete this.lock[keyword]
    }
  }

  async addDeposit (recipient, token, start, end) {
    // Check that we haven't already recorded this deposit
    try {
      await this.db.get(Buffer.concat([DEPOSIT_PREFIX, getCoinId(token, end)]))
      log('Deposit already recorded with token type:', token.toString('hex'), ', start:', start.toString('hex'), 'and end:', end.toString('hex'))
      return
    } catch (err) {
    }
    while (!this.attemptAcquireLocks([token.toString(16)])) {
      // Wait before attempting again
      await timeout(timeoutAmt())
    }
    const deposit = getDepositTransaction(Web3.utils.bytesToHex(recipient), token, start, end, this.blockNumber)
    const depositEncoded = deposit.encoded
    try {
      // Put the new owned coin range and the new total deposits
      const ops = [
        { type: 'put', key: Buffer.concat([DEPOSIT_PREFIX, getCoinId(token, end)]), value: Buffer.from([1]) },
        { type: 'put', key: getAddressToCoinKey(recipient, token, deposit.tr.end), value: Buffer.from(depositEncoded, 'hex') },
        { type: 'put', key: getCoinToTxKey(token, deposit.tr.end), value: Buffer.from(depositEncoded, 'hex') }
      ]
      await this.db.batch(ops)
    } catch (err) {
      throw err
    }
    this.releaseLocks([recipient, token.toString(16)])
    log('Added deposit with token type:', token.toString('hex'), ', start:', start.toString('hex'), 'and end:', end.toString('hex'))
    return depositEncoded
  }

  validateTransaction (tx) {
    // Make sure the block number is correct
    if (!tx.block.eq(this.blockNumber)) {
      throw new Error('Transfer record blockNumber mismatch!')
    }
    // Check that no ranges overlap... we may want to disable this
    for (const [i, tr] of tx.transfers.entries()) {
      // Check that none of the other transfer records overlap
      for (let j = 0; j < tx.transfers.length; j++) {
        if (j !== i && tx.transfers[j].start.lt(tr.end) && tx.transfers[j].end.gt(tr.start)) {
          throw new Error('Transfer record ranges overlap!')
        }
      }
    }
  }

  async getTransactionLock (tx) {
    const senders = tx.transfers.map((transfer) => transfer.sender)
    while (!this.attemptAcquireLocks(senders)) {
      // Wait before attempting again
      await timeout(timeoutAmt())
    }
  }

  async releaseTransactionLock (tx) {
    const senders = tx.transfers.map((transfer) => transfer.sender)
    this.releaseLocks(senders)
  }

  validateAffectedRanges (tx) {
    // For all affected ranges, check all affected ranges are owned by the correct sender and blockNumber
    for (const tr of tx.transfers) {
      for (const ar of tr.affectedRanges) {
        if (tr.sender.toLowerCase() !== ar.decodedTx.tr.recipient.toLowerCase()) {
          throw new Error('Affected range check failed! Transfer record sender = ' + tr.sender.toLowerCase() + 'and the affected range recipient = ' + ar.decodedTx.tr.recipient.toLowerCase())
        }
        if (ar.decodedTx.block.eq(this.blockNumber) && ar.decodedTx.tr.sender !== DEPOSIT_SENDER) {
          throw new Error('Affected range check failed! Affected range block = ' + ar.decodedTx.block.toString() + ' and this block = ' + this.blockNumber.toString())
        }
      }
    }
  }

  async writeTransactionToDB (tx) {
    let dbBatch = []
    for (const tr of tx.transfers) {
      // For every transfer, get all of the DB operations we need to perform
      let batchOps
      batchOps = await this.getTransferBatchOps(tx, tr, tr.affectedRanges)
      dbBatch = dbBatch.concat(batchOps)
    }
    const txEncoding = tx.encoded
    // Write the transaction to the DB and tx log
    await this.db.batch(dbBatch)
    this.writeStream.write(Buffer.from(txEncoding, 'hex'))
    // And add to our recent transaction cache
    this.addRecentTransaction(txEncoding)
  }

  async getTransferBatchOps (transaction, transfer, affectedRanges) {
    const dbBatch = []
    // Begin by queuing up the deletion of all affected ranges.
    for (const arEntry of affectedRanges) {
      const arRecipient = Buffer.from(Web3.utils.hexToBytes(arEntry.decodedTx.tr.recipient))
      dbBatch.push({ type: 'del', key: arEntry.key })
      dbBatch.push({ type: 'del', key: Buffer.concat([ADDRESS_PREFIX, arRecipient, arEntry.key.slice(1)]) }) // Delete the address -> end mapping
    }
    // Now add back the ranges which were not entirely covered by this transfer.
    let arEntry = affectedRanges[0]
    let ar = arEntry.decodedTx
    if (!ar.tr.start.eq(transfer.start)) {
      // Reduce the first affected range's end position. Eg: ##### becomes ###$$
      const arRecipient = Buffer.from(Web3.utils.hexToBytes(ar.tr.recipient))
      ar.tr.end = transfer.start
      // Get the affectedTransaction so that when we create the new address->coin mapping we preserve the transaction
      const affectedTransaction = await this.db.get(Buffer.concat([ADDRESS_PREFIX, arRecipient, arEntry.key.slice(1)]))
      dbBatch.push({ type: 'put', key: getCoinToTxKey(ar.tr.token, ar.tr.end), value: Buffer.from(ar.encoded, 'hex') })
      dbBatch.push({ type: 'put', key: getAddressToCoinKey(arRecipient, ar.tr.token, ar.tr.end), value: affectedTransaction })
    }
    arEntry = affectedRanges[affectedRanges.length - 1]
    ar = arEntry.decodedTx
    if (!ar.tr.end.eq(transfer.end)) {
      // Increase the last affected range's start position. Eg: ##### becomes $$###
      const arRecipient = Buffer.from(Web3.utils.hexToBytes(ar.tr.recipient))
      ar.tr.start = transfer.end
      // Get the affectedTransaction so that when we create the new address->coin mapping we preserve the transaction
      const affectedTransaction = await this.db.get(Buffer.concat([ADDRESS_PREFIX, arRecipient, arEntry.key.slice(1)]))
      dbBatch.push({ type: 'put', key: affectedRanges[affectedRanges.length - 1].key, value: Buffer.from(ar.encoded, 'hex') })
      dbBatch.push({ type: 'put', key: Buffer.concat([ADDRESS_PREFIX, arRecipient, affectedRanges[affectedRanges.length - 1].key.slice(1)]), value: affectedTransaction })
    }
    // Add our new transfer record
    const trRecipient = Buffer.from(Web3.utils.hexToBytes(transfer.recipient))
    const transferAsTx = new UnsignedTransaction({transfers: [transfer], block: transaction.block})
    dbBatch.push({ type: 'put', key: getCoinToTxKey(transfer.token, transfer.end), value: Buffer.from(transferAsTx.encoded, 'hex') })
    dbBatch.push({ type: 'put', key: getAddressToCoinKey(trRecipient, transfer.token, transfer.end), value: Buffer.from(transaction.encoded, 'hex') })
    // And finally apply the batch operations
    return dbBatch
  }

  async addTransaction (tx) {
    // Check that the transaction is well formatted
    this.validateTransaction(tx)
    // Acquire lock on all of the transfer record senders
    await this.getTransactionLock(tx)
    log('Attempting to add transaction from:', tx.transfers[0].sender)
    try {
      // Get the ranges which the transaction affects and attach them to the transaction object
      await this.addAffectedRangesToTx(tx)
      // Check that all of the affected ranges are valid
      await this.validateAffectedRanges(tx)
      // All checks have passed, now write to the DB
      await this.writeTransactionToDB(tx)
    } catch (err) {
      this.releaseTransactionLock(tx)
      throw err
    }
    this.releaseTransactionLock(tx)
    log('Added transaction from:', tx.transfers[0].recipient)
    this.addRecentTransaction(tx)
    // Record that this block is not empty
    this.isCurrentBlockEmpty = false
    return new SignedTransaction(tx).encoded
  }

  isCorrectTokenType (tokenType, coinID) {
    const prefixAndType = Buffer.concat([COIN_ID_PREFIX, tokenType.toArrayLike(Buffer, 'big', TYPE_BYTE_SIZE)])
    return Buffer.compare(prefixAndType, coinID.slice(0, 5)) === 0
  }

  async addAffectedRangesToTx (tx) {
    for (const [i, tr] of tx.transfers.entries()) {
      const affectedRange = await this._getAffectedRanges(tr.token, tr.start, tr.end)
      if (affectedRange.length === 0) { // If there are no affected ranges then this transfer must be invalid
        throw new Error('No affected ranges!')
      }
      for (let i = 0; i < affectedRange.length; i++) {
        affectedRange[i].decodedTx = decodeTransaction(affectedRange[i].value)
      }
      tx.transfers[i].affectedRanges = affectedRange
    }
  }

  async _getAffectedRanges (token, start, end) {
    // TODO: Handle results which are undefined
    const it = this.db.iterator({
      gt: getCoinToTxKey(new BN(token), new BN(start))
    })
    const affectedRanges = []
    let result = await itNext(it)
    // Check that the prefix & token type match the transfer we are looking for
    if (!this.isCorrectTokenType(token, result.key)) {
      await itEnd(it)
      return []
    }
    affectedRanges.push(result)
    while (this.isCorrectTokenType(token, result.key) && Buffer.compare(result.key.slice(5), end.toArrayLike(Buffer, 'big', START_BYTE_SIZE)) < 0) {
      result = await itNext(it)
      affectedRanges.push(result)
    }
    await itEnd(it)
    return affectedRanges
  }

  async getOwnedRanges (address) {
    while (!this.attemptAcquireLocks([address])) {
      // Wait before attempting again
      await timeout(timeoutAmt())
    }
    // Get the ranges
    const addressBuffer = Buffer.from(Web3.utils.hexToBytes(address))
    const it = this.db.iterator({
      gt: getAddressToCoinKey(addressBuffer, new BN(0), new BN(0))
    })
    const ownedRanges = []
    let result = await itNext(it)
    while (result.key && Buffer.compare(addressBuffer, result.key.slice(1, 21)) === 0) {
      ownedRanges.push(result)
      result = await itNext(it)
    }
    await itEnd(it)
    this.releaseLocks([address])
    return ownedRanges
  }

  getRecentTransactions (start, end) {
    if (start === undefined) start = 0
    if (end === undefined) start = this.recentTransactions.length
    return this.recentTransactions.slice(start, end)
  }

  addRecentTransaction (encodedTx) {
    this.recentTransactions.unshift(encodedTx)
    if (this.recentTransactions.length > RECENT_TX_CACHE_SIZE) {
      this.recentTransactions.pop()
    }
  }

  async getTransactions (address, startBlock, endBlock) {
    const ownedRanges = await this.getOwnedRanges(address)
    const transactions = new Set()
    for (const range of ownedRanges) {
      const serialized = new UnsignedTransaction(range.value)
      if (serialized.block < startBlock || serialized > endBlock) {
        continue
      }

      transactions.add(range.value)
    }
    return transactions
  }
}

module.exports = State
