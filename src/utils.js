const path = require('path')
const START_BYTE_SIZE = require('./constants.js').START_BYTE_SIZE
const TYPE_BYTE_SIZE = require('./constants.js').TYPE_BYTE_SIZE
const BLOCK_TX_PREFIX = require('./constants.js').BLOCK_TX_PREFIX
const TEST_DB_DIR = require('./constants.js').TEST_DB_DIR
const DEPOSIT_SENDER = require('./constants.js').DEPOSIT_SENDER
const soliditySha3 = require('web3').utils.soliditySha3
const UnsignedTransaction = require('plasma-utils').serialization.models
  .UnsignedTransaction
const fs = require('fs')
const _ = require('lodash')

const appRoot = path.join(__dirname, '..')

/**
 * Adds a range to a list of ranges.
 * @param {Array} rangeList A list of ranges.
 * @param {BigNum} start Start of the range.
 * @param {BigNum} end End of the range.
 * @param {number} numSize Number of bytes in start and end.
 */
const addRange = (rangeList, start, end, numSize = 16) => {
  // Find the ranges to the left and right of our range.
  let leftRange, rightRange
  let insertionPoint = _.sortedIndexBy(rangeList, start, (n) => {
    n.toString(16, numSize)
  })

  // If the insertion point found an end poisition equal to our start, change it to the next index (find insertion on the right side)
  if (
    insertionPoint > 0 &&
    insertionPoint < rangeList.length &&
    insertionPoint % 2 === 1 &&
    rangeList[insertionPoint].eq(start)
  ) {
    insertionPoint++
  }

  if (insertionPoint > 0 && rangeList[insertionPoint - 1].eq(start)) {
    leftRange = insertionPoint - 2
  }
  if (insertionPoint < rangeList.length && rangeList[insertionPoint].eq(end)) {
    rightRange = insertionPoint
  }

  // Set the start and end of our new range based on the deleted ranges
  if (leftRange !== undefined) {
    start = rangeList[leftRange]
  }
  if (rightRange !== undefined) {
    end = rangeList[rightRange + 1]
  }

  // Delete the leftRange and rightRange if we found them
  if (leftRange !== undefined && rightRange !== undefined) {
    rangeList.splice(leftRange + 1, 2)
    return
  } else if (leftRange !== undefined) {
    rangeList.splice(leftRange, 2)
    insertionPoint -= 2
  } else if (rightRange !== undefined) {
    rangeList.splice(rightRange, 2)
  }

  rangeList.splice(insertionPoint, 0, start)
  rangeList.splice(insertionPoint + 1, 0, end)
}

/**
 * Removes a range from a list of ranges.
 * @param {Array} rangeList A list of ranges.
 * @param {BigNum} start Start of the range to remove.
 * @param {BigNum} end End of the range to remove.
 */
const subtractRange = (rangeList, start, end) => {
  let affectedRange
  let arStart
  let arEnd
  for (let i = 0; i < rangeList.length; i += 2) {
    arStart = rangeList[i]
    arEnd = rangeList[i + 1]
    if (arStart.lte(start) && end.lte(arEnd)) {
      affectedRange = i
      break
    }
  }

  if (affectedRange === undefined) {
    throw new Error('No affected range found! Must be an invalid subtraction.')
  }

  // Remove the range.
  rangeList.splice(affectedRange, 2)

  // Create new sub-ranges based on what we deleted
  if (!arStart.eq(start)) {
    // # rangeList += [arStart, start - 1]
    rangeList.splice(affectedRange, 0, arStart)
    rangeList.splice(affectedRange + 1, 0, start)
    affectedRange += 2
  }

  if (!arEnd.eq(end)) {
    // # rangeList += [end + 1, arEnd]
    rangeList.splice(affectedRange, 0, end)
    rangeList.splice(affectedRange + 1, 0, arEnd)
  }
}

/**
 * Computes the coin ID given start and token type.
 * @param {BigNum} type The coin's token type.
 * @param {BigNum} start Start of the range.
 * @returns {Buffer} Coin ID as a buffer.
 */
const getCoinId = (type, start) => {
  const buffers = [
    type.toArrayLike(Buffer, 'big', TYPE_BYTE_SIZE),
    start.toArrayLike(Buffer, 'big', START_BYTE_SIZE),
  ]
  return Buffer.concat(buffers)
}

/**
 * Creates a defer function which allows us
 * to add our promise to a queue of messages.
 * @returns {Promise} The defer function.
 */
const defer = () => {
  const deferred = {
    promise: null,
    resolve: null,
    reject: null,
  }
  deferred.promise = new Promise((resolve, reject) => {
    deferred.resolve = resolve
    deferred.reject = reject
  })
  return deferred
}

/**
 * Creates a JSON-RPC object given a method and params.
 * @param {string} method Name of the JSON-RPC method.
 * @param {Array} params Any method params.
 */
const jsonrpc = (method, params) => {
  return {
    jsonrpc: '2.0',
    method,
    params,
  }
}

/**
 * Promisified version of leveldown's `it.next(cb)`.
 * @param {*} it An iterator.
 * @returns {Promise} Next value in the iterator.
 */
const itNext = (it) => {
  return new Promise((resolve, reject) => {
    it.next((err, key, value) => {
      if (err) {
        reject(err)
      }
      resolve({ key, value })
    })
  })
}

/**
 * Promisified version of leveldown's `it.end(cb)`.
 * @param {*} it An iterator.
 * @returns {Promise} Last value in the iterator.
 */
const itEnd = (it) => {
  return new Promise((resolve, reject) => {
    it.end((err) => {
      if (err) {
        reject(err)
      }
      resolve()
    })
  })
}

/**
 * Creates a leveldb key for a transaction.
 * @param {number} blockNumber Block in which the tx was included.
 * @param {BigNum} type Token type of the coin transferred.
 * @param {BigNum} start Start of the range transferred.
 * @returns {Buffer} The leveldb key.
 */
const makeBlockTxKey = (blockNumber, type, start) => {
  return Buffer.concat([BLOCK_TX_PREFIX, blockNumber, getCoinId(type, start)])
}

/**
 * Reads a config file, sets defaults, and returns the config object.
 * @param {string} configFilePath Path to the config file.
 * @param {string} mode Mode to run in ("test" or "production").
 * @returns {Object} The config object.
 */
const readConfigFile = (configFilePath, mode) => {
  const config = JSON.parse(fs.readFileSync(configFilePath, 'utf8'))
  setConfigDefaults(config, mode)
  return config
}

/**
 * Sets defaults in the config file.
 * @param {Object} config A config object.
 * @param {string} mode Mode to run in ("test" or "production").
 * @returns {Object} Config object with defaults set.
 */
const setConfigDefaults = (config, mode) => {
  if (mode === 'test') {
    config.dbDir = TEST_DB_DIR + +new Date()
  }
  config.dbDir = path.join(appRoot.toString(), config.dbDir)

  const defaultConfig = {
    txLogDir: config.dbDir + '/tx-log/',
    stateDBDir: config.dbDir + '/state-db/',
    blockDBDir: config.dbDir + '/block-db/',
    ethDBDir: config.dbDir + '/eth-db/',
  }

  return {
    ...config,
    ...defaultConfig,
  }
}

/**
 * Computes the Ethereum sha3 (keccak256) hash of a value.
 * @param {*} value Value to hash.
 * @returns {Buffer} Hash of the value.
 */
const sha3 = (value) => {
  // Requires '0x' + becuase web3 only interprets strings as bytes if they start with 0x
  const hashString = '0x' + value.toString('hex')
  const solidityHash = soliditySha3(hashString)
  return Buffer.from(solidityHash.slice(2), 'hex') // Slice 2 to remove the dumb 0x
}

/**
 * Creates a transaction that represents a deposit.
 * @param {string} owner Owner of the deposited asset.
 * @param {BigNum} token Token deposited.
 * @param {BigNum} start Start of the range deposited.
 * @param {BigNum} end End of the range deposited.
 * @param {BigNum} block Block in which the deposit occurred.
 * @returns {UnsignedTransaction} Deposit transaction.
 */
function getDepositTransaction(owner, token, start, end, block) {
  const tx = new UnsignedTransaction({
    block,
    transfers: [
      { sender: DEPOSIT_SENDER, recipient: owner, token, start, end },
    ],
  })
  tx.tr = tx.transfers[0]
  return tx
}

module.exports = {
  addRange,
  subtractRange,
  defer,
  jsonrpc,
  itNext,
  itEnd,
  getCoinId,
  readConfigFile,
  sha3,
  appRoot,
  getDepositTransaction,
  makeBlockTxKey,
}
