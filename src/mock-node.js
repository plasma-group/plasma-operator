const Web3 = require('web3')
const BN = Web3.utils.BN
const utils = require('../src/utils.js')
const TYPE_BYTE_SIZE = require('../src/constants.js').TYPE_BYTE_SIZE
const models = require('plasma-utils').serialization.models
const Signature = require('plasma-utils').serialization.models.Signature
const UnsignedTransaction = require('plasma-utils').serialization.models
  .UnsignedTransaction
const SignedTransaction = models.SignedTransaction
const log = require('debug')('info:node')

const fakeSig = {
  // Used when isSigned is set to false
  v: 'ff',
  r: '0000000000000000000000000000000000000000000000000000000000000000',
  s: '0000000000000000000000000000000000000000000000000000000000000000',
}

/**
 * Mocks a real node for testing.
 */
class MockNode {
  constructor(operator, account, peerList) {
    this.operator = operator
    this.account = account
    this.peerList = peerList
    this.ranges = []
    this.pendingRanges = []
  }

  /**
   * Adds a range to the list of ranges.
   * @param {BigNum} start Start of the range.
   * @param {BigNum} end End of the range.
   */
  addRange(start, end) {
    utils.addRange(this.ranges, start, end)
  }

  /**
   * Removes a range from the list of ranges.
   * @param {BigNum} start Start of the range.
   * @param {BigNum} end End of the range.
   */
  subtractRange(start, end) {
    utils.subtractRange(this.ranges, start, end)
  }

  /**
   * Processes any pending range updates.
   */
  processPendingRanges() {
    for (const pr of this.pendingRanges) {
      this.addRange(pr[0], pr[1])
    }
    this.pendingRanges = []
  }

  /**
   * Deposits coins into the plasma chain.
   * @param {BigNum} coinType Token type to deposit.
   * @param {BigNum} amount Amount to deposit.
   */
  async deposit(coinType, amount) {
    // Submit the deposit.
    const encodedDeposit = await this.operator.addDeposit(
      Buffer.from(Web3.utils.hexToBytes(this.account.address)),
      coinType,
      amount
    )

    const deposit = new UnsignedTransaction(encodedDeposit).transfers[0]
    const start = new BN(utils.getCoinId(deposit.token, deposit.start))
    const end = new BN(utils.getCoinId(deposit.token, deposit.end))
    log(
      this.account.address,
      'adding range from deposit with start:',
      start.toString('hex'),
      '- end:',
      end.toString('hex')
    )

    // Add the deposit to the list of ranges.
    this.addRange(new BN(start), new BN(end))
  }

  /**
   * Gets a random subrange from an existing range.
   * @param {BigNum} startBound Start of the existing range.
   * @param {BigNum} endBound End of the existing range.
   * @returns {Array} New range as an array of [start, end].
   */
  getRandomSubrange(startBound, endBound) {
    // Compute offsets from the original range.
    const totalSize = endBound.sub(startBound).toNumber()
    const startOffset = Math.floor(Math.random() * totalSize)
    const endOffset = Math.floor(Math.random() * (totalSize - startOffset))

    // Create the new range.
    const start = startBound.add(new BN(startOffset))
    const end = endBound.sub(new BN(endOffset))
    return [start, end]
  }

  /**
   * Sends a random transaction to the operator.
   * @param {BigNum} blockNumber Block the tx should be included in.
   * @param {number} maxSize Max coins to send.
   * @param {boolean} isSigned If the transaciton should be signed.
   */
  async sendRandomTransaction(blockNumber, maxSize, isSigned) {
    // Can't send if we don't have any coins.
    if (this.ranges.length === 0) {
      log('got no money to send!')
      return
    }

    // Pick a random range to send.
    let startIndex = Math.floor(Math.random() * (this.ranges.length / 2))
    startIndex -= startIndex % 2

    // Figure out the start/end bounds of that range.
    const startBoundId = this.ranges[startIndex]
    const endBoundId = this.ranges[startIndex + 1]
    const startBound = new BN(
      startBoundId.toArrayLike(Buffer, 'big', 16).slice(TYPE_BYTE_SIZE)
    )
    const endBound = new BN(
      endBoundId.toArrayLike(Buffer, 'big', 16).slice(TYPE_BYTE_SIZE)
    )

    // Compute the actual random range to send.
    let start, end
    if (maxSize === undefined) {
      ;[start, end] = this.getRandomSubrange(startBound, endBound)
    } else {
      start = startBound
      end = startBound.add(new BN(Math.floor(Math.random()) * maxSize + 1))
    }
    const type = new BN(
      startBoundId.toArrayLike(Buffer, 'big', 16).slice(0, TYPE_BYTE_SIZE)
    )
    const startId = new BN(utils.getCoinId(type, start))
    const endId = new BN(utils.getCoinId(type, end))

    // Get a random recipient that isn't us.
    let recipient = this
    while (recipient === this) {
      recipient = this.peerList[
        Math.floor(Math.random() * this.peerList.length)
      ]
    }

    // Create the transaction.
    const tx = this.makeTx(
      {
        sender: this.account.address,
        recipient: recipient.account.address,
        token: type,
        start,
        end,
      },
      blockNumber,
      isSigned
    )

    // Submit the transaction.
    const txResult = await this.operator.addTransaction(tx)
    if (txResult.error !== undefined) {
      // This means we got an error! Probably need to update the block number
      log('Error in transaction! We may need to update the block number...')
      return false
    }

    // Try to update ranges if the send was successful.
    log(
      this.account.address,
      'trying to send a transaction with',
      'start:',
      new BN(startId).toString('hex'),
      '-- end',
      new BN(endId).toString('hex')
    )
    try {
      this.subtractRange(startId, endId)
    } catch (err) {
      // TODO: Figure out how to handle this.
      console.log('WARNING: squashing subtract range error')
      return
    }

    recipient.pendingRanges.push([new BN(startId), new BN(endId)])
    log('sent a transaction!')
  }

  /**
   * Creates a transaction.
   * @param {Array} tr Transfers in the transaction.
   * @param {BigNum} block Block in which the tx should be included.
   * @param {boolean} isSigned Whether the tx should be signed.
   */
  makeTx(tr, block, isSigned) {
    // Create the signature.
    let sig
    if (isSigned) {
      const txHash = new UnsignedTransaction({ block, transfers: [tr] }).hash
      const encodedSig = this.account.sign(txHash)
      sig = new Signature(encodedSig)
    } else {
      sig = fakeSig
    }

    // Create the transaction object.
    const tx = new SignedTransaction({
      transfers: [tr],
      signatures: [sig],
      block: block,
    })
    return tx
  }
}

module.exports = MockNode
