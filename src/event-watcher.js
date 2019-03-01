const log = require('debug')('info:event-watcher')

/**
 * Watches for events and notifies listeners.
 */
class EventWatcher {
  /**
   * @param {Web3} web3 Web3 instance to use to listen for events.
   * @param {Contract} contract Contract object to watch.
   * @param {string} topic Topic to listen for.
   * @param {number} finalityDepth Number of blocks before considering an event final.
   * @param {number} pollInterval Number of ms to wait before polling for new events.
   */
  constructor(web3, contract, topic, finalityDepth, pollInterval) {
    this.web3 = web3
    this.contract = contract
    this.topic = topic
    this.finalityDepth = finalityDepth
    this.pollInterval = pollInterval
    this.lastLoggedBlock = 0
  }

  /**
   * Subscribes to the event watcher.
   * @param {Function} callback Function to call when the event is triggered.
   */
  subscribe(callback) {
    if (this.pollInterval !== undefined) {
      this.subscribePolling(callback)
    } else {
      this.subscribeWebSockets(callback)
    }
  }

  /**
   * Polls for the event regularly
   * @param {Function} callback Function to call when the event is triggered.
   */
  subscribePolling(callback) {
    log('Subscribing to topic by polling')
    // Poll for the most recent events every $pollInterval milliseconds
    setInterval(async () => {
      // Check to see if there's a new Ethereum block.
      const block = await this.web3.eth.getBlockNumber()
      if (block === this.lastLoggedBlock) {
        return
      }
      log('New block event triggered! Last logged block:', this.lastLoggedBlock)

      // Get events since the last time we checked.
      this.contract.getPastEvents(
        this.topic,
        {
          fromBlock: this.lastLoggedBlock + 1,
          toBlock: block - this.finalityDepth,
        },
        callback
      )

      // Update the last seen block.
      this.lastLoggedBlock = block - this.finalityDepth
    }, this.pollInterval)
  }

  /**
   * Subscribes to an event via websockets.
   * @param {Function} callback Function to call when the event is triggered.
   */
  subscribeWebSockets(callback) {
    log('Subscribing to topic with websockets')
    this.web3.eth.subscribe('newBlockHeaders', (err, block) => {
      if (err) {
        throw err
      }
      log('New block event triggered! Last logged block:', this.lastLoggedBlock)

      // Get events since the last time we checked.
      this.contract.getPastEvents(
        this.topic,
        {
          fromBlock: this.lastLoggedBlock + 1,
          toBlock: block.number - this.finalityDepth,
        },
        callback
      )

      // Update the last seen block.
      this.lastLoggedBlock = block.number - this.finalityDepth
    })
  }
}

module.exports = EventWatcher
