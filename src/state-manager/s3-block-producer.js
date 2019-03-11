// Manages uploads of txn logs to S3

const fs = require('fs-extra')
const path = require('path')
const klaw = require('klaw')
const chokidar = require('chokidar')
const AWS = require('aws-sdk')
const _ = require('lodash')
let S3

const TEMP_FILE_NAME = 'tmp-tx-log.bin'

class S3BlockProducer {
  /**
   * S3 Block Producer monitors for newly created block files and uploads them to S3. 
   * It will also sync existing block files to S3.
   * 
   * It's designed to operate on filesystem events, allowing it to run completely decoupled
   * from the state-service. 
   * @param {*} txBucketName The name of the S3 bucket for storing blocks
   * @param {*} txLogDirectory The directory containing blocks
   * @param {*} fileListeners Listeners to trigger when new block files are added
   */
  constructor (txBucketName, txLogDirectory, fileListeners) {
    this.txBucketName = txBucketName
    this.txLogDirectory = txLogDirectory
    this.uploadQueue = []
    this.fileListeners = fileListeners || []
  }

  async init () {
    S3 = new AWS.S3({ apiVersion: '2006-03-01' })
    // Validate txLogDirectory exists
    await fs.exists(path.join(__dirname, this.txLogDirectory))

    // Validate S3 bucket access
    if (this.txBucketName.length > 0) {
      try {
        await S3.headBucket({ Bucket: this.txBucketName }).promise()
      } catch (e) {
        console.log(e)
        throw new Error(`S3 bucket ${this.txBucketName} non-existent or inaccessible.`)
      }
      
      // Setup file monitor for new blocks
      await this.setupFileMonitor()

      // Fill transfer queue backlog
      await this.initUploadBacklog()

    }
  }
  
  async setupFileMonitor () {
    let watcher = chokidar.watch(this.txLogDirectory, {
      ignored: /(^|[\/\\])\../,
      persistent: true,
      // don't trigger any events for pre-exisiting files
      ignoreInitial: true
    })
    const watcherSetup = () => new Promise((resolve) => {
      watcher
      .on('add', path => {
        if (path.indexOf(TEMP_FILE_NAME) == -1) {
          // prevent duplicate entries
          if (_.findIndex(this.uploadQueue, (item) => item == path) == -1) {
            this.uploadQueue.push(path)
          }
          // fire the file listeners - mainly for integration testing
          for (const listener of this.fileListeners) {
            listener(path)
          }
        }
      })
      .on('ready', () => {
        resolve()
      })
    })
    await watcherSetup()
  }

  // Compare local filesystem with S3 to find files that need to be uploaded
  async initUploadBacklog () {
    // Fetch last uploaded tx log to s3
    async function getHighestS3Block(bucketName) {
      // iterate listItem api till highest block # is found
      let topKey = null
      let params = {
        Bucket: bucketName,
        MaxKeys: 1000,
      }
      let s3ItemListResult = { IsTruncated: true }

      while (s3ItemListResult.IsTruncated) {
        s3ItemListResult = await S3.listObjectsV2(params).promise()
        params.ContinuationToken = s3ItemListResult.NextContinuationToken
        // find max block number
        topKey = s3ItemListResult.Contents
                  .map(item => item.Key)  
                  .reduce((prev, curr) => curr > prev || prev == null ? curr : prev,
                          topKey)
      }
      
      return topKey
    }
    // get the top key stored in S3
    const topS3Key = await getHighestS3Block(this.txBucketName)

    const backlog = []

    // iterate through files currently in the directory to queue items for upload
    const dirStream = klaw(this.txLogDirectory)
    for await (const item of dirStream) {
      if (!item.stats.isDirectory()) {
        let key = path.basename(item.path)
        // dont include temp files in queue
        if ((!topS3Key || key > topS3Key) && key.indexOf(TEMP_FILE_NAME) == -1) {
          backlog.push(item.path)
        }
      }
    }

    // ensure blocks are uploaded in ascending order
    this.uploadQueue = backlog.concat(this.uploadQueue)
    this.uploadQueue.sort()
  }
}

module.exports = S3BlockProducer