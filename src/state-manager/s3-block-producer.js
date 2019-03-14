// Manages uploads of txn logs to S3

const fs = require('fs-extra')
const log = require('debug')('info:state')
const path = require('path')
const klaw = require('klaw')
const chokidar = require('chokidar')
const AWS = require('aws-sdk')
const _ = require('lodash')
const sleep = require('util').promisify(setTimeout)
let S3

const TEMP_FILE_NAME = 'tmp-tx-log.bin'
const UPLOAD_TASK_INTERVAL_MS = 1000
const UPLOAD_ERROR_THRESHOLD = 5

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
   * @param {*} uploadTaskInterval Override the default uploader task polling interval
   */
  constructor ({txBucketName, txLogDirectory, fileListeners, uploadTaskInterval}) {
    this.txBucketName = txBucketName
    this.txLogDirectory = txLogDirectory
    this.uploadQueue = []
    this.fileListeners = fileListeners || []
    this.uploadTaskInterval = uploadTaskInterval || UPLOAD_TASK_INTERVAL_MS
    // the index of the latest uploaded block
    this.currentBlock = -1
    // used to stop polling when deactivated
    this.active = true
    // reference to the file monitor
    this.fileWatch = null
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
        throw new Error(`S3 bucket "${this.txBucketName}" non-existent or inaccessible.`)
      }
      
      // Setup file monitor for new blocks
      await this.initFileMonitor()

      // Fill transfer queue backlog
      await this.initUploadBacklog()

      // Start S3 queue uploader task in background (no await)
      this.initQueueUploader()

    }
  }
  
  async initFileMonitor () {
    let watcher = this.fileWatch = chokidar.watch(this.txLogDirectory, {
      ignored: /(^|[\/\\])\../,
      persistent: true,
      // don't trigger any events for pre-exisiting files
      ignoreInitial: true
    })
    const watcherSetup = () => new Promise((resolve) => {
      watcher
      .on('add', blockPath => {
        if (blockPath.indexOf(TEMP_FILE_NAME) == -1) {
          // prevent duplicate entries
          if (_.findIndex(this.uploadQueue, (item) => item == blockPath) == -1) {
            const blockIdx = +path.basename(blockPath)
            // don't queue uploaded blocks
            if (blockIdx > this.currentBlock) {
              this.uploadQueue.push(blockPath)
              // ensure queue is sorted in case file events arrive out of order
              this.uploadQueue.sort()
            }
          }
          // fire the file listeners - mainly for integration testing
          for (const listener of this.fileListeners) {
            listener(blockPath)
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
    this.currentBlock = topS3Key || -1

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

  // kick-off queue uploader task
  async initQueueUploader() {

    let missingBlockErrorCount = 0
    await sleep(this.uploadTaskInterval)
    while (this.active) {
      if (this.uploadQueue.length > 0) {
        const nextBlockPath = this.uploadQueue[0]
        const nextBlock = path.basename(nextBlockPath)
        // if next item in queue is currentBlock + 1
        if (+nextBlock === this.currentBlock + 1) {
          // continue with upload, set error count to 0, log block # uploaded
          missingBlockErrorCount = 0
          try {
            await this.uploadFileToS3(nextBlockPath)
            this.uploadQueue = this.uploadQueue.slice(1)
            this.currentBlock = +nextBlock
            log(`S3 upload task completed transfer of Block #${nextBlock}`)
          } catch (e) {
            log(`S3 upload task failed to upload Block #${nextBlock} due to error - ${e.toString()}`)
          }
        } else {
          // increase error count
          missingBlockErrorCount++
          // if error threshold reached, log an error
          if (missingBlockErrorCount == UPLOAD_ERROR_THRESHOLD) {
            log(`S3 upload task is missing Block #${this.currentBlock + 1} and stalled until file is queued.`)
          }
        }
      }

      await sleep(this.uploadTaskInterval)
    }

  }

  // upload a file to S3 from the given local filepath
  async uploadFileToS3(filePath) {
    const key = path.basename(filePath)
    const fileStream = fs.createReadStream(filePath)
    const params = {
      Bucket: this.txBucketName,
      Key: key,
      Body: fileStream
    }
    await S3.putObject(params).promise()
  }

  async stop() {
    // stop upload queue polling
    this.active = false
    // stop monitoring filesystem
    if (this.fileWatch) {
      this.fileWatch.close()
    }
  }
}

module.exports = S3BlockProducer