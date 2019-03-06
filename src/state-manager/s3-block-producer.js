// Manages uploads of txn logs to S3

const fs = require('fs-extra')
const path = require('path')
const klaw = require('klaw')
const AWS = require('aws-sdk')
const S3 = new AWS.S3({ apiVersion: '2006-03-01' })

class S3BlockProducer {
  constructor (txBucketName, txLogDirectory) {
    this.txBucketName = txBucketName
    this.txLogDirectory = txLogDirectory
    this.uploadQueue = []
    // block producer will silently no-op if not configured.
    this.active = false
  }

  async init () {
    // Validate txLogDirectory exists
    await fs.exists(path.join(__dirname, this.txLogDirectory))

    // Validate S3 bucket access
    if (this.txBucketName.length > 0) {
      try {
        await S3.headBucket({ Bucket: this.txBucketName }).promise()
          active = true
      } catch (e) {
          throw new Error(`S3 bucket ${this.txBucketName} non-existent or inaccessible.`)
      }
      
      // Fill transfer queue backlog
      await initUploadBacklog()

      // Setup file monitor for new blocks
    }
  }

  // Compare local filesystem with S3 to find files that need to be uploaded
  async initUploadBacklog () {
    // Fetch last uploaded tx log to s3
    async function getHighestS3Block() {
      // iterate listItem api till highest block # is found
      let topKey = null;
      let params = {
        Bucket: this.txBucketName,
        MaxKeys: 1000,
      }
      let s3ItemListResult = { IsTruncated: true };

      while (s3ItemListResult.IsTruncated) {
        s3ItemListResult = await S3.listObjectsV2(params).promise()
        params.ContinuationToken = s3ItemListResult.NextContinuationToken
        // find max block number
        topKey = s3ItemListResult.Contents
                  .map(item => item.Key)  
                  .reduce((prev, curr) => curr > prev ? curr : prev, topKey)
      }
      
      return topKey
    }
    // get the top key stored in S3
    const topS3Key = getHighestS3Block()

    // iterate through files currently in the directory to queue items for upload
    const dirStream = klaw(path.join(__dirname, this.txLogDirectory))
    for await (const item of dirStream) {
      if (!item.stats.isDirectory()) {
        let key = path.basename(item.path)
        if (key > topS3Key) {
          this.uploadQueue.push(item.path)
        }
      }
    }

    // ensure blocks are uploaded in ascending order
    this.uploadQueue.sort()
  }

}