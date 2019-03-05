// Manages uploads of txn logs to S3

const fs = require('fs-extra')
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
    // Validate S3 bucket access
    if (this.txBucketName.length > 0) {
      try {
        await S3.headBucket({ Bucket: this.txBucketName }).promise()
          active = true
      } catch (e) {
          throw new Error(`S3 bucket ${this.txBucketName} non-existent or inaccessible.`)
      }
      
      // Fill transfer queue backlog asynchronously

    }



  }
}