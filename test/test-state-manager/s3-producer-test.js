/* eslint-env mocha */

// unit tests for S3 uploading / filesystem
'use strict'

const _ = require('lodash')
const sysPath = require('path')
const rimraf = require('rimraf')
const fs = require('fs-extra')
const chokidar = require('chokidar')
const chai = require('chai')
const sinon = require('sinon')
chai.should()
chai.use(require('sinon-chai'))
const AWS = require('aws-sdk-mock')
const web3 = require('web3')
const S3BlockProducer = require('../../src/state-manager/s3-block-producer')
const BLOCKNUMBER_BYTE_SIZE = require('./../../src/constants').BLOCKNUMBER_BYTE_SIZE
const BN = web3.utils.BN
const expect = chai.expect

function getFixturePath (subPath) {
  return sysPath.join(
    __dirname,
    'test-fixtures',
    subdir && subdir.toString() || '',
    subPath
  )
}

let subdir = 0,
    testCount = 1,
    mochaIt = it,
    fixturesPath = getFixturePath(''),
    PERM_ARR = 0x1ed // rwe, r+e, r+e; 755

if (!fs.readFileSync(__filename).toString().match(/\sit\.only\(/)) {
  it = function() {
    testCount++
    mochaIt.apply(this, arguments)
  }
  it.skip = function() {
    testCount--
    mochaIt.skip.apply(this, arguments)
  }
}

before((done) => {
  rimraf(sysPath.join(__dirname, 'test-fixtures'), function(err) {
    if (err) throw err
      fs.mkdir(fixturesPath, PERM_ARR, function(err) {
        if (err) throw err
        while (subdir < testCount) {
          subdir++
          fixturesPath = getFixturePath('')
          fs.mkdirSync(fixturesPath, PERM_ARR)
        }
        subdir = 0
        done()
      })
  })
})

beforeEach(function() {
  subdir++
  fixturesPath = getFixturePath('')
})

afterEach(() => {
  AWS.restore('S3')
})

const waitFor = (spies) => new Promise((resolve) => {
  function isSpyReady(spy) {
    return Array.isArray(spy) ? spy[0].callCount >= spy[1] : spy.callCount
  }
  function finish() {
    clearInterval(intrvl)
    clearTimeout(to)
    resolve()
  }
  let intrvl = setInterval(function() {
    if (spies.every(isSpyReady)) finish()
  }, 5)
  let to = setTimeout(finish, 3500)
})

describe('filesystem integration testing', () => {

    it('should add existing files to upload queue when bucket is empty', async () => {
      AWS.mock('S3', 'headBucket', {})
      AWS.mock('S3', 'listObjectsV2', {Contents: []})

      // create 3 "blocks"
      let fixturePath = getFixturePath('')
      let filenames = new Array(3).fill(1).map((_, i) => sysPath.join(fixturePath, new BN(i).toString(10, BLOCKNUMBER_BYTE_SIZE * 2)))
      filenames.forEach((file) => {
        fs.writeFileSync(file, 'b')
      })
      const s3Uploader = new S3BlockProducer('test-bucket', fixturePath)
      await s3Uploader.init()
      expect(s3Uploader.uploadQueue).to.be.eql(filenames)
    })

    it('should not duplicate items in the upload queue', async () => {
      AWS.mock('S3', 'headBucket', {})
      AWS.mock('S3', 'listObjectsV2', {Contents: [{
        // put first block into bucket
        Key: new BN(0).toString(10, BLOCKNUMBER_BYTE_SIZE * 2)
      }]})

      // create 3 "blocks" locally
      let fixturePath = getFixturePath('')
      let filenames = new Array(3).fill(1).map((_, i) => sysPath.join(fixturePath, new BN(i).toString(10, BLOCKNUMBER_BYTE_SIZE * 2)))
      filenames.forEach((file) => {
        fs.writeFileSync(file, 'b')
      })
      const s3Uploader = new S3BlockProducer('test-bucket', fixturePath)
      await s3Uploader.init()
      // first file key is already in S3 so only include last two in queue
      expect(s3Uploader.uploadQueue).to.be.eql(filenames.slice(1))
    })

    it('should queue newly added files for upload', async () => {
      AWS.mock('S3', 'headBucket', {})
      AWS.mock('S3', 'listObjectsV2', {Contents: []})

      // create a new block file after s3 upload file monitoring has started
      let fixturePath = getFixturePath('')
      const newFilePath = sysPath.join(fixturePath, new BN(5).toString(10, BLOCKNUMBER_BYTE_SIZE * 2))
      // use a file listener to await new file event
      const fileSpy = sinon.spy()
      const s3Uploader = new S3BlockProducer('test-bucket', fixturePath, [fileSpy])
      await s3Uploader.init()
      // add block number 5 file
      fs.writeFileSync(newFilePath, 'b')
      // wait for listener to be called
      await waitFor([fileSpy])
      fileSpy.should.have.been.calledOnce
      fileSpy.should.have.been.calledWith(newFilePath)
      expect(s3Uploader.uploadQueue).to.be.eql([newFilePath])
    })

    it('should ensure files are uploaded in ascending order', async () => {

      // create 1 block during initialization
      let fixturePath = getFixturePath('')
      const newFilePath = sysPath.join(fixturePath, new BN(5).toString(10, BLOCKNUMBER_BYTE_SIZE * 2))

      AWS.mock('S3', 'headBucket', {})
      AWS.mock('S3', 'listObjectsV2', (_, callback) => {
        // create a new block file while polling s3 bucket
        fs.writeFile(newFilePath, 'b', () => {
          callback(null, {Contents: []})
        })
      })

      // create 3 pre-existing "blocks"
      let filenames = new Array(3).fill(1).map((_, i) => sysPath.join(fixturePath, new BN(i).toString(10, BLOCKNUMBER_BYTE_SIZE * 2)))
      filenames.forEach((file) => {
        fs.writeFileSync(file, 'b')
      })

      // use a file listener to await new file event
      const fileSpy = sinon.spy()
      const s3Uploader = new S3BlockProducer('test-bucket', fixturePath, [fileSpy])
      await s3Uploader.init()
      // wait for listener to be called
      await waitFor([fileSpy])
      fileSpy.should.have.been.calledOnce
      fileSpy.should.have.been.calledWith(newFilePath)
      expect(s3Uploader.uploadQueue).to.be.eql(filenames.concat([newFilePath]))
    })
})


