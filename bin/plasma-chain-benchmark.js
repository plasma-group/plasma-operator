#!/usr/bin/env node
const genAccounts = require('../test/mock-accounts').genAccounts
const Benchmark = require('benchmark')
const logUpdate = require('log-update')
const program = require('commander')
const colors = require('colors') // eslint-disable-line no-unused-vars
const MockNode = require('../src/mock-node.js')
const axios = require('axios')
const constants = require('../src/constants.js')
const UnsignedTransaction = require('plasma-utils').serialization.models.UnsignedTransaction
const BN = require('web3').utils.BN
const Web3 = require('web3')
const cluster = require('cluster');
const numCPUs = require('os').cpus().length;

const http = axios.create({
    baseURL: 'http://localhost:3000'
})

const depositType = new BN(999 + Math.floor(Math.random() * 10000))
const depositAmount = new BN(10000000, 'hex')

let txs = 0
let idCounter
let totalDeposits = {}

const operator = {
    addTransaction: async (tx) => {
      const encodedTx = tx.encoded
      let txResponse
      txResponse = await http.post('/api', {
        method: constants.ADD_TX_METHOD,
        jsonrpc: '2.0',
        id: idCounter++,
        params: [
          encodedTx
        ]
      })
      return txResponse.data
    },
    addDeposit: async (recipient, token, amount) => {
      // First calculate start and end from token amount
      const tokenTypeKey = token.toString()
      if (totalDeposits[tokenTypeKey] === undefined) {
        totalDeposits[tokenTypeKey] = new BN(0)
      }
      const start = new BN(totalDeposits[tokenTypeKey])
      totalDeposits[tokenTypeKey] = new BN(totalDeposits[tokenTypeKey].add(amount))
      const end = new BN(totalDeposits[tokenTypeKey])
      let txResponse
      txResponse = await http.post('/api', {
        method: constants.DEPOSIT_METHOD,
        jsonrpc: '2.0',
        id: idCounter++,
        params: {
          recipient: Web3.utils.bytesToHex(recipient),
          token: token.toString(16),
          start: start.toString(16),
          end: end.toString(16)
        }
      })
      try {
      return new UnsignedTransaction(txResponse.data.deposit)
      } catch(e) {}
    },
    getBlockNumber: async () => {
      const response = await http.post('/api', {
        method: constants.GET_BLOCK_NUMBER_METHOD,
        jsonrpc: '2.0',
        id: idCounter++,
        params: []
      })
      return new BN(response.data.result, 10)
    }
}

async function getMockNode(operator) {
    [sender, recipient] = genAccounts(2)
    const recipientNode = new MockNode(operator, recipient, [])
    var node = new MockNode(operator, sender, [recipientNode])
    try {
        await node.deposit(depositType, depositAmount)
    } catch(e) {}
    return node
}

function sendAsync(blockNumber, node) {
    return node.sendRandomTransaction(blockNumber, 2, true)
}

function onCycle(event) {
    txs++
}

program
  .command('*')
  .description('find out the transaction throughput rate your machine')
  .action(async (none, cmd) => {


    if(cluster.isMaster) {
        console.log('\n~~~~~~~~~plasma~~~~~~~~~chain~~~~~~~~~benchmark~~~~~~~~~tool~~~~~~~~~'.rainbow)

        for (let i = 0; i < numCPUs; i++) {
            cluster.fork();
        }

        cluster.setMaxListeners(0)

        setInterval(function(){
            let respondedWorkers = 0
            let totalHz = 0;
            let totalTxs = 0;

            for (worker in cluster.workers) {
                if(cluster.workers[worker].isConnected) {
                    cluster.workers[worker].send(true)
                } else {
                    cluster.workers[worker].on("online", function(){
                        cluster.workers[worker].send(true)
                    })
                }
                cluster.on("message", function(worker, {hz, txs}){
                    totalHz += hz
                    totalTxs += txs
                    respondedWorkers++
                    if(respondedWorkers == numCPUs) {
                        cluster.removeAllListeners("message")
                        logUpdate(`
Transaction throughput: ${Math.round(totalHz)} txs/sec

Total transactions: ${totalTxs} txs
                    
                        `.cyan)
                    }
                })
            }
        },100)

    } else {
        const node = await getMockNode(operator)
        var blockNumber = await operator.getBlockNumber()
        setInterval(async function(){
            blockNumber = await operator.getBlockNumber()
        }, 1000)
    
        var bench = new Benchmark('TPS', {
            minSamples:1000000000,
            async:true,
            onCycle,
            defer: true,
            fn: async function(deferred){
                await sendAsync(blockNumber, node)
                deferred.resolve()
            }
    
        });
        bench.run()
        process.on("message", function(){
            process.send({hz: bench.hz, txs})
        })
    }
  })

program.parse(process.argv)
