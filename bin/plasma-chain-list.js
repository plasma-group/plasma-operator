#!/usr/bin/env node
const path = require('path')
const program = require('commander')
const colors = require('colors') // eslint-disable-line no-unused-vars
const getAccount = require('./utils.js').getAccount
const plasmaRegistryCompiled = require('plasma-contracts').plasmaRegistryCompiled
const Web3 = require('web3')
const readConfigFile = require('../src/utils.js').readConfigFile

program
  .description('lists all Plasma chains in the current registry')
  .option('-n, --newRegistry', 'Deploy a new Plasma Network in addition to a Plasma Chain')
  .action(async (none, cmd) => {
    const account = await getAccount()
    if (account === null) {
      return
    }
    // Get the config
    const configFile = (process.env.CONFIG) ? process.env.CONFIG : path.join(__dirname, '..', 'config.json')
    console.log('Reading config file from:', configFile)
    const config = readConfigFile(configFile)
    if (config.plasmaRegistryAddress === undefined) {
      console.log('Error:'.red, 'No Plasma Registry specified!')
      return
    }
    const web3 = new Web3()
    if (config.web3WebsocketProvider) {
      web3.setProvider(new Web3.providers.WebsocketProvider(config.web3WebsocketProvider))
    } else if (config.web3HttpProvider === undefined) {
      throw new Error('Web3 provider undefined!')
    } else {
      web3.setProvider(new Web3.providers.HttpProvider(config.web3HttpProvider))
    }
    const plasmaRegistryCt = new web3.eth.Contract(plasmaRegistryCompiled.abi, config.plasmaRegistryAddress)
    plasmaRegistryCt.getPastEvents('NewPlasmaChain', {
      fromBlock: 0,
      toBlock: 'latest'
    }, (err, res) => {
      if (err) {
        console.log('Error! Try checking your network')
        console.log(err)
      }
      console.log('~~~~~~~~~~~~~~~~~~~'.rainbow, '\nList of Plasma Chains Deployed to the registry:'.white.bold, config.plasmaRegistryAddress.white.bold)
      for (const ethEvent of res) {
        const plasmaChainAddress = ethEvent.returnValues['0']
        const operatorAddress = ethEvent.returnValues['0']
        const name = Buffer.from(ethEvent.returnValues['2'].slice(2), 'hex').toString('utf8')
        const ip = Buffer.from(ethEvent.returnValues['3'].slice(2), 'hex').toString('utf8')
        console.log(`
${'~~~~~~~~~~~~~~~~~~~'.rainbow}
Chain Name: ${name.white.bold}
Operator Address: ${operatorAddress.white.bold}
Plasma Chain Address: ${plasmaChainAddress.white.bold}
Chain IP: ${ip.white.bold}
        `)
      }
      process.exit(0)
    })
  })

program.parse(process.argv)
