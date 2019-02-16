#!/usr/bin/env node
const program = require('commander')
const colors = require('colors') // eslint-disable-line no-unused-vars
const appRoot = require('../src/utils.js').appRoot
const readConfigFile = require('../src/utils.js').readConfigFile
const path = require('path')

const configFile = (process.env.CONFIG) ? process.env.CONFIG : path.join(appRoot.toString(), 'config.json')
const config = readConfigFile(configFile)
const isRinkeby = config.web3HttpProvider != undefined && config.web3HttpProvider.includes('rinkeby')

const rinkebyStep2 = `# On Rinkeby testnet, send your new Operator address ~0.5 ETH.
You can use a faucet to get test ETH for free here: ${'https://faucet.rinkeby.io/'.green}`
const rinkebyStep3 = `${'$ plasma-chain deploy'.green} # deploys a new Plasma Chain.
Note you will be prompted for a unique Plasma Chain name & IP address.
If you are running on your laptop, just set the IP to \`0.0.0.0\` as you probably don't
want to reveal your IP to the public. However, if you are running in a data center and would
like to accept Plasma transactions & serve a block explorer to the public, go ahead and set an IP.`
const customStep2 = '# On your Ethereum node, send your new Operator address ~0.5 ETH'
const customStep3 = `${'$ plasma-chain deploy [-n]'.green} # deploys a new Plasma Chain. If you want
to also deploy a new Plasma Network Registry, use the \`-n\` flag. Note you will be prompted for a unique
Plasma Chain name & IP address (public to the Ethereum chain you deploy to)`
const provider = config.web3WebsocketProvider != undefined ? config.web3WebsocketProvider : config.web3HttpProvider

const introText = `
${'~~~~~~~~~plasma~~~~~~~~~chain~~~~~~~~~operator~~~~~~~~~'.rainbow}
${'Deploy a new Plasma Chain in just a couple commands.'.white.bold} 🤞😁

Note that Plasma Chains require a constant connection to the Ethereum network.
You can set your Ethereum node in your config file located: ${configFile.toString().yellow}
(All configs & DB files are located in this directory--I promise I won't pollute your home directory!)
Right now your Web3 provider is set to: ${provider.blue}

To deploy a new Plasma Chain, use the following commands:

1) ${'$ plasma-chain account new'.green}  # create a new account

2) ${(isRinkeby) ? rinkebyStep2 : customStep2}

3) ${(isRinkeby) ? rinkebyStep3 : customStep3}

4) ${'$ plasma-chain start'.green} # start your new Plasma Chain
You can also view your local block explorer at http:127.0.0.1:8000

[optional]
5) ${'$ plasma-chain testSwarm'.green} # spam your Plasma Chain with tons of test transactions 😁
${'WARNING: This is experimental software--use it carefully!'.yellow}

${'<3'.red}
`

program
  .version('0.0.1')
  .description(introText)
  .command('deploy', 'deploy a new plasma chain')
  .command('start', 'start the operator')
  .command('account [cmd]', 'manage accounts')
  .command('testSwarm', 'start a swarm of test nodes')
  .command('list', 'list all Plasma Chains on the Registry')
  .action((command) => {
  })
  .parse(process.argv)
