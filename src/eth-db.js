const path = require('path')
const fs = require('fs')
const ETH_DB_FILENAME = require('./constants.js').ETH_DB_FILENAME
const log = require('debug')('info:eth-db')

function loadEthDB (config) {
  const ethDBPath = path.join(config.ethDBDir, ETH_DB_FILENAME)
  let ethDB = {}
  if (fs.existsSync(ethDBPath)) {
    // Load the db if it exists
    ethDB = JSON.parse(fs.readFileSync(ethDBPath, 'utf8'))
  }
  if (config.plasmaRegistryAddress !== undefined) {
    ethDB.plasmaRegistryAddress = config.plasmaRegistryAddress
  }
  return ethDB
}

function writeEthDB (config, ethDB) {
  if (!fs.existsSync(config.ethDBDir)) {
    log('Creating a new eth db directory because it does not exist')
    fs.mkdirSync(config.ethDBDir, { recursive: true })
  }
  fs.writeFileSync(path.join(config.ethDBDir, ETH_DB_FILENAME), JSON.stringify(ethDB))
}

module.exports = {
  loadEthDB,
  writeEthDB
}
