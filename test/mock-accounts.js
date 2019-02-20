const Web3 = require('web3')

function genAccounts(num = 100) {
  // Generate a bunch of private keys
  const privateKeys = []
  for (let i = 0; i < num; i++) {
    privateKeys.push(Web3.utils.sha3(i.toString()))
  }
  // Generate a bunch of accounts
  const accounts = []
  for (const privateKey of privateKeys) {
    accounts.push(new Web3().eth.accounts.privateKeyToAccount(privateKey))
  }

  return accounts

}

function genTestTx() {
  const accounts = genAccounts(2)
  // Generate a sample transaction
  const sampleTr = {
    sender: accounts[0].address,
    recipient: accounts[1].address,
    type: new Web3.utils.BN('0'),
    start: new Web3.utils.BN('10'),
    offset: new Web3.utils.BN('1'),
    block: new Web3.utils.BN('0')
  }

  const sig = {
    v: new Web3.utils.BN('0'),
    r: new Web3.utils.BN('0'),
    s: new Web3.utils.BN('0')
  }

  const testTx = {
    transferRecords: [sampleTr],
    signatures: [sig]
  }

  return testTx
}

module.exports = {
  accounts: genAccounts(),
  testTx: genTestTx(),
  genAccounts,
}
