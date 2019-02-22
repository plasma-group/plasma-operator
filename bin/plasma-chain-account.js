#!/usr/bin/env node
const fs = require('fs')
const path = require('path')
const program = require('commander')
const colors = require('colors') // eslint-disable-line no-unused-vars
const inquirer = require('inquirer')
const appRoot = require('../src/utils.js').appRoot
const Web3 = require('web3')
const KEYSTORE_DIR = require('../src/constants.js').KEYSTORE_DIR

const web3 = new Web3()
const keystoreDirectory = path.join(appRoot.toString(), KEYSTORE_DIR)

program
  .command('new')
  .description('creates a new account')
  .option('-p, --plaintext', 'Store the private key in plaintext')
  .action(async (none, cmd) => {
    if (!fs.existsSync(keystoreDirectory)) {
      fs.mkdirSync(keystoreDirectory)
    }
    // Check that there are no accounts already -- no reason to have more than one operator account
    const accounts = fs.readdirSync(keystoreDirectory)
    if (accounts.length) {
      console.log('Account already created! Try starting the operator with `operator start`')
      return
    }
    // There are no accounts so create one!
    const newAccount = await createAccount(cmd.plaintext)
    if (newAccount === undefined) {
      return
    }
    console.log('Created new account with address:', newAccount.address.green)
    const keystorePath = path.join(keystoreDirectory, new Date().getTime() + '--' + newAccount.address)
    console.log('Saving account to:', keystorePath.yellow)
    fs.writeFileSync(keystorePath, newAccount.keystoreFile)
    // Create new password file
  })

program
  .command('list')
  .description('list all accounts')
  .action((none, cmd) => {
    let counter = 0
    if (!fs.existsSync(keystoreDirectory)) {
      console.log('No accounts found!')
      return
    }
    fs.readdirSync(keystoreDirectory).forEach(file => {
      console.log('Account #' + counter++ + ':', file.split('--')[1])
    })
  })

async function createAccount (isPlaintext) {
  if (isPlaintext) {
    const newAccount = web3.eth.accounts.create()
    return {
      address: newAccount.address,
      keystoreFile: JSON.stringify(newAccount)
    }
  }
  // If it's not plaintext, we need to prompt for password
  console.log('Your new account is locked with a password. Please give a password. Do not forget this password.')
  const response = await inquirer.prompt([{
    type: 'password',
    name: 'password',
    message: 'Passphrase:'
  },
  {
    type: 'password',
    name: 'retypePassword',
    message: 'Repeat passphrase:'
  }])
  if (response.password !== response.retypePassword) {
    console.log('Passwords do not match! Try again...'.red)
    return
  }
  const newAccount = web3.eth.accounts.create()
  const encryptedAccount = newAccount.encrypt(response.password)
  return {
    address: newAccount.address,
    keystoreFile: JSON.stringify(encryptedAccount)
  }
}

program.parse(process.argv)

if (program.args.length === 1) {
  console.log('Command not found. Try `' + 'operator help account'.yellow + '` for more options')
}
