const convict = require('convict')

// Define a schema
let config = convict({
  env: {
    doc: 'The application environment.',
    format: ['production', 'development', 'test'],
    default: 'development',
    env: 'NODE_ENV'
  },
  txn_bucket: {
    doc: 'The S3 bucket name for storing transaction logs',
    format: '*',
    default: '',
    env: 'TXN_LOG_BUCKET'
  }
})

// Perform validation
config.validate({ allowed: 'strict' })

module.exports = config
