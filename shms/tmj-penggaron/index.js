// require('dotenv').config({path: '../../.env'})
require('dotenv').config({path:__dirname+'/.env'})
// const logger = require('js-logger')
// logger.useDefaults({
//     defaultLevel: logger.INFO,
//     formatter: function (messages, context) {
//         messages.unshift(new Date())
//     }
// })

// logger.debug(process.env) // remove this after you've confirmed it working


// const { hostname } = require('os')
// require sensor scipts here
require('./rst_tiltmeter')
require('./wisen_tiltmeter')

