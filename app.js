require('dotenv').config()
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
require('./shms/tmj-penggaron/rst_tiltmeter')
require('./shms/tmj-penggaron/wisen_tiltmeter')
