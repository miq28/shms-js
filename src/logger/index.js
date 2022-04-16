const logger = require('js-logger')
logger.useDefaults({
    defaultLevel: logger.DEBUG,
    formatter: function (messages, context) {
        // messages = new Date() + messages
        messages.unshift(new Date())
        // console.log(messages)
    }
})

module.exports = logger