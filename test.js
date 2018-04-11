const pico = require('pico-common/bin/pico-cli')
const { ensure } = pico.export('pico/test')
const mysql = require('./index')

let client

ensure('ensure mysql loaded', function(cb){
	cb(null, !!mysql)
})
ensure('ensure mysql create', function(cb){
	mysql.create({ path: '', env: 'pro' }, {}, (err, cli) => {
		if (err) return cb(err)
		client = cli
		cb(null, !!client)
	})
})
