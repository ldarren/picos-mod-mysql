const pico = require('pico-common/bin/pico-cli')
const { test } = pico.export('pico/test')
const mysql = require('./index')

let client

test('ensure mysql loaded', function(cb){
	cb(null, !!mysql)
})
test('ensure mysql create', function(cb){
	mysql.create({ path: '', env: 'pro' }, {}, (err, cli) => {
		if (err) return cb(err)
		client = cli
		cb(null, !!client)
	})
})
