const pico = require('pico-common/bin/pico-cli')
const { test } = pico.export('pico/test')
const mysql = require('./index')

const appConfig = { ath: '', env: 'pro' }
const libConfig = require('./env.json')
let client

test('ensure mysql loaded', cb => {
	cb(null, !!mysql)
})

test('ensure mysql create', cb => {
	mysql.create(appConfig, libConfig, (err, cli) => {
		if (err) return cb(err)
		client = cli
		cb(null, !!client)
	})
})

test('test select query builder', cb => {
	client.query().select().from('user').where({id: 3, state: 1}).toSQL((err, sql, params) => {
		if (err) return cb(err)
		cb(null, 'select * from user where id = ? and state = ?;' === sql && params[0] === 3 && params[1] === 1)
	})
})

test('test select query', cb => {
	client.query().select().from('user').where({id: 3, state: 1}).exec((err, result) => {
		if (err) return cb(err)
		cb(null, true)
	})
})
