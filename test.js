const pico = require('pico-common/bin/pico-cli')
const { test } = pico.export('pico/test')
const mysql = require('./index')

const appConfig = { path: '', env: 'pro' }
const libConfig = {
	master: {
		host: 'db4free.net',
		port: 3306,
		user: 'picos_tester',
		password: '',
		database: 'picos_test',
		acquireTimeout: 120000,
		waitForConnections: true,
		connectionLimit: 1,
		queueLimit: 1
	}
}
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
	console.log(err, result)
		if (err) return cb(err)
		cb(null, true)
	})
})
