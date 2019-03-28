const pico = require('pico-common/bin/pico-cli')
const { test } = pico.export('pico/test')
const mysql = require('./index')

let client

test('ensure mysql loaded', cb => {
	cb(null, !!mysql)
})

test('ensure mysql create', cb => {
	mysql.create({ path: '', env: 'pro' }, {}, (err, cli) => {
		if (err) return cb(err)
		client = cli
		cb(null, !!client)
	})
})

test('test select query builder', cb => {
	client.query().select().from('user').where({id: 3, state: 1}).toSQL((err, sql, conds) => {
		if (err) return cb(err)
		cb(null, 'select * from user where id = ? and state = ?;' === sql && conds[0] === 3 && conds[1] === 1)
	})
})
