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
	client.q().select(1).toSQL((err, sql, params) => {
		if (err) return cb(err)
		cb(null,
			'select 1;' === sql &&
			params.length === 0)
	})
})

test('test select query builder with bracket', cb => {
	client.q()
		.select()
		.from('user')
		.where({id: 3, state: 1})
		.or(q => {
			return q.where('name','foo').where('age', '>', 56)
		})
		.toSQL((err, sql, params) => {
			if (err) return cb(err)
			cb(null,
				'select * from user where id = ? and state = ? or (name = ? and age > ?);' === sql &&
			params.length === 4 &&
			params[0] === 3 &&
			params[1] === 1 &&
			params[2] === 'foo' &&
			params[3] === 56)
		})
})

test('test select query builder where in', cb => {
	client.q()
		.select()
		.from('user')
		.where({id: 3})
		.or(q => {
			return q.where('name', 'in', ['a', 'b', 'c']).where('age', '>', 56)
		})
		.toSQL((err, sql, params) => {
			if (err) return cb(err)
			cb(null,
				'select * from user where id = ? or (name in (?) and age > ?);' === sql &&
			params.length === 3 &&
			params[0] === 3 &&
			params[1].length === 3 &&
			params[1][0] === 'a' &&
			params[1][1] === 'b' &&
			params[1][2] === 'c' &&
			params[2] === 56)
		})
})

test('test insert query builder', cb => {
	client.q()
		.insert(['firstname', 'lastname'])
		.into('user')
		.values([['foo', 'bar'], ['hello', 'world']])
		.toSQL((err, sql, params) => {
			console.log('>>>', err, sql, params)
			if (err) return cb(err)
			cb(null,
				'insert into user (firstname,lastname) values ?;' === sql &&
			params.length === 2 &&
			params[0][0] === 'foo' &&
			params[0][1] === 'bar' &&
			params[1][0] === 'hello' &&
			params[1][1] === 'world')
		})
})
/*
test('test select query', cb => {
	client.q().select().from('user').where({id: 3, state: 1}).exec((err, result) => {
		if (err) return cb(err)
		cb(null, true)
	})
})
*/
