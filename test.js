const pico = require('pico-common/bin/pico-cli')
const { series } = pico.export('pico/test')
const mysql = require('./index')

const appConfig = { ath: '', env: 'pro' }
const libConfig = require('./env.json')
let client
let tuser

series('mysql', function() {
	this.test('ensure mysql loaded', cb => {
		cb(null, !!mysql)
	})

	this.test('ensure mysql create', cb => {
		mysql.create(appConfig, libConfig, (err, cli) => {
			if (err) return cb(err)
			client = cli
			tuser = client.t('user_test', client.hash(), ['username'])
			cb(null, !!client)
		})
	})

	this.test('test select query builder', cb => {
		client.q().select(1).toSQL((err, sql, params) => {
			if (err) return cb(err)
			cb(null,
				'select 1;' === sql &&
			params.length === 0)
		})
	})
	/*
	this.test('test select query builder with bracket', cb => {
		client.q()
			.select()
			.from('user_test')
			.where({id: 3, state: 1})
			.or(q => {
				return q.where('name','foo').where('age', '>', 56)
			})
			.toSQL((err, sql, params) => {
				if (err) return cb(err)
				cb(null,
					'select * from user_test where id = ? and state = ? or (name = ? and age > ?);' === sql &&
				params.length === 4 &&
				params[0] === 3 &&
				params[1] === 1 &&
				params[2] === 'foo' &&
				params[3] === 56)
			})
	})

	this.test('test select query builder where in', cb => {
		client.q()
			.select()
			.from('user_test')
			.where({id: 3})
			.or(q => {
				return q.where('name', 'in', ['a', 'b', 'c']).where('age', '>', 56)
			})
			.toSQL((err, sql, params) => {
				if (err) return cb(err)
				cb(null,
					'select * from user_test where id = ? or (name in (?) and age > ?);' === sql &&
				params.length === 3 &&
				params[0] === 3 &&
				params[1].length === 3 &&
				params[1][0] === 'a' &&
				params[1][1] === 'b' &&
				params[1][2] === 'c' &&
				params[2] === 56)
			})
	})

	this.test('test insert query builder', cb => {
		client.q()
			.insert(['firstname', 'lastname'])
			.into('user_test')
			.values([['foo', 'bar'], ['hello', 'world']])
			.toSQL((err, sql, params) => {
				if (err) return cb(err)
				const p0 = params[0]
				cb(null,
					'insert into user_test (firstname,lastname) values ?;' === sql &&
				p0.length === 2 &&
				p0[0][0] === 'foo' &&
				p0[0][1] === 'bar' &&
				p0[1][0] === 'hello' &&
				p0[1][1] === 'world')
			})
	})

	this.test('test insert query exec', cb => {
		client.q()
			.insert(['v'])
			.into('hash')
			.values([['ClientId'], ['UserPoolId']])
			.exec((err, reply) => {
				if (err) return cb(err)
				cb(null, reply.affectedRows === 2)
			})
	})

	this.test('test select query exec', cb => {
		client.q()
			.select('k', 'v')
			.from('hash')
			.exec((err, reply) => {
				if (err) return cb(err)
				cb(null, reply.length === 2)
			})
	})

	this.test('test transposer ready', cb => {
		tuser().ready.on(() => cb(null, true))
	})

	this.test('test transposer insert', cb => {
		tuser().insert(['username', 'email', 'phone']).values([['test1', 'test1@yopmail.com', '8574883'],['test2', 'test2@yopmail.com', '9823747']]).toSQL((err, sqls, paramss) => {
			if (err) return cb(err)
			cb(null,
				2 === sqls.length &&
				2 === paramss.length &&
				'insert into user_test (`username`) values (\'test1\'), (\'test2\')' === client.format(sqls[0], paramss[0]) &&
				'insert into user_test_map (host_id, k, v1, v2) values (4, NULL, \'test1@yopmail.com\'), (4, NULL, \'test2@yopmail.com\'), (5, NULL, \'8574883\'), (5, NULL, \'9823747\')' === client.format(sqls[1], paramss[1])
			)
		})
	})

	this.test('test transposer insert exec 1', cb => {
		tuser().insert(['username', 'email', 'phone']).values([['test1', 'test1@yopmail.com', '8574883'],['test2', 'test2@yopmail.com', '9823747']]).exec((err, result) => {
			if (err) return cb(err)
			cb(null, true)
		})
	})

	this.test('test transposer insert exec 2', cb => {
		tuser().insert({username: 'test3', email:'test3@ym.com'}).exec((err, result) => {
			if (err) return cb(err)
			cb(null, true)
		})
	})

	this.test('test transposer insert exec 3', cb => {
		tuser().insert(['username', 'email', 'phone']).values([{username: 'test4', email:'test4@ym.com'}, {username: 'test5', phone:'123'}]).exec((err, result) => {
			if (err) return cb(err)
			cb(null, true)
		})
	})
*/
	this.test('test transposer select', cb => {
		tuser().select('email', 'phone', 'username').where({username: 'test2'}).toSQL((err, sqls, paramss) => {
			if (err) return cb(err)
			cb(null,
				1 === sqls.length &&
				1 === paramss.length &&
				'select h.username,m.k,m.v1,m.v2,m.state,h.id from `user_test` h left join `user_test_map` m on m.host_id = h.id where h.`username` = \'test2\' and m.k in (4, 5)' === client.format(sqls[0], paramss[0]))
		})
	})

	this.test('test transposer select all', cb => {
		tuser().select().toSQL((err, sqls, paramss) => {
			if (err) return cb(err)
			cb(null,
				1 === sqls.length &&
				1 === paramss.length &&
				'select h.*,m.k,m.v1,m.v2,m.state from `user_test` h left join `user_test_map` m on m.host_id = h.id' === client.format(sqls[0], paramss[0]))
		})
	})
/*
	this.test('test transposer select exec', cb => {
		tuser().select('email', 'phone', 'username').where({username: 'test2'}).exec((err, result) => {
			if (err) return cb(err)
			cb(null, true)
		})
	})
*/
})
