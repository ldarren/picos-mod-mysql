const pico = require('pico-common/bin/pico-cli')
const { parallel } = pico.export('pico/test')

module.exports = client => {
	parallel('QueryBuilder toSQL test', function(){
		this.test('test basic select', cb => {
			client.q().select(1, '"hello"').toSQL((err, sql, params) => {
				if (err) return cb(err)
				cb(null,
					'select 1,"hello";' === sql &&
				params.length === 0)
			})
		})

		this.test('test select with bracket', cb => {
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
						'select * from `user_test` where `id` = ? and `state` = ? or (`name` = ? and `age` > ?);' === sql &&
					params.length === 4 &&
					params[0] === 3 &&
					params[1] === 1 &&
					params[2] === 'foo' &&
					params[3] === 56)
				})
		})

		this.test('test select where in', cb => {
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
						'select * from `user_test` where `id` = ? or (`name` in (?) and `age` > ?);' === sql &&
					params.length === 3 &&
					params[0] === 3 &&
					params[1].length === 3 &&
					params[1][0] === 'a' &&
					params[1][1] === 'b' &&
					params[1][2] === 'c' &&
					params[2] === 56)
				})
		})

		this.test('test insert', cb => {
			client.q()
				.insert(['firstname', 'lastname'])
				.into('user_test')
				.values([['foo', 'bar'], ['hello', 'world']])
				.toSQL((err, sql, params) => {
					if (err) return cb(err)
					const p0 = params[0]
					cb(null,
						'insert into `user_test` (`firstname`,`lastname`) values ?;' === sql &&
					p0.length === 2 &&
					p0[0][0] === 'foo' &&
					p0[0][1] === 'bar' &&
					p0[1][0] === 'hello' &&
					p0[1][1] === 'world')
				})
		})

		this.test('test update', cb => {
			client.q('user_test')
				.update({firstname: 'foo', lastname: 'bar'})
				.where({id: 3})
				.toSQL((err, sql, params) => {
					if (err) return cb(err)
					cb(null,
						'update `user_test` set ? where `id` = ?;' === sql &&
					params.length === 2 &&
					params[0] instanceof Object &&
					params[1] === 3)
				})
		})

		this.test('test delete all', cb => {
			client.q('user_test')
				.delete()
				.toSQL((err, sql, params) => {
					if (err) return cb(err)
					cb(null,
						'delete from `user_test`;' === sql &&
					params.length === 0)
				})
		})

		this.test('test delete with cond', cb => {
			client.q('user_test')
				.delete({id: 3})
				.toSQL((err, sql, params) => {
					if (err) return cb(err)
					cb(null,
						'delete from `user_test` where `id` = ?;' === sql &&
					params.length === 1 &&
					params[0] === 3)
				})
		})

		this.test('test delete with where', cb => {
			client.q('user_test')
				.where({id: 3})
				.delete()
				.toSQL((err, sql, params) => {
					if (err) return cb(err)
					cb(null,
						'delete from `user_test` where `id` = ?;' === sql &&
					params.length === 1 &&
					params[0] === 3)
				})
		})
	})
}
