const pico = require('pico-common/bin/pico-cli')
const { series } = pico.export('pico/test')

module.exports = client => {
	series('# Transposer Exec Tests', function(){
		const tuser = client.t('user_test', client.hash(), ['username'])

		this.test('test transposer ready', cb => {
			tuser().ready.on(() => cb(null, true))
		})

		this.test('test transposer insert', cb => {
			tuser().insert(['username', 'email', 'phone', 'state']).values([['test1', 'test1@yopmail.com', '+658765432', 2], ['test2', 'test2@yopmail.com', '+608765432', 2]]).exec((err, res1, res2) => {
				if (err) return cb(err)
				cb(null, 2 === res1.affectedRows && 4 === res2.affectedRows)
			})
		})

		this.test('test transposer insert without values', cb => {
			tuser().insert({username: 'test3', email:'test3@ym.com'}).exec((err, res1, res2) => {
				if (err) return cb(err)
				cb(null, 1 === res1.affectedRows && 1 === res2.affectedRows)
			})
		})

		this.test('test transposer select with attri', cb => {
			tuser().select('email', 'phone', 'state').where({state: 2}).exec((err, result) => {
				if (err) return cb(err)
				cb(err, 2 === result.length)
			})
		})

		this.test('test transposer update', cb => {
			tuser().update({state: 2, cby: 1}).where({username: 'test2'}).exec((err, result1, result2) => {
				if (err) return cb(err)
				cb(err, 1 === result2.affectedRows)
			})
		})

		this.test('test transposer select with index and attri', cb => {
			tuser().select('email', 'phone', 'state').where({username: 'test2', state: 2}).exec((err, result) => {
				if (err) return cb(err)
				cb(err, 1 === result.length)
			})
		})
/*
		this.test('test transposer delete exec', cb => {
			tuser().delete().where('username', 'in', ['test1', 'test2']).exec((err, result) => {
				console.log('delete', err, result)
				cb(err, true)
			})
		})
*/
	})
}
