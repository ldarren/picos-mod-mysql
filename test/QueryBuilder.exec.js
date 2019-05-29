const pico = require('pico-common/bin/pico-cli')
const { series } = pico.export('pico/test')


module.exports = client => {
	series('QueryBuilder exec', function(){

		this.test('test delete all', cb => {
			client.q('user_test')
				.delete()
				.exec((err, reply) => {
					if (err) return cb(err)
					cb(null, true)
				})
		})

		this.test('test insert 4 rows', cb => {
			client.q('user_test')
				.insert(['username', 'cby'])
				.values([
					['test1', 0],
					['test2', 0],
					['test3', 0],
					['test4', 0],
				])
				.exec((err, reply) => {
					if (err) return cb(err)
					cb(null, 4 === reply.affectedRows)
				})
		})

		this.test('test update 1 rows', cb => {
			client.q('user_test')
				.update({cby: 1})
				.where({id: 1})
				.exec((err, reply) => {
					if (err) return cb(err)
					cb(null, true)
				})
		})

		this.test('test select all 4 rows', cb => {
			client.q('user_test')
				.select('username', 'cby')
				.where('username', 'in', ['test1', 'test2', 'test3', 'test4'])
				.exec((err, reply) => {
					if (err) return cb(err)
					cb(null, 4 === reply.length)
				})
		})

		this.test('test delete 4 rows exec', cb => {
			client.q('user_test')
				.delete('username', 'in', ['test1', 'test2', 'test3', 'test4'])
				.exec((err, reply) => {
					if (err) return cb(err)
					cb(null, reply.affectedRows === 4)
				})
		})
	})
}
