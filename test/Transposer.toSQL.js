const pico = require('pico-common/bin/pico-cli')
const { series } = pico.export('pico/test')
const opt = {
	keys: {
		'user': 1,
		'group': 2,
		'username': 3,
		'email': 4,
		'phone': 5,
		'name': 6,
		'env': 7,
		'perm': 8,
	},
	vals: {
		1: 'user',
		2: 'group',
		3: 'username',
		4: 'email',
		5: 'phone',
		6: 'name',
		7: 'env',
		8: 'perm',
	}
}

module.exports = client => {
	series('# Transposer toSQL Tests', function(){
		const tuser = client.t('user_test', client.hash('hash', opt), ['username'])
/*
		this.test('test transposer delete all', cb => {
			tuser().delete().toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					2 === sqls.length &&
					2 === paramss.length &&
					'delete from user_test;' === client.format(sqls[0], paramss[0]) &&
					'delete from user_test_map ;' === client.format(sqls[1], paramss[1]))
			})
		})
*/
		this.test('test transposer insert', cb => {
			tuser().insert(['username', 'email', 'phone']).values([['test1', 'test1@yopmail.com', '8574883'],['test2', 'test2@yopmail.com', '9823747']]).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					2 === sqls.length &&
					2 === paramss.length &&
					'insert into `user_test` (`username`) values (\'test1\'), (\'test2\')' ===
						client.format(sqls[0], paramss[0]) &&
					'insert into `user_test_map` (host_id, k, v1, v2) values (\'ID0\', 4, NULL, \'test1@yopmail.com\'), (\'ID1\', 4, NULL, \'test2@yopmail.com\'), (\'ID0\', 5, NULL, \'8574883\'), (\'ID1\', 5, NULL, \'9823747\')' ===
						client.format(sqls[1], paramss[1])
				)
			})
		})

		this.test('test transposer select all', cb => {
			tuser().select().toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					1 === sqls.length &&
					1 === paramss.length &&
					2 === paramss[0].length &&
					'select h.*,m.k,m.v1,m.v2,m.state from `user_test` h left join `user_test_map` m on m.host_id = h.id' === client.format(sqls[0], paramss[0]))
			})
		})
		
		this.test('test transposer update with index', cb => {
			tuser().update({email: 'update1@yopmail.com'}).where({username: 'hello'}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)

				cb(null,
					2 === sqls.length &&
					2 === paramss.length &&
					3 === paramss[0].length &&
					4 === paramss[1].length &&
					'select id from `user_test` h where h.`username` = \'hello\'' === client.format(sqls[0], paramss[0]) &&
					'update `user_test_map` set v2 = \'update1@yopmail.com\' where host_id = \'ID0\' and k = 4' === client.format(sqls[1], paramss[1]))
			})
		})

		this.test('test transposer update with id', cb => {
			tuser().update({email: 'update1@yopmail.com', phone: 123, username: 'hello'}).where({id:1}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					3 === sqls.length &&
					3 === paramss.length &&
					3 === paramss[0].length &&
					4 === paramss[1].length &&
					4 === paramss[2].length &&
					'update `user_test` set `username` = \'hello\' where id = \'ID0\'' === client.format(sqls[0], paramss[0]) &&
					'update `user_test_map` set v2 = \'update1@yopmail.com\' where host_id = \'ID0\' and k = 4' === client.format(sqls[1], paramss[1]) &&
					'update `user_test_map` set v1 = 123 where host_id = \'ID0\' and k = 5' === client.format(sqls[2], paramss[2]))
			})
		})

		this.test('test transposer select all with index', cb => {
			tuser().where({username: 'test2'}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					1 === sqls.length &&
					1 === paramss.length &&
					'select h.*,m.k,m.v1,m.v2,m.state from `user_test` h left join `user_test_map` m on m.host_id = h.id where h.`username` = \'test2\'' ===
						client.format(sqls[0], paramss[0]))
			})
		})
/*
		this.test('test transposer delete with id', cb => {
			tuser().delete({id: 2}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					2 === sqls.length &&
					2 === paramss.length &&
					1 === paramss[0].length &&
					'delete from user_test where id=1;' === client.format(sqls[0], paramss[0]) &&
					'delete from user_test_map where host_id=1;' === client.format(sqls[1], paramss[1]))
			})
		})
*/
		this.test('test transposer select non-index with id', cb => {
			tuser().select('email', 'phone').where({id: 1}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					1 === sqls.length &&
					1 === paramss.length &&
					4 === paramss[0].length &&
					'select m.k,m.v1,m.v2,m.state,m.host_id as id from `user_test_map` m where m.`host_id` = 1 and m.k in (4, 5)' ===
						client.format(sqls[0], paramss[0]))
			})
		})

		this.test('test transposer select index only with id', cb => {
			tuser().select('username').where({id: 1}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					1 === sqls.length &&
					1 === paramss.length &&
					3 === paramss[0].length &&
					'select h.username,h.id from `user_test` h where h.`id` = 1' === client.format(sqls[0], paramss[0]))
			})
		})

		this.test('test transposer select index with id', cb => {
			tuser().select('username', 'email', 'phone').where({id: 1}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					1 === sqls.length &&
					1 === paramss.length &&
					5 === paramss[0].length &&
					'select h.username,m.k,m.v1,m.v2,m.state,h.id from `user_test` h left join `user_test_map` m on m.host_id = h.id where h.`id` = 1 and m.k in (4, 5)' ===
						client.format(sqls[0], paramss[0]))
			})
		})
		/*
		this.test('test transposer delete with index', cb => {
			tuser().delete({username: 'hello'}).toSQL((err, sqls, paramss) => {
				if (err) return cb(err)
				cb(null,
					3 === sqls.length &&
					3 === paramss.length &&
					1 === paramss[0].length &&
					'select id from user_test where username=\'hello\'' === client.format(sqls[0], paramss[0]) &&
					'delete from user_test where id=1;' === client.format(sqls[1], paramss[1]) &&
					'delete from user_test_map where host_id=1;' === client.format(sqls[2], paramss[2]))
			})
		})
*/
	})
}
