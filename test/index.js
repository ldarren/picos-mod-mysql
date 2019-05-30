const pico = require('pico-common/bin/pico-cli')
const { setup, series } = pico.export('pico/test')
const mysql = require('../index')

const appConfig = { ath: '', env: 'pro' }
// sample.env.json -> env.json
const libConfig = require('./env.json')

setup({
	stdout: true,
	end: function(result){
		//console.log(JSON.stringify(result))
	},
	//fname: 'test_result.json'
})

series('# Mysql Tests', function() {
	this.test('ensure mysql loaded', cb => {
		cb(null, !!mysql)
	})

	this.test('ensure mysql create', cb => {
		mysql.create(appConfig, libConfig, (err, cli) => {
			if (err) return cb(err)
			require('./util')(cli)
			require('./QueryBuilder.toSQL')(cli)
			//require('./QueryBuilder.exec')(cli)
			require('./Transposer.toSQL')(cli)
			require('./Transposer.exec')(cli)
			cb(null, !!cli)
		})
	})
})
