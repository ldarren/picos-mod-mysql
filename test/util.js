const pico = require('pico-common/bin/pico-cli')
const { parallel } = pico.export('pico/test')
const util = require('../src/util')

module.exports = client => {
	parallel('util test', function(){
		this.test('valid name should have quote', cb => {
			cb(null, '`lastname`,`$dollar`,`_underscore`,`caMel`,`a1phan0meric`' ===
				util.join(['lastname', '$dollar', '_underscore', 'caMel', 'a1phan0meric']))
		})
		this.test('number should have no quote', cb => {
			cb(null, '`hello`,123' === util.join(['hello',123]))
		})
		this.test('asterisk should have no quote', cb => {
			cb(null, '*' === util.join(['*']))
		})
		this.test('explicit string should have no quote', cb => {
			cb(null, '"hello"' === util.join(['"hello"']))
		})
		this.test('mysql function have no quote', cb => {
			cb(null, 'NOW()' === util.join(['NOW()']))
		})
	})
}
