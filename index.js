const mysql = require('mysql')
const args= require('pico-args')
const QueryBuilder = require('./src/QueryBuilder')
const Transposer = require('./src/Transposer')
const Hash = require('./src/Hash')

function onRemove(nodeId){
	console.log('REMOVED NODE : ' + nodeId)
}

function Client(clusterCfg, poolCfgs){
	const cluster = mysql.createPoolCluster(clusterCfg)
	for (let key in poolCfgs){
		cluster.add(key, poolCfgs[key])
	}
	cluster.on('remove', onRemove)

	this.cluster = cluster
	this.hashes = {}
}

Client.prototype = {
	bye(cb){
		this.cluster.end(cb)
	},
	format(){
		return mysql.format(...arguments)
	},
	read(){
		this.cluster.of('*').query(...arguments)
	},
	write(){
		this.cluster.of('master').query(...arguments)
	},
	q(){
		return new QueryBuilder(this.cluster, ...arguments)
	},
	hash(name = 'hash', opt = {}){
		let h = this.hashes[name]
		if (!h){
			h = new Hash(this, name, opt)
			this.hashes[name] = h
		}

		return h
	},
	t(name, hash, index, attr){
		return (pname = '*') => {
			return new Transposer(this.cluster.of(pname), hash, name, index, attr)
		}
	}
}

module.exports={
	create(appConfig, libConfig, next){
		const clusterCfg = {
			canRetry: true,
			removeNodeErrorCount: 5,
			restoreNodeTimeout: 0,
			defaultSelector: 'RR'
		}
		const poolCfg = {
			host: 'localhost',
			port: 3306,
			user: null,
			password: null,
			database: null,
			acquireTimeout: 1000,
			waitForConnections: true,
			connectionLimit: 2,
			queueLimit: 0
		}

		Object.assign(clusterCfg, libConfig.cluster || {})
		args.print('MySQL Cluster Options', clusterCfg)

		const configs = Object.keys(libConfig).reduce((acc, k) => {
			if ('cluster' === k || 'mod' === k) return acc
			const c = Object.assign({}, poolCfg, libConfig[k])
			args.print(`MySQL ${k} Options`, c)
			acc[k] = c
			return acc
		}, {})

		return next(null, new Client(clusterCfg, configs))
	}
}
