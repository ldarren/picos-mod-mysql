const mysql = require('mysql')
const args= require('pico-args')
const QueryBuilder = require('./QueryBuilder')
const Transposer = require('./Transposer')
const Hash = require('./Hash')

function listen(key, pool){
	pool.acquire = conn => {
		console.log(key, 'acquired', conn.threadId)
	}
	pool.connection = conn => {
		console.log(key, 'connected', conn.threadId)
	}
	pool.enqueue = () => {
		console.log(key, 'enqueueing')
	}
	pool.release = conn => {
		console.log(key, 'released', conn.threadId)
	}
}

function onRemove(nodeId){
	console.log('REMOVED NODE : ' + nodeId)
}

function Client(clusterCfg, poolCfgs){
	const cluster = mysql.createPoolCluster(clusterCfg)
	Object.keys(poolCfgs).reduce((acc, k) => {
		acc.add(k, poolCfgs[k])
		listen(k, acc.of(k))
		return acc
	}, cluster)
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
	hash(name, key, value){
		name = name || 'hash'
		let h = this.hashes[name]
		if (!h){
			h = new Hash(this, name, key, value)
			this.hashes[name] = h
		}

		return h
	},
	t(name, hash, index){
		return (pname = '*') => {
			return new Transposer(this.cluster.of(pname), hash, name, index)
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
