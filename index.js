const mysql = require('mysql')
const args= require('pico-args')

function decode(obj,hash,ENUM){
	const keys=Object.keys(obj)
	for(let i=0,k; (k=keys[i]); i++) {
		if (-1===ENUM.indexOf(k)) continue
		obj[k]=hash.key(obj[k])
	}
	return obj
}

function decodes(rows,hash,ENUM){
	for(let i=0,r; (r=rows[i]); i++){
		this.decode(r,hash,ENUM)
	}
	return rows
}

function encode(obj,by,hash,INDEX,ENUM){
	const arr=[]
	for(let i=0,k; (k=INDEX[i]); i++){
		if (-1===ENUM.indexOf(k)) arr.push(obj[k])
		else arr.push(hash.val(obj[k]))
	}
	arr.push(by)
	return arr
}

function mapDecode(rows=[], output={}, hash, ENUM, key='id'){
	for(let i=0,r,k; (r=rows[i]); i++) {
		if (r[key] !== output.id) continue
		k=hash.key(r.k)
		if (-1===ENUM.indexOf(k)) output[k]=r.v1 || r.v2
		else output[k]=hash.key(r.v2)
		r.k=r.v1=r.v2=undefined
	}
	return output
}

function mapDecodes(rows=[], outputs=[], hash, ENUM, key='id'){
	for(let i=0,o; (o=outputs[i]); i++){
		this.mapDecode(rows, o, hash, ENUM, key)
	}
	return outputs
}

function mapEncode(obj, by, hash, INDEX, ENUM){
	const
		id=obj.id,
		arr=[]

	for(let i=0,keys=Object.keys(obj),key,k,v; (key=keys[i]); i++){
		if(INDEX.indexOf(key)>-1)continue
		k=hash.val(key)
		v=obj[key]
		if (!k || null == v) continue
		if (-1===ENUM.indexOf(key)){
			if(v.charAt) arr.push([id,k,v,null,by])
			else arr.push([id,k,null,v,by])
		}else{
			arr.push([id,k,null,hash.val(v),by])
		}
	}
	return arr
}

function map2Encode(obj1, obj2, map, by, hash, INDEX, ENUM){
	const
		id1=obj1.id,
		id2=obj2.id,
		arr=[]

	for(let i=0,keys=Object.keys(map),key,k,v; (key=keys[i]); i++){
		if(INDEX.indexOf(key)>-1)continue
		k=hash.val(key)
		v=map[key]
		if (!k || null == v) continue
		if (-1===ENUM.indexOf(key)){
			if(v.charAt) arr.push([id1,id2,k,v,null,by])
			else arr.push([id1,id2,k,null,v,by])
		}else{
			arr.push([id1,id2,k,null,hash.val(v),by])
		}
	}
	return arr
}

function listDecode(rows,key,hash,ENUM){
	const
		k=hash.val(key),
		notEnum=(-1===ENUM.indexOf(key))

	for(let i=0,r; (r=rows[i]); i++) {
		if (r.k!==k)continue
		if (notEnum) r[key]=(r.v1 || r.v2)
		else r[key]=hash.key(r.v2)
		r.k=r.v1=r.v2=undefined
	}
	return rows
}

function listEncode(id, key, list, by, hash, INDEX, ENUM){
	if (!key || !list || !list.length) return
	const
		arr=[],
		k=hash.val(key),
		notEnum=(-1===ENUM.indexOf(key))

	if(!k || INDEX.indexOf(key)>-1) return arr
	for(let i=0,v; (v=list[i]); i++){
		if (notEnum){
			if(v.charAt) arr.push([id,k,v,null,by])
			else arr.push([id,k,null,v,by])
		}else{
			arr.push([id,k,null,hash.val(v),by])
		}
	}
	return arr
}

function extractConditions(conds, params, joint){
	let str = ''
	if (!conds.length) return str
	const cond = conds.shift()
	if (cond.charAt) return ' ' + cond + ' ' + extractConditions(conds, params, cond)
	if (Array.isArray(cond[0])) return '(' + extractConditions(cond, params, 'and') + ')'
	if (conds[0] && !conds[0].charAt) conds.unshift(joint)

	str += cond[0] + ' ' + cond[1] + ' '
	if (Array.isArray(cond[2])){
		params.push(cond[2])
		str += '(?)'
	} else {
		str += '?'
		params.push(cond[2])
	}

	return str + extractConditions(conds, params, joint)
}

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

function QueryBuilder(cluster, tname, pname){
	this.cluster = cluster
	this.tname = tname
	this.pname = pname
	this.cond = []
}

QueryBuilder.prototype = {
	select(){
		this.op = 'select'
		if (!this.pname) this.pname = '*'

		if (arguments.length) {
			if (1 === arguments.length){
				const arg = arguments[0]
				if (Array.isArray(arg)) this.ret = arg
				else this.ret = [arg]
			}else{
				this.ret = Array.from(arguments)
			}
		}else{
			this.ret = ['*']
		}

		return this
	},
	insert(fields){
		this.op = 'insert'
		if (!this.pname) this.pname = 'master'
	
		this.fields = fields
		return this
	},
	update(){
		this.op = 'update'
		if (!this.pname) this.pname = 'master'

		return this
	},
	delete(){
		this.op = 'delete'
		if (!this.pname) this.pname = 'master'

		return this
	},
	from(tname){
		this.tname = tname
		return this
	},
	into(tname){
		this.tname = tname
		return this
	},
	where(){
		const arg0 = arguments[0]

		switch(arguments.length){
		case 1:
			if (Array.isArray(arg0)){
				this.cond.push(...arg0)
				break
			}
			Object.keys(arg0).reduce((cond, k) => {
				cond.push([k, '=', arg0[k]])
				return cond
			}, this.cond)
			break
		case 2:
			this.cond.push([arg0, '=', arguments[1]])
			break
		case 3:
			this.cond.push([arg0, arguments[1], arguments[2]])
			break
		}
		return this
	},
	and(join){
		this.cond.push('and')
		if (join) this.cond.push(join(new QueryBuilder()).cond)
		return this
	},
	or(join){
		this.cond.push('or')
		if (join) this.cond.push(join(new QueryBuilder()).cond)
		return this
	},
	values(data){
		this.values = data
		return this
	},
	validate(){
		if (!this.pname) return 'missing pool name'

		switch(this.op){
		case 'select':
			if (!this.ret || !Array.isArray(this.cond)) return 'missing return or conditions'
			return
		case 'insert':
			if (!this.tname || !this.fields || !this.values) return 'missing table name or fields or values'
			return
		case 'update':
			if (!this.tname) return 'missing table name'
			return
		case 'delete':
			if (!this.tname) return 'missing table name'
			return
		default:
			return `unknown operation ${this.op}`
		}
	},
	toSQL(cb){
		const err = this.validate()
		if (err) return cb(err)

		let params = []
		let conds
		let sql = this.op
		switch(this.op){
		case 'select':
			sql += ' ' + this.ret.join(',')
			if (this.tname) sql += ' from ' + this.tname
			if (this.cond.length){
				conds = extractConditions(this.cond, params, 'and')
				sql += ' where ' + conds
			}
			break
		case 'insert':
			sql += ` into ${this.tname} (${this.fields.join(',')}) values ?`
			params = this.values
			break
		default:
			return cb('coming soon')
		}
		sql += ';'
		return cb(err, sql, params)
	},
	exec(cb){
		this.toSQL((err, sql, params) => {
			if (err) return cb(err)
			const pool = this.cluster.of(this.pname)
			if (!pool) return cb(`invalid pool name ${this.pname}`)
			pool.query(sql, params, cb)
		})
	}
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
}

Client.prototype = {
	bye(cb){
		this.cluster.end(cb)
	},
	q(tname, pname){
		return new QueryBuilder(this.cluster, tname, pname)
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
			if ('cluster' !== k) {
				const c = Object.assign({}, poolCfg, libConfig[k])
				args.print(`MySQL ${k} Options`, c)
				acc[k] = c
			}
			return acc
		}, {})

		return next(null, new Client(clusterCfg, configs))
	}
}
