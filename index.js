const mysql = require('mysql')
const args= require('pico-args')

function listen(key, pool){
	pool.acquire = (conn) => {
		console.log(k, 'acquired', conn.threadId)
	}
	pool.connection = (conn) => {
		console.log(k, 'connected', conn.threadId)
	}
	pool.enqueue = (conn) => {
		console.log(k, 'enqueueing')
	}
	pool.release = (conn) => {
		console.log(k, 'released', conn.threadId)
	}
}

function onRemove(nodeId){
	console.log('REMOVED NODE : ' + nodeId);
}

function Query(op, tname, pname = '*'){
	this.op = op
	this.tname = tname
	this.pname = pname
}

Query.prototype = {
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
	select(tname, pname){
		return new Query('select', tname, pname)
	},
	query(){
		return this.conn.query(...arguments)
	},
	format(){
		return mysql.format(...arguments)
	},
	decode(obj,hash,ENUM){
		const keys=Object.keys(obj)
		for(let i=0,k; k=keys[i]; i++) {
			if (-1===ENUM.indexOf(k)) continue
			obj[k]=hash.key(obj[k])
		}
		return obj 
	},
	decodes(rows,hash,ENUM){
		for(let i=0,r; r=rows[i]; i++){ 
			this.decode(r,hash,ENUM) 
		}
		return rows
	},
	encode(obj,by,hash,INDEX,ENUM){
		const arr=[]
		for(let i=0,k; k=INDEX[i]; i++){ 
			if (-1===ENUM.indexOf(k)) arr.push(obj[k])
			else arr.push(hash.val(obj[k]))
		}
		arr.push(by)
		return arr
	},
	mapDecode(rows=[], output={}, hash, ENUM, key='id'){
		for(let i=0,r,k; r=rows[i]; i++) {
			if (r[key] !== output.id) continue
			k=hash.key(r.k)
			if (-1===ENUM.indexOf(k)) output[k]=r.v1 || r.v2
			else output[k]=hash.key(r.v2)
			r.k=r.v1=r.v2=undefined
		}
		return output
	},
	mapDecodes(rows=[], outputs=[], hash, ENUM, key='id'){
		for(let i=0,o; o=outputs[i]; i++){
			this.mapDecode(rows, o, hash, ENUM, key)
		}
		return outputs
	},
	mapEncode(obj, by, hash, INDEX, ENUM){
		const
		id=obj.id,
		arr=[]

		for(let i=0,keys=Object.keys(obj),key,k,v; key=keys[i]; i++){
			if(INDEX.indexOf(key)>-1)continue
			k=hash.val(key)
			v=obj[key]
			if (!k || undefined===v) continue
			if (-1===ENUM.indexOf(key)){
				if(v.charAt) arr.push([id,k,v,null,by])
				else arr.push([id,k,null,v,by])
			}else{
				arr.push([id,k,null,hash.val(v),by])
			}
		}
		return arr
	},
	map2Encode(obj1, obj2, map, by, hash, INDEX, ENUM){
		const
		id1=obj1.id,
		id2=obj2.id,
		arr=[]

		for(let i=0,keys=Object.keys(map),key,k,v; key=keys[i]; i++){
			if(INDEX.indexOf(key)>-1)continue
			k=hash.val(key)
			v=map[key]
			if (!k || undefined===v) continue
			if (-1===ENUM.indexOf(key)){
				if(v.charAt) arr.push([id1,id2,k,v,null,by])
				else arr.push([id1,id2,k,null,v,by])
			}else{
				arr.push([id1,id2,k,null,hash.val(v),by])
			}
		}
		return arr
	},
	listDecode(rows,key,hash,ENUM){
		const
		k=hash.val(key),
		notEnum=(-1===ENUM.indexOf(key))

		for(let i=0,r; r=rows[i]; i++) {
			if (r.k!==k)continue
			if (notEnum) r[key]=(r.v1 || r.v2)
			else r[key]=hash.key(r.v2)
			r.k=r.v1=r.v2=undefined
		}
		return rows
	},
	listEncode(id, key, list, by, hash, INDEX, ENUM){
		if (!key || !list || !list.length) return cb()
		const
		arr=[],
		k=hash.val(key),
		notEnum=(-1===ENUM.indexOf(key))

		if(!k || INDEX.indexOf(key)>-1) return arr
		for(let i=0,v; v=list[i]; i++){
			if (notEnum){
				if(v.charAt) arr.push([id,k,v,null,by])
				else arr.push([id,k,null,v,by])
			}else{
				arr.push([id,k,null,hash.val(v),by])
			}
		}
		return arr
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
			connectionLimit: 2,
			acquireTimeout: 1000,
			waitForConnections: true,
			connectionLimit: 1,
			queueLimit: 0
        }

		Object.assign(clusterCfg, libConfig.cluster || {})
		args.print('MySQL Cluster Options', clusterCfg)

		const configs = Object.keys(libConfig).reduce((acc, k) => {
			let c
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
