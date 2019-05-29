const Ready = require('./Ready')

function keyValues(arr, key, value){
	return arr.reduce((acc, obj) => {
		acc[obj[key]] = obj[value]
		return acc
	}, {})
}

function Hash(client, name, {k = 'k', v = 'v', keys, vals}){
	this.KEYS = keys
	this.VALS = vals
	this.ready = new Ready
	client.read('SELECT ??, ?? FROM ?? WHERE state = 1', [k, v, name], (err, result) => {
		if (err) return console.error(err)
		this.KEYS = keyValues(result, v, k)
		this.VALS = keyValues(result, k, v)
		this.ready.did()
	})
}

Hash.prototype = {
	key(v){
		return this.KEYS[v]
	},
	val(k){
		return this.VALS[k]
	},
	replace(arr){
		const keys = this.KEYS
		return arr.map(a => (keys[a] || a) )
	},
	verify(keys, index){
		const unknown=[]
		const vals = this.VALS
		for(let i=0,k; (k=keys[i]); i++){
			if(vals[k]) continue
			if (-1 !== index.indexOf(k)) unknown.push(k)
		}
		return unknown
	}
}

module.exports = Hash
