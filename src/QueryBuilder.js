const util = require('./util')

function extractConditions(index, conds, params, joint){
	let str = ''
	if (!conds.length) return str
	const c = conds.shift()
	// [and, or], generated of the system no escape needed
	if (c.charAt) return ' ' + c + ' ' + extractConditions(index, conds, params, c)
	// added an "or" or "and" if no adjunction
	if (conds[0] && !conds[0].charAt) conds.unshift(joint)
	if (Array.isArray(c[0])) return '(' + extractConditions(index, c, params, 'and') + ')'

	if (index && !index.includes(c[0])) return extractConditions(index, conds, params, joint)
	str += `\`${c[0]}\` ${c[1]} `
	params.push(c[2])
	if (Array.isArray(c[2])){
		str += '(?)'
	} else {
		str += '?'
	}

	return str + extractConditions(index, conds, params, joint)
}

function QueryBuilder(cluster, tname, pname){
	this.cluster = cluster
	this.tname = tname
	this.pname = pname
	this.op = 'select'
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

		if (Array.isArray(fields)){
			this.fields = fields
		}else{
			this.fields = Object.keys(fields)
			this.values = this.fields.map(k => fields[k])
		}
		return this
	},
	update(set){
		this.op = 'update'
		if (!this.pname) this.pname = 'master'
		this.set = set

		return this
	},
	delete(){
		this.op = 'delete'
		if (!this.pname) this.pname = 'master'
		this.where(...arguments)

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
		if (this.fields && Array.isArray(data) && !Array.isArray(data[0]) && data[0] instanceof Object){
			this.values = data.map(d => {
				return this.fields.map(k => {
					const v = d[k]
					if (v == null) return null
					return v
				})
			})
		}else{
			this.values = data
		}
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
			if (!this.tname || !this.set || !(this.set instanceof Object) || Array.isArray(this.set)) return 'missing table name or set'
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

		const params = []
		let conds
		let sql = this.op
		switch(this.op){
		case 'select':
			sql += ' ' + util.join(this.ret)
			if (this.tname) sql += ` from \`${this.tname}\``
			break
		case 'insert':
			sql += ` into \`${this.tname}\` (${util.join(this.fields)}) values ?`
			params.push(this.values)
			break
		case 'update':
			sql += ` \`${this.tname}\` set ?`
			params.push(this.set)
			break
		case 'delete':
			sql += ` from \`${this.tname}\``
			break
		default:
			return cb('operation not found: '+this.op)
		}
		if (this.cond.length){
			conds = extractConditions(null, this.cond, params, 'and')
			sql += ' where ' + conds
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

module.exports = QueryBuilder
