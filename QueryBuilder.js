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

		const params = []
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
			params.push(this.values)
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

module.exports = QueryBuilder
