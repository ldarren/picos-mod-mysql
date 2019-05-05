const QueryBuilder = require('./QueryBuilder')
const Ready = require('./Ready')

const validcmp = ['=', '<', '>', '=<', '>=', '<>', '!=', 'in', 'not in', 'like']

function validateCmp(cmp){
	if (validcmp.includes(cmp)) return cmp
	console.error('invalid sql compare', cmp)
	return validcmp[0]
}

function makeHostCond(cond, params){
	const lhs = cond[0]
	const cmp = validateCmp(cond[1])
	const rhs = cond[2]

	params.push(lhs, rhs)
	return Array.isArray(rhs) ? `h.?? ${cmp} (?)` : `h.?? ${cmp} ?`
}

function makeMapCond(cond, hash, params){
	const lhs = cond[0]
	const cmp = validateCmp(cond[1])
	const rhs = cond[2]
	let rhsVar = '?'
	let v = 'v1'

	if (Array.isArray(rhs)){
		if (!rhs.length) {
			console.error('invalid condition', lhs, cmp, rhs)
			return ''
		}
		rhsVar = '(?)'
		v = rhs[0].charAt ? 'v2' : 'v1'
	}

	params.push(hash.key(lhs), rhs)
	return `(m.k = ? and m.${v} ${cmp} ${rhsVar})`
}

function makeFilter(index, conds, i, join, hash, params){
	if (i >= conds.length) return ''

	const c = conds[i++]
	if (Array.isArray(c)){
		// if missing join, add in implicit join
		if (Array.isArray(conds[i])) {
			conds.splice(i, 0, join)
		}

		const c0 = c[0]
		if (Array.isArray(c0)){
			// sub-condition
			return makeFilter(index, c0, 0, 'and', hash, params) + makeFilter(index, conds, i, join, hash, params)
		}
		if (index && index.includes(c0)) return makeHostCond(c, params) + makeFilter(index, conds, i, join, hash, params)
		return makeMapCond(c, hash, params) + makeFilter(index, conds, i, join, hash, params)
	}

	// c is a string
	return ' ' + c + ' ' + makeFilter(index, conds, i, c, hash, params)
}

function extractMapKey(index, ret, hash){
	if (isAll(ret)) return []
	return ret.reduce((acc, r) => {
		if (index.includes(r)) return acc
		acc.push(hash.key(r))
		return acc
	}, [])
}

// return 0 - no conditions, 1 - host only, 2 - map only, 3 - both
function condType(index, conds, i){
	if (i >= conds.length) return 0

	const c = conds[i++]
	if (Array.isArray(c)){
		const c0 = c[0]
		if (Array.isArray(c0)){
			// sub-condition
			return condType(index, c0, 0) | condType(index, conds, i)
		}
		if (index && index.includes(c0)) return 1 | condType(index, conds, i)
		return 2 | condType(index, conds, i)
	}

	// c is a string
	return condType(index, conds, i)
}

function makeSource(name, index, ret, conds, params){
	const all = isAll(ret)
	let type = 0
	if (all){
		type = 1 | 2
	} else {
		type = ret.reduce((acc, r) => {
			if (index.includes(r)) return acc | 1
			return acc | 2
		}, type)
		type |= condType(index, conds, 0)
	}

	switch(type){
	case 0:
	default:
		console.error('invalid source type', type, index, ret, conds)
		// falls through
	case 1:
		params.push(name)
		return '?? h'
	case 2:
		params.push(name + '_map')
		return '?? m'
	case 3:
		params.push(name, name + '_map')
		return '?? h left join ?? m on m.host_id = h.id'
	}
}

function isAll(ret){
	return !ret || !ret.length || ret.includes('*')
}

function makeReturn(index, ret){
	const all = isAll(ret)
	let arr = []
	if (all){
		arr.push('h.*')
	}else{
		ret.reduce((acc, r) => {
			if (index.includes(r)) acc.push('h.' + r)
			return acc
		}, arr)
	}
	if (all || arr.length !== ret.length) arr.push('m.k', 'm.v1', 'm.v2', 'm.state')
	if (!all && !arr.includes('h.id')) arr.push('h.id')
	return arr
}

function makeHostInsert(name, index, fields, values, params){
	const valuess = Array.isArray(values[0]) ? values : [values]
	const hostValues = []
	for (let i = 0, l = valuess.length; i < l; i++) hostValues.push([])

	const hostFields = fields.filter((f, i) => {
		if (!index.includes(f)) return false
		valuess.forEach((v, j) => hostValues[j].push(v[i]))
		return true
	})

	params.push(hostFields, hostValues)
	return `insert into \`${name}\` (??) values ?`
}

function hasMapFields(index, fields){
	return fields.some(f => index.includes(f))
}

// insert into xxx (a, b) values (1, 2), (3, 4)
// insert into xxx_map (k, v1, v2) values (a, 1, null), (a, 3, null), (b, 2, null), (b, 4, null)
function makeMapInsert(name, index, fields, values, hash, params){
	const valuess = Array.isArray(values[0]) ? values : [values]

	const mapValues = fields.reduce((acc, f, i) => {
		if (index.includes(f)) return acc
		const k = hash.key(f)
		valuess.forEach(v => {
			const vi = v[i]
			if (Number.isInteger(vi)) acc.push([k, vi, null])
			acc.push([k, null, vi])
		})
		return acc
	}, [])

	params.push(mapValues)
	return `insert into \`${name}_map\` (host_id, k, v1, v2) values ?`
}

function normalise(arr, hash){
	if (!arr.length) return arr
	if (!arr[0].k) return arr
	const map = arr.reduce((acc, a) => {
		const obj = acc[a.id] || {}
		Object.assign(obj, a)
		acc[a.id] = obj
		obj[hash.val(a.k)] = a.v2 || a.v1
		obj[hash.val(a.k) + '_state'] = a.state
		return acc
	}, {})

	return Object.keys(map).map(k => {
		const obj = map[k]
		delete obj.k
		delete obj.v1
		delete obj.v2
		return obj
	})
}

function Transposer(pool, hash, name, index){
	QueryBuilder.call(this)
	this.ready = new Ready
	this.hash = hash
	hash.ready.on(() => this.ready.did() )
	this.pool = pool
	this.name = name
	this.index = index
}

Transposer.prototype = Object.assign({}, QueryBuilder.prototype, {
	from: null,
	into: null,
	validate(){
		switch(this.op){
		case 'select':
			return
		case 'insert':
		{
			if (!this.values.length) return 'insert must come with values'
			if (!this.fields.length) return 'insert must come with fields'
			const missing = this.index.filter(i => !this.fields.includes(i))
			if (missing.length) return 'missing index in insert ' + missing.join(',')
			return
		}
		case 'update':
		case 'delete':
		default:
			return
		}
	},
	toSQL(cb){
		const err = this.validate()
		if (err) return cb(err)

		let paramss = []
		let sqls = []
		let params = []
		let sql = ''
		switch(this.op){
		case 'select':
		{
			sql = 'select '
			sql += makeReturn(this.index, this.ret)
			sql += ' from '
			sql += makeSource(this.name, this.index, this.ret, this.cond, params)
			if (this.cond.length){
				sql += ' where '
				sql += makeFilter(this.index, this.cond, 0, 'and', this.hash, params)
			}
			const mapRet = extractMapKey(this.index, this.ret, this.hash)
			if (mapRet.length){
				params.push(mapRet)
				sql += ' and m.k in (?)'
			}
			sqls.push(sql)
			paramss.push(params)
			return cb(err, sqls, paramss)
		}
		case 'insert':
			sql = makeHostInsert(this.name, this.index, this.fields, this.values, params)
			sqls.push(sql)
			paramss.push(params)
			if (hasMapFields(this.index, this.fields)){
				params = []
				sql = makeMapInsert(this.name, this.index, this.fields, this.values, this.hash, params)
				sqls.push(sql)
				paramss.push(params)
			}
			return cb(err, sqls, paramss)
		case 'update':
		case 'delete':
		default:
			return cb('coming soon')
		}
	},
	exec(cb){
		this.toSQL((err, sqls, paramss) => {
			if (err) return cb(err)

			this.pool.query(sqls[0], paramss[0], (err, result1) => {
				if (err) return cb(err)
				if (1 === sqls.length) {
					if (!Array.isArray(result1)) return cb(err, result1)
					return cb(err, normalise(result1, this.hash))
				}
				// insertId is in sequential https://dev.mysql.com/doc/refman/8.0/en/innodb-auto-increment-handling.html
				const params = paramss[1][0]
				for (let j = 0, jl = params.length; j < jl; j += result1.affectedRows){
					for(let id = result1.insertId, i = 0, l = id + result1.affectedRows; id < l; id++, i++){
						params[j + i].unshift(id)
					}
				}

				this.pool.query(sqls[1], paramss[1], (err, result2) => {
					if (err) return cb(err)
					return cb(err, result1, result2)
				})
			})
		})
	}
})

module.exports = Transposer
