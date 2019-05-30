const QueryBuilder = require('./QueryBuilder')
const Ready = require('./Ready')

const validcmp = ['=', '<', '>', '=<', '>=', '<>', '!=', 'in', 'not in', 'like']

function validateCmp(cmp){
	if (validcmp.includes(cmp)) return cmp
	console.error('invalid sql compare', cmp)
	return validcmp[0]
}

function vType(v, v1 = 'v1', v2 = 'v2'){
	if (v == null) return v1
	if ('string' === typeof v) return v2
	return v1
}

function extractMapKey(index, attr, ret, hash){
	if (isAll(ret)) return []
	return ret.reduce((acc, r) => {
		if (index.includes(r) || attr.includes(r)) return acc
		acc.push(hash.key(r))
		return acc
	}, [])
}

// return 0 - no conditions/all, 1 - host only, 2 - map only, 3 - both, 4 - id
function condType(index, attr, conds, i){
	if (i >= conds.length) return 0

	const c = conds[i++]
	if (Array.isArray(c)){
		const c0 = c[0]
		if (Array.isArray(c0)){
			// sub-condition
			return condType(index, attr, c0, 0) | condType(index, attr, conds, i)
		}
		let type = 2
		if ('id' === c0) return 4 | condType(index, attr, conds, i)
		else if (index.includes(c0) || attr.includes(c0)) return 1 | condType(index, attr, conds, i)
		return type | condType(index, attr, conds, i)
	}

	// c is a string
	return condType(index, attr, conds, i)
}

function getConditionType(index, attr, conds){
	return condType(index, attr, conds, 0)
}

function isAll(ret){
	return !ret || !ret.length || ret.includes('*')
}

function getReturnType(index, attr, ret){
	if (isAll(ret)){
		return 0
	}
	return ret.reduce((acc, r) => {
		if ('id' === r) return acc | 4
		else if (index.includes(r)) return acc | 1
		else if (attr.includes(r)) return acc
		return acc | 2
	}, 0)
}

function makeIdCond(cond, type, params){
	const lhs = cond[0]
	const cmp = validateCmp(cond[1])
	const rhs = cond[2]

	if (!type || type & 1){
		params.push(lhs, rhs)
		return Array.isArray(rhs) ? `h.?? ${cmp} (?)` : `h.?? ${cmp} ?`
	}
	params.push('host_id', rhs)
	return Array.isArray(rhs) ? `m.?? ${cmp} (?)` : `m.?? ${cmp} ?`
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
	let v = vType(rhs)

	if (Array.isArray(rhs)){
		if (!rhs.length) {
			console.error('invalid condition', lhs, cmp, rhs)
			return ''
		}
		rhsVar = '(?)'
		v = vType(rhs[0])
	}

	params.push(hash.key(lhs), rhs)
	return `(m.k = ? and m.${v} ${cmp} ${rhsVar})`
}

function makeFilter(index, attr, type, conds, i, join, hash, params){
	if (i >= conds.length) return ''

	const c = conds[i++]
	if (Array.isArray(c)){
		// if missing join, add in implicit join
		if (Array.isArray(conds[i])) {
			conds.splice(i, 0, join)
		}

		const c0 = c[0]
		let cond
		// sub-condition
		if (Array.isArray(c0)) cond = makeFilter(index, attr, type, c0, 0, 'and', hash, params)
		else if ('id' === c0) cond = makeIdCond(c, type, params)
		else if (index.includes(c0) || attr.includes(c0)) cond = makeHostCond(c, params)
		else cond = makeMapCond(c, hash, params)

		return cond + makeFilter(index, attr, type, conds, i, join, hash, params)
	}

	// c is a string
	return ' ' + c + ' ' + makeFilter(index, attr, type, conds, i, c, hash, params)
}

function makeSource(name, index, attr, type, params){
	if (!type || (type & 1 && type & 2)){
		params.push(name, name + '_map')
		return '?? h left join ?? m on m.host_id = h.id'
	}
	if (type & 1) {
		params.push(name)
		return '?? h'
	}
	if (type & 2) {
		params.push(name + '_map')
		return '?? m'
	}
	return console.error('invalid source type', type)
}

function makeReturn(index, attr, ret, retType, condType){
	let arr = []
	const all = 0 === retType
	if (all){
		arr.push('h.*')
	} else if (1 & retType){
		ret.reduce((acc, r) => {
			if (index.includes(r) || attr.includes(r)) acc.push('h.' + r)
			return acc
		}, arr)
	}
	if (all || 2 & retType) arr.push('m.k', 'm.v1', 'm.v2', 'm.state')
	if (all) return arr
	// must have id in select
	if (1 & retType || 1 & condType) arr.push('h.id')
	else arr.push('m.host_id as id')

	return arr
}

function makeHostInsert(name, index, attr, fields, values, params){
	const valuess = Array.isArray(values[0]) ? values : [values]
	const hostValues = []
	for (let i = 0, l = valuess.length; i < l; i++) hostValues.push([])

	const hostFields = fields.filter((f, i) => {
		if (!index.includes(f) && !attr.includes(f)) return false
		valuess.forEach((v, j) => hostValues[j].push(v[i]))
		return true
	})

	params.push(hostFields, hostValues)
	return `insert into \`${name}\` (??) values ?`
}

function hasMapFields(index, attr, fields){
	return fields.some(f => !index.includes(f) && !attr.includes(f))
}

// insert into xxx (a, b) values (1, 2), (3, 4)
// insert into xxx_map (host_id, k, v1, v2) values (a, 1, null), (a, 3, null), (b, 2, null), (b, 4, null)
function makeMapInsert(name, index, attr, fields, values, hash, params){
	const valuess = Array.isArray(values[0]) ? values : [values]

	const mapValues = fields.reduce((acc, f, i) => {
		if (index.includes(f) || attr.includes(f)) return acc
		const k = hash.key(f)
		valuess.forEach((v, j) => {
			const vi = v[i]
			if (vType(vi, 1, 0)) acc.push(['ID' + j, k, vi, null])
			acc.push(['ID' + j, k, null, vi])
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

function replaceId(key, ids){
	if (Array.isArray(key)) return key.map(k => replaceId(k, ids))
	if (ids[key]) return ids[key]
	return key
}

function exec(pool, hash, sqls, paramss, idx, ids, results, cb){
	if (sqls.length <= idx) return cb(null, results)
	const params = replaceId(paramss[idx], ids)
	pool.query(sqls[idx++], params, (err, result) => {
		if (err) return cb(err, results)
		if (Array.isArray(result)){
			const offset = Object.keys(ids).length
			result.reduce((acc, r, i) => {
				if (r.id) acc['ID' + (offset + i)] = r.id
				return acc
			}, ids)
			results.push(normalise(result, hash))
		} else {
			if (result.insertId) {
				for(let id = result.insertId, i = Object.keys(ids).length, l = id + result.affectedRows; id < l; id++, i++){
					ids['ID'+i] = id
				}	
			}
			results.push(result)
		}
		exec(pool, hash, sqls, paramss, idx, ids, results, cb)
	})
}

function Transposer(pool, hash, name, index, attr = ['state', 'cby', 'uby', 'cat', 'uat']){
	QueryBuilder.call(this)
	this.ready = new Ready
	this.hash = hash
	hash.ready.on(() => this.ready.did() )
	this.pool = pool
	this.name = name
	this.index = index
	this.attr = attr
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
		// select phone, email from user where username = ?;
		// to
		// select h.id,m.k,m.v1,m.v2,m.state from user h left join user_map m on m.host_id = h.id where h.username = ?;
		{
			const retType = getReturnType(this.index, this.attr, this.ret)
			const condType = getConditionType(this.index, this.attr, this.cond)

			sql = 'select '
			sql += makeReturn(this.index, this.attr, this.ret, retType, condType)
			sql += ' from '
			sql += makeSource(this.name, this.index, this.attr, retType | condType, params)
			if (condType){
				sql += ' where '
				sql += makeFilter(this.index, this.attr, retType | condType, this.cond, 0, 'and', this.hash, params)
			}
			const mapRet = extractMapKey(this.index, this.attr, this.ret, this.hash)
			if (mapRet.length){
				params.push(mapRet)
				sql += ' and m.k in (?)'
			}
			sqls.push(sql)
			paramss.push(params)
			return cb(err, sqls, paramss)
		}
		case 'insert':
		// insert into user (username, email, phone) values ?;
		// to
		// insert into user (username) values ?;
		// insert into user_map (host_id, k, v) values ?
			sql = makeHostInsert(this.name, this.index, this.attr, this.fields, this.values, params)
			sqls.push(sql)
			paramss.push(params)
			if (hasMapFields(this.index, this.attr, this.fields)){
				params = []
				sql = makeMapInsert(this.name, this.index, this.attr, this.fields, this.values, this.hash, params)
				sqls.push(sql)
				paramss.push(params)
			}
			return cb(err, sqls, paramss)
		case 'update':
		// update user set email = 'b', phone = 'c' where 'username' = 'a';
		// to
		// select id from user where username = 'a';
		// update user set username = 'a' where id = 12;
		// update user_map set v1 = ? where host_id = 12 and k = 3;
		// update user_map set v1 = ? where host_id = 12 and k = 4;
		{
			const hostSet = {}
			const mapSet = []
			for (let key in this.set){
				if (this.index.includes(key) || this.attr.includes(key)) hostSet[key] = this.set[key]
				else mapSet.push(key)
			}
			const condType = getConditionType(this.index, this.attr, this.cond)
			if (!(condType & 4)){
				params = [this.name]
				sqls.push(`select id from ?? h where ${makeFilter(this.index, this.attr, condType, this.cond, 0, 'and', this.hash, params)}`)
				paramss.push(params)
			}
			if (Object.keys(hostSet).length){
				sqls.push('update ?? set ? where id = ?')
				paramss.push([this.name, hostSet, 'ID0'])
			}
			if (mapSet.length){
				let v
				mapSet.forEach(ms => {
					v = this.set[ms]
					sqls.push(`update ?? set ${vType(v, 1, 0) ? 'v1 = ?' : 'v2 = ?'} where host_id = ? and k = ?`)
					paramss.push([this.name + '_map', v, 'ID0', this.hash.key(ms)])
				})
			}
			return cb(err, sqls, paramss)
		}
		case 'delete':
		// delete from user where 'username' = 'a';
		// to
		// select id from user where username = 'a';
		// delete from user where id = 12;
		// delete from user_map where host_id = 12;
		// fall through
		default:
			return cb('coming soon')
		}
	},
	exec(cb){
		this.toSQL((err, sqls, paramss) => {
			console.log('Transposer sql', sqls)
			console.log('Transposer param', JSON.stringify(paramss))
			if (err) return cb(err)
			
			exec(this.pool, this.hash, sqls, paramss, 0, {}, [], (err, results) => {
				return cb(err, ...results)
			})
		})
	}
})

module.exports = Transposer
