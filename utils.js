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
	str += c[0] + ' ' + c[1] + ' '
	params.push(c[2])
	if (Array.isArray(c[2])){
		str += '(?)'
	} else {
		str += '?'
	}

	return str + extractConditions(index, conds, params, joint)
}

module.exports = {
	extractConditions,
	decode(obj,hash,ENUM){
		const keys=Object.keys(obj)
		for(let i=0,k; (k=keys[i]); i++) {
			if (-1===ENUM.indexOf(k)) continue
			obj[k]=hash.key(obj[k])
		}
		return obj
	},

	decodes(rows,hash,ENUM){
		for(let i=0,r; (r=rows[i]); i++){
			this.decode(r,hash,ENUM)
		}
		return rows
	},

	encode(obj,by,hash,INDEX,ENUM){
		const arr=[]
		for(let i=0,k; (k=INDEX[i]); i++){
			if (-1===ENUM.indexOf(k)) arr.push(obj[k])
			else arr.push(hash.val(obj[k]))
		}
		arr.push(by)
		return arr
	},

	mapDecode(rows=[], output={}, hash, ENUM, key='id'){
		for(let i=0,r,k; (r=rows[i]); i++) {
			if (r[key] !== output.id) continue
			k=hash.key(r.k)
			if (-1===ENUM.indexOf(k)) output[k]=r.v1 || r.v2
			else output[k]=hash.key(r.v2)
			r.k=r.v1=r.v2=undefined
		}
		return output
	},

	mapDecodes(rows=[], outputs=[], hash, ENUM, key='id'){
		for(let i=0,o; (o=outputs[i]); i++){
			this.mapDecode(rows, o, hash, ENUM, key)
		}
		return outputs
	},

	mapEncode(obj, by, hash, INDEX, ENUM){
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
	},

	map2Encode(obj1, obj2, map, by, hash, INDEX, ENUM){
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
	},

	listDecode(rows,key,hash,ENUM){
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
	},

	listEncode(id, key, list, by, hash, INDEX, ENUM){
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
}
