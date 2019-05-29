function Ready(count = 1){
	this.cbs = []
	this.count = count
}

Ready.prototype = {
	on(cb){
		if (this.done()) return cb()
		this.cbs.push(cb)
	},
	did(count = 1){
		this.count -= count
		if (!this.done()) return
		this.cbs.forEach(cb => cb())
		this.cbs.length = 0
	},
	done(){
		return this.count <= 0
	}
}

module.exports = Ready
