function Ready(){
	this.cbs = []
	this.done = false
}

Ready.prototype = {
	on(cb){
		if (this.done) return cb()
		this.cbs.push(cb)
	},
	did(done = true){
		this.done = done
		this.cbs.forEach(cb => cb())
		this.cbs.length = 0
	}
}

module.exports = Ready
