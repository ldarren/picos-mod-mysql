const
pico=require('pico-common/pico-cli'),
ensure= pico.export('pico/test').ensure,
mysql=require('./index')

let client

ensure('ensure mysql loaded', function(cb){
	cb(null, !!mysql)
})
ensure('ensure mysql create', function(cb){
	mysql.create({path:'',env:'pro'},{},(err, cli)=>{
		if (err) return cb(err)
		client=cli
		cb(null, !!client)
	})
})
