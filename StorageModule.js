var fs = require("fs");
var jsonpack = require('jsonpack')
var LineByLineReader = require('line-by-line')
var Lock = require('rwlock');


var StorageModule = function (silent) {
	this.storageFilePath = null;
	this.indexFilePath = null;
	this.map = {};
	this.dataPosition = 0;

	this.indexLock = new Lock();
	this.dataLock = new Lock();
	this.silent = silent;
};

StorageModule.prototype.init = function(fw, onFinished) {
    this.fw = fw;

    if(typeof(fw.config.StorageFile) === "string"){
    	this.storageFilePath = fw.config.StorageFile;
    } else {
    	if(this.silent !== true)
    		console.log("[StorageModule] StorageFile not specified in configuration, so using ../storage.data");
    	this.storageFilePath = "../storage.data";
    }

    this.indexFilePath = this.storageFilePath + ".index";

    if(!fs.existsSync(this.storageFilePath))
    	fs.openSync(this.storageFilePath, 'w')
    if(!fs.existsSync(this.indexFilePath))
    	fs.openSync(this.indexFilePath, 'w')

    var t = this;
	fs.stat(t.storageFilePath, function (err, stats) {
		t.dataPosition = stats.size;

		var lr = new LineByLineReader(t.indexFilePath);
		lr.on('line', function (line) {
			var l = line.split(";");
		    t.map[l[0]] = {p: parseInt(l[1]), l: parseInt(l[2])};
		});

		lr.on('end', function () {
			if(t.silent !== true)
				console.log("[StorageModule] Data loaded")
		    onFinished.call(t);
		});
	});
}

StorageModule.prototype.onMessage = function (req, callback) {
	if(typeof(req.body.key) !== "string"){
		callback({error: "Invalid request"});
		return;
	}

	this.get(req.body.key, function(err, val){
		if(val.allowClient == true)
			callback(val);
		else
			callback({error: "Not allowed from client"});
	})
}

StorageModule.prototype.get = function(key, callback){
	if(key in this.map){
		var buffer = new Buffer(parseInt(this.map[key].l));
		var t = this;

		this.dataLock.readLock(function (release) {
			fs.open(t.storageFilePath, 'r', '0666', function(err, fd){
				fs.read(fd, buffer, 0, t.map[key].l, t.map[key].p, function(err, len, data){
					fs.close(fd, function(){release();})

					var s = new Buffer(data.toString('ascii'), 'base64').toString('ascii')
					s = jsonpack.unpack(s);
					//console.log(s);
					//s = JSON.parse(s);
					callback.call(t, null, s);
				})
			})
		});
	}
	else {
		callback.call(this, null, {});
	}
}

StorageModule.prototype.getMultiple = function(keys, callback){
	var res = [];
	var numHandled = 0;

	if(keys.length <= 0){
		callback(null, []);
		return;
	}

	function getNext(){
		if(numHandled >= keys.length){
			callback(null, res);
			return;
		}

		this.get(keys[numHandled], function(err, data){
			numHandled++;
			res.push(data);
			getNext.call(this);
		})
	}
	getNext.call(this);
}

StorageModule.prototype.setMultiple = function(keysAndValues, callback){

	if(typeof keysAndValues !== "object"){
		callback("Need an object");
		return;
	}

	var handled = 0;
	var num = 0;
	for(i in keysAndValues)
		num++;

	for(i in keysAndValues){
		this.set(i, keysAndValues[i], function(err, data){
			handled += 1;

			if(handled >= num)
				callback(null, handled);
		})
	}
}


StorageModule.prototype.set = function(key, value, callback){
	if(typeof callback !== "function")
		callback = function(){};

	if(typeof key !== "string"){
		callback("No key specified", value);
		return;
	}

	key = key.replace(/(\r\n|\n|\r|;)/gm,"");

	if(key.length < 1){
		callback("No key specified", value);
		return;
	}

	if(typeof value !== "object"){
		callback("Invalid value for set", value);
		return;
	}

	var valueToStore = new Buffer(jsonpack.pack(JSON.stringify(value ? value : {}))).toString('base64');
	

	var t = this;
	//console.log(key + " beginning set")
	this.dataLock.writeLock(function (release) {
		//console.log(key + " got write lock")
		fs.appendFile(t.storageFilePath, valueToStore, function(err){
			t.dataPosition += valueToStore.length;
			//console.log(key + " data file appended")

			release();

			t.indexLock.writeLock(function (release) {
				//console.log(key + " got index lock")

				var idx = t.dataPosition - valueToStore.length;
				fs.appendFile(t.indexFilePath, key + ";" + idx + ";" + valueToStore.length + "\n", function(err){
					//console.log(key + " index file appended")

					t.map[key] = {p: idx, l: valueToStore.length};
					//console.log(t.map);
					release();
					callback(null, value);
				});
			})
		});
	});
}

module.exports = StorageModule;