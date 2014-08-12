var ST = require("./StorageModule.js")
var fs = require("fs")

fs.truncateSync("../storage.data")
fs.truncateSync("../storage.data.index")

var st = new ST(true);
st.init({config: {}}, function(){

	st.set("test2", {test1: "hej med dig 1", ny: 2}, function(){
		st.get("test2", function(err, data){
			assertEq("InitialWrite2", data.ny, 2)
		})
	});

	st.set("test3", {test1: "hej med dig 2", ny: 3}, function(){
		st.get("test3", function(err, data){
			assertEq("InitialWrite2", data.ny, 3)
			onFirstWrite();
		})
	});
});

function onFirstWrite(){
	st = null;
	var st = new ST(true);
	st.init({config: {}}, function(){
		runTestsAfterReload();
	});
}

function runTestsAfterReload(){
	st.get("test2", function(err, data){
		assertEq("AfterReload1", data.ny, 2)
	})	
	st.get("test3", function(err, data){
		assertEq("AfterReload2", data.ny, 3)
		st.set("test3", {ny: 5}, function(){
			st.get("test3", function(err, data){
				assertEq("Overwrite1", data.ny, 5)
			});
		})
	})

	st.setMultiple({"m1": {val: 1}, "m2": {val: 2}}, function(){
		st.getMultiple(["m1", "m2", "m3"], function(err, res){
			assertEq("MultipleLength", res.length, 3)
			assertEq("MultipleVal1", res[0].val, 1)
			assertEq("MultipleVal2", res[1].val, 2)
			
			var n = 0;
			for(i in res[2])
				n++;
			assertEq("MultipleVal3NotExists", n, 0)
		});
	});

	st.set({t:1}, {val: 1}, function(err){
		assertEq("InvalidKey", typeof err, "string")
	})

	st.set("key", "val", function(err){
		assertEq("InvalidValue", typeof err, "string")
	})
}

function assertEq(caseText, v1, v2){
	if(v1 === v2)
		console.log("[Success] " + caseText)
	else
		console.error("[FAIL] " + caseText, v1, v2)
}