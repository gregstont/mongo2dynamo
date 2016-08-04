var fs = require('fs-extra');
var uuid = require('node-uuid');
var AWS = require('aws-sdk');

AWS.config.update({
    region: "us-west-2"
});

var docClient = new AWS.DynamoDB.DocumentClient();

var itemsSaved = 0;
var removeNumberLong = true;
var keepExistingIds = false;


function hasJsonExtension(fileName) {
	return fileName.endsWith(".json")
}

var inputFiles = fs.readdirSync("in").filter(hasJsonExtension);
var oidMap = {};

for(fileIndex in inputFiles) {

	var fileName = inputFiles[fileIndex];
	console.log("Reading file " + fileName);

	var lineArray = fs.readFileSync("in/"+fileName).toString().split("\n");

	for(i in lineArray) {
		var line = lineArray[i];
		if(line) {
			console.log("Processing line: " + line);

			if(removeNumberLong) {
				line = line.replace(/\{"\$numberLong":"([0-9]*)"\}/gm,'$1');
				console.log("Removed $numberLong: ", line);
			}

			var regex = /_id\":\{"\$oid":"(.{24})"\}/gm;
			if(keepExistingIds) {
				line = line.replace(regex,'id":"$1"');
				console.log("Removed $oid: ", line);
			}
			else {
		        var uid = uuid.v4();
				var replace = "id\":\""+uid+"\"";

				line = line.replace(regex,replace);
				console.log("Replaced $oid: ", line);
			}

			var json = JSON.parse(line);
			console.log("Original JSON:\n", json);

			//remove empty strings //TODO: make this recursive
			for (var key in json) {
				if (json.hasOwnProperty(key)) {
					if(typeof json[key] == 'string' && json[key]=="") {
						delete json[key];
					}
			  	}
			}

			console.log("Modified JSON:\n", json);

			var tableName = fileName.substring(0,fileName.indexOf('.'));
			var jsonToSend = {
				TableName: tableName,
				Item: json
			};

			console.log("JSON to import:\n",jsonToSend);

			docClient.put(jsonToSend, function(err, data) {
		        if (err) {
		            console.log(err);
		        }else{
		            console.log(++itemsSaved + " items saved",data);
		        }
		    });

			console.log("\n");
		}
	}

	if(fileIndex < inputFiles.length - 1) {
		console.log("////////////////////////////////////////////////////////////////////////////");
		console.log("////////////////////////////////////////////////////////////////////////////\n\n");
	}

	
}


