var fs = require('fs-extra');
var attr = require('dynamodb-data-types').AttributeValue;
var uuid = require('node-uuid');

var inputFiles = fs.readdirSync("in");
var filesWritten = 0;

if(!fs.existsSync("out")) {
	fs.mkdirSync("out");
}

for(fileIndex in inputFiles) {

	var fileName = inputFiles[fileIndex];

	//only process .json files
	if(fileName.endsWith('.json')) {
		console.log("Reading file " + fileName);

		var lineArray = fs.readFileSync("in/"+fileName).toString().split("\n");
		var jsonArray = []

		console.log("Converting to DynamoDB style JSON");
		for(i in lineArray) {
			var line = lineArray[i];
			if(line) {
				console.log("Processing line: " + line);

				line = line.replace(/\{"\$numberLong":"([0-9]*)"\}/gm,'$1');
				console.log("Removed $numberLong: ", line);

				//line = line.replace("N")

				var json = JSON.parse(line);
				json.id = uuid.v4();
				delete json._id;
				console.log("Original JSON:\n", json);

				var dynamoFormat = attr.wrap(json);
				jsonArray[i] = dynamoFormat;
				console.log("DynamoDB format:\n", dynamoFormat);

				console.log("\n");
			}

		}

		var list = [];
		for(i in jsonArray) {
			list.push({PutRequest: {Item :jsonArray[i]}});
		}

		var outputJson = {};
		var tableName = fileName.substring(0,fileName.indexOf('.'));
		outputJson[tableName] = list;

		console.log(JSON.stringify(outputJson,null,4));
		console.log("Writing " + fileName + "...");

		fs.writeFile("out/"+fileName, JSON.stringify(outputJson,null,4), function(err) {
		    if(err) {
		        return console.log(err);
		    }

		    console.log("Write finished");
		    filesWritten++;
		    console.log(filesWritten + " files created.");
		}); 
	}
	
}


