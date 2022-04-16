// import toNanoDate from 'influx'
const fs = require('fs');
const Influx = require('influx');
const toNanoDate = require('influx').toNanoDate;
const stripBom = require('strip-bom');
// var expect = require('chai').expect;



// https://stackoverflow.com/a/48724909
// Also make sure you have installed moment-timezone with
// npm install moment-timezone --save
const moment = require('moment-timezone');

const path = require('path');
var chokidar = require('chokidar');



// Influxdb config
const db_name = 'tmj_penggaron';
var measurement_name = 'wisen_tiltmeter';
const influx = new Influx.InfluxDB({
	host: 'localhost',
	database: db_name,
	// schema: [
		// {
			// measurement: measurement_name,
			// fields: {
				// value: Influx.FieldType.FLOAT,
				// dateTime: Influx.FieldType.STRING
			// },
			// tags: ['formula', 'alias']
		// }
	// ]
});

var watchedPaths = [
	// "/home/sammy/ftp/files/node/*.txt",
	// "/home/sammy/ftp/files/combined/*.txt"
	// "/home/kudus/ftp/combined/*.txt"
	// "/home/kudus/ftp/wisen/*/combined/*"
	// "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/1_Angle/1_Angle*.txt",
	//"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/Angle/Angle_*.txt",
	"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/Angle/Angle_*.txt",
	"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/SinAngle/SinAngle_*.txt",
	"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/DeltaAngle/DeltaAngle_*.txt",
	"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/DeltaSinAngle/DeltaSinAngle_*.txt",
	"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/DeltaDisplacement/DeltaDisplacement_*.txt",
	"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/Batt/Batt_*.txt",
	"/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/Temperature/Temperature_*.txt",
	
];

// function to handle 'UnhandledRejection' error
process.on('unhandledRejection', function (err) {
	// console.error(err);
	console.error(err.message);
});


// Something to use when events are received.
const log = console.log.bind(console);

var watcher = chokidar.watch(watchedPaths, {
	// ignored: /^\./,
	ignored: /(^|[\/\\])\../,
	ignoreInitial: true,
	persistent: true,
	awaitWriteFinish: {
		// stabilityThreshold: 10000, // default: 2000
		pollInterval: 100 // default: 100
	},
	atomic: true
});

var jobNum = 0;

watcher
	.on('ready', function (path) {
		log('Initial scan complete. Ready for changes!');
	})
	.on('add', function (path) {
		var time = new Date();
		jobNum++;
		console.log('Job %d - file added, file: %s', jobNum, path);
		run(jobNum, path);
	})
	.on('change', function (path) {
		var time = new Date();
		jobNum++;
		console.log('Job %d - file changed, file: %s', jobNum, path);
		run(jobNum, path);
	})
	// .on('unlink', function (path) {
	// 	var time = new Date();
	// 	// console.log(time.toISOString(), 'File', path, 'has been removed');
	// 	console.log('File', path, 'has been removed');
	// })
	.on('error', function (error) {
		var time = new Date();
		// console.error(time.toISOString(), 'Error happened', error);
		console.error('Error happened', error);
	})


moment.tz.add([
	'Asia/Jakarta|BMT +0720 +0730 +09 +08 WIB|-77.c -7k -7u -90 -80 -70|01232425|-1Q0Tk luM0 mPzO 8vWu 6kpu 4PXu xhcu|31e6',
	'Etc/UTC|UTC|0|0|'
]);

var hrstart = process.hrtime();

// Split EOL, source: https://stackoverflow.com/a/52947649
function splitLines(t) { return t.split(/\r\n|\r|\n/); }

async function main() {
	for (const product of products) {
		await sell(product);
	}
}


async function run(jobNum, _path) {
	
	var start = new Date();

	var job = jobNum;
	var path = _path;

	var dataStr = await processFile(path);
	var dataObj = await processData(job, dataStr);

	// JavaScript: async/await with forEach(), see:
	// https://codeburst.io/javascript-async-await-with-foreach-b6ba62bbf404
	var array = Object.keys(dataObj);
	// console.log(array);


	for (let index = 0; index < array.length; index++) {

		var element = array[index];
		// console.log('Checking existance of table ', element);

		var influxPointsArray = [];

		// var tableExist = await checkTable(job, pool, element);
		// if (tableExist) {
		if (true) {
			// console.log('Job %d - Inserting %s data...', job, element);
			// console.log('Table exist!', 'number of lines to insert:', dataObj[element].length);
			// console.log(element, 'table exist!');
			// await insertData(pool, element, dataObj);
			
			await insertData(job, path, element, dataObj, influxPointsArray);

			if (influxPointsArray.length) {
				console.log('Job %d - [%s Influx] Inserting %d data', job, element, influxPointsArray.length);
				for (var i = 0; i < influxPointsArray.length; i++) {
					await influx.writePoints(influxPointsArray[i]);
				}
			}
		}
		else
			console.log(element, 'table NOT exist');
	}

	var end = new Date() - start;
	console.log('Job %d - Completed in %s secs', job, (end / 1000).toFixed(2));
	// console.log();

}


// Open a file on the server and return its content:
async function processFile(fileName) {

	// check file & folder
	// console.log('Checking csv file to process:', fileName);
	fs.access(fileName, error => {
		try {
			// The check succeeded
		} catch (error) {
			console.log('Cannot access file', fileName);
		}
	});

	// console.log('Creating folder for saving data:', savePath);
	// fs.mkdirSync(savePath, { recursive: true });

	// console.log('Reading csv file...');
	// dataStr = fs.readFileSync(fileName, 'utf8');
	return stripBom(fs.readFileSync(fileName, 'utf8'));
}

async function processData(_job, dataStr) {

	var job = _job;

	var lines = splitLines(dataStr);

	// console.log('Number of lines to process:', lines.length);

	// console.log(lines);

	// process.exit();

	var myObj = new Object;
	
/* 	//-- for Wisen type csv file only

	lines.forEach(function (element, index) {
		if (lines[index].length) {

			var temp = lines[index].split(',');
			// console.log(temp);

			if (!myObj.hasOwnProperty(temp[1])) {
				myObj[temp[1]] = [];
			}

			myObj[temp[1]].push(lines[index]);
			// console.log(lines[index]);
		}
	});
	
	//-- End of for Wisen type csv file only */
		
	// Campbell TOA5 contains 4 lines of info. we should process start from line 5 (i = 4)
	for (var i = 0, len = lines.length; i < len; i++) {
		if (lines[i].length) {
			
			// remove all double quotes in the lines
			// source: https://stackoverflow.com/a/19156197
			lines[i] = lines[i].replace(/['"]+/g, '');
			// console.log(lines[i]);

			var temp = lines[i].split(',');
			// console.log(temp);

			if (!myObj.hasOwnProperty(measurement_name)) {
				myObj[measurement_name] = [];
			}


			myObj[measurement_name].push(lines[i]);
			// console.log(lines[i]);
		}
	}

	console.log('Job %d - keys found: %d, measurement_name [%s]', job, Object.keys(myObj).length, measurement_name);
	// console.log(myObj);

	// dataObj = myObj;
	return myObj;
}


async function insertData(_job, _path, key, objectvar, _influxPointsArray) {

	var job = _job;
	var path = _path;
	var influxPointsArray = _influxPointsArray;

	// await checkTable(job, pool, key);

	// console.log('Inserting', key, 'data...');

	var inserts = [];

	var influxPoints = [];


	var affectedRows = 0;

	var badRows = 0;
	var badRowsArray = [];
	
	// console.log('objectvar[key].length:', objectvar[key].length);

	for (var i = 0; i < objectvar[key].length; i++) {

		var tempArray = objectvar[key][i];

		inserts = tempArray.split(',');

		// console.log('inserts', inserts);

		// check for empty element in array element, see
		// https://stackoverflow.com/a/5747008
		// then replace with null
		inserts.findIndex(function (currentValue, index) {
			if (currentValue) {
				if (currentValue === 'NULL') {
					inserts[index] = null;
				}
			}
			else {
				inserts[index] = null;
			}
		});

		inserts.forEach(function (element, index) {			
			var num = Number(element);
			if (element != null && !isNaN(num)) {
				inserts[index] = num;
			}
			
			
		});
		
		
		
		// console.log('inserts', inserts);
		
		



		// if (path.includes(measurement_name)) {
		if (true) {
		// if (false) {
			//*********** Check if the datetime format is in String or Epoch format
			// If the format is in string, then add epoch format AFTER it
			// If the format is in epoch, then add string format BEFORE it
			
			// console.log('inserts[0].length', inserts[0].length);
			
			// console.log('inserts', inserts);
			// console.log('inserts.length', inserts.length);
			var numberOfColumn = 4
			var dateTimeColumnNumber = 0

			if (inserts.length == numberOfColumn) {
				if (inserts[dateTimeColumnNumber].length == undefined) { // check if format is a number, not a string
					var day = moment.unix((inserts[dateTimeColumnNumber])).utcOffset(7*60);
					// var day = moment(1318781876406); //milliseconds

					var dateTimeStr = day.format('YYYY-MM-DD HH:mm:ss')
					// console.log(day.format('YYYY-MM-DD HH:mm:ss'));
					inserts.splice(0, 0, dateTimeStr);
					
					// console.log(inserts);
					
					var temp_3 = {
						// measurement: inserts[2],
						measurement: measurement_name,
						tags: {
							formula: inserts[2],
							alias: inserts[3],
						},
						fields: {
							dateTimeStr: inserts[0],
							value: inserts[4],
							epoch: inserts[1],
						},
						timestamp: inserts[1] * 1000000000,
					}
					influxPoints.push(temp_3);
					
					// console.log('objectvar[key].length', objectvar[key].length);


					// if (i == 0) {
						// influxPointsArray.push(influxPoints);
					// }
					if ((i != 0 && (i % 1000 === 0)) || i == objectvar[key].length - 1) {

						influxPointsArray.push(influxPoints);
						influxPoints = [];
					}
				}
				else if (inserts[dateTimeColumnNumber].length) { // if true, then format is string
					if (inserts[dateTimeColumnNumber].includes(":") && inserts[dateTimeColumnNumber].length == 19) {
						// console.log('Timestamp is in DATETIME format');
						// console.log('inserts[0]', inserts[0]);

						// https://stackoverflow.com/a/58351810
						// var dateString = moment(inserts[0]).tz('Asia/Jakarta').format();
						// var dateString = moment(inserts[0]).tz('Etc/UTC').format();
						// var dateString = moment(inserts[0]).tz('Asia/Jakarta').format();
						var dateString = moment(inserts[dateTimeColumnNumber]).utcOffset("+07:00", true); // moment([2016, 0, 1, 0, 0, 0]).utcOffset(-5, true) // Equivalent to "2016-01-01T00:00:00-05:00"
						// var dateString = inserts[0].replace(/\s/g, "T") + '+0700';
						// console.log(dateString);
						// var myDate = new Date(dateString);
						// console.log(dateString.format("X"));
						// console.log((dateString.utc()).format("X"));
						var epoch = moment(dateString).format("X");
						inserts.splice(1, 0, Number(epoch));
						// console.log(epoch);
						// console.log('inserts:', inserts);
						// console.log(inserts);



						//*********** Check for abnormal sensor value
						// If abnormal (bigger than 99999), then replace to null
						if (inserts[4] >= 99999 || inserts[4] <= -99999) {
							// console.log(inserts[4]);
							inserts[4] = null;
							// console.log(inserts[4]);
						}

						// insert format:
						// ['2020-01-01 04:16:30',
						// 	1577826990,
						// 	'VW_SETTLEMENT',
						// 	'TENGAH',
						// 	2.74]
						
						// var measurementType;
						// if (path.includes(measurement_combined_name)) {
							// measurementType = measurement_combined_name;
						// }

						var temp_3 = {
							// measurement: inserts[2],
							measurement: measurement_name,
							tags: {
								formula: inserts[2],
								alias: inserts[3],
							},
							fields: {
								dateTimeStr: inserts[0],
								value: inserts[4],
								epoch: inserts[1] * 1000,
							},
							timestamp: inserts[1] * 1000000000,
						}
						influxPoints.push(temp_3);
						
						// console.log('objectvar[key].length', objectvar[key].length);


						// if (i == 0) {
							// influxPointsArray.push(influxPoints);
						// }
						if ((i != 0 && (i % 1000 === 0)) || i == objectvar[key].length - 1) {

							influxPointsArray.push(influxPoints);
							influxPoints = [];
						}
					}
				}
				else {
					console.log("BAD ROW");
				}
			}
			else {
				badRowsArray.push(tempArray);
				// badRows++;
			}
		}
	}
}


class StringIdGenerator {
	constructor(chars = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ') {
		this._chars = chars;
		this._nextId = [0];
	}

	next() {
		const r = [];
		for (const char of this._nextId) {
			r.unshift(this._chars[char]);
		}
		this._increment();
		return r.join('');
	}

	_increment() {
		for (let i = 0; i < this._nextId.length; i++) {
			const val = ++this._nextId[i];
			if (val >= this._chars.length) {
				this._nextId[i] = 0;
			} else {
				return;
			}
		}
		this._nextId.push(0);
	}

	*[Symbol.iterator]() {
		while (true) {
			yield this.next();
		}
	}
}
