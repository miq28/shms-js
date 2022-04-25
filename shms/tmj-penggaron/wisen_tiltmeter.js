// const logger = require('../../logger')
// require('dotenv').config()
// https://stackoverflow.com/a/48724909
// Also make sure you have installed moment-timezone with
// npm install moment-timezone --save
const moment = require('moment-timezone');
const fs = require('fs');
// const Influx = require('influx');
// const toNanoDate = require('@influxdata/influxdb-client').toNanoDate;
const stripBom = require('strip-bom');
const path = require('path');
let chokidar = require('chokidar');


const { InfluxDB, Point, HttpError } = require('@influxdata/influxdb-client')

const URL = process.env.URL
const TOKEN = process.env.TOKEN
const ORG = process.env.ORG
const BUCKET = process.env.BUCKET


const measurement_name = 'wisen_tiltmeter';

const Logger = require('js-logger')

var logger2 = Logger.get(measurement_name);



let watchedPaths = [

    "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/Angle/Angle_*.txt",
    // "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/SinAngle/SinAngle_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/DeltaAngle/DeltaAngle_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/DeltaSinAngle/DeltaSinAngle_*.txt",
    // "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/DeltaDisplacement/DeltaDisplacement_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/Batt/Batt_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/Temperature/Temperature_*.txt",

];

// function to handle 'UnhandledRejection' error
process.on('unhandledRejection', function (err) {
    // console.error(err);
    console.error(err.message);
});


// Something to use when events are received.
// const log = Logger.info.bind(console);

let watcher = chokidar.watch(watchedPaths, {
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

let jobNum = 0;

watcher
    .on('ready', function () {
        logger2.info('Initial scan complete. Ready for changes!');
        // const path = '/home/shms/ftp/2005_tmj_penggaron/wisen_tiltmeter/DeltaAngle/DeltaAngle_2022-01-28.txt'
        // logger2.debug(`Job ${jobNum} - file found, file: ${path}`);
        // jobNum++;
        // run(jobNum, path);
    })
    .on('add', function (path) {
        let time = new Date();
        jobNum++;
        console.log('Job %d - file added, file: %s', jobNum, path);
        run(jobNum, path);
    })
    .on('change', function (path) {
        let time = new Date();
        jobNum++;
        console.log('Job %d - file changed, file: %s', jobNum, path);
        run(jobNum, path);
    })
    // .on('unlink', function (path) {
    // 	let time = new Date();
    // 	// console.log(time.toISOString(), 'File', path, 'has been removed');
    // 	console.log('File', path, 'has been removed');
    // })
    .on('error', function (error) {
        let time = new Date();
        // console.error(time.toISOString(), 'Error happened', error);
        console.error('Error happened', error);
    })


moment.tz.add([
    'Asia/Jakarta|BMT +0720 +0730 +09 +08 WIB|-77.c -7k -7u -90 -80 -70|01232425|-1Q0Tk luM0 mPzO 8vWu 6kpu 4PXu xhcu|31e6',
    'Etc/UTC|UTC|0|0|'
]);

let hrstart = process.hrtime();

// Split EOL, source: https://stackoverflow.com/a/52947649
function splitLines(t) { return t.split(/\r\n|\r|\n/); }

async function main() {
    for (const product of products) {
        await sell(product);
    }
}


async function run(jobNum, _path) {

    let start = new Date();

    let job = jobNum;
    let path = _path;

    let dataStr = stripBom(fs.readFileSync(_path, 'utf8'));
    let dataObj = await processData(job, dataStr);

    // JavaScript: async/await with forEach(), see:
    // https://codeburst.io/javascript-async-await-with-foreach-b6ba62bbf404
    // let array = Object.keys(dataObj);
    // // console.log(dataObj);


    // let element = array[index];
    // console.log('Checking existance of table ', element);

    // let influxPointsArray = [];

    // let tableExist = await checkTable(job, pool, element);
    // if (tableExist) {
    if (true) {
        // console.log('Job %d - Inserting %s data...', job, element);
        // console.log('Table exist!', 'number of lines to insert:', dataObj[element].length);
        // console.log(element, 'table exist!');
        // await insertData(pool, element, dataObj);

        let influxPointsArray = await insertData(job, path, dataObj);

        if (influxPointsArray.length) {
            const writeApi = new InfluxDB({ url: URL, token: TOKEN }).getWriteApi(ORG, BUCKET, 'ns')
            // logger2.debug(influxPointsArray[i])
            logger2.info(`Job ${job} - Uploading ${influxPointsArray.length} data...`);
            writeApi.writePoints(influxPointsArray)

            // WriteApi always buffer data into batches to optimize data transfer to InfluxDB server and retries
            // writing upon server/network failure. writeApi.flush() can be called to flush the buffered data,
            // close() also flushes the remaining buffered data and then cancels pending retries.
            writeApi
                .close()
                .then(() => {
                    // logger2.debug('FINISHED ... now try ./query.ts')
                    let end = new Date() - start;
                    logger2.info(`Job ${job} - Completed in ${(end / 1000).toFixed(2)} secs`);
                })
                .catch(e => {
                    logger2.debug(e)
                    if (e instanceof HttpError && e.statusCode === 401) {
                        logger2.debug('Run ./onboarding.js to setup a new InfluxDB database.')
                    }
                    logger2.error('\nFinished ERROR')
                })
        } else {
            logger2.info(`Job ${job} - - No data to upload...`)
        }
    }
    else
        console.log(element, 'table NOT exist');

    // let end = new Date() - start;
    // console.log('Job %d - Completed in %s secs', job, (end / 1000).toFixed(2));
    // console.log();

}

async function processData(_job, dataStr) {
    logger2.debug(`Job ${_job} - Process Data`)
    // let job = _job;

    let lines = splitLines(dataStr);

    let myObj = []

    // TOA5 contains 4 lines of info. we should process start from line 5 (i = 4)
    // let columnNameObj = new Object;
    //let dataTable;
    // let tableHeaderArray = [];
    for (let i = 0, len = lines.length; i < len; i++) {
        if (lines[i].length) {

            // remove all double quotes in the lines
            // source: https://stackoverflow.com/a/19156197
            lines[i] = lines[i].replace(/['"]+/g, '');
            // console.log(lines[i]);

            myObj.push(lines[i]);
        }
    }

    return myObj;
}


async function insertData(_job, _path, objectvar) {
    // logger2.debug('insertData')
    let job = _job;
    let path = _path;
    // let influxPointsArray = _influxPointsArray;

    // await checkTable(job, pool, key);

    // console.log('Inserting', key, 'data...');

    let inserts = [];

    let influxPoints = [];


    let affectedRows = 0;

    let badRows = 0;
    let badRowsArray = [];

    // console.log('objectvar[key].length:', objectvar[key].length);

    for (let i = 0; i < objectvar.length; i++) {
        // logger2.debug('UHUYYYY')

        let tempArray = objectvar[i];

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
            let num = Number(element);
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
            let numberOfColumn = 4
            let dateTimeColumnNumber = 0

            if (inserts.length == numberOfColumn) {
                // logger2.debug('DORRRR')
                if (inserts[dateTimeColumnNumber].length == undefined) { // check if format is a number, not a string
                    // logger2.debug('DERRRR')
                    let day = moment.unix((inserts[dateTimeColumnNumber])).utcOffset(7 * 60);
                    // let day = moment(1318781876406); //milliseconds

                    let dateTimeStr = day.format('YYYY-MM-DD HH:mm:ss')
                    // console.log(day.format('YYYY-MM-DD HH:mm:ss'));
                    inserts.splice(0, 0, dateTimeStr);

                    // console.log(inserts);

                    let temp_3 = {
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
                    // influxPoints.push(temp_3);


                    const point = new Point(measurement_name)
                        .tag('formula', inserts[2])
                        .tag('alias', inserts[3])
                        .stringField('dateTimeStr', inserts[0])
                        .floatField('value', inserts[4])
                        // .uintField('epoch', inserts[1])
                        .floatField('epoch', inserts[1])
                        .timestamp(inserts[1] * 1000000000)


                    // logger2.debug('DORRRR')


                    influxPoints.push(point);

                    // console.log('objectvar[key].length', objectvar[key].length);


                    // if (i == 0) {
                    // influxPointsArray.push(influxPoints);
                    // }
                    // if ((i != 0 && (i % 1000 === 0)) || i == objectvar[key].length - 1) {

                    //     influxPointsArray.push(influxPoints);
                    //     influxPoints = [];
                    // }
                }
                else if (inserts[dateTimeColumnNumber].length) { // if true, then format is string
                    // logger2.debug('DORRRR 22')
                    if (inserts[dateTimeColumnNumber].includes(":") && inserts[dateTimeColumnNumber].length == 19) {
                        // console.log('Timestamp is in DATETIME format');
                        // console.log('inserts[0]', inserts[0]);

                        // https://stackoverflow.com/a/58351810
                        // let dateString = moment(inserts[0]).tz('Asia/Jakarta').format();
                        // let dateString = moment(inserts[0]).tz('Etc/UTC').format();
                        // let dateString = moment(inserts[0]).tz('Asia/Jakarta').format();
                        let dateString = moment(inserts[dateTimeColumnNumber]).utcOffset("+07:00", true); // moment([2016, 0, 1, 0, 0, 0]).utcOffset(-5, true) // Equivalent to "2016-01-01T00:00:00-05:00"
                        // let dateString = inserts[0].replace(/\s/g, "T") + '+0700';
                        // console.log(dateString);
                        // let myDate = new Date(dateString);
                        // console.log(dateString.format("X"));
                        // console.log((dateString.utc()).format("X"));
                        let epoch = moment(dateString).format("X");
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

                        // let measurementType;
                        // if (path.includes(measurement_combined_name)) {
                        // measurementType = measurement_combined_name;
                        // }

                        let temp_3 = {
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
                        // influxPoints.push(temp_3);

                        const point = new Point(measurement_name)
                            .tag('formula', inserts[2])
                            .tag('alias', inserts[3])
                            .stringField('dateTimeStr', inserts[0])
                            .floatField('value', inserts[4])
                            .uintField('epoch', inserts[1] * 1000)
                            // .floatField('epoch', inserts[1] * 1000)
                            .timestamp(inserts[1] * 1000000000)


                        // logger2.debug('DORRRR')

                        influxPoints.push(point);

                        // if ((i != 0 && (i % 1000 === 0)) || i == objectvar.length - 1) {

                        //     influxPointsArray.push(influxPoints);
                        //     influxPoints = [];
                        // }
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

    return influxPoints
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
