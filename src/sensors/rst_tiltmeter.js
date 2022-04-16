#!/usr/bin/env node
//////////////////////////////////////////
// Shows how to use InfluxDB write API. //
//////////////////////////////////////////

const logger = require('../logger')

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
// logger.debug('*** WRITE POINTS ***')

const measurement_name = 'rst_tiltmeter';


let watchedPaths = [
    // "/home/sammy/ftp/files/node/*.txt",
    // "/home/sammy/ftp/files/combined/*.txt"
    // "/home/kudus/ftp/combined/*.txt"
    // "/home/kudus/ftp/wisen/*/combined/*"

    // 'DTLAngle/DTLAngle_*.txt',


    "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLAngle/DTLAngle_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLSinAngle/DTLSinAngle_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLDeltaAngle/DTLDeltaAngle_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLDeltaSinAngle/DTLDeltaSinAngle_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLDeltaDisplacement/DTLDeltaDisplacement_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLBatt/DTLBatt_*.txt",
    "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLTemperature/DTLTemperature_*.txt",

    // "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLDeltaAngle/DTLDeltaAngle_2022-01-28.txt",

    // "/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/*/*test*.txt",

];

// arr.forEach((element) => {
//     path.join(rootPath, element)
// });

// function to handle 'UnhandledRejection' error
process.on('unhandledRejection', function (err) {
    // console.error(err);
    logger.error(err.message);
});


// Something to use when events are received.
// const log = console.log.bind(console);

let watcher = chokidar.watch(watchedPaths, {
    // ignored: /^\./,
    ignored: /(^|[\/\\])\../,
    ignoreInitial: true,
    alwaysStat: true,
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
        logger.info('Initial scan complete. Ready for changes!');
        // let time = new Date();
        // jobNum++;
        const path = '/home/shms/ftp/2005_tmj_penggaron/rst_tiltmeter/DTLDeltaAngle/DTLDeltaAngle_2022-01-28.txt'
        logger.debug(`Job ${jobNum} - file found, file: ${path}`);
        jobNum++;
        run(jobNum, path);
 
        // watcher.add(path)
        // console.log(watchedPaths)
    })
    .on('add', function (path) {
        let time = new Date();
        jobNum++;
        logger.info(`Job ${jobNum} - file added: ${path}`);
        run(jobNum, path);
    })
    .on('change', function (path) {
        let time = new Date();
        jobNum++;
        logger.info(`Job ${jobNum} - file changed: ${path}`);
        run(jobNum, path);
    })
    // .on('unlink', function (path) {
    // 	let time = new Date();
    // 	// console.log(time.toISOString(), 'File', path, 'has been removed');
    // 	console.log('File', path, 'has been removed');
    // })
    .on('error', function (error) {
        // let time = new Date();
        // console.error(time.toISOString(), 'Error happened', error);
        logger.error('Error happened', error);
    })

// moment.tz.add([
//     'Asia/Jakarta|BMT +0720 +0730 +09 +08 WIB|-77.c -7k -7u -90 -80 -70|01232425|-1Q0Tk luM0 mPzO 8vWu 6kpu 4PXu xhcu|31e6',
//     'Etc/UTC|UTC|0|0|'
// ]);

// let hrstart = process.hrtime();


// Split EOL, source: https://stackoverflow.com/a/52947649
function splitLines(t) { return t.split(/\r\n|\r|\n/); }



async function run(jobNum, _path) {

    let start = new Date();

    let job = jobNum;
    // let path = _path;

    // let dataStr = await removeBOM(path);
    // logger.debug('*** Remove BOM ***')
    let dataStr = stripBom(fs.readFileSync(_path, 'utf8'));
    let dataObj = await processData(job, dataStr);

    // logger.debug('dataObj', dataObj.length)

    const influxPointsArray = await insertData(job, _path, dataObj);
    // logger.debug(temp)
    // influxPointsArray.push(temp)

    if (Array.isArray(influxPointsArray) && influxPointsArray.length > 0) {

        // writeApi.writePoints(influxPointsArray)
        // for (let i = 0; i < influxPointsArray.length; i++) {
        logger.info(`Job ${job} - [${measurement_name}] Uploading ${influxPointsArray.length} data...`);

        // // create a write API, expecting point timestamps in nanoseconds (can be also 's', 'ms', 'us')
        const writeApi = new InfluxDB({ url: URL, token: TOKEN }).getWriteApi(ORG, BUCKET, 'ns')

        writeApi.writePoints(influxPointsArray)

        // WriteApi always buffer data into batches to optimize data transfer to InfluxDB server and retries
        // writing upon server/network failure. writeApi.flush() can be called to flush the buffered data,
        // close() also flushes the remaining buffered data and then cancels pending retries.
        writeApi
            .close()
            .then(() => {
                // logger.debug('FINISHED ... now try ./query.ts')
                let end = new Date() - start;
                logger.info(`Job ${job} - [${measurement_name}] Completed in ${(end / 1000).toFixed(2)} secs`);
            })
            .catch(e => {
                logger.debug(e)
                if (e instanceof HttpError && e.statusCode === 401) {
                    logger.debug('Run ./onboarding.js to setup a new InfluxDB database.')
                }
                logger.error('\nFinished ERROR')
            })
    } else {
        logger.info('Sorry... No data to upload...')
    }
}

async function processData(_job, dataStr) {
    logger.debug(`Job ${_job} - *** Process Data ***`)
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
    logger.debug(`Job ${_job} - *** Insert Data ***`)
    // let job = _job;
    // let path = _path;
    // let influxPointsArray = _influxPointsArray;

    // await checkTable(job, pool, key);

    logger.debug(`Job ${_job} - Inserting ${measurement_name} data to array...`);
    // console.log('Path = ', path);

    let inserts = [];

    let influxPoints = [];

    let badRowsArray = [];

    // console.log('objectvar.length:', objectvar.length);

    // Get table Name & header Field
    let tableName;
    let headerFields;
    for (let i = 0; i < 4; i++) {
        let tempArray = objectvar[i];
        if (i == 0) {
            let temp = tempArray.split(',');
            tableName = temp[temp.length - 1];
            logger.debug(`Job ${_job} - table name: ${tableName}`);
        }
        if (i == 1) {
            headerFields = tempArray.split(',');
            // logger.debug('headerFields', headerFields);
        }

    }

    // start at row 4, to skip TOA5 HEADER
    for (let i = 4; i < objectvar.length; i++) {

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

        let numberOfColumn = headerFields.length;
        let dateStrColumnNumber = 0

        if (inserts.length == numberOfColumn && inserts[dateStrColumnNumber].includes(":") && inserts[dateStrColumnNumber].length == 19) {

            // logger.debug('Timestamp is in DATETIME format');

            // console.log('inserts[0]', inserts[0]);

            // https://stackoverflow.com/a/58351810
            // let dateString = moment(inserts[0]).tz('Asia/Jakarta').format();
            // let dateString = moment(inserts[0]).tz('Etc/UTC').format();
            // let dateString = moment(inserts[0]).tz('Asia/Jakarta').format();
            let dateString = moment(inserts[dateStrColumnNumber]).utcOffset("+07:00", true); // moment([2016, 0, 1, 0, 0, 0]).utcOffset(-5, true) // Equivalent to "2016-01-01T00:00:00-05:00"
            // let dateString = inserts[0].replace(/\s/g, "T") + '+0700';
            // console.log(dateString);
            // let myDate = new Date(dateString);
            // console.log(dateString.format("X"));
            // console.log((dateString.utc()).format("X"));
            let epoch = moment(dateString).format("X");
            inserts.splice(dateStrColumnNumber + 1, 0, Number(epoch));
            // console.log(epoch);
            // console.log('inserts:', inserts);
            // console.log(inserts);


            /**
             * Check for abnormal sensor value e.g 99999, -99999, NAN, NULL, etc.
             */
            let initialIndex = 3;
            // If abnormal (bigger than 99999), then replace to null
            for (let k = initialIndex; k < inserts.length; k++) {
                if (inserts[k] >= 99999 || inserts[k] <= -99999 || inserts[k] === 'NAN' || inserts[k] === 'INF') {
                    // console.log(inserts[4]);
                    inserts[k] = null;
                    // console.log(inserts[4]);
                }
            }

            // logger.debug('inserts', inserts)

            let dataStartIndex = initialIndex;

            for (let k = initialIndex; k < inserts.length; k++) {
                // Only push good data
                if (k >= dataStartIndex && inserts[k] !== null) {
                    //console.log(headerField[k-1], inserts[k]);
                    const point = new Point(measurement_name)
                        .tag('formula', tableName)
                        .tag('alias', headerFields[k - 1])
                        .stringField('dateTimeStr', inserts[0])
                        .floatField('value', inserts[k])
                        .uintField('epoch', epoch * 1000)
                        .timestamp(epoch * 1000000000)


                    influxPoints.push(point);

                    // logger.debug(i+1, objectvar.length)

                    // if ((i != 0 && (i % 1000 === 0)) || i == objectvar.length - 1) {
                    //     logger.debug('HERE')
                    //     logger.debug(influxPoints[influxPoints.length-1])
                    //     return influxPoints

                    //     influxPointsArray.push(influxPoints);
                    //     // console.log(influxPointsArray);
                    //     influxPoints = [];
                    // }
                }
            }

        }
        else {
            logger.info(`Job ${_job} - Timestamp is NOT in DATETIME format`);
            badRowsArray.push(tempArray);
            // badRows++;
        }
    }

    // FINALLY, WE RETURN CLEAN DATA TO UPLOAD TO DATABASE!!

    // logger.debug(influxPoints.length, influxPoints[influxPoints.length-1])
    return influxPoints
}




// WriteApi always buffer data into batches to optimize data transfer to InfluxDB server and retries
// writing upon server/network failure. writeApi.flush() can be called to flush the buffered data,
// close() also flushes the remaining buffered data and then cancels pending retries.
// writeApi
//     .close()
//     .then(() => {
//         logger.debug('FINISHED ... now try ./query.ts')
//     })
//     .catch(e => {
//         logger.debug(e)
//         if (e instanceof HttpError && e.statusCode === 401) {
//             logger.debug('Run ./onboarding.js to setup a new InfluxDB database.')
//         }
//         logger.debug('\nFinished ERROR')
//     })






