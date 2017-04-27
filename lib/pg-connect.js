import pg from 'pg';
import Debug from 'debug';
import * as config from '../config.json';

const debug = Debug('pg'),
    pool = new pg.Pool(config.pm1.postgres);

pool.on('error', function (err, client) {
    console.error('idle client error', err.message, err.stack);
});

const query = function (text, values) {
    debug(`Query: ${text} - Values: ${Array.isArray(values) ? values.join(', ') : values}`);
    return new Promise((resolve, reject) => {
        pool.query(text, values, (err, result) => {
            if (err) {
                debug(`Error: ${err.message}`);
                return reject(err);
            }
            debug(`Result rows: ${result.rows ? result.rows.length : result.rows}`);
            resolve(result);
        });
    });
};

const connect = function () {
    return new Promise((resolve, reject) => {
        pool.connect(err => {
            if (err) {
                debug(`Error: ${err.message}`);
                return reject(err);
            }
            debug('Pool connected.');
            resolve();
        });
    });
};

export {
    query,
    connect
};
