import pg from 'pg';
import Debug from 'debug';
import assert from 'assert';
import * as config from '../config.json';

const debug = Debug('pg'),
    pool = {};

const query = function (text, values, db = undefined) {
    assert(Object.keys(pool).length > 0, 'Not connected to any DB. Boo!');

    if (!db) {
        db = Object.keys(pool)[0];
    }

    debug(`Query ${db}: ${text} - Values: ${Array.isArray(values) ? values.join(', ') : values}`);

    return new Promise((resolve, reject) => {
        pool[db].query(text, values, (err, result) => {
            if (err) {
                debug(`Error: ${err.message}`);
                return reject(err);
            }
            debug(`Result rows: ${result.rows ? result.rows.length : result.rows}`);
            resolve(result);
        });
    });
};

const connect = function (db) {
    if (!pool[db]) {
        pool[db] = new pg.Pool(config[db].postgres);

        pool[db].on('error', function (err, client) {
            console.error('idle client error', err.message, err.stack);
        });
    }

    return new Promise((resolve, reject) => {
        pool[db].connect(err => {
            if (err) {
                debug(`Error: ${err.message}`);
                return reject(err);
            }
            debug(`Pool connected to ${db}.`);
            resolve();
        });
    });
};

export {
    query,
    connect
};
