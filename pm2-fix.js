const Promise = require('bluebird');
/*
import fs from 'fs';
import path from 'path';
import Debug from 'debug';
import Chance from 'chance';
import slug from 'slug';
import moment from 'moment';
*/
const pgc = require('./lib/pg-connect');

pgc.connect('pm2heroku')
    .then(() => pgc.query('SELECT t.* FROM public.event_fields t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            if (row.id === 'movie_timestamp') {
                console.log(row.event_id, row.id, parseFloat(row.value));
                return pgc.query(
                    'UPDATE event_fields SET value = $1 WHERE id = $2 AND event_id = $3',
                    [Math.round(parseFloat(row.value) * 100).toString(), 'movie_timestamp', row.event_id]
                )
            }
        }, {concurrency: 1});
    })
    .then(() => pgc.query('SELECT t.* FROM public.events t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            console.log(row.id, row.utc_timestamp);
            return pgc.query(
                'UPDATE events SET utc_timestamp = $1 WHERE id = $2',
                [Math.round(row.utc_timestamp * 100), row.id]
            )
        }, {concurrency: 1});
    });