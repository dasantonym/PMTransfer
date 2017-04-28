import Debug from 'debug';
import fs from 'fs';
import path from 'path';
import assert from 'assert';
import Promise from 'bluebird';
import Chance from 'chance';
import moment from 'moment';
import crypto from 'crypto';

import * as pgc from './lib/pg-connect';
import { storeUser } from './lib/util';

const debug = Debug('pmtransfer:import'),
    chance = new Chance();

const apiKeyPrefix = '0310X',
    apiKeyLength = 16;

let _users;

pgc.connect('pm2')
    .then(() => {
        return new Promise((resolve, reject) => {
            fs.readdir(path.join('.', 'data', 'users'), (err, entries) => {
                if (err) {
                    return reject(err);
                }
                resolve(entries);
            });
        });
    })
    .then(entries => {
        const users = [];
        return Promise.map(entries, entry => {
            if (path.extname(entry) !== '.json') {
                return;
            }
            return new Promise((resolve, reject) => {
                fs.readFile(path.join('.', 'data', 'users', entry), (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    users.push(JSON.parse(data));
                    resolve();
                });
            });
        }, {concurrency: 8})
            .then(() => {
                return users;
            });
    })
    .then(users => {
        return Promise.map(users, user => {
            const password = crypto.createHash('sha1').update(user.password).digest('hex'),
                apiKey = `${apiKeyPrefix}${chance.string({
                    length: apiKeyLength - apiKeyPrefix.length,
                    pool: 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
                })}`;
            return pgc.query(
                    'INSERT INTO public.users (email, name, password, api_access_key, user_role_id) VALUES ($1, $2, $3, $4, $5) RETURNING *',
                    [user.email, user.name, password, apiKey, user.user_role_id]
                )
                .then(res => {
                    assert(res.rows.length === 1);
                    user.id = res.rows[0].id;
                    return storeUser(user);
                });
        }, {concurrency: 1});
    })
    .then(users => {
        _users = users;
    })
    .then(() => {
        process.exit(0);
    });
    /*
    .catch(err => {
        process.stderr.write(`${err.message}\n`);
        process.exit(err.code);
    });
    */