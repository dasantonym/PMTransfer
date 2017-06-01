import Debug from 'debug';
import fs from 'fs';
import path from 'path';
import assert from 'assert';
import Promise from 'bluebird';
import Chance from 'chance';
import moment from 'moment';
import crypto from 'crypto';

import * as pgc from './lib/pg-connect';
import { storeUser, findUserByLegacyId, storeGroup } from './lib/util';

const debug = Debug('pmtransfer:import'),
    chance = new Chance();

const apiKeyPrefix = '0310X',
    apiKeyLength = 16;

let _users, _groups;

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
        return new Promise((resolve, reject) => {
            fs.readdir(path.join('.', 'data', 'groups'), (err, entries) => {
                if (err) {
                    return reject(err);
                }
                const dataFiles = [];
                entries.map(entry => {
                    if (path.extname(entry) === '.json') {
                        dataFiles.push(path.basename(entry, '.json'));
                    }
                });
                resolve(dataFiles);
            });
        });
    })
    .then(groups => {
        return Promise.map(groups, groupFile => {
            return new Promise((resolve, reject) => {
                fs.readFile(path.join('.', 'data', 'groups',`${groupFile}.json`), (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(JSON.parse(data));
                });
            })
            .then(group => {
                let author = findUserByLegacyId(_users, group.legacy.account_id);
                if (!author) {
                    author = findUserByLegacyId(_users, 1);
                }
                return pgc.query(
                    'INSERT INTO public.event_groups (title, description, created_at, created_by_user_id) VALUES ($1, $2, $3, $4) RETURNING *',
                    [group.title, group.description, moment(group.created_at), author.id]
                )
                    .then(res => {
                        assert(res.rows.length === 1);
                        group.id = res.rows[0].id;
                        return storeGroup(group);
                    });
            });
        }, {concurrency: 1});
    })
    .then(groups => {
        _groups = groups;
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