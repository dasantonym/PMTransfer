import Debug from 'debug';
import fs from 'fs';
import path from 'path';
import Promise from 'bluebird';
import request from 'superagent';

const debug = Debug('pmtransfer:import'),
    apiHost = process.env.API_HOST,
    apiKey = process.env.API_KEY;

new Promise((resolve, reject) => {
    fs.readdir(path.join('.', 'data', 'users'), (err, entries) => {
        if (err) {
            return reject(err);
        }
        resolve(entries);
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
        return new Promise((resolve, reject) => {
            let payload = Object.assign({}, user);
            delete payload.legacy;
            request.post(`${apiHost}/api/v1/user.json`)
                .send(payload)
                .set('X-Access-Key', apiKey)
                .set('Accept', 'application/json')
                .end((err, res) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(res);
                });
        });
    }, {concurrency: 1});
})
.then(() => {
    process.exit(0);
})
.catch(err => {
    process.stderr.write(`${err.message}\n`);
    process.exit(err.code);
});