import fs from 'fs';
import path from 'path';
import slug from 'slug';
import Debug from 'debug';

const debug = Debug('pmtransfer:io');

const storeUser = function (user) {
    return new Promise((resolve, reject) => {
        const nameSlug = slug(user.name, {replacement: '_', lower: true}),
            userFile = path.join('.', 'data', 'users', `user-${user.legacy.id}-${nameSlug}.json`);
        fs.writeFile(userFile, JSON.stringify(user, null, '\t'), err => {
            if (err) {
                return reject(err);
            }
            debug(`Stored user-${user.legacy.id}-${nameSlug}.json`);
            resolve();
        });
    });
};

export {
    storeUser
};