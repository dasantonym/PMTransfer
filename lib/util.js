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
            resolve(user);
        });
    });
};

const findUserByLegacyId = function (users, legacyId) {
    let user = null;

    users.forEach(_user => {
        if (_user.legacy.id === legacyId) {
            user = _user;
        }
    });

    return user;
};

const storeGroup = function (group) {
    return new Promise((resolve, reject) => {
        const nameSlug = slug(group.title, {replacement: '_', lower: true}),
            groupFile = path.join('.', 'data', 'groups', `group-${group.legacy.id}-${nameSlug}.json`);
        fs.writeFile(groupFile, JSON.stringify(group, null, '\t'), err => {
            if (err) {
                return reject(err);
            }
            debug(`Stored group-${group.legacy.id}-${nameSlug}.json`);
            resolve(group);
        });
    });
};

export {
    storeUser,
    findUserByLegacyId,
    storeGroup
};