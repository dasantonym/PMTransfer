import Promise from 'bluebird';
import fs from 'fs';
import path from 'path';
import Debug from 'debug';
import Chance from 'chance';
import slug from 'slug';
import moment from 'moment';

import * as pgc from './lib/pg-connect';
import { storeUser } from './lib/util';

const debug = Debug('pmtransfer:dump'),
    chance = new Chance();

let _users = [], _groups = [],
    _tags = [], _castings = [],
    _eventsTags = [], _eventsUsers = [];

const roleIdMap = {
    'ballet_master': 'annotator',
    'group_user': 'annotator',
    'user': 'annotator',
    'director': 'annotator',
    'group_admin': 'annotator',
    'manager': 'annotator'
};


//
//
// Utility functions

function getTagsForPieceId(pieceId) {
    const tags = [];
    _tags.forEach(tag => {
        if (pieceId === tag.legacy.piece_id) {
            tags.push(tag);
        }
    });
    return tags;
}

function getUsersForPieceId(pieceId) {
    const users = [];
    _castings.forEach(casting => {
        if (pieceId === casting.piece_id) {
            const user = getUserById(casting.user_id);
            users.push(user);
        }
    });
    return users;
}

function getUsersForEventId(eventId) {
    const users = [];
    _eventsUsers.forEach(eventUser => {
        if (eventId === eventUser.event_id) {
            const user = getUserById(eventUser.user_id);
            users.push(user);
        }
    });
    return users;
}

function getTagsForEventId(eventId) {
    const tags = [];
    _eventsTags.forEach(eventTag => {
        if (eventId === eventTag.tag_id) {
            const tag = getTagById(eventTag.tag_id);
            tags.push(tag);
        }
    });
    return tags;
}

function getTagById(tagId) {
    let res = undefined;
    _tags.forEach(tag => {
        if (!res && tagId === tag.legacy.id) {
            res = tag;
        }
    });
    if (!res) {
        process.stderr.write(`WARNING: No tag found for id ${tagId}\n`);
    }
    return res;
}

function getGroupByPieceId(pieceId) {
    let res = undefined;
    _groups.forEach(group => {
        if (!res && pieceId === group.legacy.id) {
            res = group;
        }
    });
    if (!res) {
        process.stderr.write(`WARNING: No group found for piece_id ${pieceId}\n`);
    }
    return res;
}


function getUserByName(userName) {
    let res = undefined;
    _users.forEach(user => {
        if (!res && userName === user.name) {
            res = Object.assign({}, user);
            if (res.password) {
                delete res.password;
            }
        }
    });
    if (!res) {
        process.stderr.write(`WARNING: No user found for name ${userName}\n`);
    }
    return res;
}

function getUserById(userId) {
    let res = undefined;
    _users.forEach(user => {
        if (!res && userId === user.legacy.id) {
            res = Object.assign({}, user);
            if (res.password) {
                delete res.password;
            }
        }
    });
    if (!res) {
        process.stderr.write(`WARNING: No user found for id ${userId}\n`);
    }
    return res;
}


//
//
// The beef starts here

pgc.connect('pm1')
    .then(() => pgc.query('SELECT t.* FROM public.tags t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            const tag = {
                name: row.name,

                legacy: {
                    id: row.id,
                    piece_id: row.piece_id,
                    tag_type: row.tag_type,
                    account_id: row.account_id
                }
            };
            _tags.push(tag);
        }, {concurrency: 8});
    })
    .then(() => pgc.query('SELECT t.* FROM public.events_tags t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            const eventTag = {
                event_id: row.event_id,
                tag_id: row.tag_id
            };
            _eventsTags.push(eventTag);
        }, {concurrency: 8});
    })
    .then(() => pgc.query('SELECT t.* FROM public.events_users t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            const eventUser = {
                event_id: row.event_id,
                user_id: row.user_id
            };
            _eventsUsers.push(eventUser);
        }, {concurrency: 8});
    })
    .then(() => pgc.query('SELECT t.* FROM public.castings t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            const casting = {
                piece_id: row.piece_id,
                user_id: row.user_id,

                updated_at: row.updated_at ? moment(row.updated_at) : undefined,

                legacy: {
                    id: row.id,
                    is_original: row.is_original,
                    cast_number: row.cast_number
                }
            };
            _castings.push(casting);
        }, {concurrency: 8});
    })
    .then(() => pgc.query('SELECT t.* FROM public.users t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            const user = {
                email: `${row.login}.${row.id}@tfc.motionbank.org`,
                password: new Array(3).fill(null).map(() => {
                    let syllable = chance.syllable();
                    return `${syllable.substring(0, 1).toUpperCase()}${syllable.substring(1)}`;
                }).join('') + chance.integer({min: 10, max: 99}),
                name: row.login,
                user_role_id: roleIdMap[row.role_name] || row.role_name,

                legacy: {
                    id: row.id,
                    scratchpad: row.scratchpad,
                    last_login: row.last_login ? moment(row.last_login) : undefined,
                    is_performer: row.is_performer,
                    role_name: row.role_name,

                    created_at: row.created_at ? moment(row.created_at) : undefined,
                    updated_at: row.updated_at ? moment(row.updated_at) : undefined
                }
            };
            _users.push(user);
            storeUser(user);
        }, {concurrency: 8});
    })
    .then(() => pgc.query('SELECT t.* FROM public.pieces t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            const group = {
                title: row.title,
                description: `PieceMaker 1 Data / Short title: ${row.short_name}`,

                created_at: row.created_at ? moment(row.created_at) : undefined,
                updated_at: row.updated_at ? moment(row.updated_at) : undefined,

                legacy: {
                    id: row.id,
                    account_id: row.account_id,
                    short_name: row.short_name,
                    is_active: row.is_active,
                    tags: getTagsForPieceId(row.id),
                    cast: getUsersForPieceId(row.id)
                }
            };
            _groups.push(group);
            return new Promise((resolve, reject) => {
                const titleSlug = slug(row.title, {replacement: '_', lower: true}),
                    groupFile = path.join('.', 'data', 'groups', `group-${row.id}-${titleSlug}.json`);
                fs.writeFile(groupFile, JSON.stringify(group, null, '\t'), err => {
                    if (err) {
                        return reject(err);
                    }
                    debug(`Stored group-${row.id}-${titleSlug}.json`);
                    const groupFolder = path.join('.', 'data', 'groups', `group-${row.id}-${titleSlug}`);
                    fs.exists(groupFolder, exists => {
                        if (exists) {
                            return resolve();
                        }
                        fs.mkdir(groupFolder, err => {
                            if (err) {
                                return reject(err);
                            }
                            debug(`Created events folder group-${row.id}-${titleSlug}`);
                            resolve();
                        });
                    });
                });
            });
        }, {concurrency: 8});
    })
    .then(() => pgc.query('SELECT t.* FROM public.events t'))
    .then(res => {
        return Promise.map(res.rows, row => {
            const event = {
                title: row.title,
                description: row.description,

                event_group_id: null,
                created_by_user_id: null,

                utc_timestamp: row.happened_at ? moment(row.happened_at).unix() * 10e-3 : undefined,
                duration: row.dur ? row.dur : undefined,

                created_at: row.created_at ? moment(row.created_at) : undefined,
                updated_at: row.updated_at ? moment(row.updated_at) : undefined,

                legacy: {
                    id: row.id,
                    piece_id: row.piece_id,
                    video_id: row.video_id,
                    parent_id: row.parent_id,
                    account_id: row.account_id,

                    created_by: row.created_by ? getUserByName(row.created_by) || row.created_by : undefined,
                    modified_by: row.modified_by ? getUserByName(row.modified_by) || row.modified_by : undefined,
                    users: getUsersForEventId(row.id),

                    event_type: row.event_type,
                    location: row.location,
                    rating: row.rating,
                    tags: getTagsForEventId(row.id),

                    state: row.state,
                    locked: row.locked
                }
            };
            return new Promise((resolve, reject) => {
                let eventPath,
                    groupTitleSlug = slug(
                        row.piece_id ? getGroupByPieceId(row.piece_id).title : '',
                        {replacement: '_', lower: true}
                    );

                if (row.piece_id) {
                    eventPath = path.join('.', 'data', 'groups', `group-${row.piece_id}-${groupTitleSlug}`, `event-${row.id}.json`);
                } else {
                    eventPath = path.join('.', 'data', 'orphans', `event-${row.id}.json`);
                    process.stderr.write(`WARNING: Found orphaned event with id ${row.id}\n`);
                }

                fs.writeFile(eventPath, JSON.stringify(event, null, '\t'), err => {
                    if (err) {
                        return reject(err);
                    }
                    debug(`Stored event-${row.id}.json`);
                    resolve();
                });
            });
        }, {concurrency: 8});
    })
    .then(() => {
        process.exit(0);
    })
    .catch(err => {
        process.stderr.write(`${err.message}\n`);
        process.exit(err.code);
    });