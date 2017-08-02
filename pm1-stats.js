import XLSX from 'xlsx';
import fs from 'fs';
import path from 'path';
import Promise from 'bluebird';
import Debug from 'debug';
import moment from 'moment';

const debug = Debug('pmtransfer:stats');

const datenum = function (v, date1904) {
    if (date1904) {
        v += 1462;
    }
    const epoch = Date.parse(v);
    return (epoch - new Date(Date.UTC(1899, 11, 30))) / (24 * 60 * 60 * 1000);
};

const range = { s: { c:10000000, r:10000000 }, e: { c:0, r:0 } },
    workbook = {
        SheetNames: ['Stats'],
        Sheets: {
            Stats: {}
        }
    };
const stats = {}, dict = {};
const groupPath = path.join('.', 'data', 'groups');

new Promise((resolve, reject) => {
    fs.readdir(groupPath, (err, entries) => {
        if (err) {
            return reject(err);
        }
        const results = [];
        entries.forEach(entry => {
            if (path.extname(path.join(groupPath, entry)) === '.json') {
                results.push(path.join(groupPath, entry));
            }
        });
        resolve(results);
    });
})
.then(groupFiles => {
    return Promise.map(groupFiles, groupFile => {
        return new Promise((resolve, reject) => {
            fs.readFile(groupFile, (err, data) => {
                if (err) {
                    return reject(err);
                }
                const group = JSON.parse(data);
                fs.readdir(path.join(groupPath, path.basename(groupFile, '.json')), (err, eventFiles) => {
                    if (err) {
                        return reject(err);
                    }
                    const groupEventFiles = [];
                    eventFiles.forEach(groupEventFile => {
                        if (path.extname(path.join(groupPath, path.basename(groupFile, '.json'), groupEventFile)) === '.json') {
                            groupEventFiles.push(path.join(groupPath, path.basename(groupFile, '.json'), groupEventFile));
                        }
                    });
                    resolve([group, groupEventFiles]);
                });
            });
        })
        .then(res => {
            const [group, groupEventFiles] = res,
                groupKey = `group-${group.legacy.id}`;
            return Promise.map(groupEventFiles, groupEventFile => {
                return new Promise((resolve, reject) => {
                    fs.readFile(groupEventFile, (err, data) => {
                        if (err) {
                            return reject(err);
                        }
                        const groupEvent = JSON.parse(data),
                        eventDate = moment.unix(groupEvent.utc_timestamp * 100),
                        statsKey = eventDate.format('YYYY-MM-DD'),
                        videoKey = groupEvent.legacy.video_id ? `video-${groupEvent.legacy.video_id}` : null;
                        if (!dict[groupKey]) {
                            dict[groupKey] = group;
                        }
                        if (!stats[groupKey]) {
                            stats[groupKey] = {
                                months: {},
                                cast: []
                            };
                        }
                        if (!stats[groupKey].months[statsKey]) {
                            stats[groupKey].months[statsKey] = {
                                users: [],
                                created: [],
                                modified: [],
                                events: {},
                                videos: {}
                            };
                        }
                        if (groupEvent.legacy.event_type === 'video') {
                            dict[`video-${groupEvent.legacy.id}`] = groupEvent;
                            stats[groupKey].months[statsKey].videos[`video-${groupEvent.legacy.id}`] = { events: {} };
                        }
                        group.legacy.cast.forEach(member => {
                            if (!member) {
                                return;
                            }
                            let name = member.name ? member.name : member;
                            if (stats[groupKey].cast.indexOf(name) === -1) {
                                stats[groupKey].cast.push(name);
                            }
                        });
                        if (Array.isArray(groupEvent.legacy.users)) {
                            groupEvent.legacy.users.forEach(user => {
                                let name = user.name ? user.name : user;
                                if (stats[groupKey].months[statsKey].users.indexOf(name) === -1) {
                                    stats[groupKey].months[statsKey].users.push(name);
                                }
                            });
                        }
                        if (groupEvent.legacy.created_by) {
                            let name = groupEvent.legacy.created_by.name ? groupEvent.legacy.created_by.name : groupEvent.legacy.created_by;
                            if (stats[groupKey].months[statsKey].created.indexOf(name) === -1) {
                                stats[groupKey].months[statsKey].created.push(name);
                            }
                        }
                        if (groupEvent.legacy.modified_by) {
                            let name = groupEvent.legacy.modified_by.name ? groupEvent.legacy.modified_by.name : groupEvent.legacy.modified_by;
                            if (stats[groupKey].months[statsKey].modified.indexOf(name) === -1) {
                                stats[groupKey].months[statsKey].modified.push(name);
                            }
                        }
                        if (videoKey && !stats[groupKey].months[statsKey].videos[videoKey]) {
                            stats[groupKey].months[statsKey].videos[videoKey] = { events: {} };
                        }
                        if (videoKey) {
                            if (!stats[groupKey].months[statsKey].videos[videoKey].events[groupEvent.legacy.event_type]) {
                                stats[groupKey].months[statsKey].videos[videoKey].events[groupEvent.legacy.event_type] = 0;
                            }
                            stats[groupKey].months[statsKey].videos[videoKey].events[groupEvent.legacy.event_type] += 1;
                        } else if (groupEvent.legacy.event_type !== 'video') {
                            if (!stats[groupKey].months[statsKey].events[groupEvent.legacy.event_type]) {
                                stats[groupKey].months[statsKey].events[groupEvent.legacy.event_type] = 0;
                            }
                            stats[groupKey].months[statsKey].events[groupEvent.legacy.event_type] += 1;
                        }
                        resolve();
                    });
                });
            }, {concurrency: 1});
        });
    }, {concurrency: 1});
})
.then(() => {
    const finalStats = {};
    Object.keys(stats).forEach(groupKey => {
        const group = {
            cast: stats[groupKey].cast,
            months: {}
        };
        Object.keys(stats[groupKey].months).forEach(monthKey => {
            const month = {
                videos: {},
                events: stats[groupKey].months[monthKey].events,
                users: stats[groupKey].months[monthKey].users,
                created: stats[groupKey].months[monthKey].created,
                modified: stats[groupKey].months[monthKey].modified
            };
            Object.keys(stats[groupKey].months[monthKey].videos).forEach(videoKey => {
                month.videos[dict[videoKey] ? dict[videoKey].title : videoKey] = stats[groupKey].months[monthKey].videos[videoKey];
            });
            group.months[monthKey] = month;
        });
        finalStats[dict[groupKey].title] = group;
    });
    return new Promise((resolve, reject) => {
        fs.writeFile(path.join('data', 'stats.json'), JSON.stringify(finalStats, null, '\t'), err => {
            if (err) {
                return reject(err);
            }
            resolve();
        });
    });
})
.then(() => {
    process.exit(0);
})
.catch(err => {
    //process.stderr.write(err.message);
    //process.exit(err.code);
    throw err;
});

function buildXLSX() {
    for (let row = 0; row < orders.length; row += 1) {
        let order_obj, values;

        if (row === 0) {
            order_obj = orders[0].toObject();
            orders.unshift(Object.keys(order_obj));
            values = orders[0];
        } else {
            order_obj = orders[row].toObject();
            values = Object.keys(order_obj).map(key => {
                return order_obj[key];
            });
        }

        for (let col = 0; col < values.length; col += 1) {
            range.s.r = Math.min(range.s.r, row);
            range.s.c = Math.min(range.s.c, col);
            range.e.r = Math.max(range.e.r, row);
            range.e.c = Math.max(range.e.c, col);

            let cell = {v: values[col]};
            if (cell.v == null) {
                continue;
            }

            let cell_ref = XLSX.utils.encode_cell({c: col, r: row});

            if (Array.isArray(cell.v)) {
                cell.v = cell.v.length;
            }
            if (cell.v instanceof Object) {
                cell.v = JSON.stringify(cell.v);
            }

            if (typeof cell.v === 'number') {
                cell.t = 'n';
            } else if (typeof cell.v === 'boolean') {
                cell.t = 'b';
            } else if (cell.v instanceof Date) {
                cell.t = 'n';
                cell.z = XLSX.SSF._table[14];
                cell.v = datenum(cell.v);
            } else {
                cell.t = 's';
            }

            workbook.Sheets.Orders[cell_ref] = cell;
        }
    }

    if (range.s.c < 10000000) {
        workbook.Sheets.Orders['!ref'] = XLSX.utils.encode_range(range);
    }

    const outfile = `/var/tmp/${req.params.year}-${req.params.month}-ticket-stats.xlsx`;
    XLSX.writeFile(workbook, outfile);
}