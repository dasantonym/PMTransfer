import * as pgc from '../lib/pg-connect';
import Debug from 'debug';
import path from 'path';
import fs from 'fs';
import Promise from 'bluebird';
import Levenshtein from 'levenshtein';
import ffmpeg from 'fluent-ffmpeg';

const debug = Debug('pmtransfer'),
    events = {}, videos = {compressed: {dir: [], files: [], dists: {}}, full: {dir: [], files: [], dists: {}}},
    videos_nofile = [], videos_notitle = [];

const videoBasePath = process.env.VIDEO_BASE_PATH;

Object.keys(videos).forEach(key => {
    videos[key].dir = fs.readdirSync(path.join(videoBasePath, key));
});

pgc.connect()
.then(() => pgc.query('SELECT t.* FROM public.events t'))
.then(res => Promise.map(res.rows, row => {
    if (row.event_type !== 'video' && !Array.isArray(events[row.event_type])) {
        events[row.event_type] = [];
    }
    if (row.event_type === 'video') {
        if (row.title) {
            let found = false;
            Object.keys(videos).forEach(key => {
                if (fs.existsSync(path.join(videoBasePath, key, row.title))) {
                    found = true;
                    videos[key].files.push(row);
                }
            });
            if (!found) {
                debug(`Nothing found for ${row.title}, creating distance array`);
                videos_nofile.push(row);
                return Promise.map(Object.keys(videos), key => {
                    let results = [];
                    return Promise.map(videos[key].dir, entry => {
                        let dist = new Levenshtein(row.title, entry).distance, lvObj,
                            filepath = path.join(videoBasePath, key, entry);
                        if (dist <= 10) {
                            lvObj = {
                                entry: entry,
                                key: key,
                                distance: dist
                            };
                            return new Promise(resolve => {
                                ffmpeg.ffprobe(filepath, function(err, metadata) {
                                    if (err) {
                                        process.stderr.write(`Error reading metadata for: ${filepath} Error: ${err.message}`);
                                        lvObj.ffmpeg_error = err.message;
                                    } else {
                                        lvObj.metadata = metadata;
                                        if (row.dur) {
                                            lvObj.duration_db = row.dur;
                                        } else {
                                            debug(`No duration for: ${row.title}`);
                                        }
                                        lvObj.duration_file = 0;
                                        metadata.streams.forEach(stream => {
                                            if (stream.duration > lvObj.duration_file) {
                                                lvObj.duration_file = stream.duration;
                                            }
                                        });
                                    }
                                    resolve();
                                });
                            })
                            .then(() => {
                                results.push(lvObj);
                            });
                        }
                    }, {concurrency: 4})
                    .then(() => {
                        if (results.length > 0) {
                            return new Promise((resolve, reject) => {
                                const filePath = path.join(__dirname, '..', 'data', 'videos', key, `${row.title}.json`);
                                fs.writeFile(filePath, JSON.stringify(results.sort((a, b) => {
                                    if (a.distance < b.distance) {
                                        return -1;
                                    } else if (a.distance > b.distance) {
                                        return 1;
                                    }
                                    return 0;
                                }), null, '\t'), err => {
                                    if (err) {
                                        return reject(err);
                                    }
                                    resolve();
                                });
                            });
                        }
                        debug(`Created distance array for ${row.title}`);
                    });
                }, {concurrency: 2});
            }
        } else {
            videos_notitle.push(row);
        }
    } else {
        events[row.event_type].push(row);
    }
}, {concurrency: 1}))
.then(() => {
    const dataPath = path.join(__dirname, '..', 'data', 'videos');
    debug(`Found ${Object.keys(events).length} types: ${Object.keys(events).join(', ')}`);

    debug(`Writing data files...`);
    fs.writeFileSync(path.join(dataPath, 'video.json'), JSON.stringify(videos, null, '\t'));
    fs.writeFileSync(path.join(dataPath, 'videos_nofile.json'), JSON.stringify(videos_nofile, null, '\t'));
    fs.writeFileSync(path.join(dataPath, 'videos_notitle.json'), JSON.stringify(videos_notitle, null, '\t'));

    debug(`Done.`);
    process.exit(0);
})
.catch(err => {
    process.stderr.write(`${err.message}\n`);
    process.exit(err.code);
});