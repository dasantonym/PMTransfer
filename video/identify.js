import Debug from 'debug';
import path from 'path';
import fs from 'fs';
import Promise from 'bluebird';

const debug = Debug('pmtransfer'),
    dataPath = path.join(__dirname, '..', 'data');

debug('Loading data...');
const videos = JSON.parse(fs.readFileSync(path.join(dataPath, 'video.json')));

debug('Parsing data...');
Promise.map(Object.keys(videos), key => {

});
