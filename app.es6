const request = require('request-promise');
const Promise = require("bluebird");
const Firebase = require("firebase");

// Patch console.log to add timestamp data.
require("console-stamp")(console, "HH:MM:ss.l");

const GBG_DATA_API_KEY = process.env.GBG_DATA_API_KEY;
const FIREBASE_SECRET = process.env.FIREBASE_SECRET;

const REFRESH_DELAY = 1000 * 60;

// Setup references to our data
const rootDbRef = new Firebase("https://styr-och-stall.firebaseio.com/");
const stationsDbRef = rootDbRef.child("stations");

// Latest data as we know it
let stationsCache = new Map();

function authenticateWithFirebase() {
    console.log('-- Authenticating with Firebase');
    return new Promise((resolve, reject) => {
        rootDbRef.authWithCustomToken(FIREBASE_SECRET, err => {
            if (!err) {
                console.log('Authentication successful.');
                resolve();
            } else {
                console.error('Authentication failed!');
                reject(err);
            }
        })
    });
}

function setStationsCache(stationsObject) {
    stationsCache.clear();
    for (let id in stationsObject) {
        if (stationsObject.hasOwnProperty(id)) {
            stationsCache.set(id, stationsObject[id]);
        }
    }
}

function updateCache() {
    console.log('-- Updating cache');
    return new Promise((resolve, reject) => {
        stationsDbRef.once("value", data => {
            setStationsCache(data.val());
            console.log('Update successful.');
            resolve();
        }, err => {
            console.error('Update failed!');
            reject(err);
        });
    });
}

function setupCacheUpdateListener() {
    stationsDbRef.on("value", data => {
        setStationsCache(data.val());
    }, err => {
        console.error('-- Value changed had an error:');
        console.error(err);

    });

    return Promise.resolve();
}

function setupFirebase() {
    return authenticateWithFirebase()
        .then(updateCache)
        .then(setupCacheUpdateListener);
}

function getBikeStations() {
    let options = {
        url: `http://data.goteborg.se/StyrOchStall/v0.1/GetBikeStations/${GBG_DATA_API_KEY}?format=json`,
        json: true
    };
    return request(options).then(data => {
        if (data) {
            return data;
        } else {
            return Promise.reject(new Error("Got no data"));
        }
    });
}

function processBikeStationsData(bikeStationsData) {
    if (bikeStationsData['Stations'] && bikeStationsData['TimeStamp']) {
        const stations = new Map();

        bikeStationsData['Stations'].forEach(({Id, Capacity, FreeBikes, FreeStands, Label, Lat, Long, State}) => {
            Id = Id.toString(); // Firebase uses string ids, lets do the same
            stations.set(Id.toString(), {
                capacity: Capacity,
                freeBikes: FreeBikes,
                freeStands: FreeStands,
                label: Label,
                lat: Lat,
                long: Long,
                state: State
            });
        });
        return Promise.resolve(stations);
    } else {
        return Promise.reject(new Error("Missing required attributes"))
    }
}

function updateStationsData(newStations) {
    var stationsToUpdate = [];

    // Check for added or changed stations
    newStations.forEach((station, id) => {
        if (!stationsCache.has(id)) {
            // Station was just added
            stationsToUpdate.push({id: id, station: station});
        } else {
            const cachedStation = stationsCache.get(id);
            const changedAttributes = ['capacity', 'freeBikes', 'freeStands',
                'label', 'lat', 'long', 'state'].filter((attr) => {
                    return station[attr] !== cachedStation[attr];
                });
            if (changedAttributes.length > 0) {
                // Some station attribute was updated
                stationsToUpdate.push({id: id, station: station});
            }
        }
    });

    // Check for removed stations
    stationsCache.forEach((station, id) =>{
        if (!newStations.has(id)) {
            stationsToUpdate.push({id: id, station: null});
        }
    });

    if(stationsToUpdate.length > 0) {
        console.log("To be updated: %s", stationsToUpdate.map(({id}) => id).join(', '));

        return Promise.map(stationsToUpdate, ({id, station}) => {
            return new Promise((resolve, reject) => {
                stationsDbRef.child(id).set(station, err => {
                    if (!err) {
                        resolve();
                    } else {
                        reject(err)
                    }
                });
            });
        });
    } else {
        return Promise.resolve();
    }
}

function run() {
    getBikeStations()
        .then(processBikeStationsData)
        .then(updateStationsData)
        .catch(err => {
            console.error(err);
            return Promise.resolve();
        })
        .delay(REFRESH_DELAY)
        .then(run)
}

function main() {
    setupFirebase().then(run);
}

export {main};
