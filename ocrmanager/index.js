var fs = require('fs');
var events = require('events');
var path = require('path');
var mkdirp = require('mkdirp');
var async = require('async');
var chokidar = require('chokidar');
var winston = require('winston');
var pdfsearchify;

module.exports = startOCRManager;

function startOCRManager(settingspath) {

    var requiredPaths = ['watchpath', 'processpath', 'finishpath', 'errorpath'];
    var optionalPaths = ['keeppath', 'logpath'];
    var optionalInts = ['workers'];
    var watcher;
    var q;

    return manageOCR;

    function manageOCR(cb) {
        async.series([
            checkOptionsFile,
            validateRequiredPaths,
            validateOptionalPaths,
            validateOptionalInts,
            setupLogger,
            setPDFSearchifyListeners,
            makeQueue,
            ], function(err) {
                if (err) {
                    winston.error(err)
                    cb(err);
                } else {
                    startWatcher();
                }
            }
        );
    }

    function setupLogger(cb) {
        var loglevel = settings.hasOwnProperty('loglevel') && settings.loglevel === 'verbose' ? 'verbose' : 'warn';
        if (settings.hasOwnProperty('filelog') && settings.filelog === true) {
            winston.add(winston.transports.File, {
                filename: './logs/ocrlog'+process.hrtime().join('')+'.txt',
            });
        }
        winston.level = loglevel;
        cb(null);
    }

    function checkOptionsFile(cb) {
        if (!settingspath) {
            return cb(new Error('No settings file passed'));
        } else {
            try {
                settings = require(settingspath);
            } catch (e) {
                return cb(new Error('Settings file is not a valid JSON object'));
            }
            pdfsearchify = require('../pdfsearchify')(settings.pdfsearchify || {});
            return cb(null);
        }
    }

    function validateRequiredPaths(cb) {
        async.forEach(requiredPaths, validatePath, function(err) {
            return cb(err);
        });
    }

    function validateOptionalPaths(cb) {
        async.forEach(optionalPaths, validateOptionalPath, function(err) {
            return cb(err);
        });
    }

    function validateOptionalInts(cb) {
        async.forEach(optionalInts, validateOptionalInt, function(err) {
            return cb(err);
        });
    }


    function validatePath(pathtype, cb) {
        if (!settings.hasOwnProperty(pathtype)) {
            return cb(new Error('Settings file must have a ' + pathtype));
        } else {
            settings[pathtype] = path.normalize(settings[pathtype] + '/');
            fs.lstat(settings[pathtype], function(err, stats) {
                if (err) {
                   return cb(err);
                } else if (!stats.isDirectory()) {
                    return cb(new Error(pathtype + ' must be a valid directory'));
                } else {
                    return cb(null);
                }
            });
        }
    }

    function validateOptionalPath(pathtype, cb) {
        if (settings.hasOwnProperty(pathtype)) {
            return validatePath(pathtype, cb);
        } else {
            return cb(null);
        }
    }

    function validateOptionalInt(option, cb) {
        if (settings.hasOwnProperty(option)) {
            if (settings[option] === parseInt(settings[option], 10)) {
                winston.warn('Setting %s to %s', option, settings[option]);
                settings[option] = parseInt(settings[option], 10);
                return cb(null);
            } else {
                winston.warn('%s is not a valid %s value', settings[option], option);
                delete settings[option];
                return cb(null);
            }
        } else {
            return cb(null);
        }
    }

    function setPDFSearchifyListeners(cb) {
        pdfsearchify.on('start', function(o) { winston.info('Starting: '+o.infile); });
        pdfsearchify.on('compose', function(o) { winston.info('Composing: '+o.outfile); });
        pdfsearchify.on('composed', function(o) { winston.info('Composed: '+o.outfile+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('startPage', function(o) { winston.info('Starting page: '+o.processInfo.pagenum); });
        pdfsearchify.on('donePage', function(o) { winston.info('Done page: '+o.processInfo.pagenum+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('extractPNM', function(o) { winston.info('Extracting PNM: '+o.processInfo.pagenum); });
        pdfsearchify.on('PNMExtracted', function(o) { winston.info('Extracted PNM: '+o.processInfo.pagenum+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('detectColor', function(o) { winston.info('Detecting Color: '+o.processInfo.pagenum); });
        pdfsearchify.on('colorDetected', function(o) { winston.info('Detected Color: '+o.processInfo.pagenum+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('deskewPNM', function(o) { winston.info('Deskewing: '+o.processInfo.pagenum); });
        pdfsearchify.on('PNMDeskewed', function(o) { winston.info('Deskewed: '+o.processInfo.pagenum+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('preprocessPage', function(o) { winston.info('Pre-processing page: '+o.processInfo.pagenum); });
        pdfsearchify.on('pagePreprocessed', function(o) { winston.info('Page pre-processed: '+o.processInfo.pagenum+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('ocrPage', function(o) { winston.info('Ocring page: '+o.processInfo.pagenum); });
        pdfsearchify.on('pageOcred', function(o) { winston.info('Ocred page: '+o.processInfo.pagenum+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('composeJBIG2', function(o) { winston.info('Composing JBIG2PDF: '+o.infile); });
        pdfsearchify.on('composedJBIG2', function(o) { winston.info('Composed JBIG2PDF: '+o.infile+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('composeJPEG', function(o) { winston.info('Composing JPEGPDF: '+o.infile); });
        pdfsearchify.on('composedJPEG', function(o) { winston.info('Composed JPEGPDF: '+o.infile+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('mergePDF', function(o) { winston.info('Merging PDF: '+o.infile); });
        pdfsearchify.on('mergedPDF', function(o) { winston.info('Merged PDF: '+o.infile+' ('+hrTimeString(o.time)+')'); });
        pdfsearchify.on('done', function(o) { winston.info('Done: '+o.infile+' ('+hrTimeString(o.time)+')'); });
        cb(null);
    }

    function hrTimeString(hrtime) {
        return hrtime[0]+hrtime[1]/1000000000;
    }

    function makeQueue(cb) {
        q = async.queue(managePDFSearchify, (settings.workers || 1));
        return cb(null);
    }

    function startWatcher() {
        watcher = chokidar.watch(settings.watchpath, { persistent: true, usePolling: true });
        watcher.on('add', function(filepath) { q.push(filepath) });
    }

    function fileExists(filepath) {
        try {
            fs.statSync(filepath);
            return true;
        } catch(e) {
            return false;
        }
    }

    function managePDFSearchify(file, cb) {
        if(fileExists(file)) {
            var startTime = process.hrtime();
            var processInfo = { "infile": file };
            async.waterfall([
                async.apply(calcDirPaths, processInfo),
                ensureProcDirPath,
                ensureOutDirPath,
                ensureKeepDirPath,
                calcProcFileName,
                calcOutFileName,
                calcKeepFileName,
                copyKeepFile,
                copyProcFile,
                ], function(err, processInfo) {
                    if (err) {
                        winston.error(err);
                        return cb(err);
                    } else {
                        return searchify(processInfo, function(err) {
                            if (err) {
                                winston.error(err);
                            }
                            winston.info('%s completed in (%s)', file, hrTimeString(process.hrtime(startTime)));
                            return cleanup(err, processInfo, cb);
                        });
                    }
                }
            );
        } else {
            winston.info('File %s no longer exists', file);
            return cb(null);
        }
    }

    function calcDirPaths(processInfo, cb) {
        processInfo.relativepath = processInfo.infile.replace(settings.watchpath,'');
        processInfo.procpath = path.normalize(path.join(settings.processpath, processInfo.relativepath));
        processInfo.outpath = path.normalize(path.join(settings.finishpath, processInfo.relativepath));
        processInfo.errorpath = path.normalize(path.join(settings.errorpath, processInfo.relativepath));
        if (settings.keeppath) {
            processInfo.keeppath = path.normalize(path.join(settings.keeppath, processInfo.relativepath));
        }
        return cb(null, processInfo);
    }

    function ensureProcDirPath(processInfo, cb) {
        return ensureDirPath('procpath', processInfo, cb);
    }

    function ensureOutDirPath(processInfo, cb) {
        return ensureDirPath('outpath', processInfo, cb);
    }

    function ensureErrorDirPath(processInfo, cb) {
        return ensureDirPath('errorpath', processInfo, cb);
    }

    function ensureKeepDirPath(processInfo, cb) {
        if (processInfo.keeppath) {
            return ensureDirPath('keeppath', processInfo, cb);
        } else {
            return cb(null, processInfo);
        }
    }

    function ensureDirPath(pathkey, processInfo, cb) {
        dirpath = path.dirname(processInfo[pathkey]);
        mkdirp(dirpath, function(err) {
            return cb(err, processInfo);
        });
    }

    function calcProcFileName(processInfo, cb) {
        return calcFileName('procpath', processInfo, cb);
    }

    function calcOutFileName(processInfo, cb) {
        return calcFileName('outpath', processInfo, cb);
    }

    function calcKeepFileName(processInfo, cb) {
        if (processInfo.keeppath) {
            return calcFileName('keeppath', processInfo, cb);
        } else {
            return cb(null, processInfo);
        }
    }

    function calcErrorFileName(processInfo, cb) {
        return calcFileName('errorpath', processInfo, cb);
    }

    function calcFileName(pathkey, processInfo, cb) {
        var parsedPath = path.parse(processInfo[pathkey]);
        var copyPath = path.parse(processInfo[pathkey]);
        var copy = 0;
        while (fileExists(path.format(copyPath))) {
            ++copy;
            copyPath.name = parsedPath.name+'('+copy+')';
            copyPath.base = copyPath.name + copyPath.ext;
        }
        processInfo[pathkey] = path.format(copyPath);
        return cb(null, processInfo);
    }

    function copyKeepFile(processInfo, cb) {
        if (processInfo.keeppath) {
            return copyFile(processInfo, processInfo.infile, processInfo.keeppath, false, cb);
        } else {
            return cb(null, processInfo);
        }
    }

    function copyProcFile(processInfo, cb) {
        return copyFile(processInfo, processInfo.infile, processInfo.procpath, true, cb);
    }

    function copyErrorFile(processInfo, cb) {
        return copyFile(processInfo, processInfo.procpath, processInfo.errorpath, true, cb);
    }

    function copyFile(processInfo, source, target, unlink, cb) {
        var cbCalled = false;
        var rd = fs.createReadStream(source);
        var wr = fs.createWriteStream(target);
        rd.on('error', function(err) { done(err); });
        wr.on('error', function(err) { done(err); });
        wr.on('close', function(ex) { done(); });
        rd.pipe(wr);

        function done(err) {
            if(!cbCalled) {
                cbCalled = true;
                if(err) {
                    cb(err);
                } else {
                    if (fileExists(target)) {
                        if (unlink) {
                            return unlinkFile(processInfo, source, target, cb);
                        } else {
                            return cb(null, processInfo);
                        }
                    } else {
                        return cb(new Error('%s not moved to %s', source, target));
                    }
                }
            }
        }
    }

    function unlinkFile(processInfo, target, newLocation, cb) {
        var dirname = path.dirname(target);
        if (!fileExists(newLocation)) {
            return cb(new Error(target+' has not been moved to '+newLocation));
        } else {
            fs.unlinkSync(target);
            return checkDeleteDirectory(processInfo, dirname, cb);
        }
    }

    function checkDeleteDirectory(processInfo, dirname, cb) {
        var dirname = path.normalize(dirname + '/');
        var failed = false;
        
        while (!failed && [settings.watchpath, settings.processpath, settings.errorpath].indexOf(dirname) === -1) {
            try {
                fs.rmdirSync(dirname);
                dirname = path.normalize(dirname+'/../');
            } catch (e) {
                failed = true;
            }
        }
        return cb(null, processInfo);
    }

    function searchify(processInfo, cb) {
        return pdfsearchify(processInfo.procpath, processInfo.outpath, cb);
    }

    function cleanup(err, processInfo, cb) {
        if (err) {
            async.waterfall([
                async.apply(ensureErrorDirPath, processInfo),
                calcErrorFileName,
                copyErrorFile,
                ], function(err) {
                    return cb(err);
                }
            );
        } else {
            unlinkFile(processInfo, processInfo.procpath, processInfo.outpath, cb);
        }
    }

    function exit() {
        running = false;
        winston.info('EXITING');
        process.exit(1);
    }
}

