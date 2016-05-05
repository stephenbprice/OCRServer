var fs = require('fs');
var events = require('events');
var path = require('path');
var mixin = require('merge-descriptors');
var exec = require('child_process').exec;
var async = require('async');

module.exports = createPDFSearchify;

function createPDFSearchify(options) {

    options = options || {};
    var upsample = options.upsample || 300;
    var downsample = options.downsample;
    var preprocess = options.preprocess || 'quick'; // quick, lat
    var keepfiles = options.keepfiles || false;

    var tmp = require('tmp');
    if (!keepfiles) {
        tmp.setGracefulCleanup();
    }

    mixin(searchify, events.EventEmitter.prototype, false);
    return searchify;

    function unlinkFilesCallback(files, cb) {
        if (keepfiles) {
            return cb()
        } else {
            return async.each(files, fs.unlink, cb);
        }
    }

    function getPDFPageCount(filename, cb) {
        exec('pdftk "'+filename+'" dump_data | grep "NumberOfPages" | cut -d":" -f2', function(err, stdout, stderr) {
            if (stdout) {
                return cb(null, parseInt(stdout.trim()));
            } else {
                return cb(stderr);
            }
        });
    }

    function dumpPDFInfo(filename, outdir, cb) {
        var outfile = path.join(outdir, 'pdfinfo.txt');
        exec('pdftk "'+filename+'" dump_data output "'+outfile+'"', function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                return cb(null, filename, outfile);
            }
        });
    }

    function updatePDFInfo(infile, infofile, outfile, cb) {
        exec('pdftk "'+infile+'" update_info "'+infofile+'" output "'+outfile+'"', function(err, stdout, stderr) {
            return cb(err);
        });
    }

    function extractPNM(processInfo, cb) {
        searchify.emit('extractPNM', { processInfo: processInfo});
        var extractPageTime = process.hrtime();
        processInfo.tmpfiles.originPNM = path.join(processInfo.outdir, 'original-'+processInfo.pagenum+'.pnm');
        exec(
            'gs -dNOPAUSE -dSAFER -sDEVICE=pnmraw '+
            '-r'+upsample+' -dFirstPage='+processInfo.pagenum+' -dLastPage='+processInfo.pagenum+' '+
            '-dBatch -o "'+processInfo.tmpfiles.originPNM+'" "'+processInfo.infile+'"',
            function(err, stdout, stderr) {
                if (err) {
                    return cb(err);
                } else {
                    searchify.emit('PNMExtracted', { processInfo: processInfo, time: process.hrtime(extractPageTime), });
                    return cb(null, processInfo);
                }
            }
        );
    }

    function detectColor(processInfo, cb) {
        searchify.emit('detectColor', { processInfo: processInfo});
        var detectColorTime = process.hrtime();
        exec('python ./utils/detectColor.py '+processInfo.tmpfiles.originPNM, function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                processInfo.colorcode = stdout;
                searchify.emit('colorDetected', { processInfo: processInfo, time: process.hrtime(detectColorTime), });
                return cb(null, processInfo);
            }
        });
    }

    function deskewPNM(processInfo, cb) {
        searchify.emit('deskewPNM', { processInfo: processInfo });
        var deskewPageTime = process.hrtime();
        processInfo.tmpfiles.deskewPNM = path.join(processInfo.outdir, 'deskew-'+processInfo.pagenum+'.pnm');
        exec('convert "'+processInfo.tmpfiles.originPNM+'" -deskew 40% "'+processInfo.tmpfiles.deskewPNM+'"', function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                searchify.emit('PNMDeskewed', { processInfo: processInfo, time: process.hrtime(deskewPageTime), });
                    return cb(err, processInfo);
            }
        });
    }

    function preprocessPage(processInfo, cb) {
        searchify.emit('preprocessPage', { processInfo: processInfo });
        var preprocessPageTime = process.hrtime();
        processInfo.tmpfiles.preprocPNM = path.join(processInfo.outdir, 'preprocessed-'+processInfo.pagenum+'.pnm');
        var convertOptions;
        switch (preprocess) {
            case 'quick':
                convertOptions = '-type grayscale -blur 1x65000 -contrast -normalize -despeckle -despeckle -threshold 50%';
                break;
            case 'lat':
            default:
                convertOptions = '-respect-parenthesis \\( -clone 0 -colorspace gray -negate -lat 15x15+5% -contrast-stretch 0 \\) -compose copy_opacity -composite -opaque none +matte -modulate 100,100 -blur 1x1 -adaptive-sharpen 0x2 -negate -define morphology:compose=darken -morphology Thinning Rectangle:1x30+0+0 -negate';
                break;
        }
        exec('convert "'+processInfo.tmpfiles.deskewPNM+'" '+convertOptions+' "'+processInfo.tmpfiles.preprocPNM+'"', function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                searchify.emit('pagePreprocessed', { processInfo: processInfo, time: process.hrtime(preprocessPageTime), });
                return cb(null, processInfo);
            }
        });
    }

    function ocrPage(processInfo, cb) {
        searchify.emit('ocrPage', { processInfo: processInfo});
        var ocrPageTime = process.hrtime();
        processInfo.hocr = path.join(processInfo.outdir, 'ocr-'+processInfo.pagenum+'.hocr');
        var outfilebase = path.join(
            path.dirname(processInfo.hocr),
            path.basename(processInfo.hocr, '.hocr')
        );
        exec('tesseract "'+processInfo.tmpfiles.preprocPNM+'" "'+outfilebase+'" hocr', function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                searchify.emit('pageOcred', { processInfo: processInfo, time: process.hrtime(ocrPageTime), });
                return cb(null, processInfo);
            }
        });
    }

    function downsamplePage(processInfo, cb) {
        if (downsample === undefined || downsample === upsample) {
            return cb(null, processInfo);
        }
        searchify.emit('downsamplePage', { processInfo: processInfo });
        var downsamplePageTime = process.hrtime();
        processInfo.pageImage = path.join(processInfo.outdir, 'downsample-'+processInfo.pagenum+'.pnm');
        exec('convert -density '+upsample+' "'+processInfo.tmpfiles.deskewPNM+'" -resample '+downsample+' "'+processInfo.pageImage+'"', function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                searchify.emit('pageDownsampled', { processInfo: processInfo, time: process.hrtime(downsamplePageTime), });
                return cb(null, processInfo);
            }
        });
    }

    function composeJBIG2(pages, tmpdir, cb) {
        var composeJBIG2Time = process.hrtime();
        var jbig2PDF = path.join(tmpdir, 'jbig2.pdf');
        var imgfiles = pages.map(function(x) { 
            if (x.colorcode === "0") return x.pageInfo.pageImage; 
        });
        if (imgfiles.length === 0) {
            searchify.emit('composedJBIG2', { processInfo: pages[0].pageInfo, time: process.hrtime(composeJBIG2Time), });
            return cb(null, null);
        } else {
            imgfiles = imgfiles.join('" "');
            searchify.emit('composeJBIG2', { processInfo: pages[0].pageInfo });
            exec('jbig2 -s -p -v "'+imgfiles+'" && ./utils/pdf.py output '+(downsample || upsample)+' > "'+jbig2PDF+'"', function(err, stdout, stderr) {
                if (err) {
                    return cb(err);
                } else {
                    searchify.emit('composedJBIG2', { processInfo: pages[0].pageInfo, time: process.hrtime(composeJBIG2Time), });
                    return cb(null, jbig2PDF);
                }
            });
        }
    }

    function composeJPEG(pages, tmpdir, cb) {
        searchify.emit('composeJPEG', { processInfo: pages[0].pageInfo });
        var composeJPEGTime = process.hrtime();
        var jpegPDF = path.join(tmpdir, 'jpeg.pdf');
        var imgfiles = []
        pages.map(function(x) {
            if (x.colorcode !== "0") return imgfiles.push(x.pageInfo.pageImage);
        });
        if (imgfiles.length === 0) {
            searchify.emit('composedJPEG', { processInfo: pages[0].pageInfo, time: process.hrtime(composeJPEGTime), });
            return cb(null, null);
        } else {
            exec('convert "'+imgfiles+'" "'+jpegPDF+'"', function(err, stdout, stderr) {
                if (err) {
                    return cb(err);
                } else {
                    searchify.emit('composedJPEG', { processInfo: pages[0].pageInfo, time: process.hrtime(composeJPEGTime), });
                    return cb(null, jpegPDF);
                }
            });
        }
    }

    function mergePDF(pages, jbig2PDF, jpegPDF, tmpdir, cb) {
        var mergePDFTime = process.hrtime();
        var mergedpdf = path.join(tmpdir, 'merged.pdf');
        var jbig2index = 1;
        var jpegindex = 1;
        var mergestatement = [];
        searchify.emit('mergePDF', { processInfo: pages[0].pageInfo });
        if (!jbig2PDF) {
            searchify.emit('mergedPDF', { processInfo: pages[0].pageInfo, time: process.hrtime(mergePDFTime), });
            return cb(null, jpegPDF);
        } else if (!jpegPDF) {
            searchify.emit('mergedPDF', { processInfo: pages[0].pageInfo, time: process.hrtime(mergePDFTime), });
            return cb(null, jbig2PDF);
        } else {
            pages.forEach(function(x) {
                if (x.colorcode === "0") {
                    mergestatement.push('A'+jbig2index);
                    jbig2index += 1;
                } else {
                    mergestatement.push('B'+jpegindex);
                    jpegindex += 1;
                }
            });
            mergestatement = mergestatement.join(' ');
            console.log(mergestatement);
            exec('pdftk A="'+jbig2PDF+'" B="'+jpegPDF+'" cat '+mergestatement+' output "'+mergedpdf+'"', function(err, stdout, stderr) {
                if (err) {
                    return cb(err);
                } else {
                    searchify.emit('mergedPDF', { processInfo: pages[0].pageInfo, time: process.hrtime(mergePDFTime), });
                    return cb(null, mergedpdf);
                }
            });
        }
    }

    function addPDFText(pages, pdf, tmpdir, cb) {
        var addTextTime = process.hrtime();
        var outfile = path.join(tmpdir, 'searchified.pdf');
        var hocrfiles = ['output="'+outfile+'"'];
        searchify.emit('add text', { processInfo: pages[0].pageInfo });
        pages.map(function(x) { hocrfiles.push(x.pagenum+'="'+x.pageInfo.hocr+'"'); });
        exec('python utils/hocr-pdf '+hocrfiles.join(' ')+' "'+pdf+'"', function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                searchify.emit('text added', { processInfo: pages[0].pageInfo, time: process.hrtime(addTextTime), });
                return cb(null, outfile);
            }
        });
    }

    function searchifyPage(processInfo, cb) {
        searchify.emit('startPage', { processInfo: processInfo });
        var startPageTime = process.hrtime();
        async.waterfall([
            async.apply(extractPNM, processInfo),
            detectColor,
            deskewPNM,
            preprocessPage,
            ocrPage,
            downsamplePage,
        ], function(err, outfile) {
            if (err) {
                return cb(err);
            } else {
                searchify.emit('donePage', { processInfo: processInfo, time: process.hrtime(startPageTime), });
                return cb(null, outfile);
            }
        });
    }

    function searchify(infile, outfile, cb) {
        var startTime = process.hrtime();
        searchify.emit('start', { infile: infile, outfile: outfile, });
        var pages = [];
        getPDFPageCount(infile, function(err, pagecount) {
            if (err) {
                return cb(err);
            }
            tmp.dir({ prefix: 'OCRServer', keep: keepfiles }, function(err, tmpdir, cleanupcb) {
                if (err) {
                    return cb(err);
                }
                dumpPDFInfo(infile, tmpdir, function(err, infile, pdfinfofile) {
                    var tasks = [];
                    for (var i = 1; i <= pagecount; i++) {
                        (function() {
                            var processInfo = {
                                "infile": infile,
                                "outdir": tmpdir,
                                "pagenum": i,
                                "colorcode": null,
                                "hocr": null,
                                "pageImage": null,
                                "tmpfiles": {}
                            }
                            tasks.push(function(cb) {
                                searchifyPage(processInfo, function(err, outInfo) {
                                    if (err) {
                                        return cb(err);
                                    }
                                    pages.push({
                                        pagenum: outInfo.pagenum,
                                        pageInfo: outInfo,
                                        colorcode: outInfo.colorcode
                                    });
                                    return cb();
                                });
                            });
                        })();
                    }
                    async.series(tasks, function(err) {
                        if (err) {
                            return cb(err);
                        }
                        pages = pages.sort(function(a,b) {return a.pagenum - b.pagenum;});
                        composeJBIG2(pages, tmpdir, function(err, jbig2pdf) {
                            if (err) {
                                return cb(err);
                            }
                            composeJPEG(pages, tmpdir, function(err, jpegpdf) {
                                if (err) {
                                    return cb(err);
                                }
                                mergePDF(pages, jbig2pdf, jpegpdf, tmpdir, function(err, mergedpdf) {
                                    if (err) {
                                        return cb(err);
                                    }
                                    addPDFText(pages, mergedpdf, tmpdir, function(err, searchifiedpdf) {
                                        if (err) {
                                            return cb(err);
                                        }
                                        updatePDFInfo(searchifiedpdf, pdfinfofile, outfile, function(err, outfile) {
                                            if (err) {
                                                return cb(err);
                                            } else {
                                                searchify.emit('done', { infile: infile, outfile: outfile, time: process.hrtime(startTime), });
                                                fs.rmdir(tmpdir, function(err) {
                                                    if (!err || err.code === 'ENOTEMPTY') {
                                                        return cb();
                                                    } else {
                                                        return cb(err);
                                                    }
                                                });
                                            }
                                        });
                                    });
                                });
                            });
                        });
                    });
                });
            });
        });
    }
}
