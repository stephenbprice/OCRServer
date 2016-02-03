var fs = require('fs');
var path = require('path');
var exec = require('child_process').exec;
var async = require('async');

var upsample = 300;

function getPDFPageCount(filename, cb) {
    exec('pdftk "'+filename+'" dump_data | grep "NumberOfPages" | cut -d":" -f2', function(err, stdout, stderr) {
        if (stdout) {
            return cb(null, parseInt(stdout.trim()));
        } else {
            return cb(stderr);
        }
    });
}

function extractPage(infile, outdir, pagenum, cb) {
    var outfile = path.join(outdir, 'original-'+pagenum+'.pnm');
    exec(
        'gs -dNOPAUSE -dSAFER -sDEVICE=pnmraw '+
        '-r'+upsample+' -dFirstPage='+pagenum+' -dLastPage='+pagenum+' '+
        '-dBatch -o "'+outfile+'" "'+infile+'"',
        function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                return cb(null, outfile);
            }
        }
    );
}

function unpaperPage(infile, outfile, cb) {
    exec('unpaper "'+infile+'" "'+outfile+'"', function(err, stdout, stderr) {
        if (err) {
            return cb(err);
        } else {
            return cb(null, outfile);
        }
    });
}

function ocrPage(infile, outfile, cb) {
    var outfilebase = path.join(
        path.dirname(outfile),
        path.basename(outfile, '.hocr')
    );
    exec('tesseract "'+infile+'" "'+outfilebase+'" hocr', function(err, stdout, stderr) {
        if (err) {
            return cb(err);
        } else {
            return cb(null, outfilebase+'.hocr');
        }
    });
}

function cleanHOCR(infile, outfile, cb) {
    fs.readFile(infile, 'utf8', function(err, data) {
        if (err) {
            return cb(err);
        }
        var result = data.replace(/='[^"]+'/g, function(s) { return s.replace(/'/g, '"'); });
        fs.writeFile(outfile, result, 'utf8', function(err) {
            if (err) {
                return cb(err);
            } else {
                return cb(null, outfile);
            }
        });
    });
}

function composePage(basefile, hocrfile, outfile, cb) {
    exec('hocr2pdf -i "'+basefile+'" -s -o "'+outfile+'" < "'+hocrfile+'"', function(err, stdout, stderr) {
        if (err) {
            return cb(err);
        } else {
            return cb(null, outfile);
        }
    });
}

function composePDF(infiles, outfile, cb) {
    var infileargs = infiles.map(function(infile) { return '"'+infile+'"'; }).join(' ');
    exec(
        'gs -dNOPAUSE -dSAFER -sDEVICE=pdfwrite '+
        '-dBatch -o "'+outfile+'" '+infileargs,
        function(err, stdout, stderr) {
            if (err) {
                return cb(err);
            } else {
                return cb(null, outfile);
            }
        }
    );
}

function searchifyPage(infile, tmpdir, pagenum, cb) {
    var originalfile
      , unpaperfile
      , ocrfile
      , pdffile;
    async.series([
        function(cb) {
            extractPage(infile, tmpdir, pagenum, function(err, outfile) {
                if (err) {
                    return cb(err);
                }
                originalfile = outfile;
                return cb();
            });
        },
        function(cb) {
            var outfile = path.join(tmpdir, 'unpaper-'+pagenum+'.pnm');
            unpaperPage(originalfile, outfile, function(err, outfile) {
                if (err) {
                    return cb(err);
                }
                unpaperfile = outfile;
                return cb();
            });
        },
        function(cb) {
            var outfile = path.join(tmpdir, 'ocr-'+pagenum+'.hocr');
            ocrPage(unpaperfile, outfile, function(err, outfile) {
                if (err) {
                    return cb(err);
                }
                ocrfile = outfile;
                return cb();
            });
        },
        function(cb) {
            var outfile = path.join(tmpdir, 'ocr-fixed-'+pagenum+'.hocr');
            cleanHOCR(ocrfile, outfile, function(err, outfile) {
                if (err) {
                    return cb(err);
                }
                ocrfile = outfile;
                return cb();
            });
        },
        function(cb) {
            var outfile = path.join(tmpdir, 'pdf-'+pagenum+'.pdf');
            composePage(unpaperfile, ocrfile, outfile, function(err, outfile) {
                if (err) {
                    return cb(err);
                }
                pdffile = outfile;
                return cb();
            });
        },
    ], function(err) {
        return cb(err, pdffile);
    });
}

function searchify(infile, outfile, tmpdir, cb) {
    var pdfpages = [];
    getPDFPageCount(infile, function(err, pagecount) {
        if (err) {
            return cb(err);
        }
        var tasks = [];
        for (var i = 1; i <= pagecount; i++) {
            (function() {
                var pagenum = i;
                tasks.push(function(cb) {
                    searchifyPage(infile, tmpdir, pagenum, function(err, outfile) {
                        if (err) {
                            return cb(err);
                        }
                        pdfpages.push(outfile);
                        return cb();
                    });
                });
            })();
        }
        async.series(tasks, function(err) {
            if (err) {
                return cb(err);
            }
            composePDF(pdfpages, outfile, function(err, outfile) {
                if (err) {
                    return cb(err);
                }
                return cb();
            });
        });
    });
}

module.exports.searchify = searchify;
