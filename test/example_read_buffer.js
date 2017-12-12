var libhdfs3 = require('../lib/hdfs');
var fs = new libhdfs3();
var util = require('util');

var targetDir = '/data';    // The target dir should contain files
var kerbTicketCachePath = undefined;                   // Set this to use kerberos

var options = {
    "nameNode": "hdfs://localhost:9000",
    "extra": {
        "dfs.client.use.datanode.hostname": "true",
        "dfs.client.read.shortcircuit": "false"
    }
};

const file = '/data/TechCrunchcontinentalUSA.parquet';

if (kerbTicketCachePath) {
    options.kerbTicketCachePath = kerbTicketCachePath;
    options.extra['hadoop.security.authentication'] = 'kerberos';
}

fs.connect(options, function(err, success) {
    if (err) {
        return console.log('Connection Error :: ', err);
    }

    fs.info(file, (err, info) => {
      if (err) {
        return console.log("Error while fetching file info - " + err);
      }

      let fh = fs.open(file);
      fs.readBuffer(fh, info.size - 4, 4, (err, buf) => {
        if (err) {
          return console.log("Error while reading - " + err);
        }

        console.log("Dumping last 4 bytes - " + buf);
      });
    });
});
