const fs = require('fs')
const glob = require('glob')
const MongoClient = require('mongodb').MongoClient;
const assert = require('assert');

// Connection URL
const url = 'mongodb://localhost:27017';

// Database Name
const dbName = 'rides';

// Create a new MongoClient
const client = new MongoClient(url, { useNewUrlParser: true, useUnifiedTopology: true });

(async function () {
  glob('D:/CyclingData/Justin/activities/*', async (err, results) => {
    client.connect(async function(err) {
      assert.equal(null, err);
      console.log("Connected successfully to server");
      const db = client.db(dbName);
    // console.log(results)
      for (let i = 0; i < results.length; i += 100) {
        let files = await results.slice(i, i + 100).map(async (c, i) => {
          let rawdata = fs.readFileSync(c)
          let ride = rawdata.toString().trim()
          let act = JSON.parse(ride)
          console.log(c)
          return act
        })
        await Promise.all(files).then(async data => {
          // console.log(data)
          let r = await db.collection('activities').insertMany(data)
          assert.equal(data.length, r.insertedCount)
          console.log('inserted ' + r.insertedCount + ' documents')
        })
        // console.log(files)
      } 
      return client.close();
    })
  })

})()