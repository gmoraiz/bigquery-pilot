import express from 'express';
import bodyParser from 'body-parser';
import {BigQuery} from '@google-cloud/bigquery';
import {Storage} from'@google-cloud/storage';

require('custom-env').env();

const app = express();

const whitelist = [
    "user_id",
    "platform",
    "version",
    "model",
    "time",
    "search",
    "type_search",
    "country",
    "state",
    "city",
];

const bigQueryConfig = {
    projectId: 'repositorio-catalogo'
};

app.use(bodyParser.json());

app.post('/init', (req, res) => {
    const storage    = new Storage();
    const bucketName = 'repositorio-catalogo' 
    async function createBucket() {
        await storage.createBucket(bucketName);
        console.log(`Bucket ${bucketName} created.`);
    }
    createBucket();
    res.json({
        success: true,
        data: bucketName
    });
});

app.post('/dataset', (req, res) => {
    const bigQuery = new BigQuery(bigQueryConfig);
    bigQuery.createDataset(req.body.name).then(results => {
        const dataset = results[0];
        res.json({
            success: true,
            data: dataset.id
        });
      })
      .catch(err => {
        console.error(err);
        res.json({
            success: false,
            data: null
        });
      });
});

app.post('/table', (req, res) => {
    const options = {
        schema: schema,
        location: 'US',
    };
    const bigQuery = new BigQuery(bigQueryConfig);
    bigQuery.dataset(req.body.datasetId).createTable(req.body.tableId).then(results => {
        const dataset = results[0];
        res.json({
            success: true,
            data: dataset.id
        });
      })
      .catch(err => {
        console.error(err);
        res.json({
            success: false,
            data: null
        });
      });
});

app.post('/rows', (req, res) => {
    // if(Object.keys(req.body).filter(x => !whitelist.includes(x)).length){
    //     res.json({
    //         success: false,
    //         msg: "Um do(s) dados listados não são permitidos"
    //     });
    // }

    const bigQuery = new BigQuery(bigQueryConfig);
    
    let rows      = req.body.rows;
    let tableId   = req.body.tableId;
    let datasetId = req.body.datasetId;

    bigQuery.dataset(datasetId).table(tableId).insert(rows).then(data => {
        res.json({
            success: true,
            data: data
        });
    }).catch(err => {
        console.error(err);
        res.json({
            success: false,
            data: null
        });
    });
    
});


app.listen(process.env.APP_PORT, () => console.log(`Server running on port ${process.env.APP_PORT}`));

//gcloud auth application-default login TO AUTH API