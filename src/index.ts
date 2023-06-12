import { Storage } from "@google-cloud/storage";
import { BigQuery } from "@google-cloud/bigquery";

const GOOGLE_CLOUD_PROJECT_ACCESS_METADATA = {
  projectId: "poc-lab-389020",
  keyFilename: `./poc-lab-key.json`,
};
const storage = new Storage(GOOGLE_CLOUD_PROJECT_ACCESS_METADATA);
const bigquery = new BigQuery(GOOGLE_CLOUD_PROJECT_ACCESS_METADATA);

async function processCsvFile() {
  try {
    const bucketName: string = "poc_lab_bucket";
    const fileName: string = "dataset.csv";
    const datasetId: string = "poc_lab_dataset";
    const tableId: string = "poc_table";

    const bucket = storage.bucket(bucketName);
    const file = bucket.file(fileName);
    await file.download({ destination: `./${fileName}` });

    const [datasets] = await bigquery.getDatasets();
    if (
      !datasets
        .map((obj: any) => {
          return obj.id;
        })
        .includes(datasetId)
    ) {
      await bigquery.createDataset(datasetId);
      console.log(`Created dataset ${datasetId}`);
      await bigquery.dataset(datasetId).table(tableId).create();
      console.log(`Created table ${tableId} in dataset ${datasetId}`);
    }

    const table = bigquery.dataset(datasetId).table(tableId);
    await table.load(`./${fileName}`, {
      autodetect: true,
    });

    console.log(
      `File from GCS at ${bucketName}/${fileName} is processed and pushed to BigQuery ${datasetId}/${tableId}`
    );
  } catch (error) {
    console.error("Error:", error);
    return error;
  }
}

processCsvFile();
