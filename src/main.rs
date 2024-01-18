use std::{fs::File, io::Write};

use aws_config::BehaviorVersion;
use aws_sdk_s3::primitives::ByteStream;
use datafusion::arrow::json;
use lambda_http::{run, service_fn, Body, Error, Request, RequestExt, Response};
use std::fs;
use std::path::Path;
// use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::error::{DataFusionError, Result};
use datafusion::prelude::*;
// use env_logger::Env;
use tokio::time::Instant;

/// This is the main body for the function.
/// Write your code inside it.
/// There are some code example in the following URLs:
/// - https://github.com/awslabs/aws-lambda-rust-runtime/tree/main/examples
async fn function_handler(event: Request) -> Result<Response<Body>, Error> {
    let mut query_tasks = Vec::new();
    let num_requests = 100;

    for i in 1..num_requests {
        query_tasks.push(tokio::spawn(compute(i)));
    }

    for task in query_tasks {
        let _ = task.await.expect("waiting failed");
    }

    let message = "Finished executing all tasks";

    let resp = Response::builder()
        .status(200)
        .header("content-type", "text/html")
        .body(message.into())
        .map_err(Box::new)?;
    Ok(resp)
}

async fn compute(id: u16) -> Result<(), DataFusionError> {
    let start = Instant::now();
    let bucket_name = "pensioncalcseast1";

    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    let s3_client = aws_sdk_s3::Client::new(&config);

    let mut file =
        File::create(format!("/tmp/input{}.parquet", id)).expect("Failed to create result file");

    let mut object = s3_client
        .get_object()
        .bucket(bucket_name)
        .key(format!("part_account/{}/pa_detail.parquet", id))
        .send()
        .await
        .expect("Failed to get the object from S3");

    let mut byte_count = 0_usize;

    while let Some(bytes) = object
        .body
        .try_next()
        .await
        .expect("Failed to get next bytes from S3")
    {
        let bytes_len = bytes.len();
        file.write_all(&bytes).expect("Failed to write data");
        byte_count = bytes_len;
    }

    tracing::info!("Wrote {byte_count} bytes");
    // create local session context
    let ctx = SessionContext::new();

    ctx.register_parquet(
        &format!("pad{}", id),
        &format!("/tmp/input{}.parquet", id),
        ParquetReadOptions::default(),
    )
    .await
    .expect("Failed to register parquet file");

    let query = (1..=48)
        .map(|i| format!("amount{}", i))
        .map(|column_name| format!("SUM({}) as {}", column_name, column_name))
        .collect::<Vec<String>>()
        .join(", ");

    let sql_query = format!(
        "SELECT stop_date, {} FROM pad{} GROUP BY stop_date",
        query, id
    );
    // execute the query
    let df = ctx.sql(&sql_query).await?;

    let filename = format!("/tmp/result{}.json", id);

    // df.write_json(&filename)
    //     .await
    //     .expect("Failed to write Json file");

    let path = Path::new(&filename);
    let file = fs::File::create(path)?;

    let mut writer = json::LineDelimitedWriter::new(file);

    let recs = df.collect().await?;
    for rec in recs {
        writer.write(&rec).expect("Write failed")
    }
    writer.finish().unwrap();

    let s3_key = format!("results/result{}.json", id);

    let body = ByteStream::from_path(Path::new(&filename)).await;

    let response = s3_client
        .put_object()
        .bucket(bucket_name)
        .body(body.unwrap())
        .key(&s3_key)
        .send()
        .await;

    match response {
        Ok(_) => {
            tracing::info!(
                filename = %filename,
                "data successfully stored in S3",
            );
            // Return `Response` (it will be serialized to JSON automatically by the runtime)
        }
        Err(err) => {
            // In case of failure, log a detailed error to CloudWatch.
            tracing::error!(
                err = %err,
                filename = %filename,
                "failed to upload data to S3"
            );
        }
    }

    let end = Instant::now();
    tracing::info!(
        "Finished executing for task {} in time {:?}",
        id,
        end - start
    );
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        // disable printing the name of the module in every log line.
        .with_target(false)
        // disabling time is handy because CloudWatch will add the ingestion time.
        .without_time()
        .init();

    run(service_fn(function_handler)).await
}
