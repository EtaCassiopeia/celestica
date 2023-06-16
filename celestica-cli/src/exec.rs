// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Execution functions

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;
use std::sync::Arc;
use std::time::Instant;

use datafusion::datasource::listing::ListingTableUrl;
use datafusion::error::{DataFusionError, Result};
use datafusion::logical_expr::{CreateExternalTable, DdlStatement, LogicalPlan};
use datafusion::prelude::SessionContext;
use object_store::ObjectStore;
use rustyline::error::ReadlineError;
use rustyline::Editor;
use url::Url;

use crate::command::{Command, OutputFormat};
use crate::helper::{unescape_input, CliHelper};
use crate::print_options::PrintOptions;

/// run and execute SQL statements and commands against a context with the given print options
pub async fn exec_from_repl(
    ctx: &mut SessionContext,
    print_options: &mut PrintOptions,
) -> rustyline::Result<()> {
    let mut rl = Editor::new()?;
    rl.set_helper(Some(CliHelper::default()));
    rl.load_history(".history").ok();

    let mut print_options = print_options.clone();

    loop {
        match rl.readline("❯ ") {
            Ok(line) if line.starts_with('\\') => {
                rl.add_history_entry(line.trim_end())?;
                let command = line.split_whitespace().collect::<Vec<_>>().join(" ");
                if let Ok(cmd) = &command[1..].parse::<Command>() {
                    match cmd {
                        Command::Quit => break,
                        Command::OutputFormat(subcommand) => {
                            if let Some(subcommand) = subcommand {
                                if let Ok(command) = subcommand.parse::<OutputFormat>() {
                                    if let Err(e) =
                                        command.execute(&mut print_options).await
                                    {
                                        eprintln!("{e}")
                                    }
                                } else {
                                    eprintln!(
                                        "'\\{}' is not a valid command",
                                        &line[1..]
                                    );
                                }
                            } else {
                                println!("Output format is {:?}.", print_options.format);
                            }
                        },
                        _ => {
                            if let Err(e) = cmd.execute(ctx, &mut print_options).await {
                                eprintln!("{e}")
                            }
                        },
                    }
                } else {
                    eprintln!("'\\{}' is not a valid command", &line[1..]);
                }
            },
            Ok(line) => {
                rl.add_history_entry(line.trim_end())?;
                match unescape_input(&line) {
                    Ok(sql) => match exec_and_print(ctx, &print_options, sql).await {
                        Ok(_) => {},
                        Err(err) => eprintln!("{err}"),
                    },
                    Err(err) => eprintln!("{err}"),
                }
            },
            Err(ReadlineError::Interrupted) => {
                println!("^C");
                continue;
            },
            Err(ReadlineError::Eof) => {
                println!("\\q");
                break;
            },
            Err(err) => {
                eprintln!("Unknown error happened {:?}", err);
                break;
            },
        }
    }

    rl.save_history(".history")
}

async fn exec_and_print(
    ctx: &mut SessionContext,
    print_options: &PrintOptions,
    sql: String,
) -> Result<()> {
    let now = Instant::now();

    let plan = ctx.state().create_logical_plan(&sql).await?;
    let df = match &plan {
        LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => {
            create_external_table(ctx, cmd)?;
            ctx.execute_logical_plan(plan).await?
        },
        _ => ctx.execute_logical_plan(plan).await?,
    };

    let results = df.collect().await?;
    print_options.print_batches(&results, now)?;

    Ok(())
}

fn create_external_table(ctx: &SessionContext, cmd: &CreateExternalTable) -> Result<()> {
    let table_path = ListingTableUrl::parse(&cmd.location)?;
    let scheme = table_path.scheme();
    let url: &Url = table_path.as_ref();

    // registering the cloud object store dynamically using cmd.options
    let store = match scheme {
        //TODO: add support for other object stores
        // "s3" => {
        //     let builder = get_s3_object_store_builder(url, cmd)?;
        //     Arc::new(builder.build()?) as Arc<dyn ObjectStore>
        // }
        _ => {
            // for other types, try to get from the object_store_registry
            ctx.runtime_env()
                .object_store_registry
                .get_store(url)
                .map_err(|_| {
                    DataFusionError::Execution(format!(
                        "Unsupported object store scheme: {}",
                        scheme
                    ))
                })?
        },
    };

    ctx.runtime_env().register_object_store(url, store);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn create_external_table_test(location: &str, sql: &str) -> Result<()> {
        let ctx = SessionContext::new();
        let plan = ctx.state().create_logical_plan(&sql).await?;

        match &plan {
            LogicalPlan::Ddl(DdlStatement::CreateExternalTable(cmd)) => {
                create_external_table(&ctx, cmd)?;
            },
            _ => assert!(false),
        };

        ctx.runtime_env()
            .object_store(ListingTableUrl::parse(location)?)?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_s3() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let region = "fake_us-east-2";
        let session_token = "fake_session_token";
        let location = "s3://bucket/path/file.parquet";

        // Missing region
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("Missing region"));

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}', 'region' '{region}', 'session_token' '{session_token}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_oss() -> Result<()> {
        let access_key_id = "fake_access_key_id";
        let secret_access_key = "fake_secret_access_key";
        let endpoint = "fake_endpoint";
        let location = "oss://bucket/path/file.parquet";

        // Should be OK
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('access_key_id' '{access_key_id}', 'secret_access_key' '{secret_access_key}', 'endpoint' '{endpoint}') LOCATION '{location}'");
        create_external_table_test(location, &sql).await?;

        Ok(())
    }

    #[tokio::test]
    async fn create_object_store_table_gcs() -> Result<()> {
        let service_account_path = "fake_service_account_path";
        let service_account_key =
            "{\"private_key\": \"fake_private_key.pem\",\"client_email\":\"fake_client_email\"}";
        let application_credentials_path = "fake_application_credentials_path";
        let location = "gcs://bucket/path/file.parquet";

        // for service_account_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('service_account_path' '{service_account_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("No such file or directory"));

        // for service_account_key
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET OPTIONS('service_account_key' '{service_account_key}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("No RSA key found in pem file"));

        // for application_credentials_path
        let sql = format!("CREATE EXTERNAL TABLE test STORED AS PARQUET
            OPTIONS('application_credentials_path' '{application_credentials_path}') LOCATION '{location}'");
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("A configuration file was passed"));

        Ok(())
    }

    #[tokio::test]
    async fn create_external_table_local_file() -> Result<()> {
        let location = "/path/to/file.parquet";

        // Ensure that local files are also registered
        let sql = format!(
            "CREATE EXTERNAL TABLE test STORED AS PARQUET LOCATION '{location}'"
        );
        let err = create_external_table_test(location, &sql)
            .await
            .unwrap_err();
        assert!(err.to_string().contains("No such file or directory"));

        Ok(())
    }
}
