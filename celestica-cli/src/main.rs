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

use std::env;
use std::path::Path;
use std::sync::Arc;

use celestica_arrow::datasource::lance_table_factory::LanceListingTableFactory;
use celestica_cli::catalog::DynamicFileCatalog;
use celestica_cli::print_format::PrintFormat;
use celestica_cli::print_options::PrintOptions;
use celestica_cli::{exec, CELESTICA_CLI_VERSION};
use clap::Parser;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::context::{SessionConfig, SessionState};
use datafusion::execution::runtime_env::{RuntimeConfig, RuntimeEnv};
use datafusion::prelude::SessionContext;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

#[derive(Debug, Parser, PartialEq)]
#[clap(author, version, about, long_about= None)]
struct Args {
    #[clap(
        short = 'p',
        long,
        help = "Path to your data, default to current directory",
        validator(is_valid_data_dir)
    )]
    data_path: Option<String>,

    #[clap(
        short = 'c',
        long,
        help = "The batch size of each query, or use DataFusion default",
        validator(is_valid_batch_size)
    )]
    batch_size: Option<usize>,

    #[clap(long, arg_enum, default_value_t = PrintFormat::Table)]
    format: PrintFormat,

    #[clap(
        short,
        long,
        help = "Reduce printing other than the results and work quietly"
    )]
    quiet: bool,
}

#[tokio::main]
pub async fn main() -> Result<()> {
    env_logger::init();
    let args = Args::parse();

    if !args.quiet {
        println!("DataFusion CLI v{}", CELESTICA_CLI_VERSION);
    }

    if let Some(ref path) = args.data_path {
        let p = Path::new(path);
        env::set_current_dir(p).unwrap();
    };

    let mut session_config = SessionConfig::from_env()?.with_information_schema(true);

    if let Some(batch_size) = args.batch_size {
        session_config = session_config.with_batch_size(batch_size);
    };

    let runtime_env = create_runtime_env()?;

    let mut state =
        SessionState::with_config_rt(session_config.clone(), Arc::new(runtime_env));
    let mut table_factories = state.table_factories_mut();
    table_factories.insert("LANCE".into(), Arc::new(LanceListingTableFactory::new()));
    let mut ctx = SessionContext::with_state(state);

    ctx.refresh_catalogs().await?;
    // install dynamic catalog provider that knows how to open files
    ctx.register_catalog_list(Arc::new(DynamicFileCatalog::new(
        ctx.state().catalog_list(),
        ctx.state_weak_ref(),
    )));

    let mut print_options = PrintOptions {
        format: args.format,
        quiet: args.quiet,
    };

    exec::exec_from_repl(&mut ctx, &mut print_options)
        .await
        .map_err(|e| DataFusionError::External(Box::new(e)))
}

fn create_runtime_env() -> Result<RuntimeEnv> {
    let rn_config = RuntimeConfig::new();
    RuntimeEnv::new(rn_config)
}

fn is_valid_file(dir: &str) -> Result<(), String> {
    if Path::new(dir).is_file() {
        Ok(())
    } else {
        Err(format!("Invalid file '{}'", dir))
    }
}

fn is_valid_data_dir(dir: &str) -> Result<(), String> {
    if Path::new(dir).is_dir() {
        Ok(())
    } else {
        Err(format!("Invalid data directory '{}'", dir))
    }
}

fn is_valid_batch_size(size: &str) -> Result<(), String> {
    match size.parse::<usize>() {
        Ok(size) if size > 0 => Ok(()),
        _ => Err(format!("Invalid batch size '{}'", size)),
    }
}
