#![deny(
    rust_2018_idioms,
    let_underscore_drop,
    clippy::assertions_on_result_states,
    clippy::bool_to_int_with_if,
    clippy::borrow_as_ptr,
    clippy::cloned_instead_of_copied,
    clippy::create_dir,
    clippy::debug_assert_with_mut_call,
    clippy::default_union_representation,
    clippy::deref_by_slicing,
    clippy::derive_partial_eq_without_eq,
    clippy::empty_drop,
    clippy::empty_line_after_outer_attr,
    clippy::empty_structs_with_brackets,
    clippy::equatable_if_let,
    clippy::expl_impl_clone_on_copy,
    clippy::explicit_deref_methods,
    clippy::explicit_into_iter_loop,
    clippy::explicit_iter_loop,
    clippy::filetype_is_file,
    clippy::filter_map_next,
    clippy::flat_map_option,
    clippy::float_cmp,
    clippy::float_cmp_const,
    clippy::format_push_string,
    clippy::from_iter_instead_of_collect,
    clippy::get_unwrap,
    clippy::implicit_clone,
    clippy::implicit_saturating_sub,
    clippy::imprecise_flops,
    clippy::index_refutable_slice,
    clippy::inefficient_to_string,
    clippy::invalid_upcast_comparisons,
    clippy::iter_on_empty_collections,
    clippy::iter_on_single_items,
    clippy::large_types_passed_by_value,
    clippy::let_unit_value,
    clippy::lossy_float_literal,
    clippy::macro_use_imports,
    clippy::manual_assert,
    clippy::manual_clamp,
    clippy::manual_instant_elapsed,
    clippy::manual_let_else,
    clippy::manual_ok_or,
    clippy::manual_string_new,
    clippy::map_unwrap_or,
    clippy::match_bool,
    clippy::match_same_arms,
    clippy::missing_const_for_fn,
    clippy::mut_mut,
    clippy::mutex_atomic,
    clippy::mutex_integer,
    clippy::naive_bytecount,
    clippy::needless_bitwise_bool,
    clippy::needless_collect,
    clippy::needless_continue,
    clippy::needless_for_each,
    clippy::nonstandard_macro_braces,
    clippy::or_fun_call,
    clippy::path_buf_push_overwrite,
    clippy::ptr_as_ptr,
    clippy::range_minus_one,
    clippy::range_plus_one,
    clippy::redundant_else,
    clippy::rest_pat_in_fully_bound_structs,
    clippy::semicolon_if_nothing_returned,
    clippy::significant_drop_in_scrutinee,
    clippy::str_to_string,
    clippy::string_add,
    clippy::string_add_assign,
    clippy::string_lit_as_bytes,
    clippy::string_to_string,
    clippy::suboptimal_flops,
    clippy::suspicious_to_owned,
    clippy::trait_duplication_in_bounds,
    clippy::trivially_copy_pass_by_ref,
    clippy::type_repetition_in_bounds,
    clippy::unchecked_duration_subtraction,
    clippy::undocumented_unsafe_blocks,
    clippy::unicode_not_nfc,
    clippy::uninlined_format_args,
    clippy::unnecessary_join,
    clippy::unnecessary_self_imports,
    clippy::unneeded_field_pattern,
    clippy::unnested_or_patterns,
    clippy::unseparated_literal_suffix,
    clippy::unused_peekable,
    clippy::unused_rounding,
    clippy::use_self,
    clippy::used_underscore_binding,
    clippy::useless_let_if_seq
)]

mod download;
mod log;
mod log_source;
mod opts;
mod render;
mod review;
mod softmax;
mod state;
mod tactics;
mod tehai;

use crate::log_source::LogSource;
use crate::opts::{AkochanOptions, Engine, InputOptions, MortalOptions, Options, OutputOptions};
use crate::render::View;
use crate::review::{Review, akochan, mortal};
use chrono::SubsecRound;
use convlog::tenhou::{GameLength, Log, RawLog};
use convlog::tenhou_to_mjai;
use std::fs::{self, File, ReadDir};
use std::io;
use std::io::prelude::*;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail, ensure};
use clap::{Parser, ValueEnum};
use serde_json as json;
use chrono::Local;

macro_rules! canonicalize {
    ($path:ident) => {{
        let p = if $path.as_os_str().is_empty() {
            Path::new(".")
        } else {
            $path.as_ref()
        };
        dunce::canonicalize(p).with_context(|| {
            format!(
                "failed to canonicalize {}: \"{}\" (does it exist?)",
                stringify!($path),
                $path.display(),
            )
        })
    }};
}

enum ReportOutput {
    File(PathBuf),
    Stdout,
}

fn process_file(input_path: &Path, output_dir: &Path) -> Result<()> {
    log!("processing file: {:?}", input_path);

    // 读取文件内容
    let mut file = File::open(input_path)
        .with_context(|| format!("failed to open file: {:?}", input_path))?;
    let mut body = String::new();
    file.read_to_string(&mut body)
        .with_context(|| format!("failed to read file: {:?}", input_path))?;

    // 解析 RawLog（原来是从 json 解析的）
    let raw_log: RawLog = json::from_str(&body)
        .with_context(|| format!("failed to parse tenhou.net/6 log from file: {:?}", input_path))?;

    // convert from RawLog to Log
    let log = Log::try_from(raw_log).context("invalid log")?;

    // convert from tenhou::Log to Vec<mjai::Event>
    let begin_convert_log = Local::now();
    log!("converting {:?} to mjai events...", input_path.file_name().unwrap_or_default());
    let events = tenhou_to_mjai(&log)
        .with_context(|| format!("failed to convert {:?} into mjai format", input_path))?;

    // 创建输出文件名（保持原文件名，但可以修改扩展名）
    let output_filename = input_path
        .file_stem()
        .unwrap_or_default()
        .to_string_lossy()
        .to_string() + ".json";

    let output_path = output_dir.join(output_filename);

    // 创建并写入输出文件
    let mut file = File::create(&output_path)
        .with_context(|| format!("failed to create output file: {:?}", output_path))?;

    for event in &events {
        let to_write = serde_json::to_string(event)
            .context("failed to serialize event to JSON")?;
        writeln!(file, "{}", to_write)
            .with_context(|| format!("failed to write to output file: {:?}", output_path))?;
    }

    log!("successfully converted {:?} -> {:?}",
         input_path.file_name().unwrap_or_default(),
         output_path.file_name().unwrap_or_default());

    Ok(())
}

fn process_directory(input_dir: &Path, output_dir: &Path) -> Result<()> {
    // 检查输入目录是否存在
    if !input_dir.exists() {
        anyhow::bail!("input directory does not exist: {:?}", input_dir);
    }

    if !input_dir.is_dir() {
        anyhow::bail!("input path is not a directory: {:?}", input_dir);
    }

    // 创建输出目录（如果不存在）
    fs::create_dir_all(output_dir)
        .with_context(|| format!("failed to create output directory: {:?}", output_dir))?;

    log!("processing directory: {:?}", input_dir);
    log!("output directory: {:?}", output_dir);

    // 遍历输入目录
    let entries = fs::read_dir(input_dir)
        .with_context(|| format!("failed to read input directory: {:?}", input_dir))?;

    let mut processed_count = 0;
    let mut error_count = 0;

    for entry in entries {
        match entry {
            Ok(entry) => {
                let path = entry.path();

                // 跳过子目录（如果你需要递归处理，可以修改这里）
                if path.is_dir() {
                    log!("skipping subdirectory: {:?}", path);
                    continue;
                }

                // 可以根据文件扩展名过滤文件
                if let Some(ext) = path.extension() {
                    // 只处理特定扩展名的文件，例如 .log、.txt 等
                    // 这里可以根据你的需求调整
                    if ext != "json" && ext != "txt" {
                        continue;
                    }
                }

                match process_file(&path, output_dir) {
                    Ok(_) => {
                        processed_count += 1;
                    }
                    Err(e) => {
                        error_count += 1;
                        eprintln!("error processing {:?}: {}", path, e);
                    }
                }
            }
            Err(e) => {
                error_count += 1;
                eprintln!("error reading directory entry: {}", e);
            }
        }
    }

    log!("processing completed: {} files processed, {} errors",
         processed_count, error_count);

    if error_count > 0 {
        anyhow::bail!("some files failed to process ({} errors)", error_count);
    }

    Ok(())
}

// 主函数示例
fn main() -> Result<()> {
    // 使用示例：从命令行参数获取输入输出目录
    let args: Vec<String> = std::env::args().collect();

    if args.len() != 3 {
        eprintln!("Usage: {} <input_directory> <output_directory>", args[0]);
        std::process::exit(1);
    }

    let input_dir = Path::new(&args[1]);
    let output_dir = Path::new(&args[2]);

    process_directory(input_dir, output_dir)
}
