use std::{io::Write, os::fd::AsFd};
use anyhow::{anyhow, Result};
use clap::Parser;
use log::{LevelFilter, info};
use simplelog::{SimpleLogger, Config};
use minix_fs::{FileSystemRef, FileSystemRefFunctionality, MinixPartition, Partition, PartitionTree};

#[derive(Parser, Debug)]
struct Args {
    /// choose a primary parition on the image
    #[arg(short)]
    part: Option<usize>,

    /// choose a subpartition
    #[arg(short)]
    subpart: Option<usize>,

    /// increase verbosity level
    #[arg(short)]
    verbosity: bool,

    imagefile: String,

    srcpath: String,

    dstpath: Option<String>,
}

fn minget(partition: &Partition, args: Args, mut dest: std::fs::File) -> Result<()> {
    let minixfs = MinixPartition::new(partition)?;
    let root_ref = minixfs.root_ref()?;
    let root = root_ref.get()?;
    let file_system_ref = root.get_at_path(args.srcpath.as_str())?;

    let FileSystemRef::FileRef(file_ref) = file_system_ref else {
        return Err(anyhow!("'srcpath' must refer to a file"))
    };
    info!("{:#?}", file_ref.inode());
    let file_bytes = file_ref.get()?;
    let result = dest.write(&file_bytes);

    match result {
        Ok(bytes_written) => {
            if bytes_written != file_bytes.len() {
                return Err(anyhow!("failed to write entire file to destination"));
            }
            Ok(())
        }
        Err(io_err) => Err(anyhow!(io_err)),
    }
}

fn minget_main(args: Args) -> Result<()> {
    let dest = if let Some(dstpath) = args.dstpath.clone() {
        std::fs::File::create(dstpath)?
    } else {
        std::io::stdout().as_fd().try_clone_to_owned()?.into()
    };
    let partition_tree = PartitionTree::new(&args.imagefile)?;
    let log_level = if args.verbosity {LevelFilter::Info} else {LevelFilter::Off};
    SimpleLogger::init(log_level, Config::default())?;
    match (args.part, args.subpart) {
        (Some(part), Some(subpart)) => {
            let PartitionTree::SubPartitions(primary_table) = partition_tree else {
                return Err(anyhow!("expected subpartitions because '-p' was given"))
            };
            let Some(Some(PartitionTree::SubPartitions(sub_table))) = &primary_table.get(part) else {
                return Err(anyhow!("invalid '-p' argument or invalid primary partition table"))
            };
            let Some(Some(PartitionTree::Partition(partition))) = &sub_table.get(subpart) else {
                return Err(anyhow!("invalid '-s' argument or invalid secondary partition table"))
            };
            minget(partition, args, dest)
        }
        (Some(part), None) => {
            let PartitionTree::SubPartitions(primary_table) = partition_tree else {
                return Err(anyhow!("expected subpartitions because '-p' was given"))
            };
            let Some(Some(PartitionTree::Partition(partition))) = &primary_table.get(part) else {
                return Err(anyhow!("invalid '-p' argument or invalid primary partition table"))
            };
            minget(partition, args, dest)
        }
        _ => {
            let PartitionTree::Partition(partition) = partition_tree else {
                return Err(anyhow!("expected image without partitions"))
            };
            minget(&partition, args, dest)
        }
    }
}

fn main() {
    let args = Args::parse();
    if let Err(error) = minget_main(args) {
        eprintln!("{}", error);
        std::process::exit(-1);
    }
}
