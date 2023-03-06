use std::fs::File;
use anyhow::{anyhow, Context, Result};
use clap::Parser;
use minix_fs::{Partition, PartitionTree, MinixPartition, Directory, FileSystemRef};

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
    verbosity: Option<bool>,

    imagefile: String,

    path: Option<String>,

}

fn minls(partition: &Partition, args: Args) -> Result<()> {
    let minixfs = MinixPartition::new(partition)?;
    let root_ref = minixfs.root_ref()?;
    let root = root_ref.get()?;
    let file_system_ref =  root.get_at_path(args.path.unwrap_or(String::from("/")).as_str())?;
    if let FileSystemRef::DirectoryRef(d) = file_system_ref {
        d.get()?.iter().for_each(|fsr| println!("{}", fsr));
    }
    else {
        println!("{}", file_system_ref);
    }
    Ok(())
}


fn main() {
    let args = Args::parse();

    let partition_tree = PartitionTree::new(&args.imagefile).expect("");

    //TODO these shouldn't be panics
    let result = match (args.part, args.subpart) {
        (Some(part), Some(subpart)) => {
            let PartitionTree::SubPartitions(primary_table) = partition_tree else {
                panic!("expected subpartitions")
            };
            let Some(Some(PartitionTree::SubPartitions(sub_table))) = &primary_table.get(part) else {
                panic!("invalid '-p' argument or invalid primary partition table")
            };
            let Some(Some(PartitionTree::Partition(partition))) = &sub_table.get(subpart) else {
                panic!("invalid '-s' argument or invalid secondary partition table")
            };
            minls(partition, args)

        }
        (Some(part), None) => {
            let PartitionTree::SubPartitions(primary_table) = partition_tree else {
                panic!("expected subpartitions")
            };
            let Some(Some(PartitionTree::Partition(partition))) = &primary_table.get(part) else {
                panic!("invalid '-p' argument or invalid primary partition table")
            };
            minls(partition, args)
        }
        _ => {
            let PartitionTree::Partition(partition) = partition_tree else {
                panic!("invalid '-p' argument or invalid primary partition table")
            };
            minls(&partition, args)
        }
    };
    if let Err(error) = result {
        println!("{}", error);
    }
}