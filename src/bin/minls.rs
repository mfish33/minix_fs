use anyhow::{anyhow, Result};
use clap::Parser;
use minix_fs::{Partition, PartitionTree, MinixPartition, FileSystemRef};

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

fn minls_main(args: Args) -> Result<()> {
    let partition_tree = PartitionTree::new(&args.imagefile)?;
    //TODO verbosity
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
            minls(partition, args)

        }
        (Some(part), None) => {
            let PartitionTree::SubPartitions(primary_table) = partition_tree else {
                return Err(anyhow!("expected subpartitions because '-p' was given"))
            };
            let Some(Some(PartitionTree::Partition(partition))) = &primary_table.get(part) else {
                return Err(anyhow!("invalid '-p' argument or invalid primary partition table"))
            };
            minls(partition, args)
        }
        _ => {
            let PartitionTree::Partition(partition) = partition_tree else {
                return Err(anyhow!("expected image without partitions"))
            };
            minls(&partition, args)
        }
    }
}


fn main() {
    let args = Args::parse();
    if let Err(error) = minls_main(args){
        // TODO print to stderr
        println!("{}", error);
    }
}