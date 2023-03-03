use anyhow::{anyhow, Context, Result};
use std::{fs, ops::Deref, os::windows::prelude::FileExt, rc::Rc};

const SECTOR_SIZE: u64 = 512;
const SUPER_BLOCK_OFFSET: u64 = 1024;
const MINIX_MAGIC_NUMBER: i16 = 0x4D5A;
const PARTITION_TABLE_OFFSET: u64 = 0x1be;
const MINIX_PARTITION_TYPE: u8 = 0x81;

macro_rules! From_Bytes {
    ($struct_name: ident) => {
        impl $struct_name {
            pub fn from_partition_offset(
                partition: &Partition,
                offset: u64,
            ) -> Result<$struct_name> {
                let mut buffer = $struct_name::get_sized_buffer();
                let bytes_read = partition.read_at(&mut buffer, offset).with_context(|| {
                    format!(
                        "Failed to read {} at offset {}",
                        stringify!($Struct_name),
                        offset
                    )
                })?;

                if bytes_read != std::mem::size_of::<$struct_name>() {
                    Err(anyhow!(
                        "Failed to create {} at offset {}. Expected {} bytes got {} bytes",
                        stringify!($Struct_name),
                        offset,
                        std::mem::size_of::<$struct_name>(),
                        bytes_read
                    ))
                } else {
                    Ok(unsafe { std::mem::transmute(buffer) })
                }
            }

            fn get_sized_buffer() -> [u8; std::mem::size_of::<$struct_name>()] {
                [0; std::mem::size_of::<$struct_name>()]
            }

            fn size() -> usize {
                std::mem::size_of::<$struct_name>()
            }
        }
    };
}

#[repr(C)]
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
struct PartitionTableEntry {
    bootind: u8,
    start_head: u8,
    start_sec: u8,
    start_cyl: u8,
    part_type: u8,
    end_head: u8,
    end_sec: u8,
    end_cyl: u8,
    l_first: u32,
    size: u32,
}
From_Bytes!(PartitionTableEntry);

#[repr(C)]
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
struct SuperBlock {
    inodes: u32,
    pad1: u16,
    i_blocks: i16,      /* # of blocks used by inode bit map */
    z_blocks: i16,      /* # of blocks used by zone bit map */
    firstdata: u16,     /* number of first data zone */
    log_zone_size: i16, /* log2 of blocks per zone */
    pad2: i16,          /* make things line up again */
    max_file: u32,      /* maximum file size */
    zones: u32,         /* number of zones on disk */
    magic: i16,         /* magic number */
    pad3: i16,          /* make things line up again */
    block_size: u16,    /* block size in bytes */
    subversion: u8,     /* filesystem sub–version */
}

const DIRECT_ZONE_COUNT: usize = 7;

#[repr(C)]
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
// TODO: Remove public
pub struct Inode {
    mode: u16,  /* mode */
    links: u16, /* number or links */
    uid: u16,
    gid: u16,
    size: u32,
    atime: i32,
    mtime: i32,
    ctime: i32,
    direct_zones: [u32; DIRECT_ZONE_COUNT],
    indirect: u32,
    two_indirect: u32,
    unused: u32,
}
From_Bytes!(Inode);

impl Inode {
    pub fn zone_iter<'b, 'a:'b>(&'b self, part: &'a MinixPartition) -> impl Iterator<Item = u32> + 'b {
       self.zone_iter_inner(part).take((self.size as u64 / part.super_block.zone_size()) as usize + 1)
    }

    fn zone_iter_inner<'b, 'a:'b>(&'b self, part: &'a MinixPartition) -> Box<dyn Iterator<Item = u32> + 'b> {
        // Compiler will potentially copy from unaligned memory. This solves that issue
        let direct_zones = self.direct_zones;
        let direct_zone_vec = direct_zones.to_vec();

        let indirect_zone = self.indirect;


        let iter_ret = IndirectIterator {
            zone_ptrs: direct_zone_vec,
            idx: 0,
        };
        
        if self.indirect == 0 {
            return Box::new(iter_ret)
        }

        let iter_ret = iter_ret.chain(IndirectIterator::new(part, indirect_zone));
        if self.two_indirect == 0 {
            return Box::new(iter_ret)
        }
        
        Box::new(iter_ret.chain(
            IndirectIterator::new(part, self.two_indirect)
                .filter(|zone| *zone != 0)
                .flat_map(|zone| IndirectIterator::new(part, zone)),
        ))
    }
}

struct IndirectIterator {
    zone_ptrs: Vec<u32>,
    idx: usize,
}

impl Iterator for IndirectIterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.zone_ptrs.get(self.idx).map(|x| *x);
        self.idx += 1;
        ret
    }
}

impl IndirectIterator {
    fn new(partition: &MinixPartition, zone: u32) -> Self {
        assert_ne!(zone, 0, "IndirectIterator expects a valid zone");

        let zone_data = partition.read_zone(zone).expect("indirect iterator could not read zone");
        let zone_ptrs: Vec<_> = zone_data
            .chunks(4)
            .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
            .collect();
        IndirectIterator { zone_ptrs, idx: 0 }
    }
}

#[repr(C)]
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
struct DirectoryEntry {
    inode: u32,
    file_name: [u8; 60],
}
From_Bytes!(DirectoryEntry);

impl SuperBlock {
    fn new(partition_entry: &Partition) -> Result<Self> {
        let super_block = SuperBlock::from_partition_offset(partition_entry, SUPER_BLOCK_OFFSET)?;
        let block_magic = super_block.magic;
        if block_magic != MINIX_MAGIC_NUMBER {
            Err(anyhow!(
                "Bad magic number. ({:x})\nThis doesn't look like a MINIX filesystem",
                block_magic
            ))
        } else {
            Ok(super_block)
        }
    }
}

impl SuperBlock {
    fn zone_size(&self) -> u64 {
        (self.block_size as u64) << self.log_zone_size
    }
}

#[derive(Debug, Clone)]
pub enum PartitionTree {
    Partition(Partition),
    SubPartitions(Box<[Option<PartitionTree>; 4]>),
}

#[derive(Debug, Clone)]
pub struct Partition {
    file: Rc<fs::File>,
    start_bytes: u64,
    size_bytes: u64,
}

impl Partition {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if offset + buf.len() as u64 > self.size_bytes {
            return Err(anyhow!("Failed to read from partition. Detected over read.\nPartition {:?}\noffset: {}, len: {}", self, offset, buf.len()));
        }
        let bytes = self.file.seek_read(buf, offset + self.start_bytes)?;
        Ok(bytes)
    }
}

pub struct MinixPartition {
    partition: Partition,
    super_block: SuperBlock,
}

impl MinixPartition {
    pub fn new(partition: Partition) -> Result<Self> {
        // TODO: Somehow check the partition table entry for the partition type??
        let super_block = SuperBlock::new(&partition)?;
        Ok(MinixPartition {
            partition,
            super_block,
        })
    }

    fn read_zone(&self, zone: u32) -> Result<Vec<u8>> {
        let zone_size = self.super_block.zone_size();
        let mut zone_data = vec![0; zone_size as usize];
        let bytes_read = self.read_at(&mut zone_data,  zone_size * zone as u64)? as u64;
        if bytes_read != zone_size {
            Err(anyhow!("Failed to read zone {} of zone_size: {}. Relieved {} bytes", zone, zone_size, bytes_read))
        } else  {
            Ok(zone_data)
        }
    }

    pub fn root_directory(&self) -> Result<Inode> {
        // Add 2 to account for first two reserved blocks
        let first_inode_block = (2 + self.super_block.i_blocks + self.super_block.z_blocks) as u64;
        let inode_file_offset = first_inode_block * self.super_block.block_size as u64;
        Inode::from_partition_offset(self, inode_file_offset)
    }
}

impl Deref for MinixPartition {
    type Target = Partition;

    fn deref(&self) -> &Self::Target {
        &self.partition
    }
}

impl PartitionTree {
    pub fn new(file_path: &str) -> Result<PartitionTree> {
        let file = fs::File::open(file_path)
            .with_context(|| format!("Could not open disk located at {}", file_path))?;
        let possible_partition = Partition {
            size_bytes: file.metadata()?.len(),
            file: Rc::new(file),
            start_bytes: 0,
        };
        Self::get_partitions(possible_partition)
    }

    pub fn get_sub_partition(&self, idx: usize) -> Option<&PartitionTree> {
        match self {
            PartitionTree::Partition(_) => None,
            PartitionTree::SubPartitions(subs) => subs[idx].as_ref(),
        }
    }

    fn get_partitions(possible_partition: Partition) -> Result<PartitionTree> {
        let mut buf = [0u8; 2];
        // TODO: MAKE CLEANER
        possible_partition
            .file
            .seek_read(&mut buf, 510 + possible_partition.start_bytes)
            .ok();

        if !(buf[0] == 0x55 && buf[1] == 0xAA) {
            // No partition table
            return Ok(PartitionTree::Partition(possible_partition));
        }

        let mut partition_table = Box::new([None, None, None, None]);
        for i in 0..partition_table.len() {
            let partition_table_entry = PartitionTableEntry::from_partition_offset(
                &possible_partition,
                PARTITION_TABLE_OFFSET + (PartitionTableEntry::size() * i) as u64,
            )?;

            // it is zero if it is an empty partition
            // TODO: Check this assumption
            if partition_table_entry.size != 0 {
                partition_table[i] = Some(PartitionTree::get_partitions(Partition {
                    file: possible_partition.file.clone(),
                    start_bytes: partition_table_entry.l_first as u64 * SECTOR_SIZE,
                    size_bytes: partition_table_entry.size as u64 * SECTOR_SIZE,
                })?);
            }
        }

        Ok(PartitionTree::SubPartitions(partition_table))
    }
}

impl TryFrom<&PartitionTree> for Partition {
    type Error = ();

    fn try_from(partition_tree: &PartitionTree) -> std::result::Result<Self, Self::Error> {
        match partition_tree {
            PartitionTree::Partition(part) => Ok(part.clone()),
            _ => std::result::Result::Err(()),
        }
    }
}

From_Bytes!(SuperBlock);

#[cfg(test)]
mod tests {
    use std::fs;

    use super::*;

    // #[test]
    // fn test_get_super_block() -> Result<()> {
    //     let file = fs::File::open("./Images/Files").unwrap();
    //     let super_block = SuperBlock::new(&file)?;
    //     println!("{:?}", super_block);
    //     Ok(())
    // }

    #[test]
    fn test_get_root_directory() -> Result<()> {
        let partition: Partition = (&PartitionTree::new("./Images/BigIndirectDirs")?).try_into().unwrap();
        let minix_partition = MinixPartition::new(partition)?;
        let root_inode = minix_partition.root_directory()?;
        println!("{:?}", root_inode);
        assert!(0o0040000 & root_inode.mode == 0o0040000);

        for zone in root_inode.zone_iter(&minix_partition) {
            if zone == 0 {
                continue;
            }
            println!("Getting zone {zone}");
            let mut i = 0;
            loop {
                let directory_entry = DirectoryEntry::from_partition_offset(
                    &minix_partition,
                    minix_partition.super_block.zone_size() * zone as u64
                        + (i * std::mem::size_of::<DirectoryEntry>()) as u64,
                )?;
                println!("{:?}", directory_entry);
                if directory_entry.inode == 0 {
                    break;
                }
                i += 1;
            }
        }

        Ok(())
    }

    #[test]
    fn test_partition_table() -> Result<()> {
        let partition_tree = PartitionTree::new("./Images/Partitioned").unwrap();
        let PartitionTree::SubPartitions(subs) = partition_tree else {
            panic!("Did not get superstitions back")
        };

        for sub_partition in *subs {
            assert!(sub_partition.is_some());
        }

        Ok(())
    }
}
