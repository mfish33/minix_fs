use anyhow::{anyhow, Context, Result};
use enum_dispatch::enum_dispatch;
use std::{fmt::Display, fs, ops::Deref, os::unix::prelude::FileExt, rc::Rc};

const SECTOR_SIZE: u64 = 512;

macro_rules! From_Bytes {
    ($struct_name: ident) => {
        #[allow(dead_code)]
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

            pub fn from_bytes(bytes: Vec<u8>) -> Vec<$struct_name> {
                let amnt = bytes.len() / std::mem::size_of::<$struct_name>();
                let struct_slice = unsafe { std::mem::transmute::<&[u8], &[$struct_name]>(&bytes) };
                Vec::from(&struct_slice[0..amnt])
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

// ===================== Partitions =====================

#[derive(Debug, Clone)]
pub struct Partition {
    file: Rc<fs::File>,
    start_bytes: u64,
    size_bytes: u64,
    partition_table_abs_offset: u64,
}

impl Partition {
    fn read_at(&self, buf: &mut [u8], offset: u64) -> Result<usize> {
        if offset + buf.len() as u64 > self.size_bytes {
            return Err(anyhow!("Failed to read from partition. Detected over read.\nPartition {:?}\noffset: {}, len: {}", self, offset, buf.len()));
        }
        let bytes = self.file.read_at(buf, offset + self.start_bytes)?;
        Ok(bytes)
    }

    fn get_partition_table(&self) -> Result<PartitionTableEntry> {
        let mut buf = PartitionTableEntry::get_sized_buffer();
        let bytes = self
            .file
            .read_at(&mut buf, self.partition_table_abs_offset)?;
        if bytes != std::mem::size_of::<PartitionTableEntry>() {
            return Err(anyhow!("Failed to read partition table entry"));
        }
        Ok(unsafe { std::mem::transmute(buf) })
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

#[derive(Debug, Clone)]
pub enum PartitionTree {
    Partition(Partition),
    SubPartitions(Box<[Option<PartitionTree>; 4]>),
}

impl PartitionTree {
    const PARTITION_TABLE_OFFSET: u64 = 0x1be;

    pub fn new(file_path: &str) -> Result<PartitionTree> {
        let file = fs::File::open(file_path)
            .with_context(|| format!("Could not open disk located at {}", file_path))?;
        let possible_partition = Partition {
            size_bytes: file.metadata()?.len(),
            file: Rc::new(file),
            start_bytes: 0,
            partition_table_abs_offset: 0,
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
            .read_at(&mut buf, 510 + possible_partition.start_bytes)
            .ok();

        if !(buf[0] == 0x55 && buf[1] == 0xAA) {
            // No partition table
            return Ok(PartitionTree::Partition(possible_partition));
        }

        let mut partition_table = Box::new([None, None, None, None]);
        for i in 0..partition_table.len() {
            let relative_partition_table_offset =
                PartitionTree::PARTITION_TABLE_OFFSET + (PartitionTableEntry::size() * i) as u64;
            let partition_table_entry = PartitionTableEntry::from_partition_offset(
                &possible_partition,
                relative_partition_table_offset,
            )?;

            // it is zero if it is an empty partition
            // TODO: Check this assumption
            if partition_table_entry.size != 0 {
                partition_table[i] = Some(PartitionTree::get_partitions(Partition {
                    file: possible_partition.file.clone(),
                    start_bytes: partition_table_entry.l_first as u64 * SECTOR_SIZE,
                    size_bytes: partition_table_entry.size as u64 * SECTOR_SIZE,
                    partition_table_abs_offset: possible_partition.start_bytes
                        + relative_partition_table_offset,
                })?);
            }
        }

        Ok(PartitionTree::SubPartitions(partition_table))
    }
}

// ===================== Minix Primitives =====================

#[derive(Debug, Clone)]
pub struct MinixPartition<'a> {
    partition: &'a Partition,
    super_block: SuperBlock,
}

impl<'a> MinixPartition<'a> {
    const PART_TYPE: u8 = 0x81;

    pub fn new(partition: &'a Partition) -> Result<Self> {
        // TODO: Somehow check the partition table entry for the partition type??
        let partition_table = partition.get_partition_table()?;
        if partition_table.part_type != MinixPartition::PART_TYPE {
            return Err(anyhow!(
                "This doesn't look like a MINIX filesystem. {}",
                partition_table.part_type
            ));
        }
        let super_block = SuperBlock::new(partition)?;
        Ok(MinixPartition {
            partition,
            super_block,
        })
    }

    fn read_zone(&self, zone: u32) -> Result<Vec<u8>> {
        let zone_size = self.super_block.zone_size();
        let mut zone_data = vec![0; zone_size as usize];
        let bytes_read = self.read_at(&mut zone_data, zone_size * zone as u64)? as u64;
        if bytes_read != zone_size {
            Err(anyhow!(
                "Failed to read zone {} of zone_size: {}. Relieved {} bytes",
                zone,
                zone_size,
                bytes_read
            ))
        } else {
            Ok(zone_data)
        }
    }

    // NOTE: inodes are 1 indexed
    fn get_inode(&self, idx: u32) -> Result<Inode> {
        assert_ne!(idx, 0, "0 is not a valid inode");
        // Add 2 to account for first two reserved blocks
        let first_inode_block = (2 + self.super_block.i_blocks + self.super_block.z_blocks) as u64;
        let inode_file_offset = first_inode_block * self.super_block.block_size as u64;
        let inode_table_offset = Inode::size() * (idx - 1) as usize;
        Inode::from_partition_offset(self, inode_file_offset + inode_table_offset as u64)
    }

    fn read_block(&self, block_idx: u32) -> Result<Vec<u8>> {
        let block_size = self.super_block.block_size as u64;
        let mut block_data = vec![0; block_size as usize];
        let bytes_read = self.read_at(&mut block_data, block_size * block_idx as u64)? as u64;
        if bytes_read != block_size {
            Err(anyhow!(
                "Failed to read block {} of block_size: {}. Relieved {} bytes",
                block_idx,
                block_size,
                bytes_read
            ))
        } else {
            Ok(block_data)
        }
    }

    pub fn root_ref(&self) -> Result<DirectoryRef> {
        let inode = self.get_inode(1)?;
        Ok(DirectoryRef {
            inode,
            name: "/".to_string(),
            partition: self,
        })
    }
}

impl<'a> Deref for MinixPartition<'a> {
    type Target = Partition;

    fn deref(&self) -> &Self::Target {
        self.partition
    }
}

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
    subversion: u8,     /* filesystem sub???version */
}
From_Bytes!(SuperBlock);

impl SuperBlock {
    const SUPER_BLOCK_OFFSET: u64 = 1024;
    const MINIX_MAGIC_NUMBER: i16 = 0x4D5A;

    fn new(partition_entry: &Partition) -> Result<Self> {
        let super_block =
            SuperBlock::from_partition_offset(partition_entry, SuperBlock::SUPER_BLOCK_OFFSET)?;
        let block_magic = super_block.magic;
        if block_magic != SuperBlock::MINIX_MAGIC_NUMBER {
            Err(anyhow!(
                "Bad magic number. ({:x})\nThis doesn't look like a MINIX filesystem",
                block_magic
            ))
        } else {
            Ok(super_block)
        }
    }

    fn zone_size(&self) -> u64 {
        (self.block_size as u64) << self.log_zone_size
    }

    fn zone_to_block(&self, zone_id: u32) -> u32 {
        zone_id * (1 << self.log_zone_size)
    }
}

const DIRECT_ZONE_COUNT: usize = 7;

#[repr(C)]
#[repr(packed)]
#[derive(Debug, Clone, Copy)]
struct Inode {
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
    pub fn zone_iter<'b, 'a: 'b>(
        &'b self,
        part: &'a MinixPartition,
    ) -> impl Iterator<Item = u32> + 'b {
        let zone_size = part.super_block.zone_size();
        let file_size = self.size as u64;
        let take_amount = if file_size % zone_size == 0 {
            (file_size / zone_size) as usize
        } else {
            (file_size / zone_size) as usize + 1
        };
        self.zone_iter_inner(part).take(take_amount)
    }

    fn zone_iter_inner<'b, 'a: 'b>(
        &'b self,
        part: &'a MinixPartition,
    ) -> Box<dyn Iterator<Item = u32> + 'b> {
        // Compiler will potentially copy from unaligned memory. This solves that issue
        let direct_zones = self.direct_zones;
        let direct_zone_vec = direct_zones.to_vec();

        let indirect_zone = self.indirect;

        let iter_ret = IndirectIterator {
            zone_ptrs: direct_zone_vec,
            idx: 0,
        };

        if self.indirect == 0 {
            return Box::new(iter_ret);
        }

        let iter_ret = iter_ret.chain(IndirectIterator::new(part, indirect_zone));
        if self.two_indirect == 0 {
            return Box::new(iter_ret);
        }

        Box::new(
            iter_ret.chain(
                IndirectIterator::new(part, self.two_indirect)
                    .filter(|zone| *zone != 0)
                    .flat_map(|zone| IndirectIterator::new(part, zone)),
            ),
        )
    }
}

struct IndirectIterator {
    zone_ptrs: Vec<u32>,
    idx: usize,
}

impl Iterator for IndirectIterator {
    type Item = u32;

    fn next(&mut self) -> Option<Self::Item> {
        let ret = self.zone_ptrs.get(self.idx).copied();
        self.idx += 1;
        ret
    }
}

impl IndirectIterator {
    fn new(partition: &MinixPartition, zone_id: u32) -> Self {
        assert_ne!(zone_id, 0, "IndirectIterator expects a valid zone");
        let block_idx = partition.super_block.zone_to_block(zone_id);

        // MINIX uses blocks instead of zones for indirect
        let block_data = partition
            .read_block(block_idx)
            .expect("indirect iterator could not read zone");
        let zone_ptrs: Vec<_> = block_data
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
    inode_idx: u32,
    file_name: [u8; 60],
}
From_Bytes!(DirectoryEntry);

// ===================== Rust API Abstraction =====================

#[enum_dispatch(FileSystemRefFunctionality)]
#[derive(Debug, Clone)]
pub enum FileSystemRef<'a> {
    DirectoryRef(DirectoryRef<'a>),
    FileRef(FileRef<'a>),
}

impl<'a, 'b: 'a> FileSystemRef<'b> {
    fn from_directory_entry(
        partition: &'b MinixPartition,
        dir_entry: DirectoryEntry,
    ) -> Result<Self> {
        const REGULAR_FILE_MASK: u16 = 0o0100000;
        const DIRECTORY_MASK: u16 = 0o0040000;
        const FILE_TYPE_MASK: u16 = 0o0170000;

        let name_vec: Vec<_> = dir_entry
            .file_name
            .into_iter()
            .take_while(|ch| *ch != 0)
            .collect();
        let name = String::from_utf8(name_vec)?;
        let inode = partition.get_inode(dir_entry.inode_idx)?;

        let inode_mode = inode.mode;
        match FILE_TYPE_MASK & inode_mode {
            REGULAR_FILE_MASK => Ok(FileSystemRef::FileRef(FileRef {
                inode,
                name,
                partition,
            })),
            DIRECTORY_MASK => Ok(FileSystemRef::DirectoryRef(DirectoryRef {
                inode,
                name,
                partition,
            })),
            _ => Err(anyhow!(
                "Could not create a file system ref. Got invalid mode {inode_mode}"
            )),
        }
    }
}

impl Display for FileSystemRef<'_> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let permisions: Permissions = self.inode().mode.into();
        let size = self.inode().size;
        write!(f, "{}{:>10} {}", permisions, size, self.name())
    }
}

#[enum_dispatch]
trait FileSystemRefFunctionality {
    fn name(&self) -> &String;
    fn inode(&self) -> &Inode;
}

#[derive(Debug, Clone)]
pub struct DirectoryRef<'a> {
    partition: &'a MinixPartition<'a>,
    inode: Inode,
    pub name: String,
}

impl<'a, 'b: 'a> DirectoryRef<'b> {
    pub fn get(&'a self) -> Result<Directory<'a, 'b>> {
        Directory::new(self)
    }
}

impl<'a> FileSystemRefFunctionality for DirectoryRef<'a> {
    fn name(&self) -> &String {
        &self.name
    }

    fn inode(&self) -> &Inode {
        &self.inode
    }
}

#[derive(Debug, Clone)]
pub struct FileRef<'a> {
    partition: &'a MinixPartition<'a>,
    inode: Inode,
    pub name: String,
}

impl<'a> FileRef<'a> {
    pub fn get(&self) -> Result<Vec<u8>> {
        let mut data = self.inode.zone_iter(self.partition).fold(
            Ok(vec![]),
            |acc: Result<Vec<u8>>, zone_id| {
                let mut acc_vec = acc?;
                if zone_id == 0 {
                    let mut zeroed = vec![0; self.partition.super_block.zone_size() as usize];
                    acc_vec.append(&mut zeroed);
                } else {
                    let mut zone_data = self.partition.read_zone(zone_id)?;
                    acc_vec.append(&mut zone_data);
                }
                Ok(acc_vec)
            },
        )?;
        // the size should be eual or smaller
        data.resize(self.inode.size as usize, 0);
        Ok(data)
    }
}

impl<'a> FileSystemRefFunctionality for FileRef<'a> {
    fn name(&self) -> &String {
        &self.name
    }

    fn inode(&self) -> &Inode {
        &self.inode
    }
}

#[derive(Debug, Clone)]
pub struct Directory<'a, 'b: 'a> {
    dir_ref: &'a DirectoryRef<'b>,
    refs: Vec<FileSystemRef<'b>>,
}

impl<'a, 'b: 'a> Directory<'a, 'b> {
    fn new(dir_ref: &'a DirectoryRef<'b>) -> Result<Directory<'a, 'b>> {
        let dir_entry_bytes: Vec<u8> = dir_ref
            .inode
            .zone_iter(dir_ref.partition)
            // TODO: This should maybe be done with an assert
            // Filter out zone_id 0 since it is valid for files but not for directories
            .filter(|zone_id| *zone_id != 0)
            .fold(Ok(vec![]), |acc: Result<Vec<u8>>, zone_id: u32| {
                let mut acc_vec = acc?;
                let mut zone_data = dir_ref.partition.read_zone(zone_id)?;
                acc_vec.append(&mut zone_data);
                Ok(acc_vec)
            })?;
        let dir_entries = DirectoryEntry::from_bytes(dir_entry_bytes);
        let refs = dir_entries
            .into_iter()
            // Need to filter out 0 since because from_bytes will just convert entire zones
            .filter(|dir_entry| dir_entry.inode_idx != 0)
            .map(|dir_entry| FileSystemRef::from_directory_entry(dir_ref.partition, dir_entry))
            .collect::<Result<Vec<_>, _>>()?;

        Ok(Self { dir_ref, refs })
    }

    pub fn iter(&'a self) -> impl Iterator<Item = &'a FileSystemRef<'b>> {
        self.refs.iter()
    }

    pub fn dir_iter(&'a self) -> impl Iterator<Item = &DirectoryRef> {
        self.refs
            .iter()
            .filter_map(|file_system_ref| match file_system_ref {
                FileSystemRef::DirectoryRef(dir) => Some(dir),
                FileSystemRef::FileRef(_) => None,
            })
    }

    pub fn file_iter(&'a self) -> impl Iterator<Item = &FileRef> {
        self.refs
            .iter()
            .filter_map(|file_system_ref| match file_system_ref {
                FileSystemRef::DirectoryRef(_) => None,
                FileSystemRef::FileRef(file) => Some(file),
            })
    }

    pub fn find_dir(&'a self, name: &str) -> Option<&DirectoryRef> {
        self.dir_iter().find(|dir_ref| dir_ref.name == name)
    }

    pub fn find_file(&'a self, name: &str) -> Option<&FileRef> {
        self.file_iter().find(|file_ref| file_ref.name == name)
    }

    pub fn find(&'a self, name: &str) -> Option<&'a FileSystemRef<'b>> {
        self.iter().find(|file_ref| file_ref.name() == name)
    }

    pub fn get_at_path(self: &'a Directory<'a, 'b>, mut path: &str) -> Result<FileSystemRef<'b>> {
        if path.len() == 0 {
            return Err(anyhow!("path must exist"));
        }
        // treat paths which lead with '/' the same as those which don't,
        // relative to some directory, "/usr/bin/gcc" == "usr/bin/gcc" should return the same file, if it exists
        if let Some('/') = path.chars().next() {
            path = &path[1..];
        }
        let path_elements: Vec<_> = path.split_terminator('/').collect();
        self.get_at_path_internal(&path_elements)
    }

    fn get_at_path_internal(
        self: &'a Directory<'a, 'b>,
        path: &[&str],
    ) -> Result<FileSystemRef<'b>> {
        if path.is_empty() {
            // if path is empty then the current directory is returned
            return Ok(FileSystemRef::DirectoryRef(self.deref().clone()));
        };

        match (self.find(path[0]), &path[1..]) {
            (Some(FileSystemRef::DirectoryRef(dir_ref)), rst_path) => {
                dir_ref.get()?.get_at_path_internal(rst_path)
            }
            // Files should not have any path left after them
            (Some(FileSystemRef::FileRef(file_ref)), []) => {
                Ok(FileSystemRef::FileRef(file_ref.clone()))
            }
            _ => Err(anyhow!("invalid path")),
        }
    }
}

impl<'a, 'b: 'a> Deref for Directory<'a, 'b> {
    type Target = DirectoryRef<'b>;

    fn deref(&self) -> &Self::Target {
        self.dir_ref
    }
}

struct Permissions(u16);

impl From<u16> for Permissions {
    fn from(other: u16) -> Self {
        Permissions(other)
    }
}

impl Display for Permissions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut perm = String::with_capacity(9);
        let letters = ['r', 'w', 'x'];

        if self.0 & 0o40000 != 0 {
            perm.push('d');
        } else {
            perm.push('-');
        }
        for i in 0..9 {
            if self.0 & (1 << (8 - i)) != 0 {
                perm.push(letters[i % letters.len()]);
            } else {
                perm.push('-');
            }
        }
        write!(f, "{}", perm)
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Ok;

    use super::*;
    use std::ffi::CString;

    #[test]
    fn test_level_one_indirection() -> Result<()> {
        let partition: Partition = (&PartitionTree::new("./Images/BigIndirectDirs")?)
            .try_into()
            .unwrap();
        let minix_partition = MinixPartition::new(&partition)?;
        let root_ref = minix_partition.root_ref()?;
        let root_dir = root_ref.get()?;
        let level_1 = root_dir.find_dir("Level1").unwrap().get()?;
        let level_2 = level_1.find_dir("Level2").unwrap().get()?;

        let contents: Vec<_> = level_2
            .iter()
            .map(|file_system_ref| file_system_ref.name())
            .cloned()
            .collect();

        let mut expected = vec![".".to_string(), "..".to_string(), "BigDir".to_string()];
        for i in 0..=950 {
            if i % 10 > 7 {
                continue;
            }
            expected.push(format!("file_{:0>3}", i))
        }
        expected.push("LastFile".to_string());

        assert_eq!(contents.len(), expected.len());
        assert_eq!(contents, expected);

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

    #[test]
    fn test_sub_partition_and_hard_disk_run() -> Result<()> {
        let partition_tree = PartitionTree::new("./Images/HardDisk").unwrap();
        let PartitionTree::SubPartitions(primary_table) = partition_tree else {
            panic!("Did not get primary partition table")
        };

        let Some(PartitionTree::SubPartitions(sub_table)) = &primary_table[0] else {
            panic!("Did not get primary partition")
        };

        let Some(PartitionTree::Partition(sub_partition)) = &sub_table[2] else {
            panic!("Did not get sub partition back")
        };

        let minix_fs = MinixPartition::new(sub_partition)?;

        let root_ref = minix_fs.root_ref()?;
        let root_dir = root_ref.get()?;

        let root_contents: Vec<_> = root_dir.iter().map(|fs_ref| fs_ref.name()).collect();
        // TODO find out why . and .. are files
        let root_expected = vec![
            ".", "..", "adm", "ast", "bin", "etc", "gnu", "include", "lib", "log", "man", "mdec",
            "preserve", "run", "sbin", "spool", "tmp", "src", "home",
        ];

        assert_eq!(root_expected, root_contents);

        let home = root_dir.find_dir("home").unwrap().get()?;
        let pnico = home.find_dir("pnico").unwrap().get()?;

        let pnico_contents: Vec<_> = pnico.iter().map(|fs_ref| fs_ref.name()).collect();

        let pnico_expected = vec![
            ".",
            "..",
            ".ashrc",
            ".ellepro.b1",
            ".ellepro.e",
            ".exrc",
            ".profile",
            ".vimrc",
            "Message",
        ];

        assert_eq!(pnico_expected, pnico_contents);

        let message_bytes = pnico.find_file("Message").unwrap().get()?;
        let message_c_string = CString::new(message_bytes)?;
        let message_string = message_c_string.to_str()?;

        assert_eq!(
            message_string,
            "Hello.\n\nIf you can read this, you're getting somewhere.\n\nHappy hacking.\n"
        );

        Ok(())
    }

    #[test]
    fn test_file_get() -> Result<()> {
        let partition_tree = PartitionTree::new("./Images/HardDisk").unwrap();
        let PartitionTree::SubPartitions(primary_table) = partition_tree else {
            panic!("Did not get primary partition table")
        };

        let Some(PartitionTree::SubPartitions(sub_table)) = &primary_table[0] else {
            panic!("Did not get primary partition")
        };

        let Some(PartitionTree::Partition(sub_partition)) = &sub_table[2] else {
            panic!("Did not get sub partition back")
        };

        let minix_fs = MinixPartition::new(sub_partition)?;

        let root_ref = minix_fs.root_ref()?;
        let root_dir = root_ref.get()?;

        let bin_dir = root_dir.get_at_path("bin").expect("bin not found");
        let pnico_dir = root_dir.get_at_path("home/pnico").expect("pnico not found");
        let message_file = root_dir
            .get_at_path("home/pnico/Message")
            .expect("message not found");
        let message_file_alt = root_dir
            .get_at_path("/home/pnico/Message")
            .expect("message alt not found");
        let message_complex = root_dir
            .get_at_path("/home/pnico/./../pnico/../../home/pnico/./Message")
            .expect("message not found along complex path");

        let FileSystemRef::DirectoryRef(bin) = bin_dir else {
            panic!("bin not dir");
        };
        let FileSystemRef::DirectoryRef(pnico) = pnico_dir else {
            panic!("pnico not dir");
        };
        let FileSystemRef::FileRef(msg) = message_file else {
            panic!("msg not file");
        };
        let FileSystemRef::FileRef(msg_alt) = message_file_alt else {
            panic!("msg alt not file");
        };
        let FileSystemRef::FileRef(msg_complex) = message_complex else {
            panic!("msg alt not file");
        };

        assert!(bin.name == "bin");
        assert!(pnico.name == "pnico");
        assert_eq!(
            CString::new(msg.get().expect("could not read msg"))?.to_str()?,
            "Hello.\n\nIf you can read this, you're getting somewhere.\n\nHappy hacking.\n"
        );
        assert_eq!(
            CString::new(msg_alt.get().expect("could not read msg"))?.to_str()?,
            "Hello.\n\nIf you can read this, you're getting somewhere.\n\nHappy hacking.\n"
        );
        assert_eq!(
            CString::new(msg_complex.get().expect("could not read msg"))?.to_str()?,
            "Hello.\n\nIf you can read this, you're getting somewhere.\n\nHappy hacking.\n"
        );

        Ok(())
    }
}
