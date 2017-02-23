use std::collections::HashMap;
use std::ffi::{OsStr, OsString};
use std::path::{Path, PathBuf};
use time;

use std::ops::{Index, IndexMut};

use fuse::{FileType, FileAttr};
use sequence_trie::SequenceTrie;

#[derive(Debug, Clone)]
pub struct Inode {
    pub path: PathBuf,
    pub attr: FileAttr, // pub visited: bool,
}

impl Inode {
    pub fn new<P: AsRef<Path>>(path: P, attr: FileAttr) -> Inode {
        Inode {
            path: PathBuf::from(path.as_ref()),
            attr: attr,
        }
    }
}

#[derive(Debug)]
pub struct InodeStore {
    inode_map: HashMap<u64, Inode>,
    ino_trie: SequenceTrie<OsString, u64>,
    last_ino: u64,
}

fn path_to_sequence(path: &Path) -> Vec<OsString> {
    path.iter().map(|s| s.to_owned()).collect()
}

impl InodeStore {
    pub fn new(perm: u16, uid: u32, gid: u32) -> InodeStore {
        let mut store = InodeStore {
            inode_map: HashMap::new(),
            ino_trie: SequenceTrie::new(),
            last_ino: 1, // 1 is reserved for root
        };
        let now = time::now_utc().to_timespec();
        let fs_root = FileAttr {
            ino: 1,
            size: 0,
            blocks: 0,
            atime: now,
            mtime: now,
            ctime: now,
            crtime: now,
            kind: FileType::Directory,
            perm: perm,
            nlink: 0,
            uid: uid,
            gid: gid,
            rdev: 0,
            flags: 0,
        };

        store.insert(Inode::new("/", fs_root));

        store
    }

    pub fn insert(&mut self, inode: Inode) {
        let ino = inode.attr.ino;
        let path = inode.path.clone();
        let sequence = path_to_sequence(&inode.path);

        if let Some(old_inode) = self.inode_map.insert(ino, inode) {
            if old_inode.path != path {
                panic!("Corrupted inode store: reinserted conflicting ino {} (path={}, \
                        oldpath={})",
                       ino,
                       path.display(),
                       old_inode.path.display());
            } else {
                println!("Updating ino {} at path {}", ino, path.display());
            }

        }

        if !self.ino_trie.insert(&sequence, ino) {
            let mut node = self.ino_trie
                .get_mut_node(&sequence)
                .expect(&format!("Corrupt inode store: couldn't insert or modify ino_trie at \
                                  {:?}",
                                 &sequence));
            // TODO: figure out why this check triggers a false alarm panic on backspacing
            // to dir and then tabbing
            // if node.value.is_some() {
            //     panic!("Corrupt inode store: reinserted ino {} into ino_trie, prev value: {}",
            // ino, node.value.unwrap());
            // }
            node.value = Some(ino);
        }
    }

    pub fn get(&self, ino: u64) -> Option<&Inode> {
        self.inode_map.get(&ino)
    }

    pub fn get_mut(&mut self, ino: u64) -> Option<&mut Inode> {
        self.inode_map.get_mut(&ino)
    }

    pub fn get_by_path<P: AsRef<Path>>(&self, path: P) -> Option<&Inode> {
        let sequence = path_to_sequence(path.as_ref());
        self.ino_trie.get(&sequence).and_then(|ino| self.get(*ino))
    }

    pub fn insert_metadata<P: AsRef<Path>>(&mut self, path: P, metadata: &FileAttr) -> &Inode {
        let ino = metadata.ino.clone();
        println!("insert metadata: {:?} {}",
                 metadata,
                 path.as_ref().display());

        self.insert(Inode::new(path, *metadata));
        self.get(ino).unwrap()
    }

    pub fn child<S: AsRef<OsStr>>(&self, ino: u64, name: S) -> Option<&Inode> {
        self.get(ino)
            .and_then(|inode| {
                let mut sequence = path_to_sequence(&inode.path);
                sequence.push(name.as_ref().to_owned());
                self.ino_trie.get(&sequence).and_then(|ino| self.get(*ino))
            })
    }

    pub fn remove(&mut self, ino: u64) {
        let sequence = {
            let ref path = self.inode_map[&ino].path;
            path_to_sequence(&path)
        };

        self.inode_map.remove(&ino);
        self.ino_trie.remove(&sequence);

        // assert!(self.inode_map.get(&ino).is_none());
        // assert!(self.ino_trie.get(&sequence).is_none());
    }
}

impl Index<u64> for InodeStore {
    type Output = Inode;

    fn index<'a>(&'a self, index: u64) -> &'a Inode {
        self.get(index).unwrap()
    }
}

impl IndexMut<u64> for InodeStore {
    fn index_mut<'a>(&'a mut self, index: u64) -> &'a mut Inode {
        self.get_mut(index).unwrap()
    }
}
