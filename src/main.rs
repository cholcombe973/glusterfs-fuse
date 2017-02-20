extern crate fuse;
extern crate gfapi_sys;
extern crate libc;
extern crate time;

use std::collections::HashMap;
use std::env;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};

use fuse::{FileAttr, Filesystem, FileType, Request, ReplyAttr, ReplyDirectory, ReplyEmpty,
           ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyData, ReplyXattr, ReplyCreate,
           ReplyLock};
use gfapi_sys::gluster::{Gluster, GlusterDirectory};
use gfapi_sys::glfs::Struct_glfs_fd;
use libc::{c_int, c_uchar, DT_REG, DT_DIR, DT_FIFO, DT_CHR, DT_BLK, DT_LNK, ENOENT, ENOSYS};
use time::Timespec;

const TTL: Timespec = Timespec { sec: 1, nsec: 0 }; // 1 second

fn filetype_from_uchar(f_type: c_uchar) -> Option<FileType> {
    match f_type {
        DT_REG => Some(FileType::RegularFile),
        DT_DIR => Some(FileType::Directory),
        DT_FIFO => Some(FileType::NamedPipe),
        DT_CHR => Some(FileType::CharDevice),
        DT_BLK => Some(FileType::BlockDevice),
        DT_LNK => Some(FileType::Symlink),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use super::GlusterFilesystem;
    // use gfapi_sys;
    use std::path::PathBuf;

    #[test]
    fn it_builds_path() {
        let mut inodes = HashMap::new();
        inodes.insert(1, PathBuf::from("/"));
        inodes.insert(12345, PathBuf::from("tmp"));
        inodes.insert(34567, PathBuf::from("test"));
        let gluster = GlusterFilesystem {
            handle: None,
            parents: vec![1, 12345, 34567],
            inodes: inodes,
        };

        assert_eq!(gluster.current_path(None).to_string_lossy(), "/tmp/test");
    }

    #[test]
    fn it_sets_parents() {
        let mut inodes = HashMap::new();
        inodes.insert(1, PathBuf::from("/"));
        inodes.insert(12345, PathBuf::from("tmp"));
        inodes.insert(34567, PathBuf::from("test"));
        let mut gluster = GlusterFilesystem {
            handle: None,
            parents: vec![1, 12345, 34567],
            inodes: inodes,
        };

        assert_eq!(gluster.current_path(None).to_string_lossy(), "/tmp/test");
        gluster.set_parent(12345);
        assert_eq!(gluster.current_path(None).to_string_lossy(), "/tmp");
    }
}

struct GlusterFilesystem {
    handle: Option<Gluster>,
    parents: Vec<u64>,
    inodes: HashMap<u64, PathBuf>,
}

static ROOT: u64 = 1;

impl GlusterFilesystem {
    fn new(volume_name: &str, server: &str, port: u16) -> Result<GlusterFilesystem, c_int> {
        let handle = Gluster::connect(volume_name, server, port).unwrap();
        let mut paths = HashMap::new();
        paths.insert(ROOT, PathBuf::from("/"));
        Ok(GlusterFilesystem {
            handle: Some(handle),
            parents: vec![ROOT],
            inodes: paths,
        })
    }
    fn stat(&self, path: &Path) -> Result<FileAttr, String> {
        let stat = self.handle().stat(path).map_err(|e| e.to_string())?;

        Ok(FileAttr {
            ino: stat.st_ino,
            size: stat.st_size as u64,
            blocks: stat.st_blocks as u64,
            atime: Timespec {
                sec: stat.st_atime,
                nsec: stat.st_atime_nsec as i32,
            },
            mtime: Timespec {
                sec: stat.st_mtime,
                nsec: stat.st_mtime_nsec as i32,
            },
            ctime: Timespec {
                sec: stat.st_ctime,
                nsec: stat.st_ctime_nsec as i32,
            },
            crtime: Timespec { sec: 1, nsec: 0 },
            kind: FileType::Directory,
            perm: 0o755,
            nlink: stat.st_nlink as u32,
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u32,
            flags: 0,
        })
    }

    fn parent(&self) -> &u64 {
        self.parents.last().unwrap_or(&ROOT)
    }

    fn handle(&self) -> &Gluster {
        self.handle.as_ref().unwrap()
    }

    fn current_path(&self, new_path: Option<PathBuf>) -> PathBuf {
        let mut path = PathBuf::new();
        for parent in &self.parents {
            if let Some(parent_path) = self.inodes.get(&parent) {
                path.push(parent_path);
            }
        }
        if let Some(p) = new_path {
            path.push(p);
        }
        println!("current_path info: {:?} {:?}", self.parents, self.inodes);
        path
    }

    fn set_parent(&mut self, inode: u64) {
        if let Some(index) = self.parents.iter().position(|&i| i == inode) {
            self.parents.truncate(index + 1);// Remove remaining elements
        } else {
            self.parents.push(inode);
        }
    }
}


impl Filesystem for GlusterFilesystem {
    fn getattr(&mut self, _req: &Request, _ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", _ino);
        println!("getattr current_path: {}",
                 self.current_path(None).to_string_lossy());
        let root = PathBuf::from("/");

        let path = if _ino == ROOT {
            &root
        } else {
            if let Some(p) = self.inodes.get(&_ino) {
                p
            } else {
                reply.error(ENOSYS);
                return;
            }
        };
        let reply_attr = self.stat(path).unwrap();
        reply.attr(&TTL, &reply_attr);
    }

    fn lookup(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEntry) {
        self.set_parent(_parent);
        println!("lookup(parent={}, name={:?})", _parent, _name);
        println!("lookup current_path: {}",
                 self.current_path(Some(_name.into())).to_string_lossy());
        if let Ok(reply_attr) = self.stat(&self.current_path(Some(_name.into()))) {
            self.inodes.insert(reply_attr.ino, _name.into());
            reply.entry(&TTL, &reply_attr, 0);
        } else {
            reply.error(ENOENT);
        }
    }
    fn readdir(&mut self,
               _req: &Request,
               _ino: u64,
               _fh: u64,
               _offset: u64,
               mut reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", _ino, _fh, _offset);
        println!("readdir current_path: {}",
                 self.current_path(None).to_string_lossy());
        let d = GlusterDirectory { dir_handle: _fh as *mut Struct_glfs_fd };
        let mut offset: u64 = 0;

        for dir_entry in d {
            println!("Dir_entry: {:?}", dir_entry);
            let device_type = filetype_from_uchar(dir_entry.file_type);
            match device_type {
                Some(d_type) => {
                    // This returns true if the buffer is full
                    let full = reply.add(dir_entry.inode, offset, d_type, dir_entry.path);
                    if full {
                        return reply.ok();
                    }
                    offset += 1;
                }
                None => {
                    return reply.error(ENOSYS);
                }
            }
        }
        reply.ok();
    }
    fn opendir(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        println!("opendir(ino={})", _ino);
        println!("opendir current_path: {}",
                 self.current_path(None).to_string_lossy());
        let root = PathBuf::from("/");

        let path = if _ino == ROOT {
            &root
        } else {
            if let Some(p) = self.inodes.get(&_ino) {
                p
            } else {
                reply.error(ENOSYS);
                return;
            }
        };
        let dir_handle = self.handle().opendir(path).unwrap();
        reply.opened(dir_handle as u64, _flags);
    }
    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: u32, reply: ReplyEmpty) {
        println!("releasedir(ino={})", _ino);
        reply.error(ENOSYS);
    }
    fn open(&mut self, _req: &Request, _ino: u64, _flags: u32, reply: ReplyOpen) {
        println!("open(ino={})", _ino);
        reply.error(ENOSYS);
    }
    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        println!("statfs(ino={})", _ino);
        reply.error(ENOSYS);
    }
    fn setattr(&mut self,
               _req: &Request,
               _ino: u64,
               _mode: Option<u32>,
               _uid: Option<u32>,
               _gid: Option<u32>,
               _size: Option<u64>,
               _atime: Option<Timespec>,
               _mtime: Option<Timespec>,
               _fh: Option<u64>,
               _crtime: Option<Timespec>,
               _chgtime: Option<Timespec>,
               _bkuptime: Option<Timespec>,
               _flags: Option<u32>,
               reply: ReplyAttr) {
        println!("setattr(ino={})", _ino);
        reply.error(ENOSYS);
    }
    fn mknod(&mut self,
             _req: &Request,
             _parent: u64,
             _name: &OsStr,
             _mode: u32,
             _rdev: u32,
             reply: ReplyEntry) {
        println!("mknod(parent={}, name={:?})", _parent, _name);
        reply.error(ENOSYS);
    }

    fn mkdir(&mut self,
             _req: &Request,
             _parent: u64,
             _name: &OsStr,
             _mode: u32,
             reply: ReplyEntry) {
        println!("mkdir(parent={}, name={:?})", _parent, _name);
        reply.error(ENOSYS);
    }
    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {
        println!("forget(ino={:?})", _ino);

    }

    fn readlink(&mut self, _req: &Request, _ino: u64, reply: ReplyData) {
        println!("readlink(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn unlink(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("unlink(name={:?})", _name);
        reply.error(ENOSYS);
    }

    fn rmdir(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("rmdir(name={:?})", _name);
        reply.error(ENOSYS);
    }

    fn symlink(&mut self,
               _req: &Request,
               _parent: u64,
               _name: &OsStr,
               _link: &Path,
               reply: ReplyEntry) {
        println!("symlink(name={:?})", _name);
        reply.error(ENOSYS);
    }

    /// Rename a file.
    fn rename(&mut self,
              _req: &Request,
              _parent: u64,
              _name: &OsStr,
              _newparent: u64,
              _newname: &OsStr,
              reply: ReplyEmpty) {
        println!("rename(name={:?})", _name);
        reply.error(ENOSYS);
    }

    /// Create a hard link.
    fn link(&mut self,
            _req: &Request,
            _ino: u64,
            _newparent: u64,
            _newname: &OsStr,
            reply: ReplyEntry) {
        println!("link(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn read(&mut self,
            _req: &Request,
            _ino: u64,
            _fh: u64,
            _offset: u64,
            _size: u32,
            reply: ReplyData) {
        println!("read(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn write(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _offset: u64,
             _data: &[u8],
             _flags: u32,
             reply: ReplyWrite) {
        println!("write(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        println!("flush(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn release(&mut self,
               _req: &Request,
               _ino: u64,
               _fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool,
               reply: ReplyEmpty) {
        println!("release(ino={:?})", _ino);
        reply.ok();
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        println!("fsync(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn fsyncdir(&mut self,
                _req: &Request,
                _ino: u64,
                _fh: u64,
                _datasync: bool,
                reply: ReplyEmpty) {
        println!("fsyncdir(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    /// Set an extended attribute.
    fn setxattr(&mut self,
                _req: &Request,
                _ino: u64,
                _name: &OsStr,
                _value: &[u8],
                _flags: u32,
                _position: u32,
                reply: ReplyEmpty) {
        println!("setxattr(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn getxattr(&mut self,
                _req: &Request,
                _ino: u64,
                _name: &OsStr,
                _size: u32,
                reply: ReplyXattr) {
        println!("getxattr(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn listxattr(&mut self, _req: &Request, _ino: u64, _size: u32, reply: ReplyXattr) {
        println!("listxattr(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn removexattr(&mut self, _req: &Request, _ino: u64, _name: &OsStr, reply: ReplyEmpty) {
        println!("removexattr(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        println!("access(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn create(&mut self,
              _req: &Request,
              _parent: u64,
              _name: &OsStr,
              _mode: u32,
              _flags: u32,
              reply: ReplyCreate) {
        println!("create(name={:?})", _name);
        reply.error(ENOSYS);
    }

    fn getlk(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _lock_owner: u64,
             _start: u64,
             _end: u64,
             _typ: u32,
             _pid: u32,
             reply: ReplyLock) {
        println!("getlk(ino={:?})", _ino);
        reply.error(ENOSYS);
    }
    fn setlk(&mut self,
             _req: &Request,
             _ino: u64,
             _fh: u64,
             _lock_owner: u64,
             _start: u64,
             _end: u64,
             _typ: u32,
             _pid: u32,
             _sleep: bool,
             reply: ReplyEmpty) {
        println!("setlk(ino={:?})", _ino);
        reply.error(ENOSYS);
    }
}

fn main() {
    println!("Hello");
    let args: Vec<String> = env::args().collect();
    let mountpoint = Path::new(&args[1]);
    if !mountpoint.exists() {
        println!("Please create the mount point");
        return;
    }
    println!("mountpoint: {:?}", mountpoint);
    let gfs = GlusterFilesystem::new("test", "localhost", 24007).unwrap();
    let _ = fuse::mount(gfs, &mountpoint, &[]);
    println!("unmounted");
}
