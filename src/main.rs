extern crate fuse;
extern crate gfapi_sys;
extern crate libc;
extern crate time;

use std::env;
use std::ffi::OsStr;
use std::path::Path;

use fuse::{FileAttr, Filesystem, FileType, Request, ReplyAttr, ReplyDirectory, ReplyEmpty,
           ReplyEntry, ReplyOpen, ReplyStatfs};
use gfapi_sys::gluster::{Gluster, GlusterDirectory};
use gfapi_sys::glfs::Struct_glfs_fd;
use libc::{c_int, c_uchar, DT_REG, DT_DIR, DT_FIFO, DT_CHR, DT_BLK, DT_LNK, ENOSYS, O_RDWR};
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

struct GlusterFilesystem {
    handle: Gluster,
}

impl GlusterFilesystem {
    fn new(volume_name: &str, server: &str, port: u16) -> Result<GlusterFilesystem, c_int> {
        let handle = Gluster::connect(volume_name, server, port).unwrap();
        Ok(GlusterFilesystem { handle: handle })
    }
}


impl Filesystem for GlusterFilesystem {
    fn getattr(&mut self, _req: &Request, _ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", _ino);
        if _ino == 1 {
            let stat = self.handle.stat(Path::new("/")).unwrap();
            let reply_attr = FileAttr {
                ino: 1,
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
            };
            reply.attr(&TTL, &reply_attr);
        } else {
            reply.error(ENOSYS);
        }
    }
    fn lookup(&mut self, _req: &Request, _parent: u64, _name: &OsStr, reply: ReplyEntry) {
        println!("lookup(parent={}, name={:?})", _parent, _name);
        reply.error(ENOSYS);
    }
    fn readdir(&mut self,
               _req: &Request,
               _ino: u64,
               _fh: u64,
               _offset: u64,
               mut reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", _ino, _fh, _offset);
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
        if _ino == 1 {
            let dir_handle = self.handle.opendir(Path::new("/")).unwrap();
            // TODO: How do I store this?
            reply.opened(dir_handle as u64, _flags);
        } else {
            reply.error(ENOSYS);
        }
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
