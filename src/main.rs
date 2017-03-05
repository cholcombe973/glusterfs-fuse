extern crate clap;
extern crate env_logger;
extern crate fuse;
extern crate gfapi_sys;
extern crate libc;
#[macro_use]
extern crate log;
extern crate sequence_trie;
extern crate time;

use std::ffi::OsStr;
use std::path::Path;
use std::str::FromStr;

use clap::{Arg, App};
use fuse::{FileAttr, Filesystem, FileType, Request, ReplyAttr, ReplyDirectory, ReplyEmpty,
           ReplyEntry, ReplyOpen, ReplyStatfs, ReplyWrite, ReplyData, ReplyXattr, ReplyCreate,
           ReplyLock};
use gfapi_sys::gluster::{Gluster, GlusterDirectory};
use gfapi_sys::glfs::Struct_glfs_fd;
use libc::{c_uchar, DT_REG, DT_DIR, DT_FIFO, DT_CHR, DT_BLK, DT_LNK, EIO, ENODATA, ENOENT, ENOSYS,
           ERANGE, S_IFMT, S_IFREG, S_IFDIR, S_IFCHR, S_IFBLK, S_IFIFO, S_IFLNK, timespec};
use time::Timespec;

mod inode;
use inode::InodeStore;

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

fn filetype_from_mode(mode_t: u32) -> Option<FileType> {
    let bits = mode_t & S_IFMT;
    match bits {
        S_IFREG => Some(FileType::RegularFile),
        S_IFDIR => Some(FileType::Directory),
        S_IFIFO => Some(FileType::NamedPipe),
        S_IFCHR => Some(FileType::CharDevice),
        S_IFBLK => Some(FileType::BlockDevice),
        S_IFLNK => Some(FileType::Symlink),
        _ => None,
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;
    use super::GlusterFilesystem;
    use std::path::PathBuf;
}

#[derive(Debug, Copy, Clone)]
pub struct MountOptions<'a> {
    path: &'a Path,
    uid: u32,
    gid: u32, // read_only: bool,
}

impl<'a> MountOptions<'a> {
    pub fn new<P: AsRef<Path>>(path: &P) -> MountOptions {
        MountOptions {
            path: path.as_ref(),
            uid: unsafe { libc::getuid() } as u32,
            gid: unsafe { libc::getgid() } as u32,
        }
    }
}

struct GlusterFilesystem {
    handle: Option<Gluster>,
    inodes: InodeStore, /* inodes: HashMap<u64, INode<'a>>,
                         * root_path: PathBuf, */
}

impl GlusterFilesystem {
    fn new(volume_name: &str,
           server: &str,
           port: u16,
           options: MountOptions)
           -> Result<(), std::io::Error> {
        let handle = Gluster::connect(volume_name, server, port).unwrap();
        let gfs = GlusterFilesystem {
            handle: Some(handle),
            inodes: InodeStore::new(0o550, options.uid, options.gid),
        };
        fuse::mount(gfs, &options.path, &[])
    }
    fn stat(&self, path: &Path) -> Result<FileAttr, String> {
        let stat = self.handle().stat(path).map_err(|e| e.to_string())?;

        let device_type = filetype_from_mode(stat.st_mode).ok_or(
            format!("Unable to determine file type of: {}", stat.st_mode))?;
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
            kind: device_type,
            // TODO: Extract the permissions from the mode_t field
            perm: 0o755,
            nlink: stat.st_nlink as u32,
            uid: stat.st_uid,
            gid: stat.st_gid,
            rdev: stat.st_rdev as u32,
            flags: 0,
        })
    }


    fn handle(&self) -> &Gluster {
        self.handle.as_ref().unwrap()
    }
}


impl Filesystem for GlusterFilesystem {
    fn getattr(&mut self, _req: &Request, ino: u64, reply: ReplyAttr) {
        trace!("getattr(ino={})", ino);
        match self.inodes.get(ino) {
            Some(inode) => reply.attr(&TTL, &inode.attr),
            None => {
                trace!("getattr ENOENT: {}", ino);
                reply.error(ENOENT);
            }
        };
    }

    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        trace!("lookup(parent={}, name=\"{}\")",
               parent,
               name.to_string_lossy());

        // Clone until MIR NLL lands
        // match self.inodes.child(parent, &name).cloned() {
        // Some(child_inode) => reply.entry(&TTL, &child_inode.attr, 0),
        // None => {
        // Clone until MIR NLL lands
        let parent_inode = self.inodes[parent].clone();
        let child_path = parent_inode.path.join(&name);
        match self.stat(&child_path) {
            Ok(file_attr) => {
                let inode = self.inodes.insert_metadata(&child_path, &file_attr).unwrap();
                reply.entry(&TTL, &inode.attr, 0)
            }
            Err(e) => {
                error!("lookup err: {:?}", e);
                reply.error(ENOENT)
            }
        }
        // }
        // }
    }

    fn readdir(&mut self,
               _req: &Request,
               _ino: u64,
               _fh: u64,
               _offset: u64,
               mut reply: ReplyDirectory) {
        trace!("readdir(ino={}, fh={}, offset={})", _ino, _fh, _offset);
        let d = GlusterDirectory { dir_handle: _fh as *mut Struct_glfs_fd };
        let mut offset: u64 = 0;

        for dir_entry in d {
            trace!("Dir_entry: {:?}", dir_entry);
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
    fn opendir(&mut self, _req: &Request, ino: u64, _flags: u32, reply: ReplyOpen) {
        trace!("opendir(ino={})", ino);
        match self.inodes.get(ino) {
            Some(inode) => {
                let path = &inode.path;
                trace!("opendir current_path: {}", path.to_string_lossy());
                let dir_handle = self.handle().opendir(path).unwrap();
                reply.opened(dir_handle as u64, _flags);
            }
            None => reply.error(ENOENT),
        }

    }
    fn releasedir(&mut self, _req: &Request, _ino: u64, _fh: u64, _flags: u32, reply: ReplyEmpty) {
        trace!("releasedir(ino={})", _ino);
        reply.error(ENOSYS);
    }

    fn open(&mut self, _req: &Request, ino: u64, flags: u32, reply: ReplyOpen) {
        trace!("open(ino={}, flags=0x{:x})", ino, flags);
        // match flags & O_ACCMODE => O_RDONLY, O_WRONLY, O_RDWR
        match self.inodes.get(ino) {
            Some(inode) => {
                let path = &inode.path;
                trace!("open current_path: {}", path.to_string_lossy());
                let file_handle = self.handle().open(path, flags as i32).unwrap();
                reply.opened(file_handle as u64, flags);
            }
            None => reply.error(ENOENT),
        }
    }

    fn statfs(&mut self, _req: &Request, _ino: u64, reply: ReplyStatfs) {
        trace!("statfs(ino={})", _ino);
        reply.error(ENOSYS);
    }
    fn setattr(&mut self,
               _req: &Request,
               ino: u64,
               mode: Option<u32>,
               uid: Option<u32>,
               gid: Option<u32>,
               _size: Option<u64>,
               atime: Option<Timespec>,
               mtime: Option<Timespec>,
               _fh: Option<u64>,
               _crtime: Option<Timespec>,
               _chgtime: Option<Timespec>,
               _bkuptime: Option<Timespec>,
               _flags: Option<u32>,
               reply: ReplyAttr) {
        trace!("setattr(ino={})", ino);

        let path = match self.inodes.get(ino) {
            Some(inode) => inode.path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        let mut times: [timespec; 2] = [timespec {
                                            tv_sec: 0,
                                            tv_nsec: 0,
                                        },
                                        timespec {
                                            tv_sec: 0,
                                            tv_nsec: 0,
                                        }];
        if let Some(access_time) = atime {
            times[0] = timespec {
                tv_sec: access_time.sec,
                tv_nsec: access_time.nsec as i64,
            };
        }
        if let Some(m_time) = mtime {
            times[1] = timespec {
                tv_sec: m_time.sec,
                tv_nsec: m_time.nsec as i64,
            };
        }

        // Change the access times if requested
        match self.handle().utimens(&path, &times) {
            Ok(_) => {
                // reply.attr(&TTL, &inode.attr);
            }
            Err(e) => {
                error!("utimens err: {:?}", e);
                // reply.error(ENOENT);
            }
        };

        // Change the file mode if requested
        if let Some(file_mode) = mode {
            match self.handle().chmod(&path, file_mode) {
                Ok(_) => {
                    // reply.attr(&TTL, &inode.attr);
                }
                Err(e) => {
                    error!("chmod err: {:?}", e);
                    // reply.error(ENOENT);
                }
            }
        }

        // Change the ownership if both uid and gid are specified
        // TODO: What if only one is set?
        if uid.is_some() && gid.is_some() {
            match self.handle().chown(&path, uid.unwrap(), gid.unwrap()) {
                Ok(_) => {
                    // reply.attr(&TTL, &inode.attr);
                }
                Err(e) => {
                    error!("chown err: {:?}", e);
                    // reply.error(ENOENT);
                }
            }
        }

        // Finally stat and return
        match self.stat(&path) {
            Ok(file_attr) => {
                let inode = self.inodes.insert_metadata(&path, &file_attr).unwrap();
                reply.attr(&TTL, &inode.attr)
            }
            Err(e) => {
                error!("getattr lookup err: {:?}", e);
                reply.error(ENOENT)
            }
        }
    }

    fn mknod(&mut self,
             _req: &Request,
             parent: u64,
             name: &OsStr,
             _mode: u32,
             _rdev: u32,
             reply: ReplyEntry) {
        trace!("mknod(parent={}, name={:?})", parent, name);
        let path = self.inodes[parent].path.join(&name);
        match self.handle().mknod(&path, _mode, _rdev as u64) {
            Ok(()) => {
                match self.stat(&path) {
                    Ok(file_attr) => {
                        let inode = self.inodes.insert_metadata(&path, &file_attr).unwrap();
                        reply.entry(&TTL, &inode.attr, file_attr.size)
                    }
                    Err(e) => {
                        error!("mknod lookup err: {:?}", e);
                        reply.error(ENOENT)
                    }
                }
            }
            Err(e) => {
                error!("mknod err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn mkdir(&mut self, _req: &Request, parent: u64, name: &OsStr, _mode: u32, reply: ReplyEntry) {
        trace!("mkdir(parent={}, name={:?})", parent, name);
        let path = self.inodes[parent].path.join(&name);
        match self.handle().mkdir(&path, _mode) {
            Ok(()) => {
                match self.stat(&path) {
                    Ok(file_attr) => {
                        let inode = self.inodes.insert_metadata(&path, &file_attr).unwrap();
                        reply.entry(&TTL, &inode.attr, file_attr.size)
                    }
                    Err(e) => {
                        error!("mkdir lookup err: {:?}", e);
                        reply.error(ENOENT)
                    }
                }
            }
            Err(e) => {
                error!("mkdir err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn forget(&mut self, _req: &Request, _ino: u64, _nlookup: u64) {
        trace!("forget(ino={:?})", _ino);

    }

    fn readlink(&mut self, _req: &Request, _ino: u64, reply: ReplyData) {
        trace!("readlink(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn unlink(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        trace!("unlink(name={:?})", name);
        let target = match self.inodes.child(parent, name) {
            Some(inode) => inode.clone(),
            None => {
                // Trying to remove a inode that doesn't exist in the cache
                reply.error(ENOENT);
                return;
            }
        };

        match self.handle().unlink(&target.path) {
            Ok(_) => {
                self.inodes.remove(target.attr.ino);
                reply.ok();
            }
            Err(e) => {
                error!("unlink err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn rmdir(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEmpty) {
        trace!("rmdir(name={:?})", name);
        let parent_inode = self.inodes[parent].clone();
        let target = parent_inode.path.join(&name);
        match self.handle().rmdir(&target) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                error!("rmdir err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn symlink(&mut self,
               _req: &Request,
               parent: u64,
               name: &OsStr,
               link: &Path,
               reply: ReplyEntry) {
        trace!("symlink(name={:?})", name);
        let parent_inode = self.inodes[parent].clone();
        // TODO Is this correct?
        let target = parent_inode.path.join(&name);

        match self.handle().symlink(&target, &link) {
            Ok(_) => {
                match self.stat(&target) {
                    Ok(file_attr) => {
                        // TODO Is this correct?
                        let inode = self.inodes.insert_metadata(&target, &file_attr).unwrap();
                        reply.entry(&TTL, &inode.attr, file_attr.size)
                    }
                    Err(e) => {
                        error!("symlink lookup err: {:?}", e);
                        reply.error(ENOENT)
                    }
                }
            }
            Err(e) => {
                error!("symlink err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    /// Rename a file.
    fn rename(&mut self,
              _req: &Request,
              parent: u64,
              name: &OsStr,
              newparent: u64,
              newname: &OsStr,
              reply: ReplyEmpty) {
        trace!("rename(name={:?} to {:?})", name, newname);
        let parent_inode = self.inodes[parent].clone();
        let child_old_path = parent_inode.path.join(&name);

        let new_parent_inode = self.inodes[newparent].clone();
        let new_child_path = new_parent_inode.path.join(&newname);
        match self.handle().rename(&child_old_path, &new_child_path) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                error!("rename err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    /// Create a hard link.
    fn link(&mut self,
            _req: &Request,
            ino: u64,
            newparent: u64,
            newname: &OsStr,
            reply: ReplyEntry) {
        trace!("link(ino={:?})", ino);
        let old_path = match self.inodes.get(ino) {
            Some(inode) => inode.path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        let new_inode = self.inodes[newparent].clone();
        let new_path = new_inode.path.join(&newname);

        match self.handle().link(&old_path, &new_path) {
            Ok(_) => {
                match self.stat(&new_path) {
                    Ok(file_attr) => {
                        let inode = self.inodes.insert_metadata(&new_path, &file_attr).unwrap();
                        reply.entry(&TTL, &inode.attr, file_attr.size)
                    }
                    Err(e) => {
                        error!("link lookup err: {:?}", e);
                        reply.error(ENOENT)
                    }
                }
            }
            Err(e) => {
                error!("link err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn read(&mut self,
            _req: &Request,
            _ino: u64,
            fh: u64,
            offset: u64,
            _size: u32,
            reply: ReplyData) {
        trace!("read(ino={:?})", _ino);

        // TODO: Use a buffer pool
        let mut fill_buffer: Vec<u8> = Vec::with_capacity(_size as usize);

        match self.handle().pread(fh as *mut Struct_glfs_fd,
                                  &mut fill_buffer,
                                  _size as usize,
                                  offset as i64,
                                  0) {
            Ok(bytes_read) => {
                fill_buffer.truncate(bytes_read as usize);
                println!("bytes_read: {} {:?}", bytes_read, fill_buffer);
                reply.data(&fill_buffer[..]);
            }
            Err(e) => {
                error!("read err: {:?}", e);
                reply.error(ENOENT);
            }

        }
    }

    fn write(&mut self,
             _req: &Request,
             ino: u64,
             fh: u64,
             offset: u64,
             data: &[u8],
             flags: u32,
             reply: ReplyWrite) {
        trace!("write(ino={:?})", ino);

        // Should already have the file handle open here
        match self.handle().pwrite(fh as *mut Struct_glfs_fd,
                                   data,
                                   data.len(),
                                   offset as i64,
                                   flags as i32) {
            Ok(bytes_written) => {
                self.inodes.get_mut(ino).unwrap().attr.size += bytes_written as u64;
                reply.written(bytes_written as u32);
            }
            Err(e) => {
                error!("write err: {:?}", e);
                reply.error(EIO);
            }
        }
    }

    fn flush(&mut self, _req: &Request, _ino: u64, _fh: u64, _lock_owner: u64, reply: ReplyEmpty) {
        trace!("flush(ino={:?})", _ino);
        reply.ok();
    }

    fn release(&mut self,
               _req: &Request,
               _ino: u64,
               fh: u64,
               _flags: u32,
               _lock_owner: u64,
               _flush: bool,
               reply: ReplyEmpty) {
        trace!("release(ino={:?})", _ino);
        match self.handle().close(fh as *mut Struct_glfs_fd) {
            Ok(_) => reply.ok(),
            Err(_) => reply.error(EIO),
        }
    }

    fn fsync(&mut self, _req: &Request, _ino: u64, _fh: u64, _datasync: bool, reply: ReplyEmpty) {
        trace!("fsync(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn fsyncdir(&mut self,
                _req: &Request,
                _ino: u64,
                _fh: u64,
                _datasync: bool,
                reply: ReplyEmpty) {
        trace!("fsyncdir(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    /// Set an extended attribute.
    fn setxattr(&mut self,
                _req: &Request,
                ino: u64,
                name: &OsStr,
                value: &[u8],
                flags: u32,
                _position: u32,
                reply: ReplyEmpty) {
        trace!("setxattr(ino={:?})", ino);
        let path = match self.inodes.get(ino) {
            Some(inode) => inode.path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        match self.handle().setxattr(&path,
                                     &name.to_string_lossy().into_owned(),
                                     value,
                                     flags as i32) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                error!("setxattr err: {:?}", e);
                reply.error(ENOENT);
            }
        }
    }

    fn getxattr(&mut self, _req: &Request, ino: u64, name: &OsStr, _size: u32, reply: ReplyXattr) {
        trace!("getxattr(ino={:?})", ino);

        let path = match self.inodes.get(ino) {
            Some(inode) => inode.path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };
        trace!("getxattr path: {:?}, name: {}",
               path,
               name.to_string_lossy());
        match self.handle().getxattr(&path, &name.to_string_lossy().into_owned()) {
            Ok(data) => {
                match self.stat(&path) {
                    Ok(file_attr) => {
                        self.inodes.insert_metadata(&path, &file_attr).unwrap();
                        if data.len() as u32 > _size {
                            reply.error(ERANGE);
                            return;
                        }
                        if _size == 0 {
                            reply.size(data.len() as u32);
                        } else {
                            reply.data(&data.as_bytes());
                        }
                    }
                    Err(e) => {
                        error!("getxattr lookup err: {:?}", e);
                        reply.error(ENODATA)
                    }
                }
            }
            Err(e) => {
                error!("getxattr err: {:?}", e);
                reply.error(ENODATA);
            }
        }
    }

    fn listxattr(&mut self, _req: &Request, _ino: u64, _size: u32, reply: ReplyXattr) {
        trace!("listxattr(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn removexattr(&mut self, _req: &Request, ino: u64, name: &OsStr, reply: ReplyEmpty) {
        trace!("removexattr(ino={:?})", ino);

        let path = match self.inodes.get(ino) {
            Some(inode) => inode.path.clone(),
            None => {
                reply.error(ENOENT);
                return;
            }
        };

        match self.handle().removexattr(&path, &name.to_string_lossy().into_owned()) {
            Ok(_) => {
                reply.ok();
            }
            Err(e) => {
                error!("removexattr err: {:?}", e);
                reply.error(ENODATA);
            }
        }
    }

    fn access(&mut self, _req: &Request, _ino: u64, _mask: u32, reply: ReplyEmpty) {
        trace!("access(ino={:?})", _ino);
        reply.error(ENOSYS);
    }

    fn create(&mut self,
              _req: &Request,
              parent: u64,
              name: &OsStr,
              mode: u32,
              flags: u32,
              reply: ReplyCreate) {
        trace!("create(name={:?})", name);

        // Clone until MIR NLL lands
        let parent_inode = self.inodes[parent].clone();
        let child_path = parent_inode.path.join(&name);
        match self.handle().create(&child_path, flags as i32, mode) {
            Ok(fh) => {
                match self.stat(&child_path) {
                    Ok(file_attr) => {
                        let inode = self.inodes.insert_metadata(&child_path, &file_attr).unwrap();
                        reply.created(&TTL, &inode.attr, file_attr.size, fh as u64, flags)
                    }
                    Err(e) => {
                        error!("create lookup err: {:?}", e);
                        reply.error(ENOENT)
                    }
                }
            }
            Err(e) => {
                error!("create err: {:?}", e);
                reply.error(ENOENT);
            }
        }


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
        trace!("getlk(ino={:?})", _ino);
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
        trace!("setlk(ino={:?})", _ino);
        reply.error(ENOSYS);
    }
}

fn main() {
    env_logger::init().unwrap();
    let matches = App::new("GlusterFS Fuse Mount")
        .version("0.1.0")
        .author("Chris Holcombe <xfactor973@gmail.com>")
        .about("Replacement for glusterfs C fuse")
        .arg(Arg::with_name("mount")
            .help("Mountpoint to bind to")
            .long("mount")
            .required(true)
            .short("m")
            .takes_value(true)
            .value_name("mount"))
        .arg(Arg::with_name("port")
            .default_value("24007")
            .help("Port GlusterD is listening on")
            .long("port")
            .short("p")
            .takes_value(true)
            .validator(|value| match u16::from_str(&value) {
                Ok(_) => Ok(()),
                Err(_) => Err(format!("Error: {} is not a valid u16 number", value)),
            })
            .value_name("port"))
        .arg(Arg::with_name("server")
            .default_value("localhost")
            .help("The GlusterD server to connect to")
            .long("server")
            .short("s")
            .takes_value(true)
            .value_name("server"))
        .arg(Arg::with_name("volume")
            .help("Gluster volume name to bind use")
            .long("volume")
            .required(true)
            .takes_value(true)
            .value_name("volume"))
        .get_matches();
    let mountpoint = matches.value_of("mount").unwrap();
    trace!("mountpoint: {:?}", mountpoint);
    // These unwraps are safe because clap has validated the input
    let _ = GlusterFilesystem::new(matches.value_of("volume").unwrap(),
                                   matches.value_of("server").unwrap(),
                                   u16::from_str(&matches.value_of("port").unwrap()).unwrap(),
                                   MountOptions::new(&mountpoint))
        .unwrap();
    trace!("unmounted");
}
