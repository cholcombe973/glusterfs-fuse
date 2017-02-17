extern crate fuse;
extern crate gfapi_sys;
extern crate libc;

use std::env;
use std::path::Path;

use fuse::{Filesystem, Request, ReplyAttr, ReplyDirectory};
use libc::ENOSYS;

struct GlusterFilesystem;

impl Filesystem for GlusterFilesystem {
    fn getattr(&mut self, _req: &Request, _ino: u64, reply: ReplyAttr) {
        println!("getattr(ino={})", _ino);
        reply.error(ENOSYS);
    }
    fn readdir(&mut self,
               _req: &Request,
               _ino: u64,
               _fh: u64,
               _offset: u64,
               reply: ReplyDirectory) {
        println!("readdir(ino={}, fh={}, offset={})", _ino, _fh, _offset);
        reply.error(ENOSYS);
    }
}

fn main() {
    let args: Vec<String> = env::args().collect();
    let mountpoint = Path::new(&args[1]);
    if !mountpoint.exists() {
        println!("Please create the mount point");
        return;
    }
    println!("mountpoint: {:?}", mountpoint);
    let _ = fuse::mount(GlusterFilesystem, &mountpoint, &[]);
    println!("mounted");
}
