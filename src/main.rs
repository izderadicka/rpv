use std::{
    io,
    os::fd::{AsRawFd, RawFd},
    sync::{atomic, Arc},
    thread::{self, JoinHandle},
};

use clap::Parser;

type MainResult = std::result::Result<(), Box<dyn std::error::Error + 'static>>;

struct Reporter {
    counter: Arc<atomic::AtomicU64>,
}
impl Reporter {
    fn new(counter: Arc<atomic::AtomicU64>) -> Self {
        Self { counter }
    }

    fn run(self) -> JoinHandle<()> {
        let counter = self.counter;

        thread::spawn(move || {
            let mut last = 0;
            let mut secs = 1;
            loop {
                let count = counter.load(atomic::Ordering::Relaxed);

                if count != last {
                    let mb = (count - last) as f64 / 1024.0 / 1024.0 / secs as f64;
                    eprint!("\r{:0.3} MiB/s", mb);
                    last = count;
                    secs = 1;
                } else {
                    secs += 1;
                }

                std::thread::sleep(std::time::Duration::from_millis(1000));
            }
        })
    }
}

#[derive(Parser, Debug)]
struct Options {
    #[arg(short = 'C', long, help = "Don't use splice(2) for copying")]
    do_not_use_splice: bool,
    #[arg(short = 's', long, default_value = "32", help = "chunk size in KiB")]
    chunk_size_kb: usize,
}

fn main() -> MainResult {
    let args = Options::parse();
    let input = io::stdin().lock();
    let output = io::stdout().lock();

    let counter = Arc::new(atomic::AtomicU64::new(0));
    let reporter = Reporter::new(counter.clone());
    reporter.run();

    let chunk_size = args.chunk_size_kb * 1024;

    if args.do_not_use_splice {
        rw_copy(input, output, counter, chunk_size)
    } else {
        splice_copy(input, output, counter, chunk_size)
    }
}

fn splice(fd_in: RawFd, fd_out: RawFd, size: usize) -> Result<usize, io::Error> {
    let res = unsafe {
        libc::splice(
            fd_in,
            std::ptr::null_mut::<libc::loff_t>(),
            fd_out,
            std::ptr::null_mut::<libc::loff_t>(),
            size,
            0,
        )
    };
    if res < 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(res as usize)
    }
}

fn splice_copy<R, W>(
    input: R,
    output: W,
    counter: Arc<atomic::AtomicU64>,
    chunk_size: usize,
) -> MainResult
where
    R: AsRawFd,
    W: AsRawFd,
{
    let fd_in = input.as_raw_fd();
    let fd_out = output.as_raw_fd();

    loop {
        let written = splice(fd_in, fd_out, chunk_size)?;
        if written == 0 {
            break;
        }
        counter.fetch_add(written as u64, atomic::Ordering::Relaxed);
    }

    Ok(())
}

fn rw_copy<R, W>(
    mut input: R,
    mut output: W,
    counter: Arc<atomic::AtomicU64>,
    chunk_size: usize,
) -> MainResult
where
    R: io::Read,
    W: io::Write,
{
    let mut buffer = vec![0u8; chunk_size];
    Ok(loop {
        let mut read = input.read(&mut buffer)?;

        if read == 0 {
            break;
        }

        let mut offset = 0;

        while read > 0 {
            let written = output.write(&buffer[offset..offset + read])?;
            offset += written;
            read -= written;

            counter.fetch_add(written as u64, atomic::Ordering::Relaxed);
        }
    })
}
