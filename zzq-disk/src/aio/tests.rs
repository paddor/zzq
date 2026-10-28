use std::os::unix::fs::OpenOptionsExt;

use super::*;
use crate::testing::TempDir;

/// A file opened with `O_DIRECT`, or `None` where the file system rejects it.
fn direct_file(path: &std::path::Path, write: bool) -> Option<Arc<File>> {
    std::fs::write(path, vec![0; 4 * ALIGNMENT]).unwrap();
    match std::fs::OpenOptions::new()
        .read(true)
        .write(write)
        .custom_flags(libc::O_DIRECT)
        .open(path)
    {
        Ok(file) => Some(Arc::new(file)),
        Err(error) if error.raw_os_error() == Some(libc::EINVAL) => {
            eprintln!("skipped: file system rejects O_DIRECT: {error}");
            None
        }
        Err(error) => panic!("{error}"),
    }
}

fn buffer(byte: u8, len: usize) -> AlignedBuf {
    let mut buffer = AlignedBuf::default();
    buffer.extend(&vec![byte; len]).unwrap();
    buffer
}

#[test]
fn buffers_stay_aligned_as_they_grow() {
    let mut buffer = AlignedBuf::default();
    buffer.reserve(10).unwrap();
    for n in 0..20_000_u32 {
        buffer.extend(&n.to_be_bytes()).unwrap();
        assert!(buffer.as_slice().as_ptr().addr().is_multiple_of(ALIGNMENT));
    }
    assert_eq!(buffer.len(), 80_000);
    assert_eq!(&buffer.as_slice()[79_996..], &19_999_u32.to_be_bytes());
    buffer.pad().unwrap();
    assert_eq!(buffer.len(), 81_920);
    buffer.clear();
    assert!(buffer.is_empty());
    buffer.pad().unwrap();
    assert!(buffer.is_empty());
    assert_eq!(
        buffer.reserve(usize::MAX).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    assert!(AioContext::<()>::new(0).is_err());
    assert!(AioContext::<()>::new(1025).is_err());
}

#[test]
fn write_is_read_back_and_signals_the_eventfd() {
    let dir = TempDir::new("aio-write");
    let path = dir.path().join("segment");
    let Some(file) = direct_file(&path, true) else {
        return;
    };
    let mut context = AioContext::<u32>::new(4).unwrap();
    context
        .submit_write(file, ALIGNMENT as u64, buffer(7, 2 * ALIGNMENT), 11)
        .unwrap();
    assert_eq!(context.in_flight(), 1);
    let mut done = Vec::new();
    while done.is_empty() {
        context
            .wait(|(job, buffer, result)| done.push((job, buffer.len(), result.unwrap())))
            .unwrap();
    }
    assert_eq!(done, [(11, 2 * ALIGNMENT, 2 * ALIGNMENT)]);
    assert_eq!(context.in_flight(), 0);
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes[..ALIGNMENT].iter().all(|&byte| byte == 0));
    assert!(
        bytes[ALIGNMENT..3 * ALIGNMENT]
            .iter()
            .all(|&byte| byte == 7)
    );

    // A completion makes the eventfd readable until the next reap drains it.
    let file = direct_file(&dir.path().join("second"), true).unwrap();
    context
        .submit_write(file, 0, buffer(1, ALIGNMENT), 12)
        .unwrap();
    let mut poll = [libc::pollfd {
        fd: context.as_fd().as_raw_fd(),
        events: libc::POLLIN,
        revents: 0,
    }];
    // SAFETY: one live pollfd.
    assert_eq!(unsafe { libc::poll(poll.as_mut_ptr(), 1, 5000) }, 1);
    let mut reaped = 0;
    while reaped == 0 {
        reaped = context
            .reap(|(job, _, result)| {
                assert_eq!(job, 12);
                result.unwrap();
            })
            .unwrap();
    }
    // A completion may signal between a drain and the collection that reaps
    // it, leaving one spurious signal. With nothing in flight, one more reap
    // clears it for good.
    assert_eq!(context.reap(|_| unreachable!()).unwrap(), 0);
    poll[0].revents = 0;
    // SAFETY: one live pollfd.
    assert_eq!(unsafe { libc::poll(poll.as_mut_ptr(), 1, 0) }, 0);
}

#[test]
fn unusable_writes_come_back_before_the_kernel() {
    let dir = TempDir::new("aio-unusable");
    let path = dir.path().join("segment");
    let Some(file) = direct_file(&path, true) else {
        return;
    };
    let mut context = AioContext::<u32>::new(1).unwrap();
    let (job, _, error) = context
        .submit_write(Arc::clone(&file), 100, buffer(1, ALIGNMENT), 2)
        .unwrap_err();
    assert_eq!((job, error.kind()), (2, io::ErrorKind::InvalidInput));
    let (_, _, error) = context
        .submit_write(Arc::clone(&file), 0, AlignedBuf::default(), 3)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let (_, _, error) = context
        .submit_write(Arc::clone(&file), 0, buffer(1, 100), 3)
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    let (_, _, error) = context
        .submit_write(
            Arc::clone(&file),
            (i64::MAX as u64) & !4095,
            buffer(1, ALIGNMENT),
            3,
        )
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(context.in_flight(), 0);

    // A full context hands the job back; after a reap it accepts again.
    context
        .submit_write(Arc::clone(&file), 0, buffer(2, ALIGNMENT), 4)
        .unwrap();
    assert!(context.is_full());
    let (job, _, error) = context
        .submit_write(Arc::clone(&file), 0, buffer(3, ALIGNMENT), 5)
        .unwrap_err();
    assert_eq!((job, error.kind()), (5, io::ErrorKind::WouldBlock));
    context
        .wait(|(_, _, result)| {
            result.unwrap();
        })
        .unwrap();
    context
        .submit_write(file, 0, buffer(3, ALIGNMENT), 5)
        .unwrap();
    context
        .wait(|(_, _, result)| {
            result.unwrap();
        })
        .unwrap();
}

#[test]
fn kernel_errors_arrive_as_failed_writes() {
    let dir = TempDir::new("aio-error");
    let path = dir.path().join("segment");
    let Some(read_only) = direct_file(&path, false) else {
        return;
    };
    let mut context = AioContext::<u32>::new(1).unwrap();
    // Most kernels reject the request at submission; otherwise it completes
    // with the error.
    let error =
        if let Err((_, _, error)) = context.submit_write(read_only, 0, buffer(1, ALIGNMENT), 1) {
            error
        } else {
            let mut error = None;
            while error.is_none() {
                context
                    .wait(|(_, _, result)| error = Some(result.unwrap_err()))
                    .unwrap();
            }
            error.unwrap()
        };
    assert_eq!(error.raw_os_error(), Some(libc::EBADF));
}

#[test]
fn dropping_the_context_waits_for_writes_in_flight() {
    let dir = TempDir::new("aio-drop");
    let path = dir.path().join("segment");
    let Some(file) = direct_file(&path, true) else {
        return;
    };
    let mut context = AioContext::<u32>::new(4).unwrap();
    for block in 0..4 {
        context
            .submit_write(
                Arc::clone(&file),
                (block * ALIGNMENT) as u64,
                buffer(block as u8 + 1, ALIGNMENT),
                block as u32,
            )
            .unwrap();
    }
    drop(context);
    let bytes = std::fs::read(&path).unwrap();
    for block in 0..4 {
        assert!(
            bytes[block * ALIGNMENT..][..ALIGNMENT]
                .iter()
                .all(|&byte| byte == block as u8 + 1)
        );
    }
}
