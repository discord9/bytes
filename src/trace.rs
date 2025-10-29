use backtrace::Backtrace;
use core::{cell::OnceCell, hash::Hash, str::FromStr};
use crossbeam_channel::{unbounded, Sender};
use quanta::Instant;
use std::{
    collections::HashMap,
    dbg,
    string::String,
    sync::{Arc, Mutex, OnceLock},
    thread,
    vec::Vec,
};

use crate::Bytes;

/// Only sample when address % 997 == 0
const SAMPLE_FACTOR: usize = 997;
/// Defines the operation type
#[derive(Debug, Copy, Clone)]
pub enum RefOp {
    /// Increment reference (clone)
    Inc,
    /// Decrement reference (drop)
    Dec,
}

// The event we pass in the queue
#[derive(Debug)]
pub struct RefCountEvent {
    /// A pointer to the data area, as a unique identifier.
    pub ptr: usize,
    /// memory usage(capacity) at the time of operation
    pub cap: usize,
    pub old_ref_cnt: usize,
    pub op: RefOp,
    /// Optional backtrace, maybe record 1 in 1000 allocations?
    pub backtrace: Option<Backtrace>,
}

pub struct BytesTracer {
    sender: Arc<Sender<RefCountEvent>>,
    collector: Arc<BytesCollector>,
}

impl BytesTracer {
    fn new() -> (Self, thread::JoinHandle<()>) {
        // Create an unbounded channel
        let (sender, receiver) = unbounded::<RefCountEvent>();
        let collector = Arc::new(BytesCollector {
            receiver,
            ptr_states: Mutex::new(HashMap::new()),
        });
        let inner = collector.clone();

        // Create a logging thread
        let handle = thread::spawn(move || {
            while let Ok(event) = inner.receiver.recv() {
                inner.handle_event(event);
            }
        });

        (
            BytesTracer {
                sender: Arc::new(sender),
                collector,
            },
            handle,
        )
    }

    // Function called by business threads to record events
    // Use #[inline] to hint the compiler to inline, reducing function call overhead
    #[inline]
    pub fn record(&self, ptr: usize, cap: usize, old_ref_cnt: usize, op: RefOp) {
        if ptr % SAMPLE_FACTOR != 0 {
            return;
        }
        // Getting thread ID and timestamp is a very fast operation
        let event = RefCountEvent {
            ptr,
            cap,
            old_ref_cnt,
            op,
            backtrace: Some(Backtrace::new_unresolved()),
        };

        // Send the event to the queue. This is a non-blocking or very low-blocking operation
        // If the channel is disconnected (logging thread panics), an error will occur here, which can be ignored
        let _ = self.sender.send(event);
    }
}

/// State maintained inside the logging thread
#[derive(Debug, Clone)]
pub struct PtrState {
    /// Use a signed integer to safely handle temporary negative values
    ref_count: i64,
    /// might record multiple backtraces at different ref count changes
    /// (ref_count, cap, op, backtrace)
    backtraces: Vec<(usize, usize, RefOp, Backtrace)>,
    // Other information can also be recorded, such as the thread ID at creation time, etc.
}

pub struct BytesCollector {
    receiver: crossbeam_channel::Receiver<RefCountEvent>,
    ptr_states: Mutex<std::collections::HashMap<usize, PtrState>>,
}

impl BytesCollector {
    pub fn handle_event(&self, event: RefCountEvent) {
        let mut states = self.ptr_states.lock().unwrap();
        let state = states.entry(event.ptr).or_insert(PtrState {
            ref_count: 0,
            backtraces: Vec::new(),
        });

        state.ref_count = event.old_ref_cnt as i64;

        match event.op {
            RefOp::Inc => state.ref_count += 1,
            RefOp::Dec => state.ref_count -= 1,
        }

        if let Some(bt) = event.backtrace {
            state
                .backtraces
                .push((event.old_ref_cnt as usize, event.cap, event.op, bt));
        }

        // Additional logic can be added here, such as logging when ref_count reaches zero
    }

    pub fn dump_states(&self) -> HashMap<usize, PtrState> {
        let states = self.ptr_states.lock().unwrap();
        (*states).clone()
    }
}

// We need a place to store the JoinHandle to wait for the logging thread to complete when the program ends
// But to simplify the API, we can choose to "detach" the thread, letting it exit with the main program
pub static GLOBAL_TRACER: OnceLock<BytesTracer> = OnceLock::new();

// Helper function for easy calling in patched code
#[inline]
pub fn trace_event(ptr: usize, cap: usize, old_ref_cnt: usize, op: RefOp) {
    GLOBAL_TRACER
        .get_or_init(|| BytesTracer::new().0)
        .record(ptr, cap, old_ref_cnt, op);
}

#[test]
fn test_bytes_collector() {
    let mut buf = String::from_str("hello world").unwrap().into_bytes();
    // increase capacity to make sure cloning will happen
    buf.reserve(100);
    let a = Bytes::from(buf);
    let b = a.clone();

    std::thread::sleep(std::time::Duration::from_millis(100));

    let states = GLOBAL_TRACER
        .get_or_init(|| BytesTracer::new().0)
        .collector
        .dump_states();
    dbg!(states);
    dbg!(b);
}
