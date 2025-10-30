use backtrace::Backtrace;
use core::{fmt::Display, hash::Hash};
use crossbeam_channel::{unbounded, RecvTimeoutError, Sender};
use inferno::flamegraph::{self, Options};
use std::{
    collections::HashMap,
    format,
    string::{String, ToString as _},
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
    vec::Vec,
};

/// Only sample when address % 997 == 0
const SAMPLE_FACTOR: usize = 997;
/// Defines the operation type
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum RefOp {
    /// Increment reference (clone)
    Inc,
    /// Decrement reference (drop)
    Dec,
}

impl Display for RefOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RefOp::Inc => write!(f, "+"),
            RefOp::Dec => write!(f, "-"),
        }
    }
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

/// Trace bytes clone/split
#[derive(Debug)]
pub struct BytesTracer {
    sender: Arc<Sender<RefCountEvent>>,
    /// bytes collector
    pub collector: Arc<BytesCollector>,
}

impl BytesTracer {
    fn new() -> (Self, thread::JoinHandle<()>) {
        // Create an unbounded channel
        let (sender, receiver) = unbounded::<RefCountEvent>();
        let collector = Arc::new(BytesCollector {
            receiver,
            ptr_states: Mutex::new(HashMap::new()),
            clock: quanta::Clock::new(),
        });
        let inner = collector.clone();
        let handle = inner.run();

        (
            BytesTracer {
                sender: Arc::new(sender),
                collector,
            },
            handle,
        )
    }

    /// Function called by business threads to record events
    /// Use #[inline] to hint the compiler to inline, reducing function call overhead
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
    last_updated_at: quanta::Instant,
}

/// Bytes collector, collect pointer states.
#[derive(Debug)]
pub struct BytesCollector {
    receiver: crossbeam_channel::Receiver<RefCountEvent>,
    /// TODO: evict ref_cnt == 0 entries
    ptr_states: Mutex<std::collections::HashMap<usize, PtrState>>,
    clock: quanta::Clock,
}

impl BytesCollector {
    fn run(self: Arc<Self>) -> JoinHandle<()> {
        // Create a logging thread
        let handle = thread::spawn(move || {
            let timeout = Duration::from_secs(60);
            loop {
                match self.receiver.recv_timeout(timeout) {
                    Ok(event) => {
                        self.handle_event(event);
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        // No events for a while, clean up outdated entries.
                        self.clear_outdated();
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        // Channel disconnected, do a final cleanup and exit.
                        self.clear_outdated();
                        break;
                    }
                }
            }
        });

        handle
    }

    fn handle_event(&self, event: RefCountEvent) {
        let mut states = self.ptr_states.lock().unwrap();
        let state = states.entry(event.ptr).or_insert(PtrState {
            ref_count: 0,
            backtraces: Vec::new(),
            last_updated_at: self.clock.now(),
        });

        // ignore out of order, and just get a rough ref count(might be off or even negative temporarily)
        match event.op {
            RefOp::Inc => state.ref_count += 1,
            RefOp::Dec => state.ref_count -= 1,
        }

        if let Some(bt) = event.backtrace {
            state
                .backtraces
                .push((event.old_ref_cnt as usize, event.cap, event.op, bt));
        }

        state.last_updated_at = self.clock.now();
    }

    /// dump the pointer states for further inspection
    pub fn dump_states(&self) -> HashMap<usize, PtrState> {
        let states = self.ptr_states.lock().unwrap();
        (*states).clone()
    }

    fn clear_outdated(&self) {
        let mut states = self.ptr_states.lock().unwrap();

        let mut to_be_deleted = Vec::new();
        for (ptr, state) in states.iter_mut() {
            if state.ref_count == 0 {
                if state.last_updated_at.elapsed().as_secs() > 60 {
                    to_be_deleted.push(*ptr);
                }
                continue;
            }
            for (_, _, _, bt) in state.backtraces.iter_mut() {
                bt.resolve();
            }
        }

        for ptr in to_be_deleted {
            states.remove(&ptr);
        }
    }

    /// Render flamegraphs to output bytes
    pub fn render_flamegraph(&self) -> Vec<u8> {
        self.clear_outdated();
        let states = {
            let tmp = self.ptr_states.lock().unwrap().clone();
            tmp
        };

        let mut stacks = Vec::new();

        for state in states.values() {
            for (ref ref_count, ref cap, ref op, bt) in &state.backtraces {
                let mut stack = String::new();
                let frames = bt.frames().iter().rev();

                for frame in frames {
                    let symbols = frame.symbols();
                    if !symbols.is_empty() {
                        for symbol in symbols {
                            if let Some(name) = symbol.name() {
                                stack.push_str(&name.to_string());
                                stack.push_str(";")
                            }
                        }
                    } else {
                        stack.push_str(&format!("{:?};", frame.ip()));
                    }
                }

                // unique stack operation, faking as a frame
                let unique_stack_op = format!(
                    "ref_count={}=>{}",
                    ref_count,
                    match op {
                        RefOp::Inc => *ref_count + 1,
                        RefOp::Dec => *ref_count - 1,
                    }
                );
                stacks.push(format!("{stack}; {unique_stack_op} {cap}"));
            }
        }

        let mut opts = Options::default();
        let mut bytes = Vec::new();
        flamegraph::from_lines(&mut opts, stacks.iter().map(|s| s.as_str()), &mut bytes).unwrap();
        bytes
    }
}

/// Global bytes tracer to trace where did the clone happen.
pub static GLOBAL_TRACER: OnceLock<BytesTracer> = OnceLock::new();

// Helper function for easy calling in patched code
#[inline]
pub fn trace_event(ptr: usize, cap: usize, old_ref_cnt: usize, op: RefOp) {
    GLOBAL_TRACER
        .get_or_init(|| BytesTracer::new().0)
        .record(ptr, cap, old_ref_cnt, op);
}
