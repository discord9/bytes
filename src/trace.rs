use backtrace::Backtrace;
use core::hash::Hash;
use crossbeam_channel::{unbounded, Sender};
use inferno::flamegraph::{self, Options};
use std::{
    collections::HashMap,
    format,
    fs::File,
    string::{String, ToString as _},
    sync::{Arc, Mutex, OnceLock},
    thread,
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
    // Other information can also be recorded, such as the thread ID at creation time, etc.
}

/// Bytes collector, collect pointer states.
#[derive(Debug)]
pub struct BytesCollector {
    receiver: crossbeam_channel::Receiver<RefCountEvent>,
    /// TODO: evict ref_cnt == 0 entries
    ptr_states: Mutex<std::collections::HashMap<usize, PtrState>>,
}

impl BytesCollector {
    fn handle_event(&self, event: RefCountEvent) {
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

    /// dump the pointer states for further inspection
    pub fn dump_states(&self) -> HashMap<usize, PtrState> {
        let states = self.ptr_states.lock().unwrap();
        (*states).clone()
    }

    /// Render flamegraphs to output file
    pub fn render_flamegraph(&self, output_file: &str) {
        let mut states = self.ptr_states.lock().unwrap();
        let mut stacks = Vec::new();

        for state in states.values_mut() {
            for (ref_count, cap, op, bt) in state.backtraces.iter_mut() {
                bt.resolve();
                if *ref_count == 0 {
                    continue;
                }
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
                let unique_stack_op = format!("ref_count={},op={:?}", ref_count, op);
                stacks.push(format!("{stack}; {unique_stack_op} {cap}"));
            }
        }

        let mut opts = Options::default();
        let mut file = File::create(output_file).unwrap();
        flamegraph::from_lines(&mut opts, stacks.iter().map(|s| s.as_str()), &mut file).unwrap();
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
