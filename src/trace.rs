use backtrace::Backtrace;
use core::{fmt::Display, hash::Hash};
use crossbeam_channel::{unbounded, RecvTimeoutError, Sender};
use inferno::flamegraph::{self, Options};
use std::{
    collections::HashMap,
    env, format,
    string::{String, ToString as _},
    sync::{Arc, Mutex, OnceLock},
    thread::{self, JoinHandle},
    time::Duration,
    vec::Vec,
};

/// Only sample when address % sample_factor == 0
static SAMPLE_FACTOR: OnceLock<usize> = OnceLock::new();

fn sample_factor() -> usize {
    *SAMPLE_FACTOR.get_or_init(|| {
        env::var("BYTES_SAMPLE_FACTOR")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(997)
    })
}
/// Defines the operation type
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub enum RefOp {
    /// Create a new reference (allocation)
    Init,
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
            RefOp::Init => write!(f, "init"),
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
    /// Maybe change to sample every X MB memory?
    ptr_count: Mutex<usize>,
}

impl BytesTracer {
    /// Create a new BytesTracer and start the logging thread
    pub fn new() -> (Self, thread::JoinHandle<()>) {
        // Create an unbounded channel
        let (sender, receiver) = unbounded::<RefCountEvent>();
        let collector = Arc::new(BytesCollector {
            receiver,
            ptr_states: Mutex::new(HashMap::new()),
            ptr_trace: papaya::HashSet::new(),
            clock: quanta::Clock::new(),
        });
        let inner = collector.clone();
        let handle = inner.run();

        (
            BytesTracer {
                sender: Arc::new(sender),
                collector,
                ptr_count: Mutex::new(0),
            },
            handle,
        )
    }

    /// Function called by business threads to record events
    /// Use #[inline] to hint the compiler to inline, reducing function call overhead
    #[inline]
    pub fn record(&self, ptr: usize, cap: usize, old_ref_cnt: usize, op: RefOp) {
        // FIXME: need a immediate check for ptr is traced
        if self.collector.ptr_trace.pin().contains(&ptr) {
            // fall through
        } else {
            if op != RefOp::Init {
                // only trace newly allocated pointers
                return;
            }
            // only trace every `sample_factor` allocations
            let count = {
                let mut count = self.ptr_count.lock().unwrap();
                *count += 1;
                *count
            };
            // new unsampled pointer should only be sampled based on sample factor
            // and is newly allocated (ref count == 1)
            if count % sample_factor() != 0 {
                return;
            }
            self.collector.ptr_trace.pin().insert(ptr);
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
    ptr_trace: papaya::HashSet<usize>,
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
            // initialize ref count from the first event
            ref_count: event.old_ref_cnt as i64,
            backtraces: Vec::new(),
            last_updated_at: self.clock.now(),
        });

        // ignore out of order, and just get a rough ref count(might be off or even negative temporarily)
        match event.op {
            RefOp::Inc => state.ref_count += 1,
            RefOp::Dec => state.ref_count -= 1,
            RefOp::Init => (),
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
        self.clear_outdated();
        let states = self.ptr_states.lock().unwrap();
        (*states).clone()
    }

    fn clear_outdated(&self) {
        let mut states = self.ptr_states.lock().unwrap();

        let mut to_be_deleted = Vec::new();
        for (ptr, state) in states.iter_mut() {
            if state.ref_count == 0
                || state
                    .backtraces
                    .iter()
                    .any(|(ref_count, _, ref_op, _)| *ref_count == 1 && *ref_op == RefOp::Dec)
            // have drop operation(consider if out of order recv might cause false positive?)
            {
                if state.last_updated_at.elapsed().as_secs() > 3 {
                    to_be_deleted.push(*ptr);
                }
                continue;
            }
        }

        let traces = self.ptr_trace.pin();
        for ptr in &to_be_deleted {
            states.remove(ptr);
            traces.remove(ptr);
        }
    }

    /// Render flamegraphs to output bytes
    pub fn render_flamegraph(&self) -> std::io::Result<Vec<u8>> {
        self.clear_outdated();
        let states = {
            let tmp = self.ptr_states.lock().unwrap().clone();
            tmp
        };

        let mut aggregated_stacks = AggregatedStacks::new();

        for state in states.values() {
            for (ref ref_count, ref cap, ref op, bt) in &state.backtraces {
                aggregated_stacks.add_event(bt, *ref_count, *op, *cap);
            }
        }

        let stacks = aggregated_stacks.render();

        if stacks.is_empty() {
            return Ok(r#"<svg xmlns="http://www.w3.org/2000/svg" width="800" height="100"><text x="50%" y="50%" dominant-baseline="middle" text-anchor="middle" font-size="24" fill="red">no bytes trace data found</text></svg>"#.as_bytes().to_vec());
        }

        let mut opts = Options::default();
        opts.title = "inuse space(estimated)&op by Bytes/BytesMut".to_string();
        opts.count_name = "bytes".to_string();
        let mut bytes = Vec::new();
        flamegraph::from_lines(&mut opts, stacks.iter().map(|s| s.as_str()), &mut bytes)?;
        Ok(bytes)
    }
}

struct AggregatedStacks {
    // key: backtrace_ips
    // value: resolved_stack_string
    resolved_backtraces: HashMap<Vec<usize>, String>,
    // key: (backtrace_ips, (ref_count, op))
    // value: aggregated_cap
    aggregated_caps: HashMap<(Vec<usize>, (usize, RefOp)), usize>,
}

impl AggregatedStacks {
    fn new() -> Self {
        Self {
            resolved_backtraces: HashMap::new(),
            aggregated_caps: HashMap::new(),
        }
    }

    fn add_event(&mut self, bt: &Backtrace, ref_count: usize, op: RefOp, cap: usize) {
        let key_ips = bt.frames().iter().map(|f| f.ip() as usize).collect::<Vec<_>>();

        self.resolved_backtraces.entry(key_ips.clone()).or_insert_with(|| {
            let mut stack = String::new();
            let mut bt = bt.clone();
            bt.resolve();
            let frames = bt.frames().iter().rev();

            for frame in frames {
                let symbols = frame.symbols();
                if !symbols.is_empty() {
                    for symbol in symbols {
                        if let Some(name) = symbol.name() {
                            let lineno = symbol.lineno().unwrap_or(0);
                            let colno = symbol.colno().unwrap_or(0);
                            stack.push_str(&format!("{}:{}:{};", name, lineno, colno));
                        }
                    }
                } else {
                    stack.push_str(&format!("{:?};", frame.ip()));
                }
            }
            stack
        });

        let real_cap = cap * sample_factor();

        *self
            .aggregated_caps
            .entry((key_ips, (ref_count, op)))
            .or_insert(0) += real_cap;
    }

    fn render(self) -> Vec<String> {
        self.aggregated_caps
            .into_iter()
            .map(|((ips, (ref_count, op)), aggregated_cap)| {
                let stack = self.resolved_backtraces.get(&ips).unwrap();
                let unique_stack_op = match op {
                    RefOp::Inc => format!("rc={}=>{}", ref_count, ref_count + 1),
                    RefOp::Dec => format!("rc={}=>{}", ref_count, ref_count - 1),
                    RefOp::Init => format!("rc={}", ref_count),
                };
                format!("{stack} {unique_stack_op} {aggregated_cap}")
            })
            .collect()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Bytes;
    use std::fs::File;
    use std::io::Write;

    #[test]
    fn trace_and_render_flamegraph() {
        let (tracer, _handle) = BytesTracer::new();
        let collector = tracer.collector.clone();
        let _ = GLOBAL_TRACER.set(tracer);

        let mut v = Vec::new();

        for _ in 0..1_000_000 {
            let mut buf = String::from("deaddeef").into_bytes();
            buf.reserve(1000);
            let b = Bytes::from(buf);
            v.push(b.clone());
            v.push(b.clone());
            v.push(b.clone());
            v.push(b);
        }

        // give some time for the collector to process events
        thread::sleep(Duration::from_millis(100));
        std::dbg!(collector.dump_states().len());

        let flamegraph_bytes = collector.render_flamegraph().unwrap();
        let mut file = File::create("flamegraph_after_clone.svg").unwrap();
        file.write_all(&flamegraph_bytes).unwrap();
        let mut i = 0;
        v.retain(|_| {
            i += 1;
            i % 4 == 0
        });

        thread::sleep(Duration::from_millis(100));
        std::dbg!(collector.dump_states().len());
        let flamegraph_bytes = collector.render_flamegraph().unwrap();
        let mut file = File::create("flamegraph_after_dec.svg").unwrap();
        file.write_all(&flamegraph_bytes).unwrap();

        drop(v);

        thread::sleep(Duration::from_secs(6));
        GLOBAL_TRACER.get().unwrap().collector.clear_outdated();
        std::dbg!(collector.dump_states().len());
        // calc ref cnt distribution, calc how many pointers are at each ref count
        std::dbg!(collector
            .dump_states()
            .iter()
            .map(|(_, v)| v.ref_count)
            .fold(HashMap::new(), |mut acc, ref_count| {
                *acc.entry(ref_count).or_insert(0) += 1;
                acc
            }));
        let flamegraph_bytes = collector.render_flamegraph().unwrap();
        let mut file = File::create("flamegraph_after_drop.svg").unwrap();
        file.write_all(&flamegraph_bytes).unwrap();
    }
}
