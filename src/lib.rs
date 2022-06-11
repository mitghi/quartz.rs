extern crate priority_queue;

use chrono::prelude::Utc;
use crossbeam::{
    channel::{bounded, unbounded, Receiver, Sender},
    select,
};
use priority_queue::DoublePriorityQueue;
use std::{
    cell::RefCell,
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use threadpool_crossbeam_channel::ThreadPool;

pub trait Trigger {
    fn next_fire_time(&self) -> Result<i64, TriggerError>;
    fn description(&self) -> String;
}

pub trait Job: Send + Sync + 'static {
    fn execute(&self);
    fn description(&self) -> String;
    fn key(&self) -> i64;
}

pub struct Scheduler {
    lock: Arc<Mutex<()>>,
    queue: Arc<Mutex<DoublePriorityQueue<Box<Task>, i64>>>,
    pool: ThreadPool,
    pub interrupt: (Sender<bool>, Receiver<bool>),
    pub exit: (Sender<bool>, Receiver<bool>),
    pub feeder: (Sender<Box<Task>>, Receiver<Box<Task>>),
    started: bool,
}

pub struct SimpleTrigger(Duration);

pub struct SimpleOnceTrigger {
    delay: Duration,
    expired: RefCell<bool>,
}

pub struct SimpleCallbackJob {
    pub callback: Box<dyn Fn(&i64) + Send + Sync + 'static>,
    description: String,
    key_value: i64,
}

pub struct Task {
    pub job: Arc<dyn Job>,
    pub trigger: Box<dyn Trigger>,
    priority: i64,
}

#[derive(Debug, Clone)]
pub struct TriggerError;

unsafe impl Send for Task {}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.job.key() == other.job.key()
    }
}

impl Eq for Task {}

impl SimpleCallbackJob {
    pub fn new(
        callback: Box<dyn Fn(&i64) + Send + Sync + 'static>,
        description: String,
        key: i64,
    ) -> Self {
        Self {
            callback,
            description,
            key_value: key,
        }
    }
}

impl Hash for Task {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.job.description().hash(state);
        self.trigger.description().hash(state);
    }
}

impl Job for Box<SimpleCallbackJob> {
    fn execute(&self) {
        (self.callback)(&self.key_value);
    }
    fn description(&self) -> String {
        self.description.clone()
    }
    fn key(&self) -> i64 {
        self.key_value
    }
}

impl Trigger for SimpleTrigger {
    fn next_fire_time(&self) -> Result<i64, TriggerError> {
        let result = nownano() + self.0.as_nanos() as i64;
        log::debug!("[*] Next fire time: {}", &result);
        return Ok(result);
    }

    fn description(&self) -> String {
        return "".to_string();
    }
}

impl fmt::Display for TriggerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "scheduler: Trigger has expired.")
    }
}

impl Trigger for SimpleOnceTrigger {
    fn next_fire_time(&self) -> Result<i64, TriggerError> {
        if *self.expired.borrow() {
            return Err(TriggerError);
        }
        let result = nownano() + self.delay.as_nanos() as i64;
        self.expired.replace(true);
        return Ok(result);
    }
    fn description(&self) -> String {
        // TODO(): implement this
        return "".to_string();
    }
}

impl SimpleOnceTrigger {
    fn new(delay: Duration) -> Self {
        Self {
            delay,
            expired: RefCell::new(false),
        }
    }
}

impl Drop for Scheduler {
    fn drop(&mut self) {
        self.stop();
    }
}

impl Scheduler {
    pub fn new() -> Self {
        Self {
            lock: Arc::new(Mutex::new(())),
            queue: Arc::new(Mutex::new(DoublePriorityQueue::new())),
            pool: ThreadPool::new(8),
            interrupt: bounded(1),
            exit: unbounded(),
            feeder: bounded(1),
            started: false,
        }
    }

    pub fn stop(&mut self) {
        let _lock = self.lock.lock().unwrap();

        if !self.started {
            return;
        }

        for _ in 0..2 {
            match self.exit.0.send(true) {
                Ok(_) => {}
                Err(err) => {
                    panic!("{}", err);
                }
            }
        }
    }

    pub fn start(&mut self) {
        let _lock = self.lock.lock().unwrap();

        if self.started {
            return;
        }

        self.started = true;
        self.start_feeder();
        self.start_execution_loop();
    }

    pub fn schedule_task(&self, task: Box<Task>) {
        let _lock = self.lock.lock().unwrap();
        self.feeder.0.send(task);
    }

    fn get_queue_len(&self) -> usize {
        let queue = self.queue.lock().unwrap();
        queue.len()
    }

    pub fn clear(&self) {
        let mut queue = self.queue.lock().unwrap();
        queue.clear();
    }

    #[rustfmt::skip]
    fn start_feeder(&self) {
        let feeder_rx = self.feeder.1.clone();
        let interrupt_tx = self.interrupt.0.clone();
        let exit_rx = self.exit.1.clone();
        let queue = self.queue.clone();
	
        thread::spawn(move || loop {
            select! {
                recv(feeder_rx) -> msg => {
                    match msg {
			Ok(value) => {
			    log::debug!("[+] Writing task to queue");
			    let priority = *&value.priority;
			    queue.lock().unwrap().push(value, priority);
			    interrupt_tx.clone().send(true);
			},
			Err(err) => {
			    panic!("{}", err);
			},
                    }
                },
                recv(exit_rx) -> msg => {
                    match msg {
			Ok(_) => {
			    return;
			},
			Err(err) => {
			    panic!("{}", err);
			}
                    }
                }
            };
        });
    }

    #[rustfmt::skip]
    fn start_execution_loop(&self) {
        let exit_rx = self.exit.1.clone();
        let feeder_tx = self.feeder.0.clone();
        let queue = self.queue.clone();
        let interrupt_rx = self.interrupt.1.clone();
        let pool = self.pool.clone();

        thread::spawn(move || loop {
            if queue.lock().unwrap().len() == 0 {
            } else {
                select! {
                    default(Duration::from_nanos(calculate_next_tick(queue.clone()).try_into().unwrap())) => {
			execute_and_reschedule(queue.clone(), feeder_tx.clone(), pool.clone());
                    },
                    recv(interrupt_rx) -> msg => {
			match msg {
                            Ok(_) => {
				log::debug!("[*] handling interrupt");
                            },
                            Err(_) => {},
			}
                    },
                    recv(exit_rx) -> msg => {
			match msg {
                            Ok(_) => {
				return
                            },
                            Err(_) => {},
			}
                    },
                }
            }
        });
    }
}

fn calculate_next_tick(target_queue: Arc<Mutex<DoublePriorityQueue<Box<Task>, i64>>>) -> i64 {
    let mut interval: i64 = 0;
    let queue = target_queue.lock().unwrap();
    if !queue.is_empty() {
        interval = park_time(*queue.peek_min().unwrap().1);
        log::debug!("[+] Next tick: {}", &interval);
    }

    interval
}

fn execute_and_reschedule(
    target_queue: Arc<Mutex<DoublePriorityQueue<Box<Task>, i64>>>,
    target_chan: Sender<Box<Task>>,
    pool: ThreadPool,
) {
    let mut queue = target_queue.lock().unwrap();
    if queue.len() == 0 {
        return;
    }
    let item = queue.pop_min();
    let (mut task, _) = item.unwrap();
    task.priority = match task.trigger.next_fire_time() {
        Ok(next_fire_time) => next_fire_time,
        Err(_) => 0,
    };
    let job_handle = task.job.clone();
    pool.execute(move || {
        job_handle.execute();
    });
    if task.priority > 0 {
        target_chan.send(task);
    }
}

#[inline(always)]
pub fn nownano() -> i64 {
    Utc::now().timestamp_nanos()
}

pub fn park_time(ts: i64) -> i64 {
    let now = nownano();
    if ts > now {
        log::debug!("[*] current and now: {} - {}", &ts, &now);
        return ts - now;
    }
    return 0;
}

#[inline(always)]
fn schedule_task(job: impl Job, trigger: impl Trigger + 'static) -> Box<Task> {
    let boxed_trigger = Box::new(trigger);
    Box::new(Task {
        job: Arc::new(job),
        priority: boxed_trigger.next_fire_time().ok().unwrap(),
        trigger: boxed_trigger,
    })
}

pub fn schedule_task_every(tick: Duration, job: impl Job) -> Box<Task> {
    schedule_task(job, SimpleTrigger(tick))
}

pub fn schedule_task_after(tick: Duration, job: impl Job) -> Box<Task> {
    schedule_task(job, SimpleOnceTrigger::new(tick))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_nownano() {
        assert_ne!(nownano(), 0);
    }

    #[test]
    fn test_callback() {
        fn run(input: &impl Job) {
            input.execute();
        }

        let a = Box::new(SimpleCallbackJob::new(
            Box::new(|a: &i64| {
                println!("[+++] From closure, value {}", a);
            }),
            "none".to_string(),
            8,
        ));

        run(&a);
        run(&a);
        run(&a);
    }

    fn create_task(tick: i64) -> Box<Task> {
        let b = Box::new(SimpleCallbackJob {
            callback: Box::new(|a: &i64| println!("[+++] From closure, value {}", a)),
            description: "".to_string(),
            key_value: tick,
        });

        let tick = Duration::new(tick as u64, 0);
        let tick_nanos = *(&tick.as_nanos()) as i64;
        Box::new(Task {
            job: Arc::new(b),
            trigger: Box::new(SimpleTrigger(tick)),
            priority: nownano() + tick_nanos,
        })
    }

    #[test]
    fn test_pqueue() {
        let mut pq: DoublePriorityQueue<Box<Task>, i64> = DoublePriorityQueue::new();
        let args: [i64; 4] = [10, 20, 40, 0];

        for arg in args {
            let v = create_task(*&arg);
            let priority = *&v.priority;
            pq.push(v, priority);
        }

        assert_eq!(pq.pop_min().unwrap().0.job.key(), 0);
        assert_eq!(pq.pop_max().unwrap().0.job.key(), 40);
    }

    #[test]
    fn test_scheduler() {
	env_logger::init();	
        let mut sched = Box::new(Scheduler::new());
        sched.start();

        let b = 10;
        let job_one = Box::new(SimpleCallbackJob {
            callback: Box::new(move |a: &i64| {
                println!("[+++] From closure, a {}, b {}", a, b);
            }),
            description: "".to_string(),
            key_value: 4,
        });

        let job_two = Box::new(SimpleCallbackJob {
            callback: Box::new(|_| {
                println!("[***] From second closure");
            }),
            description: "".to_string(),
            key_value: 8,
        });

        sched.schedule_task(schedule_task_every(Duration::from_secs(1), job_one));
        sched.schedule_task(schedule_task_every(Duration::from_secs(4), job_two));
        thread::sleep(Duration::from_secs(40));
    }
}
