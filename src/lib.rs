extern crate priority_queue;

use chrono::prelude::Utc;
use crossbeam::{
    channel::{bounded, unbounded, Receiver, Sender},
    select,
};
use priority_queue::DoublePriorityQueue;
use std::{
    borrow::Borrow,
    cell::RefCell,
    fmt,
    hash::{Hash, Hasher},
    sync::{Arc, Mutex},
    thread,
    time::Duration,
};
use threadpool_crossbeam_channel::ThreadPool;

/// Trigger is the trait for implementing triggers.
pub trait Trigger {
    /// next_fire_time calculates the next tick in which
    /// the job should execute. Returning an error
    /// signals the scheduler to remove the task.
    fn next_fire_time(&self) -> Result<i64, TriggerError>;
    fn description(&self) -> String;
}

/// Job is the trait that an implementor must conform to
/// in order to be schedulable and runnable by the scheduler.
pub trait Job: Send + Sync + 'static {
    /// execute is the method for executing the task. It gets
    /// called by the scheduler.
    fn execute(&self);
    /// description is user-defined description associated with
    /// the task.
    fn description(&self) -> String;
    /// key returns unique key associated with the task.
    fn key(&self) -> i64;
}

/// Scheduler implements the main scheduler.
pub struct Scheduler {
    lock: Arc<Mutex<()>>,
    queue: Arc<Mutex<DoublePriorityQueue<Box<Task>, i64>>>,
    pool: ThreadPool,
    interrupt: (Sender<bool>, Receiver<bool>),
    exit: (Sender<bool>, Receiver<bool>),
    feeder: (Sender<Box<Task>>, Receiver<Box<Task>>),
    started: bool,
}

/// SimpleTrigger fires after each `Duration`.
pub struct SimpleTrigger(Duration);

/// SimpleOnceTrigger fires once after specified delay.
pub struct SimpleOnceTrigger {
    delay: Duration,
    expired: RefCell<bool>,
}

/// SimpleCallbackJob executes `callback`.
pub struct SimpleCallbackJob {
    pub callback: Box<dyn Fn(&i64) + Send + Sync + 'static>,
    description: String,
    key_value: i64,
}

/// Task is a schedulable unit which encapsulates
/// the `job` and its `trigger`.
pub struct Task {
    pub job: Arc<dyn Job>,
    pub trigger: Box<dyn Trigger>,
    priority: i64,
    key: i64,
}

/// ScheduleJob contains meta data of
/// the scheduled job.
pub struct ScheduledJob {
    pub job: Arc<dyn Job>,
    pub trigger_description: String,
    pub next_runtime: i64,
}

#[derive(Debug, Clone)]
pub struct TriggerError;

unsafe impl Send for Task {}

impl PartialEq for Task {
    fn eq(&self, other: &Self) -> bool {
        self.job.key() == other.job.key()
    }
}

impl PartialEq for ScheduledJob {
    fn eq(&self, other: &Self) -> bool {
        self.job.key() == other.job.key()
    }
}

impl Eq for Task {}
impl Eq for ScheduledJob {}

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
        self.job.key().hash(state);
    }
}

impl Hash for ScheduledJob {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.job.key().hash(state);
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

impl fmt::Debug for ScheduledJob {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "ScheduledJob{{next_runtime: {}, job_description: {}}}",
            self.next_runtime,
            self.job.description()
        )
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

impl Borrow<i64> for Box<Task> {
    fn borrow(&self) -> &i64 {
        &self.key
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

    /// stop stops the execution loop and shuts down all
    /// channels.
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

    /// start starts the execution loop.
    pub fn start(&mut self) {
        let _lock = self.lock.lock().unwrap();

        if self.started {
            return;
        }

        self.started = true;
        self.start_feeder();
        self.start_execution_loop();
    }

    /// schedule_task schedules the given `task`.
    pub fn schedule_task(&self, task: Box<Task>) {
        let _lock = self.lock.lock().unwrap();
        _ = self.feeder.0.send(task);
    }

    /// clear drains all tasks.
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
			    _ = interrupt_tx.clone().send(true);
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
		select! {
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
                    }
		}
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

    /// get_scheduled_job returns meta data of
    /// job associated with the given `key`.
    pub fn get_scheduled_job(&self, key: i64) -> Option<ScheduledJob> {
        let _queue = self.queue.lock().unwrap();

        for (task, priority) in _queue.iter() {
            if task.job.key() == key {
                return Some(ScheduledJob {
                    job: task.job.clone(),
                    trigger_description: task.trigger.description(),
                    next_runtime: *priority,
                });
            }
        }

        None
    }

    /// delete_task deletes a task from scheduler.
    pub fn delete_task(&self, key: i64) -> bool {
        let mut _queue = self.queue.lock().unwrap();
        match _queue.remove(&key) {
            Some(_) => true,
            None => false,
        }
    }

    /// get_task_keys returns all task keys.
    pub fn get_task_keys(&self) -> Vec<i64> {
        let _queue = self.queue.lock().unwrap();
        let mut result = Vec::new();
        for (task, _) in _queue.iter() {
            result.push(task.job.key());
        }

        result
    }
}

fn calculate_next_tick(target_queue: Arc<Mutex<DoublePriorityQueue<Box<Task>, i64>>>) -> i64 {
    let mut interval: i64 = 0;
    let _queue = target_queue.lock().unwrap();
    if !_queue.is_empty() {
        interval = park_time(*_queue.peek_min().unwrap().1);
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
        _ = target_chan.send(task);
    }
}

#[inline(always)]
pub fn nownano() -> i64 {
    Utc::now().timestamp_nanos()
}

fn park_time(ts: i64) -> i64 {
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
        key: *&job.key(),
        job: Arc::new(job),
        priority: boxed_trigger.next_fire_time().ok().unwrap(),
        trigger: boxed_trigger,
    })
}

/// schedule_task_every schedules the given `job`
/// with a trigger that fires after every `tick`.
pub fn schedule_task_every(tick: Duration, job: impl Job) -> Box<Task> {
    schedule_task(job, SimpleTrigger(tick))
}

/// schedule_task_after schedules the given `job`
/// to run once after `tick`.
pub fn schedule_task_after(tick: Duration, job: impl Job) -> Box<Task> {
    schedule_task(job, SimpleOnceTrigger::new(tick))
}

/// schedule_task_with schedules the given `job`
/// with the given `trigger`.
pub fn schedule_task_with(job: impl Job, trigger: impl Trigger + 'static) -> Box<Task> {
    schedule_task(job, trigger)
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
            key: *&b.key(),
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

        thread::sleep(Duration::from_secs(1));

        assert_eq!(sched.get_task_keys().iter().sum::<i64>(), 12);

        thread::sleep(Duration::from_secs(20));

        assert_eq!(sched.get_scheduled_job(4).unwrap().job.key(), 4);
        assert_eq!(sched.get_scheduled_job(8).unwrap().job.key(), 8);
        assert_eq!(sched.get_scheduled_job(16), None);
        assert_eq!(sched.delete_task(8), true);
        assert_eq!(sched.delete_task(4), true);
        assert_eq!(sched.delete_task(16), false);

        thread::sleep(Duration::from_secs(40));
    }
}
