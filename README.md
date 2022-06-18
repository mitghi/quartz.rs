# quartz.rs

Minimalist scheduling library for Rust

### Example

```rust
use std::{thread, time::Duration};

// MyTask is simple task run by scheduler.
struct MyTask;

// MyWetter is simple task which requests
// whether report.
struct MyWetter;

// Implement Job trait for `MyTask` to make
// `MyTask` schedulable.
impl quartz_sched::Job for Box<MyTask> {
    fn execute(&self) {
        println!("[+] Executing 'MyTask'");
    }
    fn description(&self) -> String {
        "my task".to_string()
    }
    fn key(&self) -> i64 {
        43
    }
}

// Implement Job trait for `MyWetter` to make
// `MyWetter` schedulable.
impl quartz_sched::Job for Box<MyWetter> {
    fn execute(&self) {
        // request whether report for Berlin
        match reqwest::blocking::get("https://wttr.in/berlin?format=3") {
            Ok(response) => match response.text() {
                Ok(result) => {
                    println!("{}", &result);
                }
                Err(_) => {}
            },
            Err(_) => {}
        }
    }
    fn description(&self) -> String {
        "my wetter".to_string()
    }
    fn key(&self) -> i64 {
        128
    }
}

// CustomTrigger is a simple custom trigger
// which quits the execution loop after
// `ref_count` reaches a certain threshold.
struct CustomTrigger {
    interval: Duration,
    ref_count: std::cell::RefCell<i64>,
}

// Implement `Trigger` for `CustomTrigger`
// to schedule the task based on value
// returned from `next_fire_time`.
impl quartz_sched::Trigger for CustomTrigger {
    fn next_fire_time(&self) -> Result<i64, quartz_sched::TriggerError> {
        if *self.ref_count.borrow() == 10 {
            return Err(quartz_sched::TriggerError);
        }
        let result = quartz_sched::nownano() + self.interval.as_nanos() as i64;
        *self.ref_count.borrow_mut() += 1;
        return Ok(result);
    }

    fn description(&self) -> String {
        format!("CustomTrigger{{ref_count: {}}}", &self.ref_count.borrow())
    }
}

fn main() {
    let mut sched: quartz_sched::Scheduler = quartz_sched::Scheduler::new();

    // start the scheduler
    // spawns execution and feeder threads
    sched.start();

    // execute after duration N
    sched.schedule_task(quartz_sched::schedule_task_after(
        Duration::from_secs(1),
        Box::new(MyTask),
    ));

    // execute every interval N
    sched.schedule_task(quartz_sched::schedule_task_every(
        Duration::from_secs(4),
        Box::new(quartz_sched::SimpleCallbackJob::new(
            Box::new(|_| {
                println!("[+] From closure");
            }),
            "".to_string(),
            8,
        )),
    ));

    // execute with custom trigger
    sched.schedule_task(quartz_sched::schedule_task_with(
        Box::new(MyWetter),
        CustomTrigger {
            interval: Duration::from_millis(2000),
            // trigger expires after `ref_count` reaches
            // a certain threshold, therefore will be
            // removed by the scheduler from the execution
            // loop.
            ref_count: std::cell::RefCell::new(0),
        },
    ));

    // get scheduled job meta info from scheduler
    match sched.get_scheduled_job(/*key*/ 8) {
        Some(job) => {
            println!("[+] Next run at tick: {}", &job.next_runtime);
        }
        None => {}
    };

    thread::sleep(Duration::from_secs(20));

    // scheduler will stop after getting dropped
    // alternatively, call sched.stop() to stop
    // the scheduler.

    // delete task associated with key from scheduler
    _ = sched.delete_task(43);
}
```

#### Status

This library is ported from [go-quartz](https://github.com/reugn/go-quartz) by [reugn](https://github.com/reugn).
Not every feature is implemented yet. Under development. Please use with caution.
