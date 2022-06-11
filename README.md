# quartz.rs

Minimalist scheduling library for Rust

This library is ported from [go-quartz](https://github.com/reugn/go-quartz) by [reugn](https://github.com/reugn).


### Example

```rust
use std::{thread, time::Duration};

struct MyTask;

impl quartz::Job for Box<MyTask> {
    fn execute(&self) {
        println!("executing mytask");
    }
    fn description(&self) -> String {
        "my task".to_string()
    }
    fn key(&self) -> i64 {
        43
    }
}

fn main() {
    let mut sched: quartz::Scheduler = quartz::Scheduler::new();
    
    // start the scheduler
    // spawns execution and feeder threads  
    sched.start();

    // execute after duration N
    sched.schedule_task(quartz::schedule_task_after(
        Duration::from_secs(4),
        Box::new(MyTask),
    ));

    // execute every interval N
    sched.schedule_task(quartz::schedule_task_every(
        Duration::from_secs(8),
        Box::new(quartz::SimpleCallbackJob::new(
            Box::new(|_| {
                println!("[+] From closure");
            }),
            "".to_string(),
            8,
        )),
    ));

    thread::sleep(Duration::from_secs(10));
    
    // scheduler will stop after getting dropped
    // alternatively, call sched.stop() to stop 
    // the scheduler.
}
```

#### Status

Not every feature is implemented yet.
Under development. Please use with caution.
