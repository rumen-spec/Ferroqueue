use std::hash::{Hash, Hasher};

use chrono::{self, DateTime, Utc};
use getset::{CopyGetters, Getters, Setters};
use uuid::Uuid;


#[derive(Debug, Getters, CopyGetters, Setters, Clone)]
pub struct Job {
    #[getset(get_copy = "pub")]
    id: Uuid,
    #[getset(get = "pub")]
    payload: String,
    #[getset(get = "pub", set = "pub")]
    state: JobState,
    #[getset(get_copy = "pub", set = "pub")]
    retries: i32,
    #[getset(get_copy = "pub", set = "pub")]
    retry_time_s: i32,
    #[getset(get_copy = "pub", set = "pub")]
    visible_at: Option<DateTime<Utc>>,
    #[getset(get_copy = "pub")]
    created_at: DateTime<Utc>
}

#[derive(Debug, Clone, Copy, Hash)]
pub struct VisibilityEntry{
    pub id: Uuid,
    pub visible_at: DateTime<Utc>
}

#[derive(Debug, Hash, Clone)]
pub enum JobState{
    READY,
    INFLIGHT,
    DLQ,
    COMPLETED
}

pub trait New {
    fn new(payload: String) -> Self;
}

impl New for Job {
    fn new(payload: String) -> Self {
        Job {
            id: Uuid::new_v4(),
            payload: payload,
            state: JobState::READY,
            retries: 0,
            retry_time_s: 0,
            visible_at: None,
            created_at: Utc::now()
        }
    }
}

impl PartialEq for Job{
    fn eq(&self, other: &Job) -> bool {
        return self.id == other.id
    }
}
impl Hash for Job{
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.id.hash(hasher);
    }
}

impl Eq for Job{}