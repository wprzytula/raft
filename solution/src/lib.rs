use async_channel::Sender;
use std::collections::{HashMap, HashSet};
use std::iter::repeat;
use std::ops::RangeInclusive;
use std::time::{Duration, SystemTime};

use executor::{Handler, ModuleRef, System};
use rand::distributions::{Distribution, Uniform};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::state_machine_manager::{
    CommandOutput, SessionManagement, StateMachineManager, StateMachineRequest,
    StateMachineResponse,
};
pub use domain::*;
use timing::{ElectionTimeout, HeartbeatTimeout, Timer, VotingReluctanceTimeout};

mod domain;

const STATE_KEYNAME: &str = "state";
const SNAPSHOT_KEYNAME: &str = "snapshot";

#[derive(PartialEq, Eq, PartialOrd, Ord)]
struct LogStamp(pub u64, pub usize);

mod timing {
    use crate::Raft;
    use executor::{Handler, ModuleRef};
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    pub trait Timeout {}

    #[derive(Default)]
    pub struct ElectionTimeout;
    impl Timeout for ElectionTimeout {}

    #[derive(Default)]
    pub struct VotingReluctanceTimeout;
    impl Timeout for VotingReluctanceTimeout {}

    #[derive(Default)]
    pub struct HeartbeatTimeout;
    impl Timeout for HeartbeatTimeout {}

    pub struct Timer<T: 'static + Timeout + Default + std::marker::Send>
    where
        Raft: Handler<T>,
    {
        timer_abort: Arc<AtomicBool>,
        marker: std::marker::PhantomData<T>,
    }

    impl<T: 'static + Timeout + Default + std::marker::Send> Timer<T>
    where
        Raft: Handler<T>,
    {
        async fn run_timer(raft_ref: ModuleRef<Raft>, interval: Duration, abort: Arc<AtomicBool>) {
            let mut interval = tokio::time::interval(interval);
            interval.tick().await;
            interval.tick().await;
            while !abort.load(Ordering::Relaxed) {
                raft_ref.send(T::default()).await;
                interval.tick().await;
            }
        }

        pub fn new() -> Self {
            Self {
                timer_abort: Arc::new(AtomicBool::new(false)),
                marker: Default::default(),
            }
        }

        pub fn stop_timer(&mut self) {
            self.timer_abort.store(true, Ordering::Relaxed);
        }

        pub fn reset_timer(&mut self, raft_ref: ModuleRef<Raft>, interval: Duration) {
            // log::debug!("Timer set to {:#?}", interval);
            self.timer_abort.store(true, Ordering::Relaxed);
            self.timer_abort = Arc::new(AtomicBool::new(false));
            tokio::spawn(Self::run_timer(
                raft_ref,
                interval,
                self.timer_abort.clone(),
            ));
        }
    }

    impl<T: 'static + Timeout + Default + std::marker::Send> Drop for Timer<T>
    where
        Raft: Handler<T>,
    {
        fn drop(&mut self) {
            self.stop_timer();
        }
    }
}

mod state_machine_manager {
    mod sessions {
        use crate::ClientSession;
        use std::collections::HashMap;
        use std::time::{Duration, SystemTime};
        use uuid::Uuid;

        pub enum Response {
            NotYetApplied,
            AlreadyApplied(Vec<u8>),
            SessionExpired,
        }

        impl ClientSession {
            pub fn new(creation_time: SystemTime) -> Self {
                Self {
                    last_activity: creation_time,
                    lowest_sequence_num_without_response: 0,
                    responses: Default::default(),
                }
            }

            pub fn save_response(&mut self, sequence_id: u64, response: Vec<u8>) {
                self.responses.insert(sequence_id, response);
            }

            pub fn reuse_response(&self, sequence_num: u64) -> Response {
                if self.lowest_sequence_num_without_response > sequence_num {
                    Response::SessionExpired
                } else {
                    match self.responses.get(&sequence_num) {
                        None => Response::NotYetApplied,
                        Some(response) => Response::AlreadyApplied(response.clone()),
                    }
                }
            }

            pub fn remove_acknowledged_lower_than(
                &mut self,
                lowest_sequence_num_without_response: u64,
            ) {
                if self.lowest_sequence_num_without_response < lowest_sequence_num_without_response
                {
                    let len_before = self.responses.len();
                    self.responses
                        .retain(|&k, _v| k >= lowest_sequence_num_without_response);
                    let len_after = self.responses.len();
                    log::debug!(
                        "Removed {} acknowledged response(s). (self={}, new={})",
                        len_before - len_after,
                        self.lowest_sequence_num_without_response,
                        lowest_sequence_num_without_response
                    );

                    self.lowest_sequence_num_without_response =
                        lowest_sequence_num_without_response;
                }
            }
        }

        pub struct SessionManagement {
            self_id: Uuid,
            session_expiration: Duration,
            // This nested HashMap is number of repeated requests awaiting for State Machine
            // application of the command, per sequence_num.
            sessions: HashMap<Uuid, ClientSession>,
            // last_activities: BinaryHeap<Reverse<(SystemTime, Uuid)>>,
        }

        impl SessionManagement {
            pub fn new(self_id: Uuid, session_expiration: Duration) -> Self {
                Self {
                    self_id,
                    session_expiration,
                    sessions: Default::default(),
                    // last_activities: Default::default(),
                }
            }

            pub fn init_from(&mut self, sessions: HashMap<Uuid, ClientSession>) {
                self.sessions = sessions;
            }

            pub fn new_session(&mut self, client_id: Uuid, creation_time: SystemTime) {
                self.sessions
                    .insert(client_id, ClientSession::new(creation_time));
                // self.last_activities.push(Reverse((creation_time, client_id)));
                log::debug!(
                    "{}: Initialized session for client: {}",
                    self.self_id,
                    client_id
                );
            }

            pub fn save_response(&mut self, client_id: Uuid, sequence_id: u64, response: Vec<u8>) {
                log::debug!(
                    "{}: Saved response for retransmissions: {:#?}",
                    self.self_id,
                    response
                );
                self.sessions
                    .get_mut(&client_id)
                    .unwrap()
                    .save_response(sequence_id, response);
            }

            pub fn reuse_response(&self, client_id: &Uuid, sequence_num: u64) -> Response {
                self.sessions
                    .get(client_id)
                    .map_or(Response::SessionExpired, |session| {
                        session.reuse_response(sequence_num)
                    })
            }

            pub fn share_sessions(&self) -> HashMap<Uuid, ClientSession> {
                self.sessions.clone()
            }

            pub fn new_activity(
                &mut self,
                client_id: Uuid,
                timestamp: SystemTime,
                lowest_sequence_num_without_response: u64,
            ) {
                if let Some(session) = self.sessions.get_mut(&client_id) {
                    log::debug!("{}: New activity of client: {}", self.self_id, client_id);
                    session.remove_acknowledged_lower_than(lowest_sequence_num_without_response);
                    session.last_activity = timestamp;
                    // self.last_activities.push(Reverse((timestamp, client_id)));
                } else {
                    log::debug!(
                        "{}: New activity of expired client!: {}",
                        self.self_id,
                        client_id
                    );
                }
            }

            pub fn expire_too_old(&mut self, current_time: SystemTime) {
                for expired_client in self
                    .sessions
                    .iter()
                    .filter_map(|(&client_id, session)| {
                        log::trace!(
                            "Last activity ({:#?}) + expiration ({:#?}) < current_time ({:#?}) ",
                            session.last_activity,
                            self.session_expiration,
                            current_time
                        );
                        if session.last_activity + self.session_expiration < current_time {
                            Some(client_id)
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<Uuid>>()
                {
                    self.sessions.remove(&expired_client).unwrap();
                    log::info!(
                        "{}: Expired client session: {}",
                        self.self_id,
                        expired_client
                    );
                }
                // while self
                //     .last_activities
                //     .peek()
                //     .map_or(false, |&Reverse((old_time, _))| {
                //         old_time + self.session_expiration < current_time
                //     })
                // {
                //     // Client session possibly expired, check it:
                //     let Reverse((_, client_that_possibly_expired)) =
                //         self.last_activities.pop().unwrap();
                //     if self
                //         .sessions
                //         .get(&client_that_possibly_expired)
                //         .map_or(false, |session| {
                //             session.last_activity + self.session_expiration < current_time
                //         })
                //     {
                //         // Session truly expired.
                //         self.sessions.remove(&client_that_possibly_expired).unwrap();
                //         log::warn!("{}: Expired client session: {}", self.self_id, client_that_possibly_expired);
                //     }
                // }
            }
        }
    }

    use crate::{
        ClientRequestResponse, ClientSession, LogEntry, LogEntryContent, Raft, StateMachine,
    };
    use async_channel::Sender;
    use executor::{Handler, ModuleRef};
    use sessions::Response;
    pub use sessions::SessionManagement;
    use std::collections::{HashMap, HashSet};
    use uuid::Uuid;

    pub enum StateMachineRequest {
        MakeSnapshot {
            for_whom: Sender<ClientRequestResponse>,
            config: HashSet<Uuid>,
        },
        LoadSnapshot {
            state: Vec<u8>,
            sessions: HashMap<Uuid, ClientSession>,
        },
        Entry(usize, LogEntry),
    }

    pub enum CommandOutput {
        CommandApplied { data: Vec<u8> },
        SessionExpired,
    }

    pub enum StateMachineResponse {
        Snapshot {
            for_whom: Sender<ClientRequestResponse>,
            snapshot: Option<Vec<u8>>,
            config: HashSet<Uuid>,
            client_sessions: HashMap<Uuid, ClientSession>,
        },
        SnapshotLoaded,
        ClientRegistered {
            log_index: usize,
            client_id: Uuid,
        },
        CommandOutcome {
            log_index: usize,
            client_id: Uuid,
            sequence_num: u64,
            output: CommandOutput,
        },
    }

    pub struct StateMachineManager {
        self_id: Uuid,
        session_management: SessionManagement,
        state_machine: Box<dyn StateMachine>,
        raft_ref: ModuleRef<Raft>,
        has_something_to_snapshot: bool,
    }

    impl StateMachineManager {
        pub fn new(
            state_machine: Box<dyn StateMachine>,
            raft_ref: ModuleRef<Raft>,
            self_id: Uuid,
            session_management: SessionManagement,
        ) -> Self {
            Self {
                self_id,
                session_management,
                state_machine,
                raft_ref,
                has_something_to_snapshot: true, // TODO: settle on initial value of this
            }
        }
    }

    #[async_trait::async_trait]
    impl Handler<StateMachineRequest> for StateMachineManager {
        async fn handle(&mut self, msg: StateMachineRequest) {
            match msg {
                StateMachineRequest::MakeSnapshot { for_whom, config } => {
                    self.raft_ref
                        .send(StateMachineResponse::Snapshot {
                            for_whom,
                            snapshot: if self.has_something_to_snapshot {
                                Some(self.state_machine.serialize().await)
                            } else {
                                None
                            },
                            config,
                            client_sessions: self.session_management.share_sessions(),
                        })
                        .await;
                    self.has_something_to_snapshot = false;
                }
                StateMachineRequest::LoadSnapshot{state, sessions} => {
                    self.state_machine.initialize(state.as_slice()).await;
                    self.session_management.init_from(sessions);
                    self.raft_ref
                        .send(StateMachineResponse::SnapshotLoaded)
                        .await;
                }
                StateMachineRequest::Entry(log_index, log_entry) => match log_entry.content {
                    LogEntryContent::Command {
                        ref data,
                        client_id,
                        sequence_num,
                        lowest_sequence_num_without_response,
                    } => {
                        self.has_something_to_snapshot = true;
                        self.session_management.expire_too_old(log_entry.timestamp);

                        // log::debug!("{}: Applying {:#?}", self.self_id, log_entry);

                        self.session_management.new_activity(
                            client_id,
                            log_entry.timestamp,
                            lowest_sequence_num_without_response,
                        );

                        // check if this command hasn't been applied yet
                        let output = match self
                            .session_management
                            .reuse_response(&client_id, sequence_num)
                        {
                            Response::NotYetApplied => {
                                let output = self.state_machine.apply(data.as_slice()).await;
                                log::debug!("{}: State machine has applied command {}.", self.self_id, log_index);
                                CommandOutput::CommandApplied { data: output }
                            }
                            Response::AlreadyApplied(output) => {
                                CommandOutput::CommandApplied { data: output }
                            }
                            Response::SessionExpired => CommandOutput::SessionExpired,
                        };

                        if let CommandOutput::CommandApplied { ref data } = output {
                            self.session_management.save_response(
                                client_id,
                                sequence_num,
                                data.clone(),
                            );
                            // log::info!(
                            //     "{}: applied entry {}.",
                            //     self.self_id,
                            //     currently_applied_idx
                            // );
                        }

                        self.raft_ref
                            .send(StateMachineResponse::CommandOutcome {
                                log_index,
                                client_id,
                                sequence_num,
                                output,
                            })
                            .await;
                    }
                    LogEntryContent::Configuration { .. } => {
                        unreachable!()
                    }
                    LogEntryContent::RegisterClient => {
                        let client_id = Uuid::from_u128(log_index as u128);
                        self.session_management
                            .new_session(client_id, log_entry.timestamp);

                        self.raft_ref
                            .send(StateMachineResponse::ClientRegistered {
                                log_index,
                                client_id,
                            })
                            .await;
                    }
                },
            }
        }
    }
}

/// State of a Raft process with a corresponding (volatile) information.
enum ProcessType {
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader {
        next_index: HashMap<Uuid, usize>,
        match_index: HashMap<Uuid, usize>,
        snapshot_offset: HashMap<Uuid, usize>,
        heartbeat_timer: Timer<HeartbeatTimeout>,
        // This is for leader step-down if too few followers have responded during last election time
        responses_collected_during_this_election: HashSet<Uuid>,
        // Where the response should be sent after commiting key-th log entry
        client_senders: HashMap<usize, Sender<ClientRequestResponse>>,
        cached_snapshot: Option<SnapshotData>,
    },
}

impl ProcessType {
    pub fn new_candidate(self_id: &Uuid) -> Self {
        ProcessType::Candidate {
            votes_received: {
                let mut votes = HashSet::new();
                votes.insert(*self_id);
                votes
            },
        }
    }

    pub fn new_leader(
        servers: &HashSet<Uuid>,
        last_log_index: usize,
        snapshot_to_cache: Option<SnapshotData>,
    ) -> Self {
        let match_index: HashMap<Uuid, usize> = servers.iter().cloned().zip(repeat(0)).collect();
        let next_index: HashMap<Uuid, usize> = servers
            .iter()
            .cloned()
            .zip(repeat(last_log_index + 1))
            .collect();
        let snapshot_offset: HashMap<Uuid, usize> =
            servers.iter().cloned().zip(repeat(0)).collect();
        let heartbeat_timer = Timer::<HeartbeatTimeout>::new();

        ProcessType::Leader {
            next_index,
            match_index,
            snapshot_offset,
            heartbeat_timer,
            responses_collected_during_this_election: Default::default(),
            client_senders: Default::default(),
            cached_snapshot: snapshot_to_cache,
        }
    }
}

impl Default for ProcessType {
    fn default() -> Self {
        ProcessType::Follower
    }
}

#[derive(Serialize, Deserialize, Debug)]
struct Log {
    first_index: usize,
    entries: Vec<LogEntry>,
    prev_term: u64,
}

impl Log {
    pub fn new(timestamp: SystemTime, servers: HashSet<Uuid>) -> Self {
        Self {
            first_index: 0,
            entries: vec![LogEntry {
                content: LogEntryContent::Configuration {
                    servers: servers.clone(),
                },
                term: 0,
                timestamp,
            }],
            prev_term: 0,
        }
    }

    pub fn last_index(&self) -> usize {
        self.first_index + self.entries.len() - 1
    }

    pub fn entry_already_snapshotted(&self, i: usize) -> bool {
        i < self.first_index
    }

    pub fn stamp(&self) -> LogStamp {
        LogStamp(
            self.entries
                .last()
                .map_or(self.prev_term, |log_entry| log_entry.term),
            self.last_index() + 1,
        )
    }

    pub fn get(&self, i: usize) -> Option<&LogEntry> {
        if self.first_index > i {
            None
        } else {
            self.entries.get(i - self.first_index)
        }
    }

    pub fn get_mut(&mut self, i: usize) -> Option<&mut LogEntry> {
        self.entries.get_mut(i - self.first_index)
    }

    pub fn subvec(&self, range: RangeInclusive<usize>) -> Vec<LogEntry> {
        if range.is_empty() {
            vec![]
        } else {
            self.entries[(range.start() - self.first_index)..=(range.end() - self.first_index)]
                .to_vec()
        }
    }

    pub fn truncate(&mut self, new_len: usize) {
        assert!(new_len >= self.first_index);
        self.entries.truncate(new_len - self.first_index)
    }

    pub fn append_entry(&mut self, new_entry: LogEntry) {
        self.entries.push(new_entry);
    }

    pub async fn save_snapshot(
        &mut self,
        stable_storage: &mut dyn StableStorage,
        snapshot: SnapshotData,
    ) {
        let last_snapshotted_log_entry_index = snapshot.last_index;
        stable_storage
            .put(
                SNAPSHOT_KEYNAME,
                bincode::serialize(&snapshot).unwrap().as_slice(),
            )
            .await
            .unwrap();
        self.prev_term = snapshot.last_term;
        self.entries.drain(
            ..usize::min(
                last_snapshotted_log_entry_index + 1 - self.first_index,
                self.entries.len(),
            ),
        );
        self.first_index = last_snapshotted_log_entry_index + 1;
    }

    pub async fn get_snapshot_for_caching(
        &self,
        stable_storage: &mut dyn StableStorage,
    ) -> Option<SnapshotData> {
        if self.first_index > 0 {
            Some(
                bincode::deserialize(
                    stable_storage
                        .get(SNAPSHOT_KEYNAME)
                        .await
                        .unwrap()
                        .as_slice(),
                )
                .unwrap(),
            )
        } else {
            None
        }
    }
}

/// Persistent state of a Raft process.
/// It shall be kept in stable storage, and updated before replying to messages.
#[derive(Serialize, Deserialize)]
struct PersistentState {
    /// Number of the current term. `0` at boot.
    pub current_term: u64,
    /// Identifier of a process which has received this process' vote.
    /// `None if this process has not voted in this term.
    voted_for: Option<Uuid>,
    /// Identifier of a process which is thought to be the leader.
    leader_id: Option<Uuid>,
    log: Log,
}

impl PersistentState {
    pub fn new(timestamp: SystemTime, servers: HashSet<Uuid>) -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: Log::new(timestamp, servers),
        }
    }
}

/// Volatile state of a Raft process.
struct VolatileState {
    process_type: ProcessType,
    commit_index: usize,
    last_applied: usize,
    last_applied_acked: usize,
    voting_reluctant: bool,
    snapshot_reception: Option<SnapshotReception>,
}

impl VolatileState {
    pub fn new() -> Self {
        Self {
            process_type: Default::default(),
            commit_index: 0,
            last_applied: 0,
            last_applied_acked: 0,
            voting_reluctant: false,
            snapshot_reception: None,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
struct SnapshotData {
    state: Vec<u8>,
    client_sessions: HashMap<Uuid, ClientSession>,
    config: HashSet<Uuid>,
    last_index: usize,
    last_term: u64,
}

struct SnapshotReception {
    pub last_index: usize,
    pub last_term: u64,
    pub last_config: HashSet<Uuid>,
    pub client_sessions: HashMap<Uuid, ClientSession>,
    pub expected_offset: usize,
    pub data: Vec<u8>,
}

impl From<SnapshotReception> for SnapshotData {
    fn from(reception: SnapshotReception) -> Self {
        Self {
            state: reception.data,
            client_sessions: reception.client_sessions,
            config: reception.last_config,
            last_index: reception.last_index,
            last_term: reception.last_term,
        }
    }
}

struct Init {
    self_ref: ModuleRef<Raft>,
    state_machine_ref: ModuleRef<StateMachineManager>,
}

pub struct Raft {
    persistent_state: PersistentState,
    volatile_state: VolatileState,
    config: ServerConfig,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Box<dyn RaftSender>,
    self_ref: Option<ModuleRef<Raft>>,
    state_machine_ref: Option<ModuleRef<StateMachineManager>>,
    election_timer: Timer<ElectionTimeout>,
    election_timeout_randomizer: Uniform<Duration>,
    voting_reluctance_timer: Timer<VotingReluctanceTimeout>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        mut config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        mut state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let self_id = config.self_id;
        let session_expiration = config.session_expiration;
        log::debug!("{}: Initializing Raft.", self_id);
        let election_timeout_range = config.election_timeout_range.clone();
        let persistent_state = stable_storage
            .get(STATE_KEYNAME)
            .await
            .map(|x| bincode::deserialize(x.as_slice()).unwrap())
            .unwrap_or(PersistentState::new(
                first_log_entry_timestamp,
                config.servers.clone(),
            ));

        let mut session_management = SessionManagement::new(self_id, session_expiration);

        // Check if snapshot must be loaded
        if persistent_state.log.first_index != 0 {
            let snapshot: SnapshotData = bincode::deserialize(
                stable_storage
                    .get(SNAPSHOT_KEYNAME)
                    .await
                    .unwrap_or_else(|| panic!("Snapshot file not present while required!"))
                    .as_slice(),
            )
            .unwrap();
            state_machine.initialize(snapshot.state.as_slice()).await;
            config.servers = snapshot.config;
            session_management.init_from(snapshot.client_sessions);
        }

        let self_ref = system
            .register_module(Self {
                persistent_state,
                volatile_state: VolatileState::new(),
                config,
                stable_storage,
                message_sender,
                self_ref: None,
                state_machine_ref: None,
                election_timer: Timer::new(),
                election_timeout_randomizer: Uniform::new_inclusive(
                    election_timeout_range.start(),
                    election_timeout_range.end(),
                ),
                voting_reluctance_timer: Timer::new(),
            })
            .await;
        let state_machine_ref = system
            .register_module(StateMachineManager::new(
                state_machine,
                self_ref.clone(),
                self_id,
                session_management,
            ))
            .await;
        self_ref
            .send(Init {
                self_ref: self_ref.clone(),
                state_machine_ref,
            })
            .await;
        log::info!("{}: Initialized Raft.", self_id);
        self_ref
    }

    fn reset_voting_reluctance_timer(&mut self) {
        log::trace!(
            "{}: resetting voting reluctance timer!",
            self.config.self_id
        );
        self.voting_reluctance_timer.reset_timer(
            self.self_ref.clone().unwrap(),
            *self.config.election_timeout_range.start(),
        )
    }

    fn reset_election_timer(&mut self) {
        log::trace!("{}: resetting election timer!", self.config.self_id);
        self.election_timer.reset_timer(
            self.self_ref.clone().unwrap(),
            self.election_timeout_randomizer
                .sample(&mut rand::thread_rng()),
        )
    }

    async fn send_msg_to(&self, target: &Uuid, content: RaftMessageContent) {
        log::trace!(
            "{}: sending message to {}: {:#?}!",
            self.config.self_id,
            target,
            content
        );
        self.message_sender
            .send(
                target,
                RaftMessage {
                    header: RaftMessageHeader {
                        source: self.config.self_id,
                        term: self.persistent_state.current_term,
                    },
                    content,
                },
            )
            .await;
    }

    /// Set the process's term to the higher number.
    fn update_term(&mut self, new_term: u64) {
        assert!(self.persistent_state.current_term < new_term);
        self.persistent_state.current_term = new_term;
        self.persistent_state.voted_for = None;
        self.persistent_state.leader_id = None;
        log::debug!(
            "{}: term updated to {}",
            self.config.self_id,
            self.persistent_state.current_term
        );
        // No reliable state update called here, must be called separately.
    }

    /// Reliably save the state.
    async fn update_state(&mut self) {
        self.stable_storage
            .put(
                STATE_KEYNAME,
                bincode::serialize(&self.persistent_state)
                    .unwrap()
                    .as_slice(),
            )
            .await
            .unwrap();
    }

    /// Common message processing.
    fn msg_received(&mut self, msg: &RaftMessage) {
        log::debug!(
            "{}: received message from {}: {:#?}!",
            self.config.self_id,
            msg.header.source,
            msg.content
        );
        if msg.header.term > self.persistent_state.current_term {
            self.update_term(msg.header.term);
            self.volatile_state.process_type = ProcessType::Follower;
        }
    }

    async fn become_candidate(&mut self) {
        self.update_term(self.persistent_state.current_term + 1);
        log::info!("{}: turned into candidate.", self.config.self_id);
        self.volatile_state.process_type = ProcessType::new_candidate(&self.config.self_id);
        self.update_state().await;
        self.try_become_leader(1).await;

        let LogStamp(last_log_term, last_log_index) = self.persistent_state.log.stamp();
        for server in self
            .config
            .servers
            .iter()
            .filter(|&addressee| *addressee != self.config.self_id)
        {
            self.send_msg_to(
                server,
                RaftMessageContent::RequestVote(RequestVoteArgs {
                    last_log_index,
                    last_log_term,
                }),
            )
            .await;
        }
    }

    async fn replicate_log_to(
        &self,
        server: Uuid,
    ) {
        if let ProcessType::Leader {
            ref next_index,
            ref snapshot_offset,
            ref cached_snapshot,
            ..
        } = self.volatile_state.process_type {
            let snapshot_offset = snapshot_offset[&server];
            let next_index = next_index[&server];

            if self
                .persistent_state
                .log
                .entry_already_snapshotted(next_index)
            {
                let (last_config, client_sessions) = if snapshot_offset == 0 {
                    (
                        Some(cached_snapshot.as_ref().unwrap().config.clone()),
                        Some(cached_snapshot.as_ref().unwrap().client_sessions.clone()),
                    )
                } else {
                    (None, None)
                };
                // InstallSnapshot
                self.send_msg_to(
                    &server,
                    RaftMessageContent::InstallSnapshot(InstallSnapshotArgs {
                        last_included_index: self.persistent_state.log.first_index - 1,
                        last_included_term: self.persistent_state.log.prev_term,
                        last_config,
                        client_sessions,
                        offset: snapshot_offset,
                        data: cached_snapshot.as_ref().unwrap().state[snapshot_offset
                            ..usize::min(
                            snapshot_offset + self.config.snapshot_chunk_size,
                            cached_snapshot.as_ref().unwrap().state.len(),
                        )]
                            .to_vec(),
                        done: snapshot_offset + self.config.snapshot_chunk_size
                            >= cached_snapshot.as_ref().unwrap().state.len(),
                    }),
                )
                    .await;
            } else {
                // AppendEntries
                let prev_log_index = next_index - 1;
                self.send_msg_to(
                    &server,
                    RaftMessageContent::AppendEntries(AppendEntriesArgs {
                        prev_log_index,
                        prev_log_term: if prev_log_index + 1 == self.persistent_state.log.first_index {
                            self.persistent_state.log.prev_term
                        } else {
                            self.persistent_state.log.get(prev_log_index).unwrap().term
                        },
                        entries: self.persistent_state.log.subvec(
                            next_index
                                ..=usize::min(
                                self.persistent_state.log.last_index(),
                                prev_log_index + self.config.append_entries_batch_size,
                            ),
                        ),
                        leader_commit: self.volatile_state.commit_index,
                    }),
                )
                    .await;
            }
        }
    }

    async fn replicate_log(&self) {
        if let ProcessType::Leader {ref match_index, ..} = self.volatile_state.process_type {
            log::debug!("{}: is replicating log!", self.config.self_id);
            for server in match_index.keys().filter(|&x| *x != self.config.self_id) {
                self.replicate_log_to(
                    *server,
                )
                    .await;
            }
        }
    }

    async fn try_become_leader(&mut self, votes_received: usize) {
        if votes_received > self.config.servers.len() / 2 {
            // become the leader
            log::info!("{}: was elected leader!", self.config.self_id);

            self.volatile_state.process_type = ProcessType::new_leader(
                &self.config.servers,
                self.persistent_state.log.last_index(),
                self.persistent_state
                    .log
                    .get_snapshot_for_caching(&mut *self.stable_storage)
                    .await,
            );
            self.persistent_state.leader_id = Some(self.config.self_id);
            self.persistent_state.voted_for = Some(self.config.self_id);
            self.update_state().await;

            match &self.volatile_state.process_type {
                ProcessType::Follower | ProcessType::Candidate { .. } => unreachable!(),
                ProcessType::Leader { .. } => {
                    self.replicate_log().await;
                }
            };
            match &mut self.volatile_state.process_type {
                ProcessType::Follower | ProcessType::Candidate { .. } => unreachable!(),
                ProcessType::Leader {
                    ref mut heartbeat_timer,
                    ..
                } => heartbeat_timer.reset_timer(
                    self.self_ref.clone().unwrap(),
                    *self.config.election_timeout_range.start() / 10,
                ),
            };
        }
    }

    fn try_commit(&mut self) {
        match self.volatile_state.process_type {
            ProcessType::Leader {
                ref match_index, ..
            } => {
                for i in
                    (self.volatile_state.commit_index + 1)..=self.persistent_state.log.last_index()
                {
                    if match_index.values().filter(|&val| *val >= i).count()
                        > self.config.servers.len() / 2
                    {
                        log::info!(
                            "{}: commited entry {}: {:#?}.",
                            self.config.self_id,
                            i,
                            self.persistent_state.log.get(i).unwrap()
                        );
                        self.volatile_state.commit_index = i;
                    } else {
                        log::warn!(
                            "{}: can't commit entry {}. ({:#?})",
                            self.config.self_id,
                            i,
                            match_index
                        );
                        break; // TODO: think about removing this `else`
                    }
                }
            }
            _ => unreachable!(),
        };
    }

    async fn try_apply(&mut self) {
        while self.volatile_state.commit_index > self.volatile_state.last_applied {
            self.volatile_state.last_applied += 1;
            let currently_applied_idx = self.volatile_state.last_applied;
            let newly_commited_entry = self
                .persistent_state
                .log
                .get(currently_applied_idx)
                .unwrap();

            self.state_machine_ref
                .as_ref()
                .unwrap()
                .send(StateMachineRequest::Entry(
                    currently_applied_idx,
                    newly_commited_entry.clone(),
                ))
                .await;
        }
    }

    async fn handle_heartbeat(&mut self, leader_id: Uuid) {
        if Some(leader_id) != self.persistent_state.leader_id {
            self.persistent_state.leader_id = Some(leader_id);
            self.update_state().await;
        }
        self.volatile_state.voting_reluctant = true;

        match &mut self.volatile_state.process_type {
            ProcessType::Follower => {
                self.reset_election_timer();
                self.reset_voting_reluctance_timer();
            }
            ProcessType::Candidate { .. } => {
                self.volatile_state.process_type = ProcessType::Follower;
                self.reset_election_timer();
                self.reset_voting_reluctance_timer();
            }
            ProcessType::Leader { .. } => {
                log::debug!("{}: ignore, heartbeat from self", self.config.self_id);
                return;
            }
        };
    }

    async fn handle_append_entries(
        &mut self,
        header: RaftMessageHeader,
        content: AppendEntriesArgs,
    ) {
        let leader_id = header.source;
        self.handle_heartbeat(leader_id).await;

        // Update log
        let success = self
            .persistent_state
            .log
            .get(content.prev_log_index)
            .map_or(false, |entry| entry.term == content.prev_log_term);
        if success {
            let first_new_entry_index = content.prev_log_index + 1;
            for i in 0..content.entries.len() {
                match self.persistent_state.log.get(first_new_entry_index + i) {
                    Some(entry) if entry.term == content.entries.get(i).unwrap().term => {
                        // Ignore, already present in the log.
                    }
                    Some(_) => {
                        // Overwrite existing entries with the leader's.
                        *self
                            .persistent_state
                            .log
                            .get_mut(first_new_entry_index + i)
                            .unwrap() = content.entries[i].clone();
                        self.persistent_state
                            .log
                            .truncate(first_new_entry_index + i);
                    }
                    None => {
                        // The rest of entries can be simply appended.
                        self.persistent_state
                            .log
                            .entries
                            .extend_from_slice(&content.entries[i..]);
                        break;
                    }
                }
            }
        } else {
            // TODO: should I do something here?
        }

        self.update_state().await;

        self.volatile_state.commit_index = usize::min(
            content.leader_commit,
            self.persistent_state.log.last_index(),
        );

        // Respond to leader
        self.send_msg_to(
            &leader_id,
            RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success,
                last_log_index: self.persistent_state.log.last_index(),
            }),
        )
        .await;

        // Apply commited commands to state machine
        self.try_apply().await;
    }

    async fn handle_append_entries_response(
        &mut self,
        header: RaftMessageHeader,
        content: AppendEntriesResponseArgs,
    ) {
        let should_resend_append_entries = match self.volatile_state.process_type {
            ProcessType::Leader {
                ref mut match_index,
                ref mut next_index,
                responses_collected_during_this_election: ref mut responses_collected,
                ..
            } => {
                responses_collected.insert(header.source);

                if content.success {
                    *match_index.get_mut(&header.source).unwrap() = content.last_log_index;
                    *next_index.get_mut(&header.source).unwrap() = content.last_log_index + 1;
                    let should_resend_append_entries = *next_index.get(&header.source).unwrap()
                        <= self.persistent_state.log.last_index();
                    self.try_commit();
                    self.try_apply().await;

                    should_resend_append_entries
                } else {
                    if content.last_log_index < next_index[&header.source] {
                        *next_index.get_mut(&header.source).unwrap() = content.last_log_index;
                    } else {
                        *next_index.get_mut(&header.source).unwrap() -= 1;
                    }

                    true
                }
            }
            _ => unreachable!(),
        };

        if should_resend_append_entries {
            log::debug!(
                "{}: Resending AppendEntries because there are still some entries to share.",
                self.config.self_id
            );
            match self.volatile_state.process_type {
                ProcessType::Leader { .. } => {
                    self.replicate_log_to(
                        header.source,
                    )
                    .await;
                }
                _ => unreachable!(),
            }
        }
    }

    async fn handle_request_vote(&mut self, header: RaftMessageHeader, content: RequestVoteArgs) {
        let candidate_id = header.source;
        self.message_sender
            .send(
                &header.source,
                RaftMessage {
                    header: RaftMessageHeader {
                        source: self.config.self_id,
                        term: self.persistent_state.current_term,
                    },
                    content: RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                        vote_granted: match &self.volatile_state.process_type {
                            ProcessType::Follower => {
                                if (match self.persistent_state.voted_for {
                                    None => true,
                                    Some(voted_for) => voted_for == candidate_id,
                                }) && self.persistent_state.log.stamp()
                                    <= LogStamp(content.last_log_term, content.last_log_index)
                                {
                                    self.persistent_state.voted_for = Some(candidate_id);
                                    true
                                } else {
                                    false
                                }
                            }
                            ProcessType::Leader { .. } | ProcessType::Candidate { .. } => false,
                        },
                    }),
                },
            )
            .await;
    }

    async fn handle_request_vote_response(
        &mut self,
        header: RaftMessageHeader,
        content: RequestVoteResponseArgs,
    ) {
        let (should_try_become_leader, votes_count) = match self.volatile_state.process_type {
            ProcessType::Follower | ProcessType::Leader { .. } => (false, 0),

            ProcessType::Candidate {
                ref mut votes_received,
            } => {
                if content.vote_granted {
                    votes_received.insert(header.source);
                    log::debug!("{}: received vote.", self.config.self_id);
                    (true, votes_received.len())
                } else {
                    (false, 0)
                }
            }
        };

        if should_try_become_leader {
            self.try_become_leader(votes_count).await;
        }
    }

    async fn make_snapshot(&mut self, for_whom: Sender<ClientRequestResponse>) {
        self.state_machine_ref
            .as_ref()
            .unwrap()
            .send(StateMachineRequest::MakeSnapshot {
                for_whom,
                config: self.config.servers.clone(), // TODO: last commited config
            })
            .await;
    }

    async fn install_snapshot(&mut self, snapshot: SnapshotData) {
        self.persistent_state
            .log
            .save_snapshot(&mut *self.stable_storage, snapshot.clone())
            .await;
        self.state_machine_ref.as_ref().unwrap().send(
            StateMachineRequest::LoadSnapshot{
                state: snapshot.state,
                sessions: snapshot.client_sessions
            }
        ).await;
        self.volatile_state.last_applied = snapshot.last_index;

        log::info!("{}: installed snapshot.", self.config.self_id);
    }

    async fn handle_install_snapshot(
        &mut self,
        header: RaftMessageHeader,
        content: InstallSnapshotArgs,
    ) {
        self.handle_heartbeat(header.source).await;
        
        let InstallSnapshotArgs {
            mut data,
            last_included_index,
            last_included_term,
            done,
            last_config,
            client_sessions,
            offset,
        } = content;

        let data_len = data.len();
        let expected_offset: usize = match self.volatile_state.snapshot_reception {
            Some(ref mut reception) if reception.last_index == last_included_index => {
                if reception.expected_offset == offset {
                    let added_len = data_len;
                    reception.data.append(&mut data);
                    reception.expected_offset += added_len;
                }
                reception.expected_offset
            }
            _ if offset == 0 => {
                self.volatile_state.snapshot_reception = Some(SnapshotReception {
                    last_index: last_included_index,
                    last_term: last_included_term,
                    last_config: last_config.unwrap(),
                    client_sessions: client_sessions.unwrap(),
                    expected_offset: data_len,
                    data,
                });
                data_len
            }
            _ => {
                self.volatile_state.snapshot_reception = None;
                0
            }
        };

        self.send_msg_to(
            &header.source,
            RaftMessageContent::InstallSnapshotResponse(InstallSnapshotResponseArgs {
                last_included_index,
                offset: expected_offset,
            }),
        )
            .await;
        if done {
            if let Some(reception) = self.volatile_state.snapshot_reception.take() {
                let snapshot: SnapshotData =
                    reception
                    .into();
                self.install_snapshot(snapshot).await;
            }
        }
    }

    async fn handle_install_snapshot_response(
        &mut self,
        header: RaftMessageHeader,
        content: InstallSnapshotResponseArgs,
    ) {
        // ignore if not leader
        // regard offset as acknum+1
        // as the leader, hold next_offset[server_id] for each server
        //

        if let ProcessType::Leader {
            ref mut snapshot_offset,
            ref cached_snapshot,
            ref mut next_index,
            ..
        } = self.volatile_state.process_type
        {
            match cached_snapshot {
                Some(cached_snapshot)
                    if content.last_included_index + 1 == self.persistent_state.log.first_index =>
                {
                    // snapshot is still valid
                    if content.offset >= cached_snapshot.state.len() {
                        // we must have already sent 'done', so no need to send anything; just transistion to AppendEntries
                        *snapshot_offset.get_mut(&header.source).unwrap() = 0;
                        *next_index.get_mut(&header.source).unwrap() =
                            self.persistent_state.log.first_index;
                        log::debug!("{}: Should go on to sending AppendEntries to {} now,\
                         as next_index should have been incremented.", self.config.self_id, header.source)
                    } else {
                        // we need to send at least one chunk more.
                        *snapshot_offset.get_mut(&header.source).unwrap() = content.offset;
                    }
                }
                _ => {
                    // snapshot is not valid anymore (or has never been if None), restart sending with the new one
                    *snapshot_offset.get_mut(&header.source).unwrap() = 0;
                }
            }
            self.replicate_log_to(
                header.source,
            )
                .await;
        }
    }
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, msg: Init) {
        self.self_ref = Some(msg.self_ref);
        self.state_machine_ref = Some(msg.state_machine_ref);
        self.reset_election_timer();
        self.reset_voting_reluctance_timer();
    }
}

#[async_trait::async_trait]
impl Handler<ElectionTimeout> for Raft {
    async fn handle(&mut self, _: ElectionTimeout) {
        log::warn!("{}: received election timeout", self.config.self_id);
        match &mut self.volatile_state.process_type {
            ProcessType::Follower | ProcessType::Candidate { .. } => {
                // (re)start being a candidate
                self.become_candidate().await;
            }
            ProcessType::Leader {
                responses_collected_during_this_election: responses_collected,
                ..
            } => {
                if responses_collected.len() < self.config.servers.len() / 2 {
                    // "a leader steps down if an election timeout elapses without a successful
                    // round of heartbeats to a majority of its cluster"

                    // explicit timer stop is unnecessary due to Drop trait implementation for Timer
                    log::warn!("{}: leader has stepped down due to unsatisfactory response rate from followers.",
                        self.config.self_id);
                    self.update_term(self.persistent_state.current_term + 1);
                    self.update_state().await;
                    self.volatile_state.process_type = ProcessType::Follower;
                } else {
                    responses_collected.clear();
                }
            }
        }
        self.reset_election_timer();
    }
}

#[async_trait::async_trait]
impl Handler<HeartbeatTimeout> for Raft {
    async fn handle(&mut self, _: HeartbeatTimeout) {
        log::trace!("{}: received heartbeat timeout", self.config.self_id);
        match &self.volatile_state.process_type {
            ProcessType::Follower | ProcessType::Candidate { .. } => {
                // ignore but notice and warn (stall info)
                log::warn!("{}: received heartbeat timeout", self.config.self_id);
            }
            ProcessType::Leader { .. } => {
                self.replicate_log().await;
                self.try_commit(); // This is especially needed for single-node environments
                self.try_apply().await;
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<VotingReluctanceTimeout> for Raft {
    async fn handle(&mut self, _: VotingReluctanceTimeout) {
        if let ProcessType::Follower | ProcessType::Candidate { .. } =
            self.volatile_state.process_type
        {
            log::warn!(
                "{}: received voting reluctance timeout",
                self.config.self_id
            );
        }

        self.volatile_state.voting_reluctant = false;
    }
}

#[async_trait::async_trait]
impl Handler<RaftMessage> for Raft {
    async fn handle(&mut self, msg: RaftMessage) {
        if matches!(msg.content, RaftMessageContent::RequestVote(..))
            && (self.volatile_state.voting_reluctant
                || matches!(self.volatile_state.process_type, ProcessType::Leader { .. }))
        {
            // A server shall ignore a RequestVote received within the minimum election timeout
            // of hearing from a current leader (Chapter 4.2.3 of [1]).
            // As a consequence of that, a leader shall always ignore a RequestVote.
            return;
        }

        self.msg_received(&msg);
        match msg.content {
            RaftMessageContent::AppendEntries(args) => {
                if msg.header.term >= self.persistent_state.current_term {
                    self.handle_append_entries(msg.header, args).await;
                } else {
                    // otherwise, regard this message as obsolete and reject it
                    self.send_msg_to(
                        &msg.header.source,
                        RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                            success: false,
                            last_log_index: self.persistent_state.log.last_index(),
                        }),
                    )
                    .await;
                }
            }

            RaftMessageContent::AppendEntriesResponse(args) => {
                if msg.header.term >= self.persistent_state.current_term {
                    self.handle_append_entries_response(msg.header, args).await;
                } // otherwise, regard this message as obsolete and ignore it
            }

            RaftMessageContent::RequestVote(args) => {
                if msg.header.term >= self.persistent_state.current_term {
                    self.handle_request_vote(msg.header, args).await;
                } else {
                    // otherwise, regard this message as obsolete and reject it
                    self.send_msg_to(
                        &msg.header.source,
                        RaftMessageContent::RequestVoteResponse(RequestVoteResponseArgs {
                            vote_granted: false,
                        }),
                    )
                    .await;
                }
            }

            RaftMessageContent::RequestVoteResponse(args) => {
                if msg.header.term >= self.persistent_state.current_term {
                    self.handle_request_vote_response(msg.header, args).await;
                } // otherwise, regard this message as obsolete and ignore it
            }

            RaftMessageContent::InstallSnapshot(args) => {
                if msg.header.term >= self.persistent_state.current_term {
                    self.handle_install_snapshot(msg.header, args).await;
                } else {
                    // otherwise, regard this message as obsolete and reject it
                    // The message returned has little sense regarding InstallSnapshot,
                    // yet merely serves the purpose of propagating term change.
                    self.send_msg_to(
                        &msg.header.source,
                        RaftMessageContent::InstallSnapshotResponse(InstallSnapshotResponseArgs {
                            last_included_index: 0,
                            offset: 0,
                        }),
                    )
                    .await;
                }
            }

            RaftMessageContent::InstallSnapshotResponse(args) => {
                if msg.header.term >= self.persistent_state.current_term {
                    self.handle_install_snapshot_response(msg.header, args)
                        .await;
                } // otherwise, regard this message as obsolete and ignore it
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
        if let ClientRequestContent::Snapshot = msg.content {
            log::info!(
                "{}: received request from client: Snapshot",
                self.config.self_id,
            );
            self.make_snapshot(msg.reply_to).await;
        } else {
            match self.volatile_state.process_type {
                ProcessType::Leader {
                    ref mut client_senders,
                    ref mut match_index,
                    ref mut next_index,
                    ..
                } => {
                    *match_index.get_mut(&self.config.self_id).unwrap() += 1;
                    *next_index.get_mut(&self.config.self_id).unwrap() += 1;
                    match msg.content {
                        ClientRequestContent::Command {
                            command,
                            client_id,
                            sequence_num,
                            lowest_sequence_num_without_response,
                        } => {
                            self.persistent_state.log.append_entry(LogEntry {
                                content: LogEntryContent::Command {
                                    data: command,
                                    client_id,
                                    sequence_num,
                                    lowest_sequence_num_without_response,
                                },
                                term: self.persistent_state.current_term,
                                timestamp: SystemTime::now(),
                            });
                            let entry_index = self.persistent_state.log.last_index();
                            log::info!(
                                "{}: received request from client {}: Command",
                                self.config.self_id,
                                client_id
                            );
                            client_senders.insert(entry_index, msg.reply_to);
                        }

                        ClientRequestContent::Snapshot => {
                            unreachable!()
                        }

                        ClientRequestContent::AddServer { .. } => {
                            unimplemented!()
                        }

                        ClientRequestContent::RemoveServer { .. } => {
                            unimplemented!()
                        }

                        ClientRequestContent::RegisterClient => {
                            // When a RegisterClient client request (src/domain.rs) is received, your implementation
                            // shall commit a RegisterClient log entry (src/domain.rs), and reply with this entry’s
                            // log index once it is committed (Figure 6.1 of [1]). However, the implementation
                            // does not have to allocate a session.
                            self.persistent_state.log.append_entry(LogEntry {
                                content: LogEntryContent::RegisterClient,
                                term: self.persistent_state.current_term,
                                timestamp: SystemTime::now(),
                            });
                            let entry_index = self.persistent_state.log.last_index();
                            client_senders.insert(entry_index, msg.reply_to);
                            log::info!(
                                "{}: received request from client: RequestClient",
                                self.config.self_id,
                            );
                        }
                    };
                }
                _ => {
                    log::debug!(
                    "{}: received request from client not being the leader, so responding with leader hint.",
                    self.config.self_id,
                );
                    let _ = msg
                        .reply_to
                        .send(match msg.content {
                            ClientRequestContent::Command {
                                client_id,
                                sequence_num,
                                ..
                            } => ClientRequestResponse::CommandResponse(CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::NotLeader {
                                    leader_hint: self.persistent_state.leader_id,
                                },
                            }),
                            ClientRequestContent::Snapshot => {
                                unreachable!()
                            }
                            ClientRequestContent::AddServer { .. } => {
                                unimplemented!()
                            }
                            ClientRequestContent::RemoveServer { .. } => {
                                unimplemented!()
                            }
                            ClientRequestContent::RegisterClient => {
                                ClientRequestResponse::RegisterClientResponse(
                                    RegisterClientResponseArgs {
                                        content: RegisterClientResponseContent::NotLeader {
                                            leader_hint: self.persistent_state.leader_id,
                                        },
                                    },
                                )
                            }
                        })
                        .await;
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<StateMachineResponse> for Raft {
    async fn handle(&mut self, msg: StateMachineResponse) {
        match msg {
            StateMachineResponse::Snapshot {
                for_whom,
                snapshot,
                config,
                client_sessions,
            } => {
                let response = ClientRequestResponse::SnapshotResponse(SnapshotResponseArgs {
                    content: match snapshot {
                        None => SnapshotResponseContent::NothingToSnapshot {
                            last_included_index: self.volatile_state.last_applied_acked,
                        },
                        Some(snapshot) => {
                            let snapshot_data = SnapshotData {
                                state: snapshot.clone(),
                                client_sessions,
                                config,
                                last_index: self.volatile_state.last_applied_acked,
                                last_term: self
                                    .persistent_state
                                    .log
                                    .get(self.volatile_state.last_applied_acked)
                                    .unwrap()
                                    .term,
                            };
                            log::debug!(
                                "{}: made snapshot: {:#?}",
                                self.config.self_id,
                                snapshot_data
                            );

                            self.persistent_state
                                .log
                                .save_snapshot(&mut *self.stable_storage, snapshot_data.clone())
                                .await;
                            self.update_state().await;
                            if let ProcessType::Leader {
                                ref mut cached_snapshot,
                                ..
                            } = self.volatile_state.process_type
                            {
                                *cached_snapshot = Some(snapshot_data)
                            }
                            SnapshotResponseContent::SnapshotCreated {
                                last_included_index: self.volatile_state.commit_index,
                            }
                        }
                    },
                });
                log::debug!(
                    "{}: sent response to client: {:#?}",
                    self.config.self_id,
                    response
                );
                let _ = for_whom.send(response).await;
            }

            StateMachineResponse::SnapshotLoaded => {
                // todo!()
            }

            StateMachineResponse::CommandOutcome {
                log_index,
                output,
                client_id,
                sequence_num,
            } => {
                self.volatile_state.last_applied_acked = log_index;
                if let ProcessType::Leader {
                    ref mut client_senders,
                    ..
                } = self.volatile_state.process_type
                {
                    if let Some(client) = client_senders.get_mut(&log_index).take() {
                        let response =
                            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: match output {
                                    CommandOutput::CommandApplied { data } => {
                                        CommandResponseContent::CommandApplied { output: data }
                                    }
                                    CommandOutput::SessionExpired => {
                                        CommandResponseContent::SessionExpired
                                    }
                                },
                            });
                        let _ = client.send(response.clone()).await;
                        log::debug!(
                            "{}: sent response to client {}: {:#?}",
                            self.config.self_id,
                            client_id,
                            response
                        );
                    }
                }
            }
            StateMachineResponse::ClientRegistered {
                log_index,
                client_id,
            } => {
                self.volatile_state.last_applied_acked = log_index;
                if let ProcessType::Leader {
                    ref mut client_senders,
                    ..
                } = self.volatile_state.process_type
                {
                    if let Some(client) = client_senders.get_mut(&log_index).take() {
                        let response = ClientRequestResponse::RegisterClientResponse(
                            RegisterClientResponseArgs {
                                content: RegisterClientResponseContent::ClientRegistered {
                                    client_id,
                                },
                            },
                        );
                        log::debug!(
                            "{}: sent response to client {}: {:#?}",
                            self.config.self_id,
                            client_id,
                            response
                        );
                        let _ = client.send(response).await;
                    }
                }
            }
        }
    }
}
