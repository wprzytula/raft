use async_channel::Sender;
use std::collections::{HashMap, HashSet};
use std::iter::repeat;
use std::time::{Duration, SystemTime};

use executor::{Handler, ModuleRef, System};
use rand::distributions::{Distribution, Uniform};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

pub use domain::*;
use sessions::Response;
use sessions::SessionManagment;
use timing::{ElectionTimeout, HeartbeatTimeout, Timer, VotingReluctanceTimeout};

mod domain;

const STATE_KEYNAME: &str = "state";

struct Init {
    self_ref: ModuleRef<Raft>,
}

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
            // log::debug!("Timer set to {:?}", interval);
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

mod sessions {
    use crate::{ClientRequestResponse, ClientSession};
    use std::cmp::Reverse;
    use std::collections::{BinaryHeap, HashMap};
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
            if self.lowest_sequence_num_without_response < lowest_sequence_num_without_response {
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

                self.lowest_sequence_num_without_response = lowest_sequence_num_without_response;
            }
        }
    }

    pub struct SessionManagment {
        self_id: Uuid,
        session_expiration: Duration,
        sessions: HashMap<Uuid, ClientSession>,
        // last_activities: BinaryHeap<Reverse<(SystemTime, Uuid)>>,
    }

    impl SessionManagment {
        pub fn new(self_id: Uuid, session_expiration: Duration) -> Self {
            Self {
                self_id,
                session_expiration,
                sessions: Default::default(),
                // last_activities: Default::default(),
            }
        }

        pub fn new_session(&mut self, client_id: Uuid, creation_time: SystemTime) {
            self.sessions.insert(client_id, ClientSession::new(creation_time));
            // self.last_activities.push(Reverse((creation_time, client_id)));
            log::debug!(
                "{}: Initialized session for client: {}",
                self.self_id,
                client_id
            );
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

        pub fn save_response(
            &mut self,
            client_id: Uuid,
            sequence_id: u64,
            response: Vec<u8>,
        ) {
            log::debug!(
                "{}: Saved response for retransmissions: {:?}",
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

        pub fn expire_too_old(&mut self, current_time: SystemTime) {
            for expired_client in self
                .sessions
                .iter()
                .filter_map(|(&client_id, session)| {
                    log::trace!(
                        "Last activity ({:?}) + expiration ({:?}) < current_time ({:?}) ",
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

/// State of a Raft process with a corresponding (volatile) information.
enum ProcessType {
    Follower,
    Candidate {
        votes_received: HashSet<Uuid>,
    },
    Leader {
        next_index: HashMap<Uuid, usize>,
        match_index: HashMap<Uuid, usize>,
        heartbeat_timer: Timer<HeartbeatTimeout>,
        // This is for leader step-down if too few followers have responded during last election time
        responses_collected_during_this_election: HashSet<Uuid>,
        // Where the response should be sent after commiting key-th log entry
        client_senders: HashMap<usize, Sender<ClientRequestResponse>>,
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

    pub fn new_leader(servers: &HashSet<Uuid>, last_log_index: usize) -> Self {
        let match_index: HashMap<Uuid, usize> = servers.iter().cloned().zip(repeat(0)).collect();
        let next_index: HashMap<Uuid, usize> = servers
            .iter()
            .cloned()
            .zip(repeat(last_log_index + 1))
            .collect();
        let heartbeat_timer = Timer::<HeartbeatTimeout>::new();

        ProcessType::Leader {
            next_index,
            match_index,
            heartbeat_timer,
            responses_collected_during_this_election: Default::default(),
            client_senders: Default::default(),
        }
    }
}

impl Default for ProcessType {
    fn default() -> Self {
        ProcessType::Follower
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
    log: Vec<LogEntry>,
}

impl PersistentState {
    pub fn new(timestamp: SystemTime, servers: HashSet<Uuid>) -> Self {
        Self {
            current_term: 0,
            voted_for: None,
            leader_id: None,
            log: vec![LogEntry {
                content: LogEntryContent::Configuration { servers },
                term: 0,
                timestamp,
            }],
        }
    }
}

/// Volatile state of a Raft process.
struct VolatileState {
    process_type: ProcessType,
    commit_index: usize,
    last_applied: usize,
    voting_reluctant: bool,
    session_management: SessionManagment,
}

impl VolatileState {
    pub fn new(self_id: Uuid, session_expiration: Duration) -> Self {
        Self {
            process_type: Default::default(),
            commit_index: 0,
            last_applied: 0,
            voting_reluctant: false,
            session_management: SessionManagment::new(self_id, session_expiration),
        }
    }
}

pub struct Raft {
    persistent_state: PersistentState,
    volatile_state: VolatileState,
    config: ServerConfig,
    state_machine: Box<dyn StateMachine>,
    stable_storage: Box<dyn StableStorage>,
    message_sender: Box<dyn RaftSender>,
    self_ref: Option<ModuleRef<Raft>>,
    election_timer: Timer<ElectionTimeout>,
    election_timeout_randomizer: Uniform<Duration>,
    voting_reluctance_timer: Timer<VotingReluctanceTimeout>,
}

impl Raft {
    /// Registers a new `Raft` module in the `system`, initializes it and
    /// returns a `ModuleRef` to it.
    pub async fn new(
        system: &mut System,
        config: ServerConfig,
        first_log_entry_timestamp: SystemTime,
        state_machine: Box<dyn StateMachine>,
        stable_storage: Box<dyn StableStorage>,
        message_sender: Box<dyn RaftSender>,
    ) -> ModuleRef<Self> {
        let election_timeout_range = config.election_timeout_range.clone();
        let self_ref = system
            .register_module(Self {
                persistent_state: stable_storage
                    .get(STATE_KEYNAME)
                    .await
                    .map(|x| bincode::deserialize(x.as_slice()).unwrap())
                    .unwrap_or(PersistentState::new(
                        first_log_entry_timestamp,
                        config.servers.clone(),
                    )),
                volatile_state: VolatileState::new(config.self_id, config.session_expiration),
                config,
                state_machine,
                stable_storage,
                message_sender,
                self_ref: None,
                election_timer: Timer::new(),
                election_timeout_randomizer: Uniform::new_inclusive(
                    election_timeout_range.start(),
                    election_timeout_range.end(),
                ),
                voting_reluctance_timer: Timer::new(),
            })
            .await;
        self_ref
            .send(Init {
                self_ref: self_ref.clone(),
            })
            .await;
        self_ref
    }

    fn last_log_entry_index(&self) -> usize {
        self.persistent_state.log.len() - 1
    }

    fn log_stamp(&self) -> LogStamp {
        LogStamp(
            self.persistent_state
                .log
                .last()
                .map(|log_entry| log_entry.term)
                .unwrap(),
            self.last_log_entry_index() + 1,
        )
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
            "{}: sending message to {}: {:?}!",
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
            "{}: received message from {}: {:?}!",
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

        let LogStamp(last_log_term, last_log_index) = self.log_stamp();
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
        next_index: &HashMap<Uuid, usize>,
        server: Uuid,
    ) {
        let prev_log_index = next_index[&server] - 1;
        self.send_msg_to(
            &server,
            RaftMessageContent::AppendEntries(AppendEntriesArgs {
                prev_log_index,
                prev_log_term: self.persistent_state.log[prev_log_index].term,
                entries: self.persistent_state.log[(next_index[&server])
                    ..(usize::min(
                    self.persistent_state.log.len(),
                    prev_log_index + self.config.append_entries_batch_size,
                ))]
                    .to_vec(),
                leader_commit: self.volatile_state.commit_index,
            }),
        )
            .await;
    }

    async fn replicate_log(
        &self,
        match_index: &HashMap<Uuid, usize>,
        next_index: &HashMap<Uuid, usize>,
    ) {
        log::debug!("{}: is replicating log!", self.config.self_id);
        for server in match_index.keys().filter(|&x| *x != self.config.self_id) {
            self.replicate_log_to(next_index, *server).await;
        }
    }

    async fn try_become_leader(&mut self, votes_received: usize) {
        if votes_received > self.config.servers.len() / 2 {
            // become the leader
            self.volatile_state.process_type =
                ProcessType::new_leader(&self.config.servers, self.last_log_entry_index());
            self.persistent_state.leader_id = Some(self.config.self_id);
            self.persistent_state.voted_for = None;
            self.update_state().await;

            match &self.volatile_state.process_type {
                ProcessType::Follower | ProcessType::Candidate { .. } => unreachable!(),
                ProcessType::Leader {
                    match_index,
                    next_index,
                    ..
                } => {
                    self.replicate_log(match_index, next_index).await;
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

            log::info!("{}: was elected leader!", self.config.self_id);
        }
    }

    fn try_commit(&mut self) {
        match self.volatile_state.process_type {
            ProcessType::Leader {
                ref match_index, ..
            } => {
                for i in (self.volatile_state.commit_index + 1)..=self.last_log_entry_index() {
                    if match_index.values().filter(|&val| *val >= i).count()
                        > self.config.servers.len() / 2
                    {
                        log::info!("{}: commited entry {}: {:?}.", self.config.self_id, i, self.persistent_state.log[i]);
                        self.volatile_state.commit_index = i;
                    } else {
                        log::warn!(
                            "{}: can't commit entry {}. ({:?})",
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
            let newly_commited_entry = &self.persistent_state.log[currently_applied_idx];

            self.volatile_state
                .session_management
                .expire_too_old(newly_commited_entry.timestamp);

            log::debug!("{}: Applying {:?}", self.config.self_id, newly_commited_entry);

            match newly_commited_entry.content {
                LogEntryContent::Command {
                    ref data,
                    client_id,
                    sequence_num,
                    lowest_sequence_num_without_response,
                } => {
                    self.volatile_state.session_management.new_activity(
                        client_id,
                        newly_commited_entry.timestamp,
                        lowest_sequence_num_without_response,
                    );

                    // check if this command hasn't been applied yet
                    let response = match self
                        .volatile_state
                        .session_management
                        .reuse_response(&client_id, sequence_num)
                    {
                        Response::NotYetApplied => {
                            let output = self.state_machine.apply(data.as_slice()).await;
                            self.volatile_state.session_management.save_response(
                                client_id,
                                sequence_num,
                                output.clone(),
                            );
                            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::CommandApplied { output },
                            })
                        }
                        Response::AlreadyApplied(output) => {
                            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::CommandApplied { output },
                            })
                        },
                        Response::SessionExpired => {
                            ClientRequestResponse::CommandResponse(CommandResponseArgs {
                                client_id,
                                sequence_num,
                                content: CommandResponseContent::SessionExpired,
                            })
                        }
                    };

                    if let ProcessType::Leader {
                        ref mut client_senders,
                        ..
                    } = self.volatile_state.process_type
                    {
                        if let Some(client) = client_senders.get_mut(&currently_applied_idx).take()
                        {
                            log::debug!(
                                "{}: sent response to client {}: {:?}",
                                self.config.self_id,
                                client_id,
                                response
                            );
                            let _ = client.send(response).await;
                        }
                    }
                }
                LogEntryContent::Configuration { .. } => {
                    unimplemented!()
                }
                LogEntryContent::RegisterClient => {
                    let client_id = Uuid::from_u128(currently_applied_idx as u128);
                    self.volatile_state
                        .session_management
                        .new_session(client_id, newly_commited_entry.timestamp);

                    if let ProcessType::Leader {
                        ref mut client_senders,
                        ..
                    } = self.volatile_state.process_type
                    {
                        if let Some(client) = client_senders.get_mut(&currently_applied_idx).take()
                        {
                            let response = ClientRequestResponse::RegisterClientResponse(
                                RegisterClientResponseArgs {
                                    content: RegisterClientResponseContent::ClientRegistered {
                                        client_id,
                                    },
                                },
                            );
                            log::debug!(
                                "{}: sent response to client {}: {:?}",
                                self.config.self_id,
                                client_id,
                                response
                            );
                            let _ = client.send(response).await;
                        }
                    }
                }
            }
            log::info!(
                "{}: applied entry {}.",
                self.config.self_id,
                currently_applied_idx
            );
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
        let success = self.last_log_entry_index() >= content.prev_log_index
            && self.persistent_state.log[content.prev_log_index].term == content.prev_log_term;
        if success {
            let first_new_entry_index = content.prev_log_index + 1;
            for i in 0..content.entries.len() {
                match self.persistent_state.log.get(first_new_entry_index + i) {
                    Some(entry) if entry.term == content.entries.get(i).unwrap().term => {
                        // Ignore, already present in the log.
                    }
                    Some(_) => {
                        // Overwrite existing entries with the leader's.
                        self.persistent_state.log[first_new_entry_index + i] =
                            content.entries[i].clone();
                        self.persistent_state
                            .log
                            .truncate(first_new_entry_index + i);
                    }
                    None => {
                        // The rest of entries can be simply appended.
                        self.persistent_state
                            .log
                            .extend_from_slice(&content.entries[i..]);
                        break;
                    }
                }
            }
        } else {
            // TODO: should I do something here?
        }

        self.update_state().await;

        self.volatile_state.commit_index =
            usize::min(content.leader_commit, self.last_log_entry_index());

        // Respond to leader
        self.send_msg_to(
            &leader_id,
            RaftMessageContent::AppendEntriesResponse(AppendEntriesResponseArgs {
                success,
                last_log_index: self.last_log_entry_index(),
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
                    let should_resend_append_entries = *next_index.get(&header.source).unwrap() <= self.last_log_entry_index();
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
            log::debug!("{}: Resending AppendEntries because there are still some entries to share.",
                self.config.self_id);
            match self.volatile_state.process_type {
                ProcessType::Leader { ref next_index, ..} => {
                    self.replicate_log_to(next_index, header.source).await;
                }
                _ => unreachable!()
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
                                }) && self.log_stamp()
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
}

#[async_trait::async_trait]
impl Handler<Init> for Raft {
    async fn handle(&mut self, msg: Init) {
        self.self_ref = Some(msg.self_ref);
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
            ProcessType::Leader {
                match_index,
                next_index,
                ..
            } => {
                self.replicate_log(match_index, next_index).await;
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
                            last_log_index: self.last_log_entry_index(),
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

            RaftMessageContent::InstallSnapshot(_) => {
                unimplemented!()
            }
            RaftMessageContent::InstallSnapshotResponse(_) => {
                unimplemented!()
            }
        }
    }
}

#[async_trait::async_trait]
impl Handler<ClientRequest> for Raft {
    async fn handle(&mut self, msg: ClientRequest) {
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
                        self.persistent_state.log.push(LogEntry {
                            content: LogEntryContent::Command {
                                data: command,
                                client_id,
                                sequence_num,
                                lowest_sequence_num_without_response,
                            },
                            term: self.persistent_state.current_term,
                            timestamp: SystemTime::now(),
                        });
                        let entry_index = self.persistent_state.log.len() - 1;
                        log::info!(
                            "{}: received request from client {}: Command",
                            self.config.self_id,
                            client_id
                        );
                        client_senders.insert(entry_index, msg.reply_to);
                    }
                    ClientRequestContent::Snapshot => {
                        unimplemented!()
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
                        self.persistent_state.log.push(LogEntry {
                            content: LogEntryContent::RegisterClient,
                            term: self.persistent_state.current_term,
                            timestamp: SystemTime::now(),
                        });
                        let entry_index = self.persistent_state.log.len() - 1;
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
                            unimplemented!()
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
