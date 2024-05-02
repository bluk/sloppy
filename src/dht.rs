//! A [`BitTorrent`][bittorrent] [Distributed Hash Table][bep_0005] node.
//!
//! # Features
//!
//! | BEP                  | Status |
//! | ---------------------|--------|
//! | [BEP 0005][bep_0005] | WIP    |
//! | [BEP 0032][bep_0032] | -      |
//! | [BEP 0033][bep_0033] | -      |
//! | [BEP 0043][bep_0043] | -      |
//! | [BEP 0044][bep_0044] | -      |
//! | [BEP 0045][bep_0045] | -      |
//! | [BEP 0046][bep_0046] | -      |
//! | [BEP 0051][bep_0051] | -      |
//!
//! [bittorrent]: http://bittorrent.org/
//! [bep_0005]: http://bittorrent.org/beps/bep_0005.html
//! [bep_0032]: http://bittorrent.org/beps/bep_0032.html
//! [bep_0033]: http://bittorrent.org/beps/bep_0033.html
//! [bep_0043]: http://bittorrent.org/beps/bep_0043.html
//! [bep_0044]: http://bittorrent.org/beps/bep_0044.html
//! [bep_0045]: http://bittorrent.org/beps/bep_0045.html
//! [bep_0046]: http://bittorrent.org/beps/bep_0046.html
//! [bep_0051]: http://bittorrent.org/beps/bep_0051.html

// TODO: Configuration for whether node IDs are valid for IP

pub mod find_node_op;

use crate::dht::find_node_op::FindNodeOp;

use anyhow::Context;
use cloudburst::dht::{
    krpc::{
        self,
        find_node::{self, METHOD_FIND_NODE},
        ping::{self, METHOD_PING},
        transaction::{self, Transaction, Transactions},
        CompactAddr, CompactAddrV4, ErrorCode, Msg, QueryArgs, RespValues, Ty,
    },
    node::{self, AddrId, AddrOptId, LocalId},
    routing::{Bucket, Table},
};
use core::{fmt, time::Duration};
use find_node_op::OpsManager;
use serde_bytes::Bytes;
use serde_derive::Serialize;
use std::{
    convert::TryFrom,
    io::{self, Cursor},
    net::{SocketAddr, SocketAddrV4},
    time::Instant,
};
use tokio::{
    net::UdpSocket,
    sync::{mpsc, oneshot},
};
use tracing::{debug, error, trace};

#[derive(Debug)]
pub enum Cmd {
    GetConfig(oneshot::Sender<Config>),
}

pub(super) async fn dht_task(
    socket: UdpSocket,
    node: Node<SocketAddrV4>,
    cmd_rx: mpsc::Receiver<Cmd>,
    completion_tx: oneshot::Sender<()>,
) -> io::Result<()> {
    let result = dht_handler(socket, node, cmd_rx).await;

    let _ = completion_tx.send(());

    result
}

async fn dht_handler(
    socket: UdpSocket,
    mut node: Node<SocketAddrV4>,
    mut cmd_rx: mpsc::Receiver<Cmd>,
) -> io::Result<()> {
    let mut read_buf = vec![0; 4096];
    let mut write_buf = vec![0; 4096];

    loop {
        send_find_node_queries(&mut node, &socket, &mut write_buf, Instant::now()).await?;

        let now = Instant::now();
        let timeout_deadline = node.timeout().map_or(
            tokio::time::Instant::from(now) + Duration::from_secs(60),
            tokio::time::Instant::from,
        );
        trace!(?now, ?timeout_deadline, "polling");

        let sleep = tokio::time::sleep_until(timeout_deadline);
        tokio::pin!(sleep);

        tokio::select! {
            res = socket.recv_from(&mut read_buf) => {
                on_recv(&mut node, &socket, &read_buf, &mut write_buf, res, Instant::now()).await?;
            }
            cmd = cmd_rx.recv() => {
                match cmd {
                    Some(cmd) => {
                        match cmd {
                            Cmd::GetConfig(tx) => {
                                let _ = tx.send(node.config.clone());
                            }
                        }
                    }
                    None => {
                        // TODO: Serialize the DHT state
                        return Ok(());
                    }
                }
            }
            () = sleep => {
                let now = Instant::now();
                trace!(?now, "timed out");
                node.on_timeout(&mut rand::thread_rng());

                while let Some(tx) = node.pop_timed_out_tx(now) {
                    let Transaction {
                        addr_opt_id,
                        tx_id,
                        method,
                        timeout_deadline,
                    } = tx;
                    trace!(tx_id = ?tx_id, addr = %addr_opt_id.addr(), node_id = ?addr_opt_id.id(), ?timeout_deadline, ?method, "tx timed out");
                    // normally, look at any locally initiated transactions and
                    // considered them timed out if they match the read event
                }

                send_pings_to_nodes(&mut node, &socket, &mut write_buf, now).await?;
            }
        };
    }
}

async fn on_recv(
    node: &mut Node<SocketAddrV4>,
    socket: &UdpSocket,
    read_buf: &[u8],
    write_buf: &mut [u8],
    recv_from_result: io::Result<(usize, SocketAddr)>,
    now: Instant,
) -> io::Result<()> {
    let (bytes_read, src_addr) = match recv_from_result {
        Ok(v) => v,
        Err(e) => {
            if e.kind() == io::ErrorKind::WouldBlock {
                return Ok(());
            }

            error!(%e, "recv_from io error");
            return Err(e);
        }
    };

    debug!(%src_addr, %bytes_read, "received");

    let filled_buf = &read_buf[..bytes_read];

    if let Ok(msg) = bt_bencode::from_slice::<Msg<'_>>(filled_buf) {
        match src_addr {
            SocketAddr::V6(_) => {}
            SocketAddr::V4(src_addr) => match node.on_recv(&msg, src_addr) {
                Ok((addr_opt_id, _existing_tx)) => {
                    if let Ty::Query = msg.ty() {
                        reply_to_query(node, socket, addr_opt_id, &msg, write_buf, now).await?;
                    }
                }
                Err(e) => {
                    error!(?e, "on_recv error");
                }
            },
        }
    }
    Ok(())
}

async fn reply_to_query(
    node: &Node<SocketAddrV4>,
    socket: &UdpSocket,
    addr_opt_id: AddrOptId<SocketAddrV4>,
    msg: &Msg<'_>,
    write_buf: &mut [u8],
    now: Instant,
) -> io::Result<()> {
    async fn send_to_socket(buf: &[u8], addr: SocketAddrV4, socket: &UdpSocket) -> io::Result<()> {
        match socket.send_to(buf, addr).await {
            Ok(_) => Ok(()),
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }

                error!(%e, "send_to io error");
                Err(e)
            }
        }
    }

    let method_name = msg.method_name_str();
    debug!(?method_name, "received query");

    let mut cursor = Cursor::new(write_buf);
    let AddrOptId { addr, id: _ } = addr_opt_id;

    match msg.method_name() {
        Some(METHOD_PING) => {
            bt_bencode::to_writer(
                &mut cursor,
                &krpc::ser::RespMsg {
                    r: ping::RespValues::new(&node.config().local_id()),
                    t: Bytes::new(msg.tx_id()),
                    v: node.config().client_version(),
                },
            )?;

            debug!(%addr, tx_id = ?msg.tx_id(), "sending ping response reply");
        }
        Some(METHOD_FIND_NODE) => {
            if let Some(Ok(query_args)) = msg.args::<find_node::QueryArgs<'_>>() {
                if let Some(target) = query_args.target() {
                    let mut nodes = Vec::with_capacity(8 * 26);
                    for neighbor in node.find_neighbors(target, now).take(8) {
                        let AddrId { addr, id } = neighbor;
                        nodes.extend_from_slice(&id.0);
                        nodes.extend_from_slice(&CompactAddrV4::from(addr).0);
                    }

                    if !nodes.is_empty() {
                        while nodes.len() < 8 * 26 {
                            nodes.extend_from_within(0..26);
                        }
                    }

                    bt_bencode::to_writer(
                        &mut cursor,
                        &krpc::ser::RespMsg {
                            r: find_node::RespValues::new(
                                &node.config().local_id(),
                                Some(Bytes::new(&nodes)),
                                None,
                            ),
                            t: Bytes::new(msg.tx_id()),
                            v: node.config().client_version(),
                        },
                    )?;

                    debug!(%addr, tx_id = ?msg.tx_id(), "sending find node response reply");
                } else {
                    return Ok(());
                }
            } else {
                return Ok(());
            }
        }
        Some(method_name) => {
            bt_bencode::to_writer(
                &mut cursor,
                &krpc::ser::ErrMsg {
                    e: (
                        ErrorCode::MethodUnknown,
                        core::str::from_utf8(method_name).unwrap_or(""),
                    ),
                    t: Bytes::new(msg.tx_id()),
                    v: node.config().client_version(),
                },
            )?;

            debug!(%addr, tx_id = ?msg.tx_id(), "sending unknown method reply");
        }
        None => {
            bt_bencode::to_writer(
                &mut cursor,
                &krpc::ser::ErrMsg {
                    e: (
                        ErrorCode::ProtocolError,
                        String::from("method name not listed"),
                    ),
                    t: Bytes::new(msg.tx_id()),
                    v: node.config().client_version(),
                },
            )?;
        }
    }

    let end = usize::try_from(cursor.position()).expect("wrote too much data in reply");
    let write_buf = cursor.into_inner();
    send_to_socket(&write_buf[..end], addr, socket).await
}

async fn send_pings_to_nodes(
    node: &mut Node<SocketAddrV4>,
    socket: &UdpSocket,
    mut write_buf: &mut [u8],
    now: Instant,
) -> io::Result<()> {
    let local_id = node.config().local_id();
    let client_version = node.config().client_version().map(<[u8]>::to_vec);
    let query_args = ping::QueryArgs::new(&local_id);
    let ping_method = Bytes::new(METHOD_PING);
    loop {
        let tx_id = node.next_tx_id(&mut rand::thread_rng())?;
        if let Some(node_to_ping) = node.find_node_to_ping(now) {
            let addr_id = *node_to_ping.addr_id();
            let addr = *addr_id.addr();

            debug!(%addr, ?tx_id, "sending ping query");

            let mut cursor = Cursor::new(write_buf);

            bt_bencode::to_writer(
                &mut cursor,
                &krpc::ser::QueryMsg {
                    a: &query_args,
                    q: ping_method,
                    t: Bytes::new(tx_id.as_ref()),
                    v: client_version.as_deref(),
                },
            )?;

            let end = usize::try_from(cursor.position()).expect("wrote too much data");
            write_buf = cursor.into_inner();

            match socket.send_to(&write_buf[..end], addr).await {
                Ok(v) => v,
                Err(e) => {
                    if e.kind() == io::ErrorKind::WouldBlock {
                        return Ok(());
                    }

                    error!(%e, "send_to io error");
                    return Err(e);
                }
            };

            node_to_ping.on_ping(tx_id);

            node.insert_tx(Transaction::new(
                addr_id.into(),
                tx_id,
                METHOD_PING,
                Instant::now() + node.config().default_query_timeout(),
            ));
        } else {
            break;
        }
    }

    Ok(())
}

async fn send_find_node_queries(
    node: &mut Node<SocketAddrV4>,
    socket: &UdpSocket,
    mut write_buf: &mut [u8],
    now: Instant,
) -> io::Result<()> {
    while let Some((target_id, addr_opt_id)) = node.next_find_node_query(now) {
        let addr: SocketAddrV4 = match addr_opt_id.addr() {
            CompactAddr::V4(addr) => (*addr).into(),
            CompactAddr::V6(_) => continue,
        };

        let tx_id = node.next_tx_id(&mut rand::thread_rng())?;
        debug!(%addr, ?tx_id, %target_id, "sending find node query");

        let mut cursor = Cursor::new(write_buf);

        bt_bencode::to_writer(
            &mut cursor,
            &krpc::ser::QueryMsg {
                a: &find_node::QueryArgs::new(&node.config().local_id(), &target_id),
                q: Bytes::new(METHOD_FIND_NODE),
                t: Bytes::new(tx_id.as_ref()),
                v: node.config().client_version(),
            },
        )?;

        let end = usize::try_from(cursor.position()).expect("wrote too much data");
        write_buf = cursor.into_inner();

        match socket.send_to(&write_buf[..end], addr).await {
            Ok(v) => v,
            Err(e) => {
                if e.kind() == io::ErrorKind::WouldBlock {
                    return Ok(());
                }

                error!(%e, "send_to io error");
                return Err(e);
            }
        };

        node.insert_tx(Transaction::new(
            AddrOptId::new(addr, addr_opt_id.id()),
            tx_id,
            METHOD_FIND_NODE,
            Instant::now() + node.config().default_query_timeout(),
        ));
        node.insert_tx_for_find_node(tx_id, target_id, addr_opt_id);
    }

    Ok(())
}

/// The configuration for the local DHT node.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct Config {
    /// Local node id
    pub local_id: LocalId,
    /// Client version identifier
    pub client_version: Option<Vec<u8>>,
    /// The default amount of time before a query without a response is considered timed out
    pub default_query_timeout: Duration,
    /// If the node is read only
    pub is_read_only_node: bool,
    /// If responses from queried nodes are strictly checked for expected node ID
    pub is_response_queried_node_id_strictly_checked: bool,
    pub routing_table_next_response_interval: Duration,
    pub routing_table_next_query_interval: Duration,
}

impl Config {
    /// Instantiate a new config with a node's local Id.
    pub fn new<I>(id: I) -> Self
    where
        I: Into<LocalId>,
    {
        Self {
            local_id: id.into(),
            client_version: None,
            default_query_timeout: Duration::from_secs(5 * 60),
            is_read_only_node: false,
            is_response_queried_node_id_strictly_checked: true,
            routing_table_next_response_interval: Duration::from_secs(15 * 60),
            routing_table_next_query_interval: Duration::from_secs(15 * 60),
        }
    }

    /// Returns the node's local Id.
    #[must_use]
    pub fn local_id(&self) -> LocalId {
        self.local_id
    }

    /// Returns the client version.
    #[must_use]
    pub fn client_version(&self) -> Option<&[u8]> {
        self.client_version.as_deref()
    }

    /// Sets the client version.
    pub fn set_client_version<I>(&mut self, client_version: I)
    where
        I: Into<Option<Vec<u8>>>,
    {
        self.client_version = client_version.into();
    }

    /// Returns the default query timeout.
    #[must_use]
    pub fn default_query_timeout(&self) -> Duration {
        self.default_query_timeout
    }

    /// Set to true if the node is read only, false otherwise.
    pub fn set_is_read_only_node(&mut self, is_read_only_node: bool) {
        self.is_read_only_node = is_read_only_node;
    }
}

const FIND_LOCAL_ID_INTERVAL: Duration = Duration::from_secs(3 * 60);

use routing::MyTable;

type MethodName = &'static [u8];
type TxWithMethod = (transaction::Id, MethodName);

#[derive(Debug)]
struct Deadlines {
    refresh_bucket: Instant,
    next_response: Instant,
    next_query: Instant,
}

impl Deadlines {
    fn new(config: &Config, now: Instant) -> Self {
        Self {
            refresh_bucket: now + routing::BUCKET_REFRESH_INTERVAL,
            next_response: now + config.routing_table_next_response_interval,
            next_query: now + config.routing_table_next_query_interval,
        }
    }
}

/// The distributed hash table.
#[derive(Debug)]
pub struct Node<Addr> {
    pub config: Config,
    pub routing_table: Table<routing::Node<Addr, transaction::Id, Instant>, Instant>,
    find_pivot_deadline: Instant,
    tx_manager: Transactions<Addr, transaction::Id, Instant>,
    ops_manager: OpsManager,
    bootstrap_addrs: Vec<String>,
}

impl<Addr> Node<Addr>
where
    Addr: Clone + Copy + Ord,
{
    /// Instantiates a new node.
    pub fn new<A, B>(config: Config, addr_ids: A, bootstrap_addrs: B, now: Instant) -> Self
    where
        Addr: Clone + Ord + Into<CompactAddr>,
        A: IntoIterator<Item = AddrId<Addr>>,
        B: IntoIterator<Item = String>,
    {
        let pivot_id = node::Id::from(config.local_id);
        let routing_table = routing::new_routing_table(
            pivot_id,
            addr_ids,
            now + config.routing_table_next_response_interval,
            now + config.routing_table_next_query_interval,
            now,
        );
        let mut dht = Self {
            config,
            routing_table,
            tx_manager: Transactions::default(),
            find_pivot_deadline: now + FIND_LOCAL_ID_INTERVAL,
            ops_manager: OpsManager::default(),
            bootstrap_addrs: bootstrap_addrs.into_iter().collect(),
        };
        let op = dht.find_node_pivot(now);
        dht.ops_manager.insert_op(op);
        dht
    }

    /// Returns the config.
    #[must_use]
    pub fn config(&self) -> &Config {
        &self.config
    }

    /// Returns a transaction ID which can be used in the next query.
    ///
    /// The ID is not reserved until [`Self::insert_tx()`] is called.
    ///
    /// # Errors
    ///
    /// If a random number cannot be generated, an error will be returned.
    #[inline]
    pub fn next_tx_id<R>(&self, rng: &mut R) -> Result<transaction::Id, rand::Error>
    where
        R: rand::Rng,
    {
        if self.tx_manager.len() == usize::from(u16::MAX) {
            // Outbound transactions are full.
            return Err(rand::Error::new("all transaction IDs are used"));
        }

        let mut num: u16 = rng.gen();
        loop {
            let tx_id = transaction::Id::from(num);
            if !self.tx_manager.contains(&tx_id) {
                return Ok(tx_id);
            }
            num = num.wrapping_add(1);
        }
    }

    /// Inserts [`Transaction`] information.
    ///
    /// When a query is sent, a `Transaction` should be inserted into the `Node`
    /// to track outstanding queries.
    ///
    /// When a message is received and processed via [`Node::on_recv()`], the
    /// message's transaction ID and inbound socket address is checked against
    /// existing `Transaction` data. If a matching `Transaction` exists, then
    /// the message is considered to be valid.
    pub fn insert_tx(&mut self, tx: Transaction<Addr, transaction::Id, Instant>) {
        self.tx_manager.insert(tx);
    }

    pub fn insert_tx_for_find_node(
        &mut self,
        tx_id: transaction::Id,
        target_id: node::Id,
        addr_opt_id: AddrOptId<CompactAddr>,
    ) {
        self.ops_manager.insert_tx(tx_id, target_id, addr_opt_id);
    }

    /// Processes a received message.
    ///
    /// When a message is received, use this callback method to process the data.
    /// If a message's transaction ID and inbound socket address matches
    /// existing `Transaction` data, then the message is considered valid.
    ///
    /// # Errors
    ///
    /// If the message is malformed, then an error is returned. If a response or
    /// error message does not have a matching `Transaction`, then the message is
    /// considered invalid and an error is returned.
    pub fn on_recv(
        &mut self,
        msg: &Msg<'_>,
        addr: Addr,
    ) -> anyhow::Result<(AddrOptId<Addr>, Option<TxWithMethod>)>
    where
        Addr: fmt::Debug + Clone + PartialEq + Into<CompactAddr>,
    {
        self.on_recv_with_now(msg, addr, Instant::now())
    }

    fn on_recv_with_now(
        &mut self,
        msg: &Msg<'_>,
        addr: Addr,
        now: Instant,
    ) -> anyhow::Result<(AddrOptId<Addr>, Option<TxWithMethod>)>
    where
        Addr: fmt::Debug + Clone + PartialEq + Into<CompactAddr>,
    {
        trace!(
            addr = ?addr,
            tx_id = ?msg.t,
            ty = ?msg.y,
            client_version = ?msg.v,
            // node_id = ?tx.addr_opt_id().id(),
            // method= ?core::str::from_utf8(tx.method).unwrap_or("non-UTF-8 method"),
            // queried_node_id = ?value.queried_node_id(),
            "received message"
        );
        let kind = msg.ty();
        match kind {
            Ty::Response => {
                let tx_id = transaction::Id::try_from(msg.tx_id())
                    .context("unrecognized transaction id format in response")?;
                let Transaction {
                    addr_opt_id,
                    tx_id,
                    method,
                    timeout_deadline: _timeout_deadline,
                } = self
                    .tx_manager
                    .on_recv(&tx_id)
                    .context("unknown transaction for response")?;

                if let Some(node_id) = addr_opt_id.id().or_else(|| {
                    msg.values::<RespValues<'_>>()
                        .and_then(|values| values.map(|values| values.id()).ok())
                        .flatten()
                }) {
                    routing::on_recv(
                        &mut self.routing_table,
                        AddrId::new(addr, node_id),
                        kind,
                        Some(&tx_id),
                        &Deadlines::new(&self.config, now),
                        now,
                    );
                }

                self.ops_manager.on_recv(
                    AddrOptId::new((*addr_opt_id.addr()).into(), addr_opt_id.id()),
                    tx_id,
                    msg,
                    now,
                );

                Ok((addr_opt_id, Some((tx_id, method))))
            }
            Ty::Error => {
                let tx_id = transaction::Id::try_from(msg.tx_id())
                    .context("unrecognized transaction id format in error")?;
                let Transaction {
                    addr_opt_id,
                    tx_id,
                    method,
                    timeout_deadline: _timeout_deadline,
                } = self
                    .tx_manager
                    .on_recv(&tx_id)
                    .context("unknown transaction for error")?;

                if let Some(node_id) = addr_opt_id.id() {
                    routing::on_recv(
                        &mut self.routing_table,
                        AddrId::new(addr, node_id),
                        kind,
                        Some(&tx_id),
                        &Deadlines::new(&self.config, now),
                        now,
                    );
                }

                self.ops_manager.on_error(
                    AddrOptId::new((*addr_opt_id.addr()).into(), addr_opt_id.id()),
                    tx_id,
                    now,
                );

                Ok((addr_opt_id, Some((tx_id, method))))
            }
            Ty::Query | Ty::Unknown => {
                let querying_node_id = msg
                    .values::<QueryArgs<'_>>()
                    .and_then(|args| args.map(|args| args.id()).ok())
                    .flatten();
                let addr_opt_id = AddrOptId::new(addr, querying_node_id);
                if let Some(node_id) = querying_node_id {
                    routing::on_recv(
                        &mut self.routing_table,
                        AddrId::new(addr, node_id),
                        kind,
                        None,
                        &Deadlines::new(&self.config, now),
                        now,
                    );
                }

                Ok((addr_opt_id, None))
            }
            _ => {
                unreachable!()
            }
        }
    }

    /// Returns the next timeout deadline.
    ///
    /// When the timeout deadline has passed, the following methods should be called:
    ///
    /// * [`Node::on_timeout()`]
    /// * [`Node::pop_timed_out_tx()`]
    /// * [`Node::find_node_to_ping()`]
    /// * [`Node::find_bucket_to_refresh()`]
    ///
    /// The timeout deadline may change if other methods are called on this
    /// instance.
    #[must_use]
    pub fn timeout(&self) -> Option<Instant> {
        [self.tx_manager.timeout(), self.routing_table.timeout()]
            .iter()
            .filter_map(|&deadline| deadline)
            .min()
    }

    /// Processes timeout events.
    ///
    /// This method should be called after the deadline returned by [`Node::timeout()`].
    ///
    /// # Errors
    ///
    /// An error is returned in the random number generator cannot generate random data.
    pub fn on_timeout<R>(&mut self, rng: &mut R)
    where
        Addr: Into<CompactAddr>,
        R: rand::Rng,
    {
        self.on_timeout_with_now(rng, Instant::now());
    }

    fn on_timeout_with_now<R>(&mut self, rng: &mut R, now: Instant)
    where
        Addr: Into<CompactAddr>,
        R: rand::Rng,
    {
        if self.find_pivot_deadline <= now {
            let op = self.find_node_pivot(now);
            self.ops_manager.insert_op(op);
        }

        self.ops_manager.cleanup();

        while let Some(bucket) = self.find_bucket_to_refresh(now) {
            bucket.set_refresh_deadline(now + Duration::from_secs(15 * 60));
            let target_id = bucket.rand_id(rng);
            let neighbors = self
                .find_neighbors(target_id, now)
                .take(8)
                .map(|a| AddrOptId::new((*a.addr()).into(), Some(a.id())));
            let find_node_op = FindNodeOp::new(target_id, 8, neighbors, now);
            self.ops_manager.insert_op(find_node_op);
        }
    }

    /// Finds a bucket to refresh.
    ///
    /// To refresh a bucket, find a random node with an `Id` in the bucket's range.
    ///
    /// ```no_run
    /// # use waynode::dht::{Node, SupportedAddr, find_node_op::FindNodeOp};
    /// # use std::{time::{Duration, Instant}, net::SocketAddr};
    /// # use cloudburst::dht::{krpc::CompactAddr, node::AddrOptId};
    /// #
    /// # let node: Node<SocketAddr> = todo!();
    /// # let now = Instant::now();
    /// if let Some(bucket) = node.find_bucket_to_refresh(now) {
    ///     bucket.set_refresh_deadline(now + Duration::from_secs(15 * 60));
    ///     let target_id = bucket.rand_id(&mut rand::thread_rng());
    ///     let neighbors = node.find_neighbors(target_id, now).take(8).map(|a| AddrOptId::new((*a.addr()).into(), Some(a.id())));
    ///     let find_node_op = FindNodeOp::new(
    ///          target_id,
    ///          8,
    ///          SupportedAddr::Ipv4,
    ///          neighbors
    ///    );
    /// }
    /// ```
    pub fn find_bucket_to_refresh(
        &mut self,
        now: Instant,
    ) -> Option<&mut Bucket<routing::Node<Addr, transaction::Id, Instant>, Instant>> {
        self.routing_table.find_bucket_to_refresh(&now)
    }

    /// Finds and processes a transaction which has timed out.
    ///
    /// Returns information about the transaction which has timed out.
    pub fn pop_timed_out_tx(
        &mut self,
        now: Instant,
    ) -> Option<Transaction<Addr, transaction::Id, Instant>>
    where
        Addr: Into<CompactAddr>,
    {
        if let Some(tx) = self.tx_manager.pop_timed_out_tx(&now) {
            if let Some(node_id) = tx.addr_opt_id().id() {
                routing::on_timeout(
                    &mut self.routing_table,
                    &AddrId::new(*tx.addr_opt_id().addr(), node_id),
                    *tx.tx_id(),
                );
            }

            self.ops_manager.on_tx_timeout(
                AddrOptId::new((*tx.addr_opt_id().addr()).into(), tx.addr_opt_id().id()),
                *tx.tx_id(),
                now,
            );

            Some(tx)
        } else {
            None
        }
    }

    /// Finds a node to query for a find node target.
    pub fn next_find_node_query(
        &mut self,
        now: Instant,
    ) -> Option<(node::Id, AddrOptId<CompactAddr>)> {
        self.ops_manager.next_addr_to_query(now)
    }

    /// Finds a node to ping.
    ///
    /// # Important
    ///
    /// [`Node::on_ping()`] must be called to indicate the node was pinged.
    pub fn find_node_to_ping(
        &mut self,
        now: Instant,
    ) -> Option<&mut routing::Node<Addr, transaction::Id, Instant>> {
        self.routing_table.find_node_to_ping(now)
    }

    /// Finds the cloesst neighbors for a given `Id`.
    ///
    /// Usually a query is directed towards a target hash value. Nodes with
    /// `Id`s which are "closer" to the target value are more likely to have the
    /// data than other nodes.
    pub fn find_neighbors(&self, id: node::Id, _now: Instant) -> impl Iterator<Item = AddrId<Addr>>
    where
        Addr: Clone,
    {
        routing::find_neighbors(&self.routing_table, id)
    }

    #[must_use]
    fn find_node(&mut self, target_id: node::Id, now: Instant) -> FindNodeOp
    where
        Addr: Into<CompactAddr>,
    {
        let bootstrap_addrs = self
            .bootstrap_addrs
            .iter()
            .filter_map(|s| {
                use std::net::ToSocketAddrs;
                s.to_socket_addrs().ok()
            })
            .flatten()
            .filter_map(|socket_addr| match socket_addr {
                SocketAddr::V4(socket_addr) => Some(AddrOptId::with_addr(socket_addr.into())),
                SocketAddr::V6(_) => None,
            });
        FindNodeOp::new(
            target_id,
            8,
            routing::find_neighbors(&self.routing_table, target_id)
                .take(8)
                .map(|a| AddrOptId::new((*a.addr()).into(), Some(a.id())))
                .chain(bootstrap_addrs),
            now,
        )
    }

    #[must_use]
    #[inline]
    fn find_node_pivot(&mut self, now: Instant) -> FindNodeOp
    where
        Addr: Into<CompactAddr>,
    {
        self.find_node(self.routing_table.pivot(), now)
    }
}

#[cfg(test)]
mod tests {
    use cloudburst::dht::krpc::ping::METHOD_PING;

    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn new_config() -> Result<Config, rand::Error> {
        Ok(Config {
            local_id: LocalId::from(node::Id::rand(&mut rand::thread_rng())?),
            client_version: None,
            default_query_timeout: Duration::from_secs(60),
            is_read_only_node: true,
            is_response_queried_node_id_strictly_checked: true,
            routing_table_next_response_interval: Duration::from_secs(15 * 60),
            routing_table_next_query_interval: Duration::from_secs(15 * 60),
        })
    }

    fn remote_addr() -> SocketAddr {
        SocketAddr::V4(SocketAddrV4::new(Ipv4Addr::new(127, 0, 0, 1), 6532))
    }

    fn node_id() -> node::Id {
        node::Id::rand(&mut rand::thread_rng()).unwrap()
    }

    #[test]
    fn test_send_message() {
        let id = node_id();
        let remote_addr = remote_addr();
        let addr_opt_id = AddrOptId::new(remote_addr, Some(id));

        let config = new_config().unwrap();

        let mut node: Node<SocketAddr> = Node::new(
            config,
            std::iter::empty(),
            std::iter::empty(),
            Instant::now(),
        );
        let tx_id = node.next_tx_id(&mut rand::thread_rng()).unwrap();
        node.insert_tx(Transaction::new(
            addr_opt_id,
            tx_id,
            METHOD_PING,
            Instant::now() + node.config().default_query_timeout,
        ));
    }
}

mod routing {
    const MAX_BUCKET_SIZE: usize = 8;

    use std::time::{Duration, Instant};

    use cloudburst::dht::{
        krpc::{transaction, Ty},
        node::{self, AddrId},
        routing::{Bucket, Table},
    };

    use super::Deadlines;

    pub(super) const BUCKET_REFRESH_INTERVAL: Duration = Duration::from_secs(3 * 60);

    pub(super) fn new_routing_table<A, Addr, TxId>(
        pivot_id: node::Id,
        addr_ids: A,
        next_response_deadline: Instant,
        next_query_deadline: Instant,
        now: Instant,
    ) -> Table<Node<Addr, TxId, Instant>, Instant>
    where
        A: IntoIterator<Item = AddrId<Addr>>,
    {
        let mut routing_table = Table::new(pivot_id, now + BUCKET_REFRESH_INTERVAL);
        let pivot_id = routing_table.pivot();
        for addr_id in addr_ids {
            let mut bucket = routing_table.find_mut(&addr_id.id());

            let mut bucket_len = bucket.len();
            if bucket.range().contains(&pivot_id) {
                while bucket_len == MAX_BUCKET_SIZE {
                    routing_table.split_last();
                    bucket = routing_table.find_mut(&addr_id.id());
                    bucket_len = bucket.len();

                    if !bucket.range().contains(&pivot_id) {
                        break;
                    }
                }
            }

            if bucket_len < MAX_BUCKET_SIZE {
                bucket.insert(Node::new(
                    addr_id,
                    next_response_deadline,
                    next_query_deadline,
                ));
                bucket.set_refresh_deadline(now + BUCKET_REFRESH_INTERVAL);
            }
        }

        routing_table
    }

    pub(super) fn on_recv<Addr>(
        table: &mut Table<Node<Addr, transaction::Id, Instant>, Instant>,
        addr_id: AddrId<Addr>,
        kind: Ty,
        tx_id: Option<&transaction::Id>,
        deadlines: &Deadlines,
        now: Instant,
    ) where
        Addr: PartialEq,
    {
        let pivot_id = table.pivot();
        let mut bucket = table.find_mut(&addr_id.id());
        if let Some(node) = bucket.iter_mut().find(|node| *node.addr_id() == addr_id) {
            node.on_msg_received(kind, tx_id, deadlines.next_response, deadlines.next_query);
            return;
        }

        let mut bucket_len = bucket.len();
        if bucket.range().contains(&pivot_id) {
            while bucket_len == MAX_BUCKET_SIZE {
                table.split_last();
                bucket = table.find_mut(&addr_id.id());
                bucket_len = bucket.len();

                if !bucket.range().contains(&pivot_id) {
                    break;
                }
            }
        }

        if bucket_len < MAX_BUCKET_SIZE {
            bucket.insert(Node::new(
                addr_id,
                deadlines.next_response,
                deadlines.next_query,
            ));
            bucket.set_refresh_deadline(deadlines.refresh_bucket);
            return;
        }
        assert_eq!(bucket_len, MAX_BUCKET_SIZE);

        bucket.retain(|node| match node.state_with_now(&now) {
            NodeState::Good | NodeState::Questionable => true,
            NodeState::Bad => false,
        });

        if bucket.len() < MAX_BUCKET_SIZE {
            bucket.insert(Node::new(
                addr_id,
                deadlines.next_response,
                deadlines.next_query,
            ));
            bucket.set_refresh_deadline(deadlines.refresh_bucket);
        }
    }

    pub(super) fn on_timeout<Addr>(
        table: &mut Table<Node<Addr, transaction::Id, Instant>, Instant>,
        addr_id: &AddrId<Addr>,
        tx_id: transaction::Id,
    ) where
        Addr: PartialEq,
    {
        let node_id = addr_id.id();
        let bucket = table.find_mut(&node_id);

        if let Some(node) = bucket.iter_mut().find(|node| *node.addr_id() == *addr_id) {
            node.on_resp_timeout(&tx_id);
        }
    }

    #[derive(Debug, PartialEq)]
    enum NodeState {
        Good,
        Questionable,
        Bad,
    }

    /// Contains the address and [`Id`] for a node with metadata about the last response.
    ///
    /// Used to store a node's information for routing queries to. Contains
    /// "liveliness" information to determine if the `Node` is still likely valid.
    #[derive(Debug, Clone)]
    pub struct Node<Addr, TxId, Instant> {
        addr_id: AddrId<Addr>,
        karma: i8,
        next_response_deadline: Instant,
        next_query_deadline: Instant,
        ping_tx_id: Option<TxId>,
    }

    impl<Addr, TxId, Instant> cloudburst::dht::routing::Node for Node<Addr, TxId, Instant> {
        fn id(&self) -> node::Id {
            self.addr_id.id()
        }
    }

    impl<A, TxId, Instant> Node<A, TxId, Instant>
    where
        Instant: cloudburst::time::Instant,
    {
        /// Instantiates a new Node.
        pub fn new(
            addr_id: AddrId<A>,
            next_response_deadline: Instant,
            next_query_deadline: Instant,
        ) -> Self {
            Self {
                addr_id,
                karma: 0,
                next_response_deadline,
                next_query_deadline,
                ping_tx_id: None,
            }
        }

        /// Returns the address.
        pub fn addr_id(&self) -> &AddrId<A> {
            &self.addr_id
        }

        /// When pinged, sets the transaction Id to identify the response or time out later.
        pub fn on_ping(&mut self, tx_id: TxId) {
            self.ping_tx_id = Some(tx_id);
        }

        /// Returns the timeout deadline when the node should be pinged.
        #[must_use]
        pub fn timeout(&self) -> &Instant {
            core::cmp::max(&self.next_response_deadline, &self.next_query_deadline)
        }

        fn state_with_now(&self, now: &Instant) -> NodeState {
            if *now < self.next_response_deadline {
                return NodeState::Good;
            }

            if *now < self.next_query_deadline {
                return NodeState::Good;
            }

            if self.karma < -2 {
                return NodeState::Bad;
            }

            NodeState::Questionable
        }

        /// Called when a message is received from the node,
        ///
        /// # Important
        ///
        /// In most situations, this method should not be directly called. The
        /// method is called from [`Bucket::on_msg_received()`].
        ///
        /// The callback modifies the internal state based on the message type and
        /// transaction ID. If the message is a response or a query, the node is
        /// considered to still be active. If the message is an error or has an
        /// unknown message type, the node's is considered to be increasingly
        /// questionable.
        pub fn on_msg_received(
            &mut self,
            kind: Ty,
            tx_id: Option<&TxId>,
            next_response_deadline: Instant,
            next_query_deadline: Instant,
        ) where
            TxId: PartialEq,
        {
            match kind {
                Ty::Response => {
                    if let Some(tx_id) = tx_id {
                        if let Some(ping_tx_id) = &self.ping_tx_id {
                            if *ping_tx_id == *tx_id {
                                self.ping_tx_id = None;
                            }
                        }
                    }
                    self.next_response_deadline = next_response_deadline;
                    self.karma = self.karma.saturating_add(1);
                    if self.karma > 3 {
                        self.karma = 3;
                    }
                }
                Ty::Query => {
                    self.next_query_deadline = next_query_deadline;
                }
                Ty::Error => {
                    if let Some(tx_id) = tx_id {
                        if let Some(ping_tx_id) = &self.ping_tx_id {
                            if *ping_tx_id == *tx_id {
                                self.ping_tx_id = None;
                            }
                        }
                    }
                    self.next_response_deadline = next_response_deadline;
                    self.karma = self.karma.saturating_sub(1);
                }
                Ty::Unknown => {
                    if let Some(tx_id) = tx_id {
                        if let Some(ping_tx_id) = &self.ping_tx_id {
                            if *ping_tx_id == *tx_id {
                                self.ping_tx_id = None;
                            }
                        }
                    }
                    self.karma = self.karma.saturating_sub(1);
                }
                _ => {
                    unreachable!()
                }
            }
        }

        /// Called when an outbound transaction has timed out.
        ///
        /// # Important
        ///
        /// In most situations, this method should not be directly called. The
        /// method is called from [`Bucket::on_resp_timeout()`].
        ///
        /// The node is considered to be in an increasingly questionable state.
        pub fn on_resp_timeout(&mut self, tx_id: &TxId)
        where
            TxId: PartialEq,
        {
            self.karma = self.karma.saturating_sub(1);

            if let Some(ping_tx_id) = &self.ping_tx_id {
                if *ping_tx_id == *tx_id {
                    self.ping_tx_id = None;
                }
            }
        }
    }

    pub(super) trait MyBucket<T> {
        fn find_node_to_ping(&mut self, now: Instant) -> Option<&mut T>;

        fn timeout(&self) -> &Instant;
    }

    impl<Addr> MyBucket<Node<Addr, transaction::Id, Instant>>
        for Bucket<Node<Addr, transaction::Id, Instant>, Instant>
    {
        /// Finds a node which should be pinged.
        ///
        /// Nodes should have be pinged occasionally in order to determine if they are still active.
        ///
        /// When a node is pinged, the [`Node::on_ping()`] method should be called
        /// on the found `Node` to store the transaction ID of the ping (and to
        /// indicate the node was recently pinged).
        fn find_node_to_ping(
            &mut self,
            now: Instant,
        ) -> Option<&mut Node<Addr, transaction::Id, Instant>> {
            self.iter_mut().find(|n| {
                n.state_with_now(&now) == NodeState::Questionable && n.ping_tx_id.is_none()
            })
        }

        /// Returns the timeout for the bucket.
        ///
        /// Nodes may need to be pinged to ensure they are still active. See [`Bucket::find_node_to_ping()`].
        ///
        /// The bucket may also need to be refreshed.
        ///
        /// A bucket is refreshed by attempting to find a random node `Id` in the
        /// bucket. See [`Bucket::rand_id()`] to find a random ID.
        ///
        /// Set the new refresh deadline by calling [`Bucket::set_refresh_deadline()`] if refreshed.
        fn timeout(&self) -> &Instant {
            if let Some(non_pinged_node_timeout) = self
                .iter()
                .filter(|n| n.ping_tx_id.is_none())
                .map(Node::timeout)
                .min()
            {
                return core::cmp::min(self.refresh_deadline(), non_pinged_node_timeout);
            }

            self.refresh_deadline()
        }
    }

    pub(super) trait MyTable<T> {
        #[must_use]
        fn timeout(&self) -> Option<Instant>;

        fn find_node_to_ping(&mut self, now: Instant) -> Option<&mut T>;
    }

    /// Finds close neighbors for the given `Id` parameter.
    ///
    /// Useful to find nodes which a query should be sent to.
    pub(super) fn find_neighbors<Addr>(
        table: &Table<Node<Addr, transaction::Id, Instant>, Instant>,
        id: node::Id,
    ) -> impl Iterator<Item = AddrId<Addr>>
    where
        Addr: Clone,
    {
        let mut nodes = table
            .iter()
            .flat_map(Bucket::iter)
            .map(|n| n.addr_id().clone())
            // .flat_map(|b| b.prioritized_nodes(now.clone()).cloned())
            .collect::<Vec<_>>();
        nodes.sort_by_key(|a| a.id().distance(id));
        nodes.into_iter()
    }

    impl<Addr> MyTable<Node<Addr, transaction::Id, Instant>>
        for Table<Node<Addr, transaction::Id, Instant>, Instant>
    where
        Addr: PartialEq,
    {
        /// The earliest deadline when at least one of the buckets in the routing
        /// table should be refreshed.
        ///
        /// Once a timeout is reached, call [`Table::find_refreshable_bucket()`] to
        /// find a bucket to refresh.
        #[must_use]
        fn timeout(&self) -> Option<Instant> {
            self.iter().map(Bucket::timeout).min().copied()
        }

        /// Finds a node which should be pinged to determine if the node is still active.
        fn find_node_to_ping(
            &mut self,
            now: Instant,
        ) -> Option<&mut Node<Addr, transaction::Id, Instant>> {
            self.iter_mut().find_map(|b| b.find_node_to_ping(now))
        }
    }
}
