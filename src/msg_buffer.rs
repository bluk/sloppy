// Copyright 2020 Bryant Luk
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use crate::{error::Error, transaction, ReadEvent};
use cloudburst::dht::{
    krpc::{self, ErrorVal, QueryArgs, RespVal},
    node::AddrOptId,
};
use serde_bytes::Bytes;
use std::{
    collections::VecDeque,
    net::SocketAddr,
    time::{Duration, Instant},
};

#[derive(Debug)]
pub(crate) struct OutboundMsg {
    tx_id: Option<transaction::Id>,
    timeout: Duration,
    pub(crate) addr_opt_id: AddrOptId<SocketAddr>,
    pub(crate) msg_data: Vec<u8>,
}

impl OutboundMsg {
    pub(crate) fn into_transaction(self) -> Option<transaction::Transaction> {
        let addr_opt_id = self.addr_opt_id;
        let timeout = self.timeout;
        self.tx_id.map(|tx_id| transaction::Transaction {
            tx_id,
            addr_opt_id,
            deadline: Instant::now() + timeout,
        })
    }
}

#[derive(Debug, Default)]
pub(crate) struct Buffer {
    inbound: VecDeque<ReadEvent>,
    outbound: VecDeque<OutboundMsg>,
}

impl Buffer {
    pub(crate) fn new() -> Self {
        Self {
            inbound: VecDeque::new(),
            outbound: VecDeque::new(),
        }
    }

    pub(crate) fn push_inbound(&mut self, msg: ReadEvent) {
        self.inbound.push_back(msg);
    }

    pub(crate) fn pop_inbound(&mut self) -> Option<ReadEvent> {
        self.inbound.pop_front()
    }

    pub(crate) fn write_query<A, T>(
        &mut self,
        args: &T,
        addr_opt_id: A,
        timeout: Duration,
        client_version: Option<&[u8]>,
        tx_manager: &mut transaction::Manager,
    ) -> Result<transaction::Id, Error>
    where
        T: QueryArgs,
        A: Into<AddrOptId<SocketAddr>>,
    {
        let tx_id = tx_manager.next_transaction_id();

        let addr_opt_id = addr_opt_id.into();

        self.outbound.push_back(OutboundMsg {
            tx_id: Some(tx_id),
            addr_opt_id,
            msg_data: bt_bencode::to_vec(&krpc::ser::QueryMsg {
                a: Some(&args.to_value()),
                q: Bytes::new(T::method_name()),
                t: Bytes::new(tx_id.as_ref()),
                v: client_version.map(Bytes::new),
            })
            .map_err(|_| Error::CannotSerializeKrpcMessage)?,
            timeout,
        });
        Ok(tx_id)
    }

    pub(crate) fn write_resp<A, T>(
        &mut self,
        transaction_id: &[u8],
        resp: Option<T>,
        addr_opt_id: A,
        client_version: Option<&[u8]>,
    ) -> Result<(), Error>
    where
        T: RespVal,
        A: Into<AddrOptId<SocketAddr>>,
    {
        self.outbound.push_back(OutboundMsg {
            tx_id: None,
            addr_opt_id: addr_opt_id.into(),
            msg_data: bt_bencode::to_vec(&krpc::ser::RespMsg {
                r: resp.map(|resp| resp.to_value()).as_ref(),
                t: Bytes::new(transaction_id),
                v: client_version.map(Bytes::new),
            })
            .map_err(|_| Error::CannotSerializeKrpcMessage)?,
            timeout: Duration::new(0, 0),
        });
        Ok(())
    }

    pub fn write_err<A, T>(
        &mut self,
        transaction_id: &[u8],
        details: &T,
        addr_opt_id: A,
        client_version: Option<&[u8]>,
    ) -> Result<(), Error>
    where
        T: ErrorVal,
        A: Into<AddrOptId<SocketAddr>>,
    {
        self.outbound.push_back(OutboundMsg {
            tx_id: None,
            addr_opt_id: addr_opt_id.into(),
            msg_data: bt_bencode::to_vec(&krpc::ser::ErrMsg {
                e: Some(&details.to_value()),
                t: Bytes::new(transaction_id),
                v: client_version.map(Bytes::new),
            })
            .map_err(|_| Error::CannotSerializeKrpcMessage)?,
            timeout: Duration::new(0, 0),
        });
        Ok(())
    }

    pub(crate) fn pop_outbound(&mut self) -> Option<OutboundMsg> {
        self.outbound.pop_front()
    }
}
