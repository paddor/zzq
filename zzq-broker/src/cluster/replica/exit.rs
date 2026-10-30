//! The node's exit: a follower dumps its state at once; a leader first
//! proposes what its lanes hold, so its state is that of its log.

use std::time::Duration;

use bytes::Bytes;

use super::{App, Replica};
use crate::cluster::image::{self, Cut};
use crate::cluster::{ExitReply, dump};

/// How long a leader may take to propose what its lanes hold before it
/// exits without a dump.
const EXIT_WAIT: Duration = Duration::from_secs(5);

#[derive(Debug)]
pub(super) struct Exiting {
    reply: ExitReply,
    until: Duration,
    /// A leader's image of its lanes, once every group is proposed.
    cut: Option<Cut>,
}

impl Replica {
    /// The broker stops: its lanes take no more input.
    pub(super) async fn begin_exit(&mut self, reply: ExitReply) {
        self.cut = None;
        let cut = match self.app {
            App::Leader => self.cut_lanes().await,
            App::Follower(_) => None,
        };
        self.exiting = Some(Exiting {
            reply,
            until: self.now() + EXIT_WAIT,
            cut,
        });
    }

    /// Answers with the exit dump once it is ready, or with none past
    /// the wait. True once answered.
    pub(super) async fn poll_exit(&mut self, now: Duration) -> bool {
        let Some(ex) = &self.exiting else {
            return false;
        };
        let dump = match &self.app {
            App::Follower(acc) => self
                .node
                .export()
                .map(|(node, op)| dump::encode(&node, &image::encode(&acc.image(op)))),
            App::Leader if now >= ex.until => None,
            App::Leader => match self.leader_dump() {
                Ok(dump) => dump,
                Err(Pending::Proposing) => return false,
                Err(Pending::Moved) => {
                    let cut = self.cut_lanes().await;
                    if let Some(ex) = &mut self.exiting {
                        ex.cut = cut;
                    }
                    return false;
                }
            },
        };
        if let Some(ex) = self.exiting.take() {
            let _ = ex.reply.send(dump);
        }
        true
    }

    /// A leader's dump: its lanes' image, once every group it cut after is
    /// proposed, through the last op its log holds.
    fn leader_dump(&self) -> Result<Option<Bytes>, Pending> {
        let Some(cut) = self.exiting.as_ref().and_then(|ex| ex.cut.as_ref()) else {
            return Ok(None);
        };
        let tip = self.node.status().log_last;
        let taken = |j| self.taken.get(&j).copied().unwrap_or(0);
        let (predecessor, position, image) = cut.finish(taken, tip).ok_or(Pending::Proposing)?;
        let Some((node, op)) = self.node.export() else {
            return Ok(None);
        };
        // A journal went on after its cut: its part no longer holds the
        // state through the log's last op.
        if predecessor != position || position != op {
            return Err(Pending::Moved);
        }
        Ok(Some(dump::encode(&node, &image)))
    }
}

/// Why a leader's dump is not ready.
#[derive(Debug)]
enum Pending {
    /// Groups the lanes sealed for the cut are not proposed yet.
    Proposing,
    /// Records came after the cut: it is cut again.
    Moved,
}
