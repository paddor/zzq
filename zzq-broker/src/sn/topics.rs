//! Topic names, topic IDs and their registration.

use zzq_proto::sn::{self, Filter, Topic};

use super::{Client, MAX_SEND, predefined};

impl Client {
    /// The topic name a PUBLISH refers to.
    pub(super) fn topic_name(&self, topic: Topic) -> Option<String> {
        match topic {
            Topic::Id(id) => self.topics.by_id.get(&id).cloned(),
            Topic::Predefined(id) => predefined(&self.shared, id).map(str::to_owned),
            Topic::Short(s) => std::str::from_utf8(&s).ok().map(str::to_owned),
            _ => None,
        }
    }

    /// The topic or filter a SUBSCRIBE or UNSUBSCRIBE refers to.
    pub(super) fn filter_name(&self, filter: Filter<'_>) -> Option<String> {
        match filter {
            Filter::Name(n) => Some(n.to_owned()),
            Filter::Predefined(id) => predefined(&self.shared, id).map(str::to_owned),
            Filter::Short(s) => std::str::from_utf8(&s).ok().map(str::to_owned),
            _ => None,
        }
    }

    /// The topic ID of `name`, assigning one if needed. `None` once the
    /// client has `SnOptions::max_topics` of them, or no ID is free.
    pub(super) fn topic_id_for(&mut self, name: &str) -> Option<u16> {
        if let Some(&id) = self.topics.ids.get(name) {
            return Some(id);
        }
        if self.topics.by_id.len() >= self.shared.opts.sn.max_topics {
            return None;
        }
        for _ in 0..u16::MAX {
            self.topics.next_id = self.topics.next_id.wrapping_add(1).max(1);
            let id = self.topics.next_id;
            if !self.topics.by_id.contains_key(&id) && predefined(&self.shared, id).is_none() {
                self.topics.by_id.insert(id, name.to_owned());
                self.topics.ids.insert(name.to_owned(), id);
                return Some(id);
            }
        }
        None
    }

    /// The topic form for a PUBLISH on `topic`, and the topic ID whose
    /// REGISTER is not acked yet. The REGISTER for a new topic goes to
    /// `out`. `None` if the client cannot get a topic ID.
    pub(super) fn topic_out(
        &mut self,
        topic: &str,
        out: &mut Vec<Vec<u8>>,
    ) -> Option<(Topic, Option<u16>)> {
        let predefined = &self.shared.opts.sn.predefined;
        if let Some((id, _)) = predefined.iter().find(|(_, t)| t == topic) {
            return Some((Topic::Predefined(*id), None));
        }
        if let [a, b] = *topic.as_bytes() {
            return Some((Topic::Short([a, b]), None));
        }
        if let Some(&id) = self.topics.ids.get(topic) {
            let pending = self
                .topics
                .registering
                .values()
                .any(|&t| t == id)
                .then_some(id);
            return Some((Topic::Id(id), pending));
        }
        if !register_fits(topic) {
            return None;
        }
        let id = self.topic_id_for(topic)?;
        let msg_id = self.msg_id();
        self.topics.registering.insert(msg_id, id);
        let reg = sn::Packet::Register {
            topic_id: id,
            msg_id,
            topic,
        };
        let mut d = Vec::with_capacity(sn::encoded_len(&reg));
        sn::encode(&reg, &mut d);
        out.push(d);
        Some((Topic::Id(id), Some(id)))
    }
}

/// Whether a REGISTER of `topic` fits a datagram.
fn register_fits(topic: &str) -> bool {
    let reg = sn::Packet::Register {
        topic_id: 0,
        msg_id: 0,
        topic,
    };
    sn::encoded_len(&reg) <= MAX_SEND
}

/// Whether a SUBSCRIBE filter names one normal topic, which gets a topic
/// ID in SUBACK.
pub(super) fn plain(filter: &str) -> bool {
    !filter.contains(['+', '#']) && filter.len() != 2 && !filter.is_empty()
}
