use super::*;
use std::fmt::Write as _;

pub(super) fn validate(d: &Durable) -> io::Result<()> {
    if d.version != 1
        || d.nodes.is_empty()
        || d.nodes.len() > 1024
        || d.blacklist.len() > 1024
        || !d.nodes.contains_key(&d.myself)
    {
        return Err(invalid("invalid cluster config"));
    }
    for (id, n) in &d.nodes {
        if !valid_id(id)
            || *id != n.id
            || !valid_id(&n.shard_id)
            || n.port == 0
            || n.bus_port == 0
            || n.ip.parse::<std::net::IpAddr>().is_err()
        {
            return Err(invalid("invalid cluster node"));
        }
        if n.parent.as_ref().is_some_and(|p| p == id || !valid_id(p)) {
            return Err(invalid("invalid cluster replica parent"));
        }
        let mut seen = vec![false; SLOT_COUNT];
        for (start, end) in &n.slots {
            if start > end || *end as usize >= SLOT_COUNT {
                return Err(invalid("invalid stored slot range"));
            }
            for slot in *start..=*end {
                if std::mem::replace(&mut seen[slot as usize], true) {
                    return Err(invalid("duplicate stored slot"));
                }
            }
        }
        if n.slots.windows(2).any(|pair| pair[0].1 >= pair[1].0) {
            return Err(invalid("stored slot ranges are not ordered"));
        }
        if n.parent.is_some() && !n.slots.is_empty() {
            return Err(invalid("replica claims primary slots"));
        }
    }
    for (slot, id) in d.importing.iter().chain(&d.migrating) {
        if *slot as usize >= SLOT_COUNT || !d.nodes.contains_key(id) {
            return Err(invalid("invalid open slot"));
        }
    }
    if d.last_vote_epoch > d.current_epoch || d.blacklist.keys().any(|id| !valid_id(id)) {
        return Err(invalid("invalid persisted cluster epoch or blacklist"));
    }
    if let Some(task) = &d.active_task
        && (!valid_id(&task.id)
            || !valid_id(&task.source)
            || !valid_id(&task.dest)
            || task.ranges.is_empty()
            || task.ranges.len() > SLOT_COUNT
            || task
                .ranges
                .iter()
                .any(|(start, end)| start > end || *end as usize >= SLOT_COUNT)
            || task.ranges.windows(2).any(|pair| pair[0].1 >= pair[1].0)
            || (!task.snapshot_id.is_empty() && !valid_id(&task.snapshot_id))
            || (!task.stream_history.is_empty() && !valid_id(&task.stream_history))
            || !matches!(task.operation.as_str(), "import" | "migrate"))
    {
        return Err(invalid("invalid persisted migration task"));
    }
    Ok(())
}
pub(super) fn valid_id(id: &str) -> bool {
    id.len() == 40 && id.bytes().all(|b| b.is_ascii_hexdigit())
}
pub(super) fn owners(d: &Durable) -> Vec<Option<String>> {
    let mut owners: Vec<Option<String>> = vec![None; SLOT_COUNT];
    for node in d.nodes.values().filter(|n| n.parent.is_none()) {
        for (start, end) in &node.slots {
            for slot in *start..=*end {
                let current = &mut owners[slot as usize];
                if current
                    .as_ref()
                    .is_none_or(|id| (node.epoch, &node.id) > (d.nodes[id].epoch, id))
                {
                    *current = Some(node.id.clone());
                }
            }
        }
    }
    owners
}
/// Coalesce an increasing sequence of distinct slot numbers.
pub(super) fn ranges(slots: impl IntoIterator<Item = u16>) -> Vec<(u16, u16)> {
    let mut result: Vec<(u16, u16)> = vec![];
    for slot in slots {
        match result.last_mut() {
            Some((_, end)) if *end + 1 == slot => *end = slot,
            _ => result.push((slot, slot)),
        }
    }
    result
}
pub(super) fn is_voter(s: &State, node: &Node) -> bool {
    node.parent.is_none()
        && node
            .slots
            .iter()
            .flat_map(|(start, end)| *start..=*end)
            .any(|slot| s.owners[slot as usize].as_ref() == Some(&node.id))
}
pub(super) fn refresh_local_claims(s: &mut State) -> io::Result<()> {
    let id = &s.durable.myself;
    let assigned = ranges(
        s.owners
            .iter()
            .enumerate()
            .filter(|(_, owner)| owner.as_ref() == Some(id))
            .map(|(slot, _)| slot as u16),
    );
    let me = s
        .durable
        .nodes
        .get_mut(id)
        .ok_or_else(|| invalid("missing self"))?;
    if me.slots != assigned {
        me.slots = assigned;
        me.revision = me.revision.saturating_add(1);
    }
    Ok(())
}
pub(super) fn parse_ranges(args: &[Bytes], paired: bool) -> io::Result<Vec<u16>> {
    if args.is_empty() || (paired && !args.len().is_multiple_of(2)) {
        return Err(invalid("wrong number of arguments"));
    }
    let mut slots = Vec::new();
    let mut seen = vec![false; SLOT_COUNT];
    let mut add = |slot: u16| -> io::Result<()> {
        if std::mem::replace(&mut seen[slot as usize], true) {
            return Err(invalid("slot specified multiple times"));
        }
        slots.push(slot);
        Ok(())
    };
    if paired {
        for pair in args.as_chunks::<2>().0 {
            let start = slot_number(&pair[0])?;
            let end = slot_number(&pair[1])?;
            if start > end {
                return Err(invalid("start slot must not exceed end slot"));
            }
            for slot in start..=end {
                add(slot)?;
            }
        }
    } else {
        for arg in args {
            add(slot_number(arg)?)?;
        }
    }
    slots.sort_unstable();
    Ok(slots)
}
pub(super) fn set_owner(s: &mut State, slot: u16, owner: Option<&str>) {
    for node in s.durable.nodes.values_mut() {
        let gap = node.slots.partition_point(|(start, _)| *start <= slot);
        let containing = gap
            .checked_sub(1)
            .filter(|index| node.slots[*index].1 >= slot);
        let contains = containing.is_some();
        let wanted = owner == Some(node.id.as_str());
        if contains != wanted {
            if let Some(index) = containing {
                let (start, end) = node.slots[index];
                match (start == slot, end == slot) {
                    (true, true) => {
                        node.slots.remove(index);
                    }
                    (true, false) => node.slots[index].0 = slot + 1,
                    (false, true) => node.slots[index].1 = slot - 1,
                    (false, false) => {
                        node.slots[index].1 = slot - 1;
                        node.slots.insert(index + 1, (slot + 1, end));
                    }
                }
            } else {
                let left = gap > 0 && node.slots[gap - 1].1 + 1 == slot;
                let right = gap < node.slots.len() && node.slots[gap].0 == slot + 1;
                match (left, right) {
                    (true, true) => {
                        let end = node.slots.remove(gap).1;
                        node.slots[gap - 1].1 = end;
                    }
                    (true, false) => node.slots[gap - 1].1 = slot,
                    (false, true) => node.slots[gap].0 = slot,
                    (false, false) => node.slots.insert(gap, (slot, slot)),
                }
            }
            if node.id == s.durable.myself {
                node.revision = node.revision.saturating_add(1);
            }
        }
    }
    s.owners[slot as usize] = owner.map(str::to_owned);
}
pub(super) fn node_line(s: &State, node: &Node, timeout: u64) -> String {
    let me = node.id == s.durable.myself;
    let live = s.live.get(&node.id);
    let down =
        !me && live.is_none_or(|l| clock().saturating_sub(l.last_seen.max(l.first_seen)) > timeout);
    let mut flags = if me {
        "myself,".to_owned()
    } else {
        String::new()
    };
    flags.push_str(if node.parent.is_some() {
        "slave"
    } else {
        "master"
    });
    if live.is_some_and(|l| l.failed) {
        flags.push_str(",fail");
    } else if down {
        flags.push_str(",fail?");
    }
    let connected = me || s.links.values().any(|link| link.node == node.id);
    let epoch = if me {
        node.epoch
    } else {
        node.parent
            .as_ref()
            .and_then(|parent| s.durable.nodes.get(parent))
            .map_or(node.epoch, |primary| primary.epoch)
    };
    let mut out = format!(
        "{} {}:{}@{} {} {} {} {} {} {}",
        node.id,
        node.ip,
        node.port,
        node.bus_port,
        flags,
        node.parent.as_deref().unwrap_or("-"),
        live.map_or(0, |l| l.ping_sent),
        live.map_or(0, |l| l.last_seen),
        epoch,
        if connected {
            "connected"
        } else {
            "disconnected"
        }
    );
    let mut range: Option<(u16, u16)> = None;
    for slot in node
        .slots
        .iter()
        .flat_map(|(start, end)| *start..=*end)
        .filter(|slot| s.owners[*slot as usize].as_ref() == Some(&node.id))
    {
        if let Some((_, end)) = range.as_mut().filter(|(_, end)| *end + 1 == slot) {
            *end = slot;
        } else {
            if let Some((start, end)) = range {
                append_range(&mut out, start, end);
            }
            range = Some((slot, slot));
        }
    }
    if let Some((start, end)) = range {
        append_range(&mut out, start, end);
    }
    if me {
        for (slot, id) in &s.durable.importing {
            let _ = write!(out, " [{slot}-<-{id}]");
        }
        for (slot, id) in &s.durable.migrating {
            let _ = write!(out, " [{slot}->-{id}]");
        }
    }
    out
}
fn append_range(out: &mut String, start: u16, end: u16) {
    if start == end {
        let _ = write!(out, " {start}");
    } else {
        let _ = write!(out, " {start}-{end}");
    }
}
pub(super) fn slot_ranges(s: &State) -> Vec<(u16, u16, String)> {
    let mut result: Vec<(u16, u16, String)> = vec![];
    for (slot, owner) in s.owners.iter().enumerate() {
        if let Some(owner) = owner {
            match result.last_mut() {
                Some((_, end, id)) if id == owner && *end + 1 == slot as u16 => *end = slot as u16,
                _ => result.push((slot as u16, slot as u16, owner.clone())),
            }
        }
    }
    result
}
pub(super) fn node_map(s: &State, node: &Node, timeout: u64) -> Resp {
    let health = if s.live.get(&node.id).is_some_and(|l| l.failed) {
        "fail"
    } else if node.id == s.durable.myself
        || s.live
            .get(&node.id)
            .is_some_and(|l| clock().saturating_sub(l.last_seen) < timeout)
    {
        "online"
    } else {
        "loading"
    };
    Resp::Map(vec![
        (bulk("id"), bulk(node.id.clone())),
        (bulk("port"), Resp::Integer(i64::from(node.port))),
        (bulk("ip"), bulk(node.ip.clone())),
        (bulk("endpoint"), bulk(node.ip.clone())),
        (
            bulk("role"),
            bulk(if node.parent.is_some() {
                "replica"
            } else {
                "master"
            }),
        ),
        (
            bulk("replication-offset"),
            Resp::Integer(node.offset.min(i64::MAX as u64) as i64),
        ),
        (bulk("health"), bulk(health)),
    ])
}
