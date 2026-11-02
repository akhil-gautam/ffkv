use super::*;
#[derive(Clone, Debug)]
pub(super) struct VectorUndo {
    nodes: BTreeMap<u64, Option<Node>>,
    incoming: BTreeMap<u64, Option<BTreeSet<(u64, usize)>>>,
    documents: BTreeMap<DocId, Option<Vec<u64>>>,
    entry: Option<u64>,
    next: u64,
    quantizer: Option<(f64, f64)>,
}
impl VectorIndex {
    pub(in super::super) fn begin_rollback(&mut self) {
        if self.undo.is_none() {
            self.undo = Some(VectorUndo {
                nodes: BTreeMap::new(),
                incoming: BTreeMap::new(),
                documents: BTreeMap::new(),
                entry: self.entry,
                next: self.next,
                quantizer: self.quantizer,
            });
        }
    }
    pub(super) fn recording_rollback(&self) -> bool {
        self.undo.is_some()
    }
    pub(super) fn remember_node(&mut self, id: u64) {
        if let Some(u) = &mut self.undo
            && let std::collections::btree_map::Entry::Vacant(e) = u.nodes.entry(id)
        {
            e.insert(self.nodes.get(&id).cloned());
        }
    }
    pub(super) fn remember_incoming(&mut self, id: u64) {
        if let Some(u) = &mut self.undo
            && let std::collections::btree_map::Entry::Vacant(e) = u.incoming.entry(id)
        {
            e.insert(self.incoming.get(&id).cloned());
        }
    }
    pub(super) fn remember_document(&mut self, id: DocId) {
        if let Some(u) = &mut self.undo
            && let std::collections::btree_map::Entry::Vacant(e) = u.documents.entry(id)
        {
            e.insert(self.documents.get(&id).cloned());
        }
    }
    pub(super) fn remember_quantization(&mut self) {
        if self.recording_rollback() {
            let ids = self.nodes.keys().copied().collect::<Vec<_>>();
            for id in ids {
                self.remember_node(id);
            }
        }
    }
    pub(in super::super) fn rollback(&mut self) {
        let Some(u) = self.undo.take() else { return };
        for (id, old) in u.nodes {
            if let Some(current) = self.nodes.remove(&id) {
                self.levels.remove(&(current.links.len() - 1, id));
                self.node_order.remove(&id);
            }
            if let Some(old) = old {
                self.levels.insert((old.links.len() - 1, id));
                self.node_order.insert(id);
                self.nodes.insert(id, old);
            }
        }
        for (id, old) in u.incoming {
            self.incoming.remove(&id);
            if let Some(old) = old {
                self.incoming.insert(id, old);
            }
        }
        for (id, old) in u.documents {
            self.documents.remove(&id);
            if let Some(old) = old {
                self.documents.insert(id, old);
            }
        }
        self.entry = u.entry;
        self.next = u.next;
        self.quantizer = u.quantizer;
    }
}
