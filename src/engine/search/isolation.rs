use super::{Catalog, Engine};
use std::{io, sync::Arc};

/// Search-only disposable execution. The caller restores source keys before rollback.
/// Drop restores the shared catalog even on an early error; explicit rollback also
/// restores each engine's original lazy attachment and reports invariant failures.
pub(crate) struct SearchRollback {
    catalog: Arc<Catalog>,
    previous: Vec<Option<Arc<Catalog>>>,
    active: bool,
}
impl SearchRollback {
    pub(crate) fn begin(engines: &mut [Engine]) -> io::Result<Self> {
        let catalog = engines
            .iter()
            .find_map(|engine| engine.search.clone())
            .unwrap_or_else(|| Arc::new(Catalog::new()));
        if engines
            .iter()
            .filter_map(|engine| engine.search.as_ref())
            .any(|other| !Arc::ptr_eq(other, &catalog))
        {
            return Err(io::Error::other(
                "disposable search requires one shared catalog",
            ));
        }
        catalog.lock().begin_rollback()?;
        let previous = engines
            .iter_mut()
            .map(|engine| engine.search.replace(catalog.clone()))
            .collect();
        Ok(Self {
            catalog,
            previous,
            active: true,
        })
    }
    pub(crate) fn rollback(mut self, engines: &mut [Engine]) -> io::Result<()> {
        let result = {
            let mut state = self.catalog.lock();
            let result = state.rollback();
            self.catalog.update_count(&state);
            result
        };
        self.active = false;
        if engines.len() != self.previous.len() {
            return Err(io::Error::other("search rollback engine count changed"));
        }
        for (engine, previous) in engines.iter_mut().zip(self.previous.drain(..)) {
            engine.search = previous;
            engine.search_plan = None;
        }
        result
    }
}
impl Drop for SearchRollback {
    fn drop(&mut self) {
        if self.active {
            let mut state = self.catalog.lock();
            let result = state.rollback();
            self.catalog.update_count(&state);
            if let Err(error) = result {
                eprintln!("disposable search rollback failed during unwinding: {error}");
            }
        }
    }
}
