//! Logical database contexts move their owned indexes together; values are never copied.
use super::{
    DatabaseState, Engine, Mutation, Resp, error, integer, integer_error, invalid_journal,
};
use bytes::Bytes;

impl Engine {
    fn put_database(&mut self, database: u32, state: DatabaseState) {
        if database == self.active_database {
            self.install_database(state);
        } else if !state.entries.is_empty() || !state.held_entries.is_empty() {
            self.databases.insert(database, state);
        }
    }

    pub(super) fn swap_database_states(&mut self, first: u32, second: u32) -> std::io::Result<()> {
        if first >= 65_536 || second >= 65_536 {
            return Err(invalid_journal());
        }
        if first == second {
            return Ok(());
        }
        let first_state = if first == self.active_database {
            self.take_database()
        } else {
            self.databases.remove(&first).unwrap_or_default()
        };
        let second_state = if second == self.active_database {
            self.take_database()
        } else {
            self.databases.remove(&second).unwrap_or_default()
        };
        self.put_database(first, second_state);
        self.put_database(second, first_state);
        Ok(())
    }

    pub(super) fn cmd_swapdb(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (Some(first), Some(second)) = (integer(&args[1]), integer(&args[2])) else {
            return integer_error();
        };
        if first < 0
            || second < 0
            || first >= i64::from(self.database_count())
            || second >= i64::from(self.database_count())
        {
            return error("ERR DB index is out of range");
        }
        let (first, second) = (first as u32, second as u32);
        if first != second {
            if let Err(error) = self.swap_database_states(first, second) {
                return self.fail_io(error);
            }
            self.record(now, || Mutation::SwapDatabases { first, second });
        }
        Resp::Simple("OK")
    }
}
