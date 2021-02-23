use crate::audit::AuditScope;
use crate::be::{IdRawEntry, IDL};
use crate::entry::{Entry, EntryCommitted, EntrySealed};
use crate::value::{IndexType, Value};
use idlset::IDLBitRange;
use kanidm_proto::v1::{ConsistencyError, OperationError};
use std::convert::{TryFrom, TryInto};
use std::time::Duration;
use uuid::Uuid;

const DBV_ID2ENTRY: &str = "id2entry";
const DBV_INDEXV: &str = "indexv";

#[derive(Debug, Clone)]
pub struct IdlXlsxConfig {
    path: String,
}

impl IdlXlsxConfig {
    pub fn new_temporary() -> Self {
        unimplemented!();
    }

    pub fn new(path: &str) -> Self {
        IdlXlsxConfig {
            path: path.to_string(),
        }
    }
}

#[derive(Debug)]
pub struct IdXlsxEntry {
    id: i64,
    data: Vec<u8>,
}

impl TryFrom<IdXlsxEntry> for IdRawEntry {
    type Error = OperationError;

    fn try_from(value: IdXlsxEntry) -> Result<Self, Self::Error> {
        if value.id <= 0 {
            return Err(OperationError::InvalidEntryID);
        }
        Ok(IdRawEntry {
            id: value
                .id
                .try_into()
                .map_err(|_| OperationError::InvalidEntryID)?,
            data: value.data,
        })
    }
}

impl TryFrom<IdRawEntry> for IdXlsxEntry {
    type Error = OperationError;

    fn try_from(value: IdRawEntry) -> Result<Self, Self::Error> {
        if value.id == 0 {
            return Err(OperationError::InvalidEntryID);
        }
        Ok(IdXlsxEntry {
            id: value
                .id
                .try_into()
                .map_err(|_| OperationError::InvalidEntryID)?,
            data: value.data,
        })
    }
}

#[derive(Clone)]
pub struct IdlXlsx {
    // excel
}

pub struct IdlXlsxReadTransaction {
    committed: bool,
    // excel
}

pub struct IdlXlsxWriteTransaction {
    committed: bool,
    // excel
}

pub trait IdlXlsxTransaction {
    fn get_conn(&self) -> &excel;

    fn get_identry(
        &self,
        au: &mut AuditScope,
        idl: &IDL,
    ) -> Result<Vec<Entry<EntrySealed, EntryCommitted>>, OperationError> {
        lperf_trace_segment!(au, "be::idl_sqlite::get_identry", || {
            self.get_identry_raw(au, idl)?
                .into_iter()
                .map(|ide| ide.into_entry(au))
                .collect()
        })
    }

    fn get_identry_raw(
        &self,
        au: &mut AuditScope,
        idl: &IDL,
    ) -> Result<Vec<IdRawEntry>, OperationError> {
        match idl {
            IDL::ALLIDS => {
                unimplemented!();
            }
            IDL::Partial(idli) | IDL::PartialThreshold(idli) | IDL::Indexed(idli) => {
                unimplemented!();
            }
        }
    }

    fn exists_idx(
        &self,
        audit: &mut AuditScope,
        attr: &str,
        itype: &IndexType,
    ) -> Result<bool, OperationError> {
        unimplemented!();
    }

    fn get_idl(
        &self,
        audit: &mut AuditScope,
        attr: &str,
        itype: &IndexType,
        idx_key: &str,
    ) -> Result<Option<IDLBitRange>, OperationError> {
        lperf_trace_segment!(audit, "be::idl_sqlite::get_idl", || {
            unimplemented!();
        })
    }

    fn name2uuid(
        &mut self,
        audit: &mut AuditScope,
        name: &str,
    ) -> Result<Option<Uuid>, OperationError> {
        lperf_trace_segment!(audit, "be::idl_sqlite::name2uuid", || {
            unimplemented!();
        })
    }

    fn uuid2spn(
        &mut self,
        audit: &mut AuditScope,
        uuid: &Uuid,
    ) -> Result<Option<Value>, OperationError> {
        lperf_trace_segment!(audit, "be::idl_sqlite::uuid2spn", || {
            unimplemented!();
        })
    }

    fn uuid2rdn(
        &mut self,
        audit: &mut AuditScope,
        uuid: &Uuid,
    ) -> Result<Option<String>, OperationError> {
        lperf_trace_segment!(audit, "be::idl_sqlite::uuid2rdn", || {
            unimplemented!();
        })
    }

    fn get_db_s_uuid(&self) -> Result<Option<Uuid>, OperationError> {
        unimplemented!();
    }

    fn get_db_d_uuid(&self) -> Result<Option<Uuid>, OperationError> {
        unimplemented!();
    }

    fn verify(&self) -> Vec<Result<(), ConsistencyError>> {
        unimplemented!();
    }
}

impl IdlXlsxTransaction for IdlXlsxReadTransaction {
    fn get_conn(&self) -> &excel {
        &self.excell
    }
}

impl Drop for IdlXlsxReadTransaction {
    // Abort - so far this has proven reliable to use drop here.
    fn drop(self: &mut Self) {
        if !self.committed {
            // Do a rollback here.
            unimplemented!();
        }
    }
}

impl IdlXlsxReadTransaction {
    pub fn new(conn: excel) -> Self {
        unimplemented!();
    }
}

impl IdlXlsxTransaction for IdlXlsxWriteTransaction {
    fn get_conn(&self) -> &excel {
        &self.excel
    }
}

impl Drop for IdlXlsxWriteTransaction {
    // Abort
    fn drop(self: &mut Self) {
        if !self.committed {
            // rollback
            unimplemented!();
        }
    }
}

impl IdlXlsxWriteTransaction {
    pub fn new(conn: excel) -> Self {
        unimplemented!();
    }

    pub fn commit(mut self, audit: &mut AuditScope) -> Result<(), OperationError> {
        lperf_trace_segment!(audit, "be::idl_sqlite::commit", || {
            // ltrace!(audit, "Commiting BE WR txn");
            assert!(!self.committed);
            self.committed = true;

            unimplemented!();
        })
    }

    pub fn get_id2entry_max_id(&self) -> Result<u64, OperationError> {
        unimplemented!();
    }

    pub fn write_identry(
        &self,
        au: &mut AuditScope,
        entry: &Entry<EntrySealed, EntryCommitted>,
    ) -> Result<(), OperationError> {
        let dbe = entry.to_dbentry();
        let data = serde_cbor::to_vec(&dbe).map_err(|_| OperationError::SerdeCborError)?;

        let raw_entries = std::iter::once(IdRawEntry {
            id: entry.get_id(),
            data,
        });

        self.write_identries_raw(au, raw_entries)
    }

    pub fn write_identries_raw<I>(
        &self,
        au: &mut AuditScope,
        mut entries: I,
    ) -> Result<(), OperationError>
    where
        I: Iterator<Item = IdRawEntry>,
    {
        unimplemented!();
    }

    pub fn delete_identry(&self, au: &mut AuditScope, id: u64) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn write_idl(
        &self,
        audit: &mut AuditScope,
        attr: &str,
        itype: &IndexType,
        idx_key: &str,
        idl: &IDLBitRange,
    ) -> Result<(), OperationError> {
        lperf_trace_segment!(audit, "be::idl_sqlite::write_idl", || {
            if idl.is_empty() {
                ltrace!(audit, "purging idl -> {:?}", idl);
                unimplemented!();
            } else {
                ltrace!(audit, "writing idl -> {}", idl);
                unimplemented!();
            }
        })
    }

    pub fn create_name2uuid(&self, audit: &mut AuditScope) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn write_name2uuid_add(
        &self,
        audit: &mut AuditScope,
        name: &str,
        uuid: &Uuid,
    ) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn write_name2uuid_rem(
        &self,
        audit: &mut AuditScope,
        name: &str,
    ) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn create_uuid2spn(&self, audit: &mut AuditScope) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn write_uuid2spn(
        &self,
        audit: &mut AuditScope,
        uuid: &Uuid,
        k: Option<&Value>,
    ) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn create_uuid2rdn(&self, audit: &mut AuditScope) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn write_uuid2rdn(
        &self,
        audit: &mut AuditScope,
        uuid: &Uuid,
        k: Option<&String>,
    ) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn create_idx(
        &self,
        audit: &mut AuditScope,
        attr: &str,
        itype: &IndexType,
    ) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn list_idxs(&self, audit: &mut AuditScope) -> Result<Vec<String>, OperationError> {
        unimplemented!();
    }

    pub unsafe fn purge_idxs(&self, audit: &mut AuditScope) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub unsafe fn purge_id2entry(&self, audit: &mut AuditScope) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn write_db_s_uuid(&self, nsid: Uuid) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn write_db_d_uuid(&self, nsid: Uuid) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn set_db_ts_max(&self, ts: &Duration) -> Result<(), OperationError> {
        unimplemented!();
    }

    pub fn get_db_ts_max(&self) -> Result<Option<Duration>, OperationError> {
        unimplemented!();
    }

    // ===== inner helpers =====
    // Some of these are not self due to use in new()
    fn get_db_version_key(&self, key: &str) -> i64 {
        unimplemented!();
    }

    fn set_db_version_key(&self, key: &str, v: i64) -> Result<(), ()> {
        unimplemented!();
    }

    pub(crate) fn get_db_index_version(&self) -> i64 {
        self.get_db_version_key(DBV_INDEXV)
    }

    pub(crate) fn set_db_index_version(&self, v: i64) -> Result<(), OperationError> {
        self.set_db_version_key(DBV_INDEXV, v).map_err(|e| {
            eprintln!("CRITICAL: rusqlite error {:?}", e);
            OperationError::SQLiteError
        })
    }

    pub(crate) fn get_allids(&self, au: &mut AuditScope) -> Result<IDLBitRange, OperationError> {
        unimplemented!();
    }

    pub fn setup(&self, audit: &mut AuditScope) -> Result<(), OperationError> {
        unimplemented!();
    }
}

impl IdlXlsx {
    pub fn new(audit: &mut AuditScope, config: IdlXlsxConfig) -> Result<Self, OperationError> {
        unimplemented!();
    }

    pub fn read(&self) -> IdlXlsxReadTransaction {
        unimplemented!();
    }

    pub fn write(&self) -> IdlXlsxWriteTransaction {
        unimplemented!();
    }

    pub fn get_pool_size(&self) -> usize {
        unimplemented!();
    }
}

#[cfg(test)]
mod tests {
    use super::{IdlXlsx, IdlXlsxConfig, IdlXlsxTransaction};
    use crate::audit::AuditScope;

    #[test]
    fn test_idl_sqlite_verify() {
        let mut audit = AuditScope::new("run_test", uuid::Uuid::new_v4(), None);
        let be_cfg = IdlXlsxConfig::new_temporary();
        let be = IdlXlsx::new(&mut audit, be_cfg).unwrap();
        let be_w = be.write();
        let r = be_w.verify();
        assert!(r.len() == 0);
    }
}
