pub(crate) mod account;
pub(crate) mod authsession;
pub(crate) mod claim;
pub(crate) mod delayed;
pub(crate) mod event;
pub(crate) mod group;
pub(crate) mod mfareg;
pub(crate) mod radius;
pub(crate) mod server;
pub(crate) mod unix;
// mod identity;

use kanidm_proto::v1::{AuthAllowed, AuthMech, UserAuthToken};

#[derive(Debug)]
pub enum AuthState {
    Choose(Vec<AuthMech>),
    Continue(Vec<AuthAllowed>),
    Denied(String),
    Success(UserAuthToken),
}
