#![deny(warnings)]
#![warn(unused_extern_crates)]
#![deny(clippy::unwrap_used)]
#![deny(clippy::expect_used)]
#![deny(clippy::panic)]
#![deny(clippy::unreachable)]
#![deny(clippy::await_holding_lock)]
#![deny(clippy::needless_pass_by_value)]
#![deny(clippy::trivially_copy_pass_by_ref)]

#[macro_use]
extern crate log;

use reqwest::header::CONTENT_TYPE;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_derive::Deserialize;
use serde_json::error::Error as SerdeJsonError;
use std::collections::BTreeMap;
use std::collections::BTreeSet as Set;
use std::fs::{metadata, File, Metadata};
use std::io::Read;
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::time::Duration;
use url::Url;
use uuid::Uuid;

use webauthn_rs::proto::{
    CreationChallengeResponse, PublicKeyCredential, RegisterPublicKeyCredential,
    RequestChallengeResponse,
};
// use users::{get_current_uid, get_effective_uid};

use kanidm_proto::v1::{
    AccountUnixExtend, AuthAllowed, AuthCredential, AuthMech, AuthRequest, AuthResponse, AuthState,
    AuthStep, CreateRequest, DeleteRequest, Entry, Filter, GroupUnixExtend, ModifyList,
    ModifyRequest, OperationError, OperationResponse, RadiusAuthToken, SearchRequest,
    SearchResponse, SetCredentialRequest, SetCredentialResponse, SingleStringRequest, TOTPSecret,
    UnixGroupToken, UnixUserToken, UserAuthToken, WhoamiResponse,
};

pub mod asynchronous;

use crate::asynchronous::KanidmAsyncClient;

pub const APPLICATION_JSON: &str = "application/json";
pub const KOPID: &str = "X-KANIDM-OPID";

#[derive(Debug)]
pub enum ClientError {
    Unauthorized,
    Http(reqwest::StatusCode, Option<OperationError>, String),
    Transport(reqwest::Error),
    AuthenticationFailed,
    EmptyResponse,
    TOTPVerifyFailed(Uuid, TOTPSecret),
    JSONDecode(reqwest::Error, String),
    JSONEncode(SerdeJsonError),
    SystemError,
}

#[derive(Debug, Deserialize)]
struct KanidmClientConfig {
    uri: Option<String>,
    verify_ca: Option<bool>,
    verify_hostnames: Option<bool>,
    ca_path: Option<String>,
    // Should we add username/pw later? They could be part of the builder
    // process ...
}

#[derive(Debug, Clone, Default)]
pub struct KanidmClientBuilder {
    address: Option<String>,
    verify_ca: bool,
    verify_hostnames: bool,
    ca: Option<reqwest::Certificate>,
    connect_timeout: Option<u64>,
}

fn read_file_metadata<P: AsRef<Path>>(path: &P) -> Result<Metadata, ()> {
    metadata(path).map_err(|e| {
        error!(
            "Unable to read metadata for {} - {:?}",
            path.as_ref()
                .to_str()
                .unwrap_or_else(|| "alert: invalid path"),
            e
        );
    })
}

impl KanidmClientBuilder {
    pub fn new() -> Self {
        KanidmClientBuilder {
            address: None,
            verify_ca: true,
            verify_hostnames: true,
            ca: None,
            connect_timeout: None,
        }
    }

    fn parse_certificate(ca_path: &str) -> Result<reqwest::Certificate, ()> {
        let mut buf = Vec::new();
        // Is the CA secure?
        let path = Path::new(ca_path);
        let ca_meta = read_file_metadata(&path)?;

        if !ca_meta.permissions().readonly() {
            warn!("permissions on {} may not be secure. Should be readonly to running uid. This could be a security risk ...", ca_path);
        }

        if ca_meta.uid() != 0 || ca_meta.gid() != 0 {
            warn!(
                "{} should be owned be root:root to prevent tampering",
                ca_path
            );
        }

        // TODO #253: Handle these errors better, or at least provide diagnostics?
        let mut f = File::open(ca_path).map_err(|_| ())?;
        f.read_to_end(&mut buf).map_err(|_| ())?;
        reqwest::Certificate::from_pem(&buf).map_err(|_| ())
    }

    fn apply_config_options(self, kcc: KanidmClientConfig) -> Result<Self, ()> {
        let KanidmClientBuilder {
            address,
            verify_ca,
            verify_hostnames,
            ca,
            connect_timeout,
        } = self;
        // Process and apply all our options if they exist.
        let address = match kcc.uri {
            Some(uri) => Some(uri),
            None => address,
        };
        let verify_ca = kcc.verify_ca.unwrap_or_else(|| verify_ca);
        let verify_hostnames = kcc.verify_hostnames.unwrap_or_else(|| verify_hostnames);
        let ca = match kcc.ca_path {
            Some(ca_path) => Some(Self::parse_certificate(ca_path.as_str())?),
            None => ca,
        };

        Ok(KanidmClientBuilder {
            address,
            verify_ca,
            verify_hostnames,
            ca,
            connect_timeout,
        })
    }

    pub fn read_options_from_optional_config<P: AsRef<Path>>(
        self,
        config_path: P,
    ) -> Result<Self, ()> {
        // If the file does not exist, we skip this function.
        let mut f = match File::open(config_path) {
            Ok(f) => f,
            Err(e) => {
                debug!("Unabled to open config file [{:?}], skipping ...", e);
                return Ok(self);
            }
        };

        let mut contents = String::new();
        f.read_to_string(&mut contents)
            .map_err(|e| eprintln!("{:?}", e))?;

        let config: KanidmClientConfig =
            toml::from_str(contents.as_str()).map_err(|e| eprintln!("{:?}", e))?;

        self.apply_config_options(config)
    }

    pub fn address(self, address: String) -> Self {
        KanidmClientBuilder {
            address: Some(address),
            verify_ca: self.verify_ca,
            verify_hostnames: self.verify_hostnames,
            ca: self.ca,
            connect_timeout: self.connect_timeout,
        }
    }

    pub fn danger_accept_invalid_hostnames(self, accept_invalid_hostnames: bool) -> Self {
        KanidmClientBuilder {
            address: self.address,
            verify_ca: self.verify_ca,
            // We have to flip the bool state here due to english language.
            verify_hostnames: !accept_invalid_hostnames,
            ca: self.ca,
            connect_timeout: self.connect_timeout,
        }
    }

    pub fn danger_accept_invalid_certs(self, accept_invalid_certs: bool) -> Self {
        KanidmClientBuilder {
            address: self.address,
            // We have to flip the bool state here due to english language.
            verify_ca: !accept_invalid_certs,
            verify_hostnames: self.verify_hostnames,
            ca: self.ca,
            connect_timeout: self.connect_timeout,
        }
    }

    pub fn connect_timeout(self, secs: u64) -> Self {
        KanidmClientBuilder {
            address: self.address,
            verify_ca: self.verify_ca,
            verify_hostnames: self.verify_hostnames,
            ca: self.ca,
            connect_timeout: Some(secs),
        }
    }

    pub fn add_root_certificate_filepath(self, ca_path: &str) -> Result<Self, ()> {
        //Okay we have a ca to add. Let's read it in and setup.
        let ca = Self::parse_certificate(ca_path)?;

        Ok(KanidmClientBuilder {
            address: self.address,
            verify_ca: self.verify_ca,
            verify_hostnames: self.verify_hostnames,
            ca: Some(ca),
            connect_timeout: self.connect_timeout,
        })
    }

    fn display_warnings(&self, address: &str) {
        // Check for problems now
        if !self.verify_ca {
            warn!("verify_ca set to false - this may allow network interception of passwords!");
        }

        if !self.verify_hostnames {
            warn!(
                "verify_hostnames set to false - this may allow network interception of passwords!"
            );
        }

        if !address.starts_with("https://") {
            warn!("address does not start with 'https://' - this may allow network interception of passwords!");
        }
    }

    // Consume self and return a client.
    pub fn build(self) -> Result<KanidmClient, reqwest::Error> {
        // Errghh, how to handle this cleaner.
        let address = match &self.address {
            Some(a) => a.clone(),
            None => {
                eprintln!("uri (-H) missing, can not proceed");
                unimplemented!();
            }
        };

        self.display_warnings(address.as_str());

        let client_builder = reqwest::blocking::Client::builder()
            .cookie_store(true)
            .danger_accept_invalid_hostnames(!self.verify_hostnames)
            .danger_accept_invalid_certs(!self.verify_ca);

        let client_builder = match &self.ca {
            Some(cert) => client_builder.add_root_certificate(cert.clone()),
            None => client_builder,
        };

        let client_builder = match &self.connect_timeout {
            Some(secs) => client_builder
                .connect_timeout(Duration::from_secs(*secs))
                .timeout(Duration::from_secs(*secs)),
            None => client_builder,
        };

        let client = client_builder.build()?;

        // Now get the origin.
        #[allow(clippy::expect_used)]
        let uri = Url::parse(&address).expect("can not fail");

        #[allow(clippy::expect_used)]
        let origin = uri
            .host_str()
            .map(|h| format!("{}://{}", uri.scheme(), h))
            .expect("can not fail");

        Ok(KanidmClient {
            client,
            addr: address,
            origin,
            builder: self,
            bearer_token: None,
        })
    }

    pub fn build_async(self) -> Result<KanidmAsyncClient, reqwest::Error> {
        // Errghh, how to handle this cleaner.
        let address = match &self.address {
            Some(a) => a.clone(),
            None => {
                eprintln!("uri (-H) missing, can not proceed");
                unimplemented!();
            }
        };

        self.display_warnings(address.as_str());

        let client_builder = reqwest::Client::builder()
            .cookie_store(true)
            .danger_accept_invalid_hostnames(!self.verify_hostnames)
            .danger_accept_invalid_certs(!self.verify_ca);

        let client_builder = match &self.ca {
            Some(cert) => client_builder.add_root_certificate(cert.clone()),
            None => client_builder,
        };

        let client_builder = match &self.connect_timeout {
            Some(secs) => client_builder
                .connect_timeout(Duration::from_secs(*secs))
                .timeout(Duration::from_secs(*secs)),
            None => client_builder,
        };

        let client = client_builder.build()?;

        Ok(KanidmAsyncClient {
            client,
            addr: address,
            builder: self,
            bearer_token: None,
        })
    }
}

#[derive(Debug)]
pub struct KanidmClient {
    client: reqwest::blocking::Client,
    addr: String,
    origin: String,
    builder: KanidmClientBuilder,
    bearer_token: Option<String>,
}

impl KanidmClient {
    pub fn get_origin(&self) -> &str {
        self.origin.as_str()
    }

    pub fn new_session(&self) -> Result<Self, reqwest::Error> {
        // Copy our builder, and then just process it.
        let builder = self.builder.clone();
        builder.build()
    }

    pub fn set_token(&mut self, new_token: String) {
        let mut new_token = Some(new_token);
        std::mem::swap(&mut self.bearer_token, &mut new_token);
    }

    pub fn get_token(&self) -> Option<&str> {
        self.bearer_token.as_deref()
    }

    pub fn logout(&mut self) -> Result<(), reqwest::Error> {
        // hack - we have to replace our reqwest client because that's the only way
        // to currently flush the cookie store. To achieve this we need to rebuild
        // and then destructure.

        let builder = self.builder.clone();
        let KanidmClient { mut client, .. } = builder.build()?;

        std::mem::swap(&mut self.client, &mut client);
        Ok(())
    }

    fn perform_post_request<R: Serialize, T: DeserializeOwned>(
        &self,
        dest: &str,
        request: R,
    ) -> Result<T, ClientError> {
        let dest = format!("{}{}", self.addr, dest);

        let req_string = serde_json::to_string(&request).map_err(ClientError::JSONEncode)?;

        let response = self
            .client
            .post(dest.as_str())
            .header(CONTENT_TYPE, APPLICATION_JSON);

        let response = if let Some(token) = &self.bearer_token {
            response.bearer_auth(token)
        } else {
            response
        };

        let response = response
            .body(req_string)
            .send()
            .map_err(ClientError::Transport)?;

        let opid = response
            .headers()
            .get(KOPID)
            .and_then(|hv| hv.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "missing_kopid".to_string());
        debug!("opid -> {:?}", opid);

        match response.status() {
            reqwest::StatusCode::OK => {}
            unexpect => return Err(ClientError::Http(unexpect, response.json().ok(), opid)),
        }

        response
            .json()
            .map_err(|e| ClientError::JSONDecode(e, opid))
    }

    fn perform_put_request<R: Serialize, T: DeserializeOwned>(
        &self,
        dest: &str,
        request: R,
    ) -> Result<T, ClientError> {
        let dest = format!("{}{}", self.addr, dest);

        let req_string = serde_json::to_string(&request).map_err(ClientError::JSONEncode)?;

        let response = self
            .client
            .put(dest.as_str())
            .header(CONTENT_TYPE, APPLICATION_JSON);

        let response = if let Some(token) = &self.bearer_token {
            response.bearer_auth(token)
        } else {
            response
        };

        let response = response
            .body(req_string)
            .send()
            .map_err(ClientError::Transport)?;

        let opid = response
            .headers()
            .get(KOPID)
            .and_then(|hv| hv.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "missing_kopid".to_string());
        debug!("opid -> {:?}", opid);

        match response.status() {
            reqwest::StatusCode::OK => {}
            unexpect => return Err(ClientError::Http(unexpect, response.json().ok(), opid)),
        }

        response
            .json()
            .map_err(|e| ClientError::JSONDecode(e, opid))
    }

    fn perform_get_request<T: DeserializeOwned>(&self, dest: &str) -> Result<T, ClientError> {
        let dest = format!("{}{}", self.addr, dest);
        let response = self.client.get(dest.as_str());

        let response = if let Some(token) = &self.bearer_token {
            response.bearer_auth(token)
        } else {
            response
        };

        let response = response.send().map_err(ClientError::Transport)?;

        let opid = response
            .headers()
            .get(KOPID)
            .and_then(|hv| hv.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "missing_kopid".to_string());
        debug!("opid -> {:?}", opid);

        match response.status() {
            reqwest::StatusCode::OK => {}
            unexpect => return Err(ClientError::Http(unexpect, response.json().ok(), opid)),
        }

        response
            .json()
            .map_err(|e| ClientError::JSONDecode(e, opid))
    }

    fn perform_delete_request(&self, dest: &str) -> Result<bool, ClientError> {
        let dest = format!("{}{}", self.addr, dest);
        let response = self.client.delete(dest.as_str());
        let response = if let Some(token) = &self.bearer_token {
            response.bearer_auth(token)
        } else {
            response
        };

        let response = response.send().map_err(ClientError::Transport)?;

        let opid = response
            .headers()
            .get(KOPID)
            .and_then(|hv| hv.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "missing_kopid".to_string());
        debug!("opid -> {:?}", opid);

        match response.status() {
            reqwest::StatusCode::OK => {}
            unexpect => return Err(ClientError::Http(unexpect, response.json().ok(), opid)),
        }

        response
            .json()
            .map_err(|e| ClientError::JSONDecode(e, opid))
    }

    // whoami
    // Can't use generic get due to possible un-auth case.
    pub fn whoami(&self) -> Result<Option<(Entry, UserAuthToken)>, ClientError> {
        let whoami_dest = format!("{}/v1/self", self.addr);
        let response = self.client.get(whoami_dest.as_str());

        let response = if let Some(token) = &self.bearer_token {
            response.bearer_auth(token)
        } else {
            response
        };

        let response = response.send().map_err(ClientError::Transport)?;

        let opid = response
            .headers()
            .get(KOPID)
            .and_then(|hv| hv.to_str().ok().map(|s| s.to_string()))
            .unwrap_or_else(|| "missing_kopid".to_string());
        debug!("opid -> {:?}", opid);

        match response.status() {
            // Continue to process.
            reqwest::StatusCode::OK => {}
            reqwest::StatusCode::UNAUTHORIZED => return Ok(None),
            unexpect => return Err(ClientError::Http(unexpect, response.json().ok(), opid)),
        }

        let r: WhoamiResponse = response
            .json()
            .map_err(|e| ClientError::JSONDecode(e, opid))?;

        Ok(Some((r.youare, r.uat)))
    }

    // auth
    pub fn auth_step_anonymous(&mut self) -> Result<AuthResponse, ClientError> {
        let auth_anon = AuthRequest {
            step: AuthStep::Cred(AuthCredential::Anonymous),
        };
        let r: Result<AuthResponse, _> = self.perform_post_request("/v1/auth", auth_anon);

        r.map(|ar| {
            if let AuthState::Success(token) = &ar.state {
                self.bearer_token = Some(token.clone());
            };
            ar
        })
    }

    pub fn auth_step_password(&mut self, password: &str) -> Result<AuthResponse, ClientError> {
        let auth_req = AuthRequest {
            step: AuthStep::Cred(AuthCredential::Password(password.to_string())),
        };
        let r: Result<AuthResponse, _> = self.perform_post_request("/v1/auth", auth_req);

        r.map(|ar| {
            if let AuthState::Success(token) = &ar.state {
                self.bearer_token = Some(token.clone());
            };
            ar
        })
    }

    pub fn auth_step_totp(&mut self, totp: u32) -> Result<AuthResponse, ClientError> {
        let auth_req = AuthRequest {
            step: AuthStep::Cred(AuthCredential::TOTP(totp)),
        };
        let r: Result<AuthResponse, _> = self.perform_post_request("/v1/auth", auth_req);

        r.map(|ar| {
            if let AuthState::Success(token) = &ar.state {
                self.bearer_token = Some(token.clone());
            };
            ar
        })
    }

    pub fn auth_step_webauthn_complete(
        &mut self,
        pkc: PublicKeyCredential,
    ) -> Result<AuthResponse, ClientError> {
        let auth_req = AuthRequest {
            step: AuthStep::Cred(AuthCredential::Webauthn(pkc)),
        };
        let r: Result<AuthResponse, _> = self.perform_post_request("/v1/auth", auth_req);

        r.map(|ar| {
            if let AuthState::Success(token) = &ar.state {
                self.bearer_token = Some(token.clone());
            };
            ar
        })
    }

    pub fn auth_anonymous(&mut self) -> Result<(), ClientError> {
        let mechs = match self.auth_step_init("anonymous") {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        if !mechs.contains(&AuthMech::Anonymous) {
            debug!("Anonymous mech not presented");
            return Err(ClientError::AuthenticationFailed);
        }

        let _state = match self.auth_step_begin(AuthMech::Anonymous) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        let r = self.auth_step_anonymous()?;

        match r.state {
            AuthState::Success(_token) => Ok(()),
            _ => Err(ClientError::AuthenticationFailed),
        }
    }

    pub fn auth_simple_password(&mut self, ident: &str, password: &str) -> Result<(), ClientError> {
        let mechs = match self.auth_step_init(ident) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        if !mechs.contains(&AuthMech::Password) {
            debug!("Password mech not presented");
            return Err(ClientError::AuthenticationFailed);
        }

        let _state = match self.auth_step_begin(AuthMech::Password) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        let r = self.auth_step_password(password)?;

        match r.state {
            AuthState::Success(_token) => Ok(()),
            _ => Err(ClientError::AuthenticationFailed),
        }
    }

    pub fn auth_password_totp(
        &mut self,
        ident: &str,
        password: &str,
        totp: u32,
    ) -> Result<(), ClientError> {
        let mechs = match self.auth_step_init(ident) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        if !mechs.contains(&AuthMech::PasswordMFA) {
            debug!("PasswordMFA mech not presented");
            return Err(ClientError::AuthenticationFailed);
        }

        let state = match self.auth_step_begin(AuthMech::PasswordMFA) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        if !state.contains(&AuthAllowed::TOTP) {
            debug!("TOTP step not offered.");
            return Err(ClientError::AuthenticationFailed);
        }

        let r = self.auth_step_totp(totp)?;

        // Should need to continue.
        match r.state {
            AuthState::Continue(allowed) => {
                if !allowed.contains(&AuthAllowed::Password) {
                    debug!("Password step not offered.");
                    return Err(ClientError::AuthenticationFailed);
                }
            }
            _ => {
                debug!("Invalid AuthState presented.");
                return Err(ClientError::AuthenticationFailed);
            }
        };

        let r = self.auth_step_password(password)?;

        match r.state {
            AuthState::Success(_token) => Ok(()),
            _ => Err(ClientError::AuthenticationFailed),
        }
    }

    pub fn auth_webauthn_begin(
        &mut self,
        ident: &str,
    ) -> Result<RequestChallengeResponse, ClientError> {
        let mechs = match self.auth_step_init(ident) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        if !mechs.contains(&AuthMech::Webauthn) {
            debug!("Webauthn mech not presented");
            return Err(ClientError::AuthenticationFailed);
        }

        let mut state = match self.auth_step_begin(AuthMech::Webauthn) {
            Ok(s) => s,
            Err(e) => return Err(e),
        };

        // State is now a set of auth continues.
        match state.pop() {
            Some(AuthAllowed::Webauthn(r)) => Ok(r),
            _ => Err(ClientError::AuthenticationFailed),
        }
    }

    pub fn auth_webauthn_complete(&mut self, pkc: PublicKeyCredential) -> Result<(), ClientError> {
        let r = self.auth_step_webauthn_complete(pkc)?;
        match r.state {
            AuthState::Success(_token) => Ok(()),
            _ => Err(ClientError::AuthenticationFailed),
        }
    }

    // search
    pub fn search(&self, filter: Filter) -> Result<Vec<Entry>, ClientError> {
        let sr = SearchRequest { filter };
        let r: Result<SearchResponse, _> = self.perform_post_request("/v1/raw/search", sr);
        r.map(|v| v.entries)
    }

    // create
    pub fn create(&self, entries: Vec<Entry>) -> Result<bool, ClientError> {
        let c = CreateRequest { entries };
        let r: Result<OperationResponse, _> = self.perform_post_request("/v1/raw/create", c);
        r.map(|_| true)
    }

    // modify
    pub fn modify(&self, filter: Filter, modlist: ModifyList) -> Result<bool, ClientError> {
        let mr = ModifyRequest { filter, modlist };
        let r: Result<OperationResponse, _> = self.perform_post_request("/v1/raw/modify", mr);
        r.map(|_| true)
    }

    // delete
    pub fn delete(&self, filter: Filter) -> Result<bool, ClientError> {
        let dr = DeleteRequest { filter };
        let r: Result<OperationResponse, _> = self.perform_post_request("/v1/raw/delete", dr);
        r.map(|_| true)
    }

    // === idm actions here ==
    pub fn idm_account_set_password(&self, cleartext: String) -> Result<bool, ClientError> {
        let s = SingleStringRequest { value: cleartext };

        let r: Result<OperationResponse, _> =
            self.perform_post_request("/v1/self/_credential/primary/set_password", s);
        r.map(|_| true)
    }

    pub fn auth_step_init(&self, ident: &str) -> Result<Set<AuthMech>, ClientError> {
        let auth_init = AuthRequest {
            step: AuthStep::Init(ident.to_string()),
        };

        let r: Result<AuthResponse, _> = self.perform_post_request("/v1/auth", auth_init);
        r.map(|v| {
            debug!("Authentication Session ID -> {:?}", v.sessionid);
            v.state
        })
        .and_then(|state| match state {
            AuthState::Choose(mechs) => Ok(mechs),
            _ => Err(ClientError::AuthenticationFailed),
        })
        .map(|mechs| mechs.into_iter().collect())
    }

    pub fn auth_step_begin(&self, mech: AuthMech) -> Result<Vec<AuthAllowed>, ClientError> {
        let auth_begin = AuthRequest {
            step: AuthStep::Begin(mech),
        };

        let r: Result<AuthResponse, _> = self.perform_post_request("/v1/auth", auth_begin);
        r.map(|v| {
            debug!("Authentication Session ID -> {:?}", v.sessionid);
            v.state
        })
        .and_then(|state| match state {
            AuthState::Continue(allowed) => Ok(allowed),
            _ => Err(ClientError::AuthenticationFailed),
        })
        // For converting to a Set
        // .map(|allowed| allowed.into_iter().collect())
    }

    // ===== GROUPS
    pub fn idm_group_list(&self) -> Result<Vec<Entry>, ClientError> {
        self.perform_get_request("/v1/group")
    }

    pub fn idm_group_get(&self, id: &str) -> Result<Option<Entry>, ClientError> {
        self.perform_get_request(format!("/v1/group/{}", id).as_str())
    }

    pub fn idm_group_get_members(&self, id: &str) -> Result<Option<Vec<String>>, ClientError> {
        self.perform_get_request(format!("/v1/group/{}/_attr/member", id).as_str())
    }

    pub fn idm_group_set_members(&self, id: &str, members: &[&str]) -> Result<bool, ClientError> {
        let m: Vec<_> = members.iter().map(|v| (*v).to_string()).collect();
        self.perform_put_request(format!("/v1/group/{}/_attr/member", id).as_str(), m)
    }

    pub fn idm_group_add_members(&self, id: &str, members: &[&str]) -> Result<bool, ClientError> {
        let m: Vec<_> = members.iter().map(|v| (*v).to_string()).collect();
        self.perform_post_request(format!("/v1/group/{}/_attr/member", id).as_str(), m)
    }

    /*
    pub fn idm_group_remove_member(&self, id: &str, member: &str) -> Result<(), ClientError> {
        unimplemented!();
    }
    */

    pub fn idm_group_purge_members(&self, id: &str) -> Result<bool, ClientError> {
        self.perform_delete_request(format!("/v1/group/{}/_attr/member", id).as_str())
    }

    pub fn idm_group_unix_token_get(&self, id: &str) -> Result<UnixGroupToken, ClientError> {
        self.perform_get_request(format!("/v1/group/{}/_unix/_token", id).as_str())
    }

    pub fn idm_group_unix_extend(
        &self,
        id: &str,
        gidnumber: Option<u32>,
    ) -> Result<bool, ClientError> {
        let gx = GroupUnixExtend { gidnumber };
        self.perform_post_request(format!("/v1/group/{}/_unix", id).as_str(), gx)
    }

    pub fn idm_group_delete(&self, id: &str) -> Result<bool, ClientError> {
        self.perform_delete_request(format!("/v1/group/{}", id).as_str())
    }

    pub fn idm_group_create(&self, name: &str) -> Result<bool, ClientError> {
        let mut new_group = Entry {
            attrs: BTreeMap::new(),
        };
        new_group
            .attrs
            .insert("name".to_string(), vec![name.to_string()]);
        self.perform_post_request("/v1/group", new_group)
            .map(|_: OperationResponse| true)
    }

    // ==== accounts
    pub fn idm_account_list(&self) -> Result<Vec<Entry>, ClientError> {
        self.perform_get_request("/v1/account")
    }

    pub fn idm_account_create(&self, name: &str, dn: &str) -> Result<bool, ClientError> {
        let mut new_acct = Entry {
            attrs: BTreeMap::new(),
        };
        new_acct
            .attrs
            .insert("name".to_string(), vec![name.to_string()]);
        new_acct
            .attrs
            .insert("displayname".to_string(), vec![dn.to_string()]);
        self.perform_post_request("/v1/account", new_acct)
            .map(|_: OperationResponse| true)
    }

    pub fn idm_account_set_displayname(&self, id: &str, dn: &str) -> Result<bool, ClientError> {
        self.perform_put_request(
            format!("/v1/account/{}/_attr/displayname", id).as_str(),
            vec![dn.to_string()],
        )
    }

    pub fn idm_account_delete(&self, id: &str) -> Result<bool, ClientError> {
        self.perform_delete_request(format!("/v1/account/{}", id).as_str())
    }

    pub fn idm_account_get(&self, id: &str) -> Result<Option<Entry>, ClientError> {
        self.perform_get_request(format!("/v1/account/{}", id).as_str())
    }

    // different ways to set the primary credential?
    // not sure how to best expose this.
    pub fn idm_account_primary_credential_set_password(
        &self,
        id: &str,
        pw: &str,
    ) -> Result<SetCredentialResponse, ClientError> {
        let r = SetCredentialRequest::Password(pw.to_string());
        self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        )
    }

    pub fn idm_account_get_attr(
        &self,
        id: &str,
        attr: &str,
    ) -> Result<Option<Vec<String>>, ClientError> {
        self.perform_get_request(format!("/v1/account/{}/_attr/{}", id, attr).as_str())
    }

    pub fn idm_account_purge_attr(&self, id: &str, attr: &str) -> Result<bool, ClientError> {
        self.perform_delete_request(format!("/v1/account/{}/_attr/{}", id, attr).as_str())
    }

    pub fn idm_account_set_attr(
        &self,
        id: &str,
        attr: &str,
        values: &[&str],
    ) -> Result<bool, ClientError> {
        let m: Vec<_> = values.iter().map(|v| (*v).to_string()).collect();
        self.perform_put_request(format!("/v1/account/{}/_attr/{}", id, attr).as_str(), m)
    }

    pub fn idm_account_primary_credential_import_password(
        &self,
        id: &str,
        pw: &str,
    ) -> Result<bool, ClientError> {
        self.perform_put_request(
            format!("/v1/account/{}/_attr/password_import", id).as_str(),
            vec![pw.to_string()],
        )
    }

    pub fn idm_account_primary_credential_set_generated(
        &self,
        id: &str,
    ) -> Result<String, ClientError> {
        let r = SetCredentialRequest::GeneratePassword;
        let res: Result<SetCredentialResponse, ClientError> = self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        );
        match res {
            Ok(SetCredentialResponse::Token(p)) => Ok(p),
            Ok(_) => Err(ClientError::EmptyResponse),
            Err(e) => Err(e),
        }
    }

    // Reg intent for totp
    pub fn idm_account_primary_credential_generate_totp(
        &self,
        id: &str,
        label: &str,
    ) -> Result<(Uuid, TOTPSecret), ClientError> {
        let r = SetCredentialRequest::TOTPGenerate(label.to_string());
        let res: Result<SetCredentialResponse, ClientError> = self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        );
        match res {
            Ok(SetCredentialResponse::TOTPCheck(u, s)) => Ok((u, s)),
            Ok(_) => Err(ClientError::EmptyResponse),
            Err(e) => Err(e),
        }
    }

    // Verify the totp
    pub fn idm_account_primary_credential_verify_totp(
        &self,
        id: &str,
        otp: u32,
        session: Uuid,
    ) -> Result<bool, ClientError> {
        let r = SetCredentialRequest::TOTPVerify(session, otp);
        let res: Result<SetCredentialResponse, ClientError> = self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        );
        match res {
            Ok(SetCredentialResponse::Success) => Ok(true),
            Ok(SetCredentialResponse::TOTPCheck(u, s)) => Err(ClientError::TOTPVerifyFailed(u, s)),
            Ok(_) => Err(ClientError::EmptyResponse),
            Err(e) => Err(e),
        }
    }

    pub fn idm_account_primary_credential_remove_totp(
        &self,
        id: &str,
    ) -> Result<bool, ClientError> {
        let r = SetCredentialRequest::TOTPRemove;
        let res: Result<SetCredentialResponse, ClientError> = self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        );
        match res {
            Ok(SetCredentialResponse::Success) => Ok(true),
            Ok(_) => Err(ClientError::EmptyResponse),
            Err(e) => Err(e),
        }
    }

    pub fn idm_account_primary_credential_register_webauthn(
        &self,
        id: &str,
        label: &str,
    ) -> Result<(Uuid, CreationChallengeResponse), ClientError> {
        let r = SetCredentialRequest::WebauthnBegin(label.to_string());
        let res: Result<SetCredentialResponse, ClientError> = self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        );
        match res {
            Ok(SetCredentialResponse::WebauthnCreateChallenge(u, s)) => Ok((u, s)),
            Ok(_) => Err(ClientError::EmptyResponse),
            Err(e) => Err(e),
        }
    }

    pub fn idm_account_primary_credential_complete_webuthn_registration(
        &self,
        id: &str,
        rego: RegisterPublicKeyCredential,
        session: Uuid,
    ) -> Result<(), ClientError> {
        let r = SetCredentialRequest::WebauthnRegister(session, rego);
        let res: Result<SetCredentialResponse, ClientError> = self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        );
        match res {
            Ok(SetCredentialResponse::Success) => Ok(()),
            Ok(_) => Err(ClientError::EmptyResponse),
            Err(e) => Err(e),
        }
    }

    pub fn idm_account_primary_credential_remove_webauthn(
        &self,
        id: &str,
        label: &str,
    ) -> Result<bool, ClientError> {
        let r = SetCredentialRequest::WebauthnRemove(label.to_string());
        let res: Result<SetCredentialResponse, ClientError> = self.perform_put_request(
            format!("/v1/account/{}/_credential/primary", id).as_str(),
            r,
        );
        match res {
            Ok(SetCredentialResponse::Success) => Ok(true),
            Ok(_) => Err(ClientError::EmptyResponse),
            Err(e) => Err(e),
        }
    }

    pub fn idm_account_radius_credential_get(
        &self,
        id: &str,
    ) -> Result<Option<String>, ClientError> {
        self.perform_get_request(format!("/v1/account/{}/_radius", id).as_str())
    }

    pub fn idm_account_radius_credential_regenerate(
        &self,
        id: &str,
    ) -> Result<String, ClientError> {
        self.perform_post_request(format!("/v1/account/{}/_radius", id).as_str(), ())
    }

    pub fn idm_account_radius_credential_delete(&self, id: &str) -> Result<bool, ClientError> {
        self.perform_delete_request(format!("/v1/account/{}/_radius", id).as_str())
    }

    pub fn idm_account_radius_token_get(&self, id: &str) -> Result<RadiusAuthToken, ClientError> {
        self.perform_get_request(format!("/v1/account/{}/_radius/_token", id).as_str())
    }

    pub fn idm_account_unix_extend(
        &self,
        id: &str,
        gidnumber: Option<u32>,
        shell: Option<&str>,
    ) -> Result<bool, ClientError> {
        let ux = AccountUnixExtend {
            shell: shell.map(|s| s.to_string()),
            gidnumber,
        };
        self.perform_post_request(format!("/v1/account/{}/_unix", id).as_str(), ux)
    }

    pub fn idm_account_unix_token_get(&self, id: &str) -> Result<UnixUserToken, ClientError> {
        self.perform_get_request(format!("/v1/account/{}/_unix/_token", id).as_str())
    }

    pub fn idm_account_unix_cred_put(&self, id: &str, cred: &str) -> Result<bool, ClientError> {
        let req = SingleStringRequest {
            value: cred.to_string(),
        };
        self.perform_put_request(
            format!("/v1/account/{}/_unix/_credential", id).as_str(),
            req,
        )
    }

    pub fn idm_account_unix_cred_delete(&self, id: &str) -> Result<bool, ClientError> {
        self.perform_delete_request(format!("/v1/account/{}/_unix/_credential", id).as_str())
    }

    pub fn idm_account_unix_cred_verify(
        &self,
        id: &str,
        cred: &str,
    ) -> Result<Option<UnixUserToken>, ClientError> {
        let req = SingleStringRequest {
            value: cred.to_string(),
        };
        self.perform_post_request(format!("/v1/account/{}/_unix/_auth", id).as_str(), req)
    }

    pub fn idm_account_get_ssh_pubkeys(&self, id: &str) -> Result<Vec<String>, ClientError> {
        self.perform_get_request(format!("/v1/account/{}/_ssh_pubkeys", id).as_str())
    }

    pub fn idm_account_post_ssh_pubkey(
        &self,
        id: &str,
        tag: &str,
        pubkey: &str,
    ) -> Result<bool, ClientError> {
        let sk = (tag.to_string(), pubkey.to_string());
        self.perform_post_request(format!("/v1/account/{}/_ssh_pubkeys", id).as_str(), sk)
    }

    pub fn idm_account_person_extend(&self, id: &str) -> Result<bool, ClientError> {
        self.perform_post_request(format!("/v1/account/{}/_person/_extend", id).as_str(), ())
    }

    /*
    pub fn idm_account_rename_ssh_pubkey(&self, id: &str, oldtag: &str, newtag: &str) -> Result<(), ClientError> {
        self.perform_put_request(format!("/v1/account/{}/_ssh_pubkeys/{}", id, oldtag).as_str(), newtag.to_string())
    }
    */

    pub fn idm_account_get_ssh_pubkey(
        &self,
        id: &str,
        tag: &str,
    ) -> Result<Option<String>, ClientError> {
        self.perform_get_request(format!("/v1/account/{}/_ssh_pubkeys/{}", id, tag).as_str())
    }

    pub fn idm_account_delete_ssh_pubkey(&self, id: &str, tag: &str) -> Result<bool, ClientError> {
        self.perform_delete_request(format!("/v1/account/{}/_ssh_pubkeys/{}", id, tag).as_str())
    }

    // ==== domain_info (aka domain)
    pub fn idm_domain_list(&self) -> Result<Vec<Entry>, ClientError> {
        self.perform_get_request("/v1/domain")
    }

    pub fn idm_domain_get(&self, id: &str) -> Result<Entry, ClientError> {
        self.perform_get_request(format!("/v1/domain/{}", id).as_str())
    }

    // pub fn idm_domain_get_attr
    pub fn idm_domain_get_ssid(&self, id: &str) -> Result<String, ClientError> {
        self.perform_get_request(format!("/v1/domain/{}/_attr/domain_ssid", id).as_str())
            .and_then(|mut r: Vec<String>|
                // Get the first result
                r.pop()
                .ok_or(
                    ClientError::EmptyResponse
                ))
    }

    // pub fn idm_domain_put_attr
    pub fn idm_domain_set_ssid(&self, id: &str, ssid: &str) -> Result<bool, ClientError> {
        self.perform_put_request(
            format!("/v1/domain/{}/_attr/domain_ssid", id).as_str(),
            vec![ssid.to_string()],
        )
    }

    // ==== schema
    pub fn idm_schema_list(&self) -> Result<Vec<Entry>, ClientError> {
        self.perform_get_request("/v1/schema")
    }

    pub fn idm_schema_attributetype_list(&self) -> Result<Vec<Entry>, ClientError> {
        self.perform_get_request("/v1/schema/attributetype")
    }

    pub fn idm_schema_attributetype_get(&self, id: &str) -> Result<Option<Entry>, ClientError> {
        self.perform_get_request(format!("/v1/schema/attributetype/{}", id).as_str())
    }

    pub fn idm_schema_classtype_list(&self) -> Result<Vec<Entry>, ClientError> {
        self.perform_get_request("/v1/schema/classtype")
    }

    pub fn idm_schema_classtype_get(&self, id: &str) -> Result<Option<Entry>, ClientError> {
        self.perform_get_request(format!("/v1/schema/classtype/{}", id).as_str())
    }

    // ==== recycle bin
    pub fn recycle_bin_list(&self) -> Result<Vec<Entry>, ClientError> {
        self.perform_get_request("/v1/recycle_bin")
    }

    pub fn recycle_bin_get(&self, id: &str) -> Result<Option<Entry>, ClientError> {
        self.perform_get_request(format!("/v1/recycle_bin/{}", id).as_str())
    }

    pub fn recycle_bin_revive(&self, id: &str) -> Result<bool, ClientError> {
        self.perform_post_request(format!("/v1/recycle_bin/{}/_revive", id).as_str(), ())
    }
}
