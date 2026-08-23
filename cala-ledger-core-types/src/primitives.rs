use rusty_money::{crypto, iso};
use serde::{Deserialize, Serialize};

use cel_interpreter::{CelResult, CelType, CelValue, ResultCoercionError};

es_entity::entity_id! { AccountId }
impl From<AccountId> for cel_interpreter::CelValue {
    fn from(id: AccountId) -> Self {
        cel_interpreter::CelValue::Uuid(id.0)
    }
}
es_entity::entity_id! { AccountSetId }
impl From<AccountSetId> for cel_interpreter::CelValue {
    fn from(id: AccountSetId) -> Self {
        cel_interpreter::CelValue::Uuid(id.0)
    }
}
es_entity::entity_id! { JournalId }
impl From<JournalId> for cel_interpreter::CelValue {
    fn from(id: JournalId) -> Self {
        cel_interpreter::CelValue::Uuid(id.0)
    }
}
es_entity::entity_id! { TxTemplateId }
impl From<TxTemplateId> for cel_interpreter::CelValue {
    fn from(id: TxTemplateId) -> Self {
        cel_interpreter::CelValue::Uuid(id.0)
    }
}
es_entity::entity_id! { TransactionId }
impl From<TransactionId> for cel_interpreter::CelValue {
    fn from(id: TransactionId) -> Self {
        cel_interpreter::CelValue::Uuid(id.0)
    }
}
es_entity::entity_id! { EntryId }
impl From<EntryId> for cel_interpreter::CelValue {
    fn from(id: EntryId) -> Self {
        cel_interpreter::CelValue::Uuid(id.0)
    }
}
es_entity::entity_id! { VelocityLimitId }
es_entity::entity_id! { VelocityControlId }

pub type BalanceId = (JournalId, AccountId, Currency);
impl From<&AccountSetId> for AccountId {
    fn from(id: &AccountSetId) -> Self {
        Self(id.0)
    }
}
impl From<AccountSetId> for AccountId {
    fn from(id: AccountSetId) -> Self {
        Self(id.0)
    }
}

#[derive(
    Default,
    Debug,
    Serialize,
    Deserialize,
    Clone,
    Copy,
    PartialEq,
    Eq,
    sqlx::Type,
    strum::Display,
    strum::EnumString,
)]
#[sqlx(type_name = "DebitOrCredit", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum DebitOrCredit {
    Debit,
    #[default]
    Credit,
}

impl TryFrom<CelResult<'_>> for DebitOrCredit {
    type Error = ResultCoercionError;

    fn try_from(CelResult { expr, val }: CelResult) -> Result<Self, Self::Error> {
        match val {
            CelValue::String(v) if v.as_ref() == "DEBIT" => Ok(DebitOrCredit::Debit),
            CelValue::String(v) if v.as_ref() == "CREDIT" => Ok(DebitOrCredit::Credit),
            v => Err(ResultCoercionError::BadExternalTypeCoercion(
                format!("{expr:?}"),
                CelType::from(&v),
                "DebitOrCredit",
            )),
        }
    }
}

impl From<DebitOrCredit> for CelValue {
    fn from(v: DebitOrCredit) -> Self {
        match v {
            DebitOrCredit::Debit => "DEBIT".into(),
            DebitOrCredit::Credit => "CREDIT".into(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BalanceRollup {
    /// Rolled up inside every posting to a member account, under an
    /// exclusive lock per (journal, set, currency).
    Synchronous,
    /// Skipped at posting time; refreshed by recalculating the sets
    /// returned from `list_eventually_consistent_ids`.
    EventuallyConsistent,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "Status", rename_all = "snake_case")]
#[serde(rename_all = "snake_case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum Status {
    #[default]
    Active,
    Locked,
}

#[derive(Default, Debug, Serialize, Deserialize, Clone, Copy, PartialEq, Eq, Hash, sqlx::Type)]
#[sqlx(type_name = "Layer", rename_all = "snake_case")]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum Layer {
    #[default]
    Settled,
    Pending,
    Encumbrance,
}

#[derive(thiserror::Error, Debug)]
pub enum ParseLayerError {
    #[error("CalaCoreTypeError - UnknownLayer: {0:?}")]
    UnknownLayer(String),
}

impl TryFrom<CelResult<'_>> for Layer {
    type Error = ResultCoercionError;

    fn try_from(CelResult { expr, val }: CelResult) -> Result<Self, Self::Error> {
        match val {
            CelValue::String(v) if v.as_ref() == "SETTLED" => Ok(Layer::Settled),
            CelValue::String(v) if v.as_ref() == "PENDING" => Ok(Layer::Pending),
            CelValue::String(v) if v.as_ref() == "ENCUMBRANCE" => Ok(Layer::Encumbrance),
            v => Err(ResultCoercionError::BadExternalTypeCoercion(
                format!("{expr:?}"),
                CelType::from(&v),
                "Layer",
            )),
        }
    }
}

impl From<Layer> for CelValue {
    fn from(l: Layer) -> Self {
        match l {
            Layer::Settled => "SETTLED".into(),
            Layer::Pending => "PENDING".into(),
            Layer::Encumbrance => "ENCUMBRANCE".into(),
        }
    }
}

const MAX_CUSTOM_UNIT_CODE_LEN: usize = 64;

/// A validated caller-defined conserved-unit code stored entirely inline.
///
/// Inline storage keeps [`Currency`] `Copy` without process-global interning,
/// allocation leaks, mutable registries, or parse-order-dependent failures.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct CustomUnitCode {
    length: u8,
    bytes: [u8; MAX_CUSTOM_UNIT_CODE_LEN],
}

impl CustomUnitCode {
    fn parse(code: &str) -> Result<Self, ParseCurrencyError> {
        let valid = !code.is_empty()
            && code.len() <= MAX_CUSTOM_UNIT_CODE_LEN
            && code.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
            });
        if !valid {
            return Err(ParseCurrencyError::InvalidUnitCode(code.to_owned()));
        }

        let mut bytes = [0; MAX_CUSTOM_UNIT_CODE_LEN];
        bytes[..code.len()].copy_from_slice(code.as_bytes());
        Ok(Self {
            length: u8::try_from(code.len()).expect("validated custom-unit length fits u8"),
            bytes,
        })
    }

    pub fn as_str(&self) -> &str {
        // Construction admits ASCII bytes only, and ASCII is valid UTF-8.
        std::str::from_utf8(&self.bytes[..usize::from(self.length)])
            .expect("validated custom-unit code must remain UTF-8")
    }
}

impl std::fmt::Display for CustomUnitCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, Eq)]
pub enum Currency {
    Iso(&'static iso::Currency),
    Crypto(&'static crypto::Currency),
    /// A caller-defined conserved unit held inline with no global registry.
    Custom(CustomUnitCode),
}

impl Currency {
    pub const BTC: Self = Self::Crypto(crypto::BTC);
    pub const USD: Self = Self::Iso(iso::USD);

    pub fn code(&self) -> &str {
        match self {
            Currency::Iso(c) => c.iso_alpha_code,
            Currency::Crypto(c) => c.code,
            Currency::Custom(code) => code.as_str(),
        }
    }

    fn parse_custom(code: &str) -> Result<Self, ParseCurrencyError> {
        CustomUnitCode::parse(code).map(Self::Custom)
    }
}

impl std::fmt::Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code())
    }
}

impl AsRef<str> for Currency {
    fn as_ref(&self) -> &str {
        self.code()
    }
}

#[cfg(feature = "json-schema")]
impl schemars::JsonSchema for Currency {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Currency".into()
    }

    fn schema_id() -> std::borrow::Cow<'static, str> {
        concat!(module_path!(), "::Currency").into()
    }

    fn json_schema(_generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        schemars::json_schema!({
            "type": "string",
            "minLength": 1,
            "maxLength": MAX_CUSTOM_UNIT_CODE_LEN,
            "pattern": "^[A-Za-z0-9_.:/-]+$"
        })
    }
}

impl Serialize for Currency {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(self.code())
    }
}

impl<'de> Deserialize<'de> for Currency {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let code = String::deserialize(deserializer)?;
        code.parse().map_err(serde::de::Error::custom)
    }
}

impl From<Currency> for CelValue {
    fn from(c: Currency) -> Self {
        c.code().to_owned().into()
    }
}

impl std::hash::Hash for Currency {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.code().hash(state);
    }
}

impl PartialEq for Currency {
    fn eq(&self, other: &Self) -> bool {
        self.code() == other.code()
    }
}

impl Ord for Currency {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.code().cmp(other.code())
    }
}

impl PartialOrd for Currency {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(thiserror::Error, Debug)]
pub enum ParseCurrencyError {
    #[error("CalaCoreTypeError - InvalidUnitCode: {0}")]
    InvalidUnitCode(String),
}

impl std::str::FromStr for Currency {
    type Err = ParseCurrencyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match iso::find(s) {
            Some(c) => Ok(Currency::Iso(c)),
            _ => match crypto::find(s) {
                Some(c) => Ok(Currency::Crypto(c)),
                _ => {
                    // Known monetary codes are case-insensitive aliases and
                    // always normalize to the canonical registry spelling.
                    // This prevents `usd` and `USD` from becoming distinct
                    // conserved units while custom codes remain case-sensitive.
                    let canonical = s.to_ascii_uppercase();
                    match iso::find(&canonical) {
                        Some(c) => Ok(Currency::Iso(c)),
                        None => match crypto::find(&canonical) {
                            Some(c) => Ok(Currency::Crypto(c)),
                            None => Currency::parse_custom(s),
                        },
                    }
                }
            },
        }
    }
}

impl TryFrom<String> for Currency {
    type Error = ParseCurrencyError;

    fn try_from(s: String) -> Result<Self, Self::Error> {
        s.parse()
    }
}

impl From<Currency> for String {
    fn from(c: Currency) -> Self {
        c.code().to_owned()
    }
}

impl TryFrom<CelResult<'_>> for Currency {
    type Error = ResultCoercionError;

    fn try_from(CelResult { expr, val }: CelResult) -> Result<Self, Self::Error> {
        match val {
            CelValue::String(v) => v.as_ref().parse::<Currency>().map_err(|e| {
                ResultCoercionError::ExternalTypeCoercionError(
                    format!("{expr:?}"),
                    format!("{v:?}"),
                    "Currency",
                    format!("{e:?}"),
                )
            }),
            v => Err(ResultCoercionError::BadExternalTypeCoercion(
                format!("{expr:?}"),
                CelType::from(&v),
                "Currency",
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::primitives::Currency;

    fn assert_copy<T: Copy>() {}

    #[test]
    fn currency_constants() {
        assert_eq!(Currency::USD, "USD".parse().unwrap());
        assert_eq!(Currency::BTC, "BTC".parse().unwrap());
    }

    #[test]
    fn custom_unit_round_trips_through_json() {
        let unit: Currency = "XAU_FINE_G".parse().unwrap();
        assert_eq!(unit.code(), "XAU_FINE_G");

        let encoded = serde_json::to_string(&unit).unwrap();
        assert_eq!(encoded, "\"XAU_FINE_G\"");
        let decoded: Currency = serde_json::from_str(&encoded).unwrap();
        assert_eq!(decoded, unit);
    }

    #[test]
    fn custom_unit_rejects_unbounded_or_ambiguous_codes() {
        assert!("".parse::<Currency>().is_err());
        assert!("a unit with spaces".parse::<Currency>().is_err());
        assert!("x".repeat(65).parse::<Currency>().is_err());
    }

    #[test]
    fn custom_units_have_no_process_global_exhaustion_limit() {
        assert_copy::<Currency>();

        for index in 0..70_000_u32 {
            let code = format!("UNIT_{index}");
            let parsed: Currency = code.parse().expect("each inline unit must parse");
            assert_eq!(parsed.code(), code);
        }

        let persisted = "PERSISTED_AFTER_70000";
        let decoded: Currency = serde_json::from_str(&format!("\"{persisted}\""))
            .expect("persisted unit must remain decodable regardless of prior ingress");
        assert_eq!(decoded.code(), persisted);
    }

    #[test]
    fn lower_case_known_codes_normalize_instead_of_creating_custom_aliases() {
        let lower_iso: Currency = "usd".parse().unwrap();
        let lower_crypto: Currency = "btc".parse().unwrap();
        let lower_custom: Currency = "widgets".parse().unwrap();
        let upper_custom: Currency = "WIDGETS".parse().unwrap();

        assert_eq!(lower_iso, Currency::USD);
        assert_eq!(lower_iso.code(), "USD");
        assert_eq!(lower_crypto, Currency::BTC);
        assert_eq!(lower_crypto.code(), "BTC");
        assert_eq!(serde_json::to_string(&lower_iso).unwrap(), "\"USD\"");
        assert_eq!(lower_custom.code(), "widgets");
        assert_ne!(lower_custom, upper_custom);
    }
}
