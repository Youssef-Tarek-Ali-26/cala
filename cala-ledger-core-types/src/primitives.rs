use rusty_money::{crypto, iso};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    sync::{OnceLock, RwLock},
};

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

#[derive(Debug, Clone, Copy, Eq)]
#[cfg_attr(feature = "json-schema", derive(schemars::JsonSchema))]
pub enum Currency {
    Iso(&'static iso::Currency),
    Crypto(&'static crypto::Currency),
    /// A caller-defined conserved unit. Custom codes are process-interned so
    /// `Currency` stays `Copy`, preserving Cala's public API and balance-key
    /// representation while allowing non-monetary ledgers.
    Custom(&'static str),
}

impl Currency {
    const MAX_CUSTOM_CODES: usize = 65_536;

    pub const BTC: Self = Self::Crypto(crypto::BTC);
    pub const USD: Self = Self::Iso(iso::USD);

    pub fn code(&self) -> &'static str {
        match self {
            Currency::Iso(c) => c.iso_alpha_code,
            Currency::Crypto(c) => c.code,
            Currency::Custom(code) => code,
        }
    }

    fn custom_codes() -> &'static RwLock<HashMap<String, &'static str>> {
        static CUSTOM_CODES: OnceLock<RwLock<HashMap<String, &'static str>>> = OnceLock::new();
        CUSTOM_CODES.get_or_init(|| RwLock::new(HashMap::new()))
    }

    fn parse_custom(code: &str) -> Result<Self, ParseCurrencyError> {
        let valid = !code.is_empty()
            && code.len() <= 64
            && code.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.' | b':' | b'/')
            });
        if !valid {
            return Err(ParseCurrencyError::InvalidUnitCode(code.to_owned()));
        }

        let mut codes = Self::custom_codes()
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(interned) = codes.get(code) {
            return Ok(Self::Custom(interned));
        }
        if codes.len() >= Self::MAX_CUSTOM_CODES {
            return Err(ParseCurrencyError::CustomUnitRegistryFull);
        }

        let interned = Box::leak(code.to_owned().into_boxed_str());
        codes.insert(code.to_owned(), interned);
        Ok(Self::Custom(interned))
    }
}

impl std::fmt::Display for Currency {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.code())
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
        c.code().into()
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
    #[error("CalaCoreTypeError - CustomUnitRegistryFull")]
    CustomUnitRegistryFull,
}

impl std::str::FromStr for Currency {
    type Err = ParseCurrencyError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match iso::find(s) {
            Some(c) => Ok(Currency::Iso(c)),
            _ => match crypto::find(s) {
                Some(c) => Ok(Currency::Crypto(c)),
                _ => Currency::parse_custom(s),
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

impl From<Currency> for &'static str {
    fn from(c: Currency) -> Self {
        c.code()
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
}
