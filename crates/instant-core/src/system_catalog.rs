//! The system catalog: deterministic attrs ($users, $files, $oauth*, ...) shared
//! by every app, owned by a hardcoded "system catalog" app.
//! Port of LEGACY/server/src/instant/system_catalog.clj.

use uuid::Uuid;

use crate::attr::{Attr, Cardinality, CheckedDataType, ValueType};
use crate::error::Result;

pub const SYSTEM_CATALOG_APP_ID: Uuid = Uuid::from_u128(0xa1111111_1111_1111_1111_111111111ca7);
pub const SYSTEM_CATALOG_USER_ID: Uuid = Uuid::from_u128(0xe1111111_1111_1111_1111_111111111ca7);

const NAME_CHARS: &str = "abcdefghijklmnopqrstuvwxzy/";

/// Packs each char as 5 bits over NAME_CHARS, padding with 1-bits to 64.
fn encode_string_to_u64(input: &str) -> u64 {
    assert!(input.len() < 13, "encode input too long: {input}");
    let mut bits: u64 = 0;
    let mut nbits: u32 = 0;
    for c in input.chars() {
        let idx = NAME_CHARS
            .find(c)
            .unwrap_or_else(|| panic!("unsupported char {c:?} in {input:?}")) as u64;
        bits = (bits << 5) | idx;
        nbits += 5;
    }
    let pad = 64 - nbits;
    (bits << pad) | ((1u64 << pad) - 1)
}

fn etype_shortcode(etype: &str) -> Option<&'static str> {
    Some(match etype {
        "$users" => "us",
        "$magicCodes" => "mc",
        "$userRefreshTokens" => "ur",
        "$oauthProviders" => "op",
        "$oauthUserLinks" => "ol",
        "$oauthClients" => "oc",
        "$oauthCodes" => "co",
        "$oauthRedirects" => "or",
        "$files" => "fi",
        "$streams" => "st",
        _ => return None,
    })
}

fn label_shortcode(label: &str) -> Option<&'static str> {
    // etype shortcodes take priority (matches the `or` in encode-system-uuid).
    if let Some(s) = etype_shortcode(label) {
        return Some(s);
    }
    Some(match label {
        "$oauthClient" => "oauclient",
        "$oauthProvider" => "oprovider",
        "$stream" => "stream",
        "$user" => "user",
        "abortReason" => "abrtreasn",
        "authCode" => "authcode",
        "clientId" => "clientid",
        "codeChallenge" => "codechall",
        "codeChallengeMethod" => "cchalmeth",
        "codeHash" => "codehash",
        "content-disposition" => "cdisp",
        "content-type" => "ctype",
        "cookieHash" => "cookihash",
        "discoveryEndpoint" => "discovend",
        "done" => "done",
        "email" => "email",
        "encryptedClientSecret" => "encclisec",
        "hashedToken" => "hashedtok",
        "id" => "id",
        "imageURL" => "imageurl",
        "key-version" => "kv",
        "hashedReconnectToken" => "hashretok",
        "linkedGuestUsers" => "lgu",
        "linkedPrimaryUser" => "lpu",
        "location-id" => "lid",
        "machineId" => "machineid",
        "meta" => "meta",
        "name" => "name",
        "path" => "path",
        "redirectUrl" => "redireurl",
        "redirectTo" => "redirecto",
        "size" => "size",
        "stateHash" => "statehash",
        "sub" => "sub",
        "sub+$oauthProvider" => "subprovid",
        "type" => "type",
        "url" => "url",
        "useSharedCredentials" => "usesharcr",
        "userInfo" => "usernfo",
        _ => return None,
    })
}

fn encode_system_uuid(type_shortcode: &str, etype: &str, label: &str) -> Uuid {
    let se = etype_shortcode(etype).unwrap_or_else(|| panic!("missing etype shortcode {etype}"));
    let sl = label_shortcode(label).unwrap_or_else(|| panic!("missing label shortcode {label}"));
    let hi = encode_string_to_u64(&format!("system{type_shortcode}"));
    let lo = encode_string_to_u64(&format!("{se}/{sl}"));
    Uuid::from_u64_pair(hi, lo)
}

pub fn attr_id(etype: &str, label: &str) -> Uuid {
    encode_system_uuid("at", etype, label)
}

pub fn ident_id(etype: &str, label: &str) -> Uuid {
    encode_system_uuid("id", etype, label)
}

pub fn files_location_id_attr() -> Uuid {
    attr_id("$files", "location-id")
}

struct Def {
    etype: &'static str,
    label: &'static str,
    unique: bool,
    indexed: bool,
    required: bool,
    checked: Option<CheckedDataType>,
    // (reverse-etype, reverse-label) for refs -- and for the one odd blob attr
    // with a reverse identity ($oauthUserLinks.$oauthProvider).
    reverse: Option<(&'static str, &'static str)>,
    is_ref: bool,
    many: bool,
    on_delete_cascade: bool,
    on_delete_reverse_cascade: bool,
}

impl Def {
    const fn new(etype: &'static str, label: &'static str) -> Self {
        Def {
            etype,
            label,
            unique: false,
            indexed: false,
            required: false,
            checked: None,
            reverse: None,
            is_ref: false,
            many: false,
            on_delete_cascade: false,
            on_delete_reverse_cascade: false,
        }
    }
    const fn uniq(mut self) -> Self {
        self.unique = true;
        self
    }
    const fn idx(mut self) -> Self {
        self.indexed = true;
        self
    }
    const fn req(mut self) -> Self {
        self.required = true;
        self
    }
    const fn checked(mut self, c: CheckedDataType) -> Self {
        self.checked = Some(c);
        self
    }
    const fn rev(mut self, etype: &'static str, label: &'static str) -> Self {
        self.reverse = Some((etype, label));
        self
    }
    const fn reference(mut self) -> Self {
        self.is_ref = true;
        self
    }
    const fn card_many(mut self) -> Self {
        self.many = true;
        self
    }
    const fn cascade(mut self) -> Self {
        self.on_delete_cascade = true;
        self
    }
    const fn cascade_reverse(mut self) -> Self {
        self.on_delete_reverse_cascade = true;
        self
    }
}

fn defs() -> Vec<Def> {
    use CheckedDataType::*;
    vec![
        // $users
        Def::new("$users", "id").uniq().idx(),
        Def::new("$users", "email").uniq().idx().checked(String),
        Def::new("$users", "type").checked(String),
        Def::new("$users", "imageURL").checked(String),
        Def::new("$users", "linkedPrimaryUser").rev("$users", "linkedGuestUsers").reference().cascade(),
        // $magicCodes
        Def::new("$magicCodes", "id").uniq().idx(),
        Def::new("$magicCodes", "codeHash").idx().checked(String),
        Def::new("$magicCodes", "email").idx().checked(String),
        // $userRefreshTokens
        Def::new("$userRefreshTokens", "id").uniq().idx(),
        Def::new("$userRefreshTokens", "hashedToken").uniq().idx().checked(String),
        Def::new("$userRefreshTokens", "$user").rev("$users", "$userRefreshTokens").idx().reference().cascade(),
        // $oauthProviders
        Def::new("$oauthProviders", "id").uniq().idx(),
        Def::new("$oauthProviders", "name").uniq().idx().checked(String),
        // $oauthUserLinks
        Def::new("$oauthUserLinks", "id").uniq().idx(),
        Def::new("$oauthUserLinks", "sub").idx().checked(String),
        Def::new("$oauthUserLinks", "$user").rev("$users", "$oauthUserLinks").idx().reference().cascade(),
        // NB: declared without :value-type :ref upstream, so it stays a blob attr
        // that happens to have a reverse identity.
        Def::new("$oauthUserLinks", "$oauthProvider").rev("$oauthProviders", "$oauthUserLinks").idx().cascade(),
        Def::new("$oauthUserLinks", "sub+$oauthProvider").uniq().idx().checked(String),
        // $oauthClients
        Def::new("$oauthClients", "id").uniq().idx(),
        Def::new("$oauthClients", "$oauthProvider").rev("$oauthProviders", "$oauthClients").reference().cascade(),
        Def::new("$oauthClients", "name").uniq().idx().checked(String),
        Def::new("$oauthClients", "clientId").idx(),
        Def::new("$oauthClients", "encryptedClientSecret").checked(String),
        Def::new("$oauthClients", "discoveryEndpoint").checked(String),
        Def::new("$oauthClients", "meta"),
        Def::new("$oauthClients", "redirectTo").checked(String),
        Def::new("$oauthClients", "useSharedCredentials").checked(Boolean),
        // $oauthCodes
        Def::new("$oauthCodes", "id").uniq().idx(),
        Def::new("$oauthCodes", "codeHash").uniq().idx().checked(String),
        Def::new("$oauthCodes", "codeChallengeMethod").checked(String),
        Def::new("$oauthCodes", "codeChallenge").checked(String),
        Def::new("$oauthCodes", "userInfo"),
        Def::new("$oauthCodes", "$oauthClient").rev("$oauthClients", "$oauthCodes").reference().cascade(),
        // $oauthRedirects
        Def::new("$oauthRedirects", "id").uniq().idx(),
        Def::new("$oauthRedirects", "stateHash").uniq().idx().checked(String),
        Def::new("$oauthRedirects", "cookieHash").checked(String),
        Def::new("$oauthRedirects", "redirectUrl").checked(String),
        Def::new("$oauthRedirects", "redirectTo").checked(String),
        Def::new("$oauthRedirects", "$oauthClient").rev("$oauthClients", "$oauthRedirects").reference().cascade(),
        Def::new("$oauthRedirects", "codeChallengeMethod").checked(String),
        Def::new("$oauthRedirects", "codeChallenge").checked(String),
        // $files
        Def::new("$files", "id").uniq().idx(),
        Def::new("$files", "path").uniq().idx().checked(String).req(),
        Def::new("$files", "size").idx().checked(Number).req(),
        Def::new("$files", "content-type").idx().checked(String),
        Def::new("$files", "content-disposition").idx().checked(String),
        Def::new("$files", "location-id").uniq().idx().checked(String).req(),
        Def::new("$files", "key-version").checked(Number),
        Def::new("$files", "url").checked(String),
        // $streams
        Def::new("$streams", "id").uniq().idx(),
        Def::new("$streams", "clientId").uniq().idx().checked(String).req(),
        Def::new("$streams", "machineId").checked(String),
        Def::new("$streams", "$files").rev("$files", "$stream").reference().card_many().uniq().cascade_reverse(),
        Def::new("$streams", "done").checked(Boolean),
        Def::new("$streams", "size").checked(Number),
        Def::new("$streams", "hashedReconnectToken").checked(String),
        Def::new("$streams", "abortReason").checked(String),
    ]
}

/// All system catalog attrs with their deterministic ids.
pub fn all_attrs() -> Vec<Attr> {
    defs()
        .into_iter()
        .map(|d| Attr {
            id: attr_id(d.etype, d.label),
            value_type: if d.is_ref { ValueType::Ref } else { ValueType::Blob },
            cardinality: if d.many { Cardinality::Many } else { Cardinality::One },
            forward_ident: ident_id(d.etype, d.label),
            etype: d.etype.to_string(),
            label: d.label.to_string(),
            reverse_ident: d.reverse.map(|(e, l)| ident_id(e, l)),
            reverse_etype: d.reverse.map(|(e, _)| e.to_string()),
            reverse_label: d.reverse.map(|(_, l)| l.to_string()),
            is_unique: d.unique,
            is_indexed: d.indexed,
            is_required: d.required,
            checked_data_type: d.checked,
            on_delete_cascade: d.on_delete_cascade,
            on_delete_reverse_cascade: d.on_delete_reverse_cascade,
            is_system: true,
        })
        .collect()
}

/// Which system attrs are shown to clients (attr.clj remove-hidden).
pub fn is_client_visible(etype: &str, label: &str) -> bool {
    match etype {
        "$users" => true,
        "$files" => !matches!(
            label,
            "content-type" | "content-disposition" | "size" | "location-id" | "key-version"
        ),
        "$streams" => !matches!(label, "machineId" | "hashedReconnectToken"),
        _ => false,
    }
}

pub fn is_editable_etype(etype: &str) -> bool {
    matches!(etype, "$users" | "$files" | "$streams")
}

/// Triples users may write directly on system attrs.
pub fn is_editable_triple_ident(etype: &str, label: &str) -> bool {
    matches!(
        (etype, label),
        ("$users", "id") | ("$files", "id") | ("$files", "path") | ("$streams", "id")
    )
}

/// Ensure the system catalog app + attrs exist. Run at server boot.
pub async fn ensure_system_catalog(pool: &sqlx::PgPool) -> Result<()> {
    let mut tx = pool.begin().await.map_err(crate::error::InstantError::from)?;
    sqlx::query(
        r#"
        INSERT INTO instant_users (id, email)
        VALUES ($1, 'system-catalog@instantdb.com')
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(SYSTEM_CATALOG_USER_ID)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        r#"
        INSERT INTO apps (id, creator_id, title)
        VALUES ($1, $2, 'System catalog')
        ON CONFLICT (id) DO NOTHING
        "#,
    )
    .bind(SYSTEM_CATALOG_APP_ID)
    .bind(SYSTEM_CATALOG_USER_ID)
    .execute(&mut *tx)
    .await?;
    for attr in all_attrs() {
        crate::attr::insert(&mut *tx, SYSTEM_CATALOG_APP_ID, &attr).await?;
    }
    tx.commit().await.map_err(crate::error::InstantError::from)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encodes_known_uuids() {
        // Verified against migration 85's hardcoded $files.location-id id and
        // DATAMODEL.md's table extracted from the legacy encoder.
        assert_eq!(
            attr_id("$files", "location-id").to_string(),
            "96653230-13ff-ffff-2a34-b40fffffffff"
        );
        assert_eq!(
            attr_id("$users", "id").to_string(),
            "96653230-13ff-ffff-a4b4-81ffffffffff"
        );
        assert_eq!(
            attr_id("$users", "email").to_string(),
            "96653230-13ff-ffff-a4b4-46010bffffff"
        );
        assert_eq!(
            attr_id("$userRefreshTokens", "hashedToken").to_string(),
            "96653230-13ff-ffff-a474-7048e41cdcaf"
        );
        assert_eq!(
            ident_id("$files", "location-id").to_string(),
            "96653231-03ff-ffff-2a34-b40fffffffff"
        );
        assert_eq!(
            attr_id("$oauthUserLinks", "sub+$oauthProvider").to_string(),
            "96653230-13ff-ffff-72f5-2a05f175503f"
        );
    }

    #[test]
    fn all_attrs_complete() {
        let attrs = all_attrs();
        assert_eq!(attrs.len(), 57);
        // ids are all distinct
        let mut ids: Vec<_> = attrs.iter().map(|a| a.id).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 57);
    }
}
