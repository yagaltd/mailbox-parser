use contacts::{EmailAddress, entity_id_for_email, normalize_email_address};
use pretty_assertions::assert_eq;

#[test]
fn normalize_basic_email() {
    let v = normalize_email_address(" Foo <FOO@Example.com> ");
    assert_eq!(v.as_deref(), Some("foo@example.com"));
}

#[test]
fn parse_email_with_name() {
    let addr = EmailAddress::parse("Ada Lovelace <ADA@Example.com>").unwrap();
    assert_eq!(addr.address, "ada@example.com");
    assert_eq!(addr.name.as_deref(), Some("Ada Lovelace"));
}

#[test]
fn entity_id_prefix() {
    let id = entity_id_for_email("user@example.com").unwrap();
    assert!(id.starts_with("email:"));
    assert_eq!(id.len(), "email:".len() + 64);
}
