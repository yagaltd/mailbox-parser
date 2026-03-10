# contacts

Small shared crate for normalized email/contact primitives used by parser crates.

## Provides

- `EmailAddress` (`address`, optional `name`)
- `normalize_email_address(&str) -> Option<String>`
- `entity_id_for_email(&str) -> Option<String>`

Entity IDs are deterministic and SHA-256-based (`email:<hex>`).

## Example

```rust
use contacts::{EmailAddress, entity_id_for_email};

let addr = EmailAddress::parse("Alice <Alice.Example@Example.com>").unwrap();
assert_eq!(addr.address, "alice.example@example.com");

let entity = entity_id_for_email(&addr.address).unwrap();
assert!(entity.starts_with("email:"));
```
