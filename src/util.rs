//! Small helpers shared across modules, kept free of dependencies on the rest
//! of the crate so that any module may use them.

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

pub fn uuid() -> String {
    hex(&random_bytes::<16>())
}

/// Bytes from the operating system's CSPRNG. The generator failing means the
/// platform cannot provide randomness at all, which nothing here can recover
/// from; carrying on with predictable ids or keys would be worse.
pub fn random_bytes<const N: usize>() -> [u8; N] {
    let mut buf = [0u8; N];
    if let Err(error) = getrandom::fill(&mut buf) {
        panic!("operating system random number generator failed: {error}");
    }
    buf
}
