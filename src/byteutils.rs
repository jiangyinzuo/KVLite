pub fn u32_from_le_bytes(bytes: &[u8]) -> u32 {
    debug_assert_eq!(bytes.len(), 4);
    let mut buf = [0u8; 4];
    buf.clone_from_slice(bytes);
    u32::from_le_bytes(buf)
}
