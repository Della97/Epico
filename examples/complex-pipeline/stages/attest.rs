use metapod_sdk::stage;
use hmac::{Hmac, Mac};
use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

// Baked-in key. For a real deployment, read from a WASI env var at init.
const HMAC_KEY: &[u8] = b"metapod-eval-key-do-not-use-in-prod";

fn canonicalize(ev: &Featured) -> String {
    // Deterministic, sorted-key JSON-ish form. Order matters for the hash.
    format!(
        "sensor_id={}|ts_ns={}|rms={:.9}|centroid={:.9}|flatness={:.9}|kurtosis={:.9}",
        ev.sensor_id, ev.ts_ns, ev.rms, ev.centroid, ev.flatness, ev.kurtosis,
    )
}

stage! {
    fn attest(ev: Featured) -> Signed {
        let payload = canonicalize(&ev);

        // BLAKE3 content hash — fast, single call.
        let content_hash = blake3::hash(payload.as_bytes()).to_hex().to_string();

        // HMAC-SHA256 over the same canonical form.
        let mut mac = HmacSha256::new_from_slice(HMAC_KEY)
            .expect("HMAC key length");
        mac.update(payload.as_bytes());
        let tag = mac.finalize().into_bytes();
        let hmac_hex = hex::encode(tag);

        Signed {
            sensor_id: ev.sensor_id,
            ts_ns: ev.ts_ns,
            rms: ev.rms,
            centroid: ev.centroid,
            flatness: ev.flatness,
            kurtosis: ev.kurtosis,
            content_hash,
            hmac: hmac_hex,
        }
    }
}