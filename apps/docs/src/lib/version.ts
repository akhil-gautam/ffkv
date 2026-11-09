// The release version, read from the server's Cargo.toml at build time so the
// site never drifts from the binary it documents.
import cargoToml from '../../../../Cargo.toml?raw';

const match = cargoToml.match(/^\[package\]\nname = "ffkv"\nversion = "([^"]+)"/m);
if (!match) throw new Error('Cargo.toml has no ffkv package version');

export const version = match[1];
/** `major.minor`, e.g. `0.1`. */
export const shortVersion = version.split('.').slice(0, 2).join('.');
