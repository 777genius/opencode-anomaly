import { createHash } from "node:crypto"

// This is a source checkpoint, not compiled-artifact admission. The exact build
// must independently extract the entry module bytes and admit their digest.
export const MAX_COMPILED_MODULE_BYTES = 64 * 1024 * 1024

export interface ExecutableIdentity {
  readonly pid: number
  readonly startTicks: string
  readonly exeDevice: string
  readonly exeInode: string
  readonly exeSha256: string
}

// Privileged in-process test seam, never supplied by the capsule/environment.
// The production reader must bound allocation before awaiting bytes().
export interface CompiledIdentityOperations {
  readonly executableIdentity: () => ExecutableIdentity
  readonly moduleBytes: (modulePath: string) => Promise<Uint8Array>
}

/**
 * Bun's Linux /$bunfs/root entry module is immutable storage inside the running
 * executable, not an independently stat-able file. Existing moduleDevice and
 * moduleInode therefore identify that backing executable. moduleSha256 still
 * hashes only the actual virtual module bytes; it never substitutes exeSha256.
 *
 * These storage fields are internal DerivedIdentity fields. The v2 canonical
 * producer record emits exeDev/exeIno and independent exeSha256/moduleSha256;
 * it neither emits a module path nor requires a separate module inode. No wire
 * keys, contract hashes, or capsule expectations change under this interpretation.
 * The Bun 1.4.0 sandbox observation and independent extraction are recorded in
 * .review-inputs/offline-probe-r807.json (445 bytes, SHA-256 8949cc47...13a2c3).
 * That probe is evidence for this byte source, not admission of an OpenCode build.
 */
export async function deriveCompiledIdentity(modulePath: string, operations: CompiledIdentityOperations) {
  requireCompiledModulePath(modulePath)

  // Copy the observations: a trusted test seam must not accidentally alias the
  // before/after snapshots and conceal drift across the asynchronous boundary.
  const before = { ...operations.executableIdentity() }
  const bytes = await operations.moduleBytes(modulePath)
  const after = operations.executableIdentity()
  if (
    before.pid !== after.pid ||
    before.startTicks !== after.startTicks ||
    before.exeDevice !== after.exeDevice ||
    before.exeInode !== after.exeInode ||
    before.exeSha256 !== after.exeSha256
  )
    throw new TypeError("producer-provenance-compiled-identity-changed")
  if (!(bytes instanceof Uint8Array) || bytes.byteLength < 1 || bytes.byteLength > MAX_COMPILED_MODULE_BYTES) {
    throw new RangeError("producer-provenance-compiled-module-bounded")
  }
  return {
    ...before,
    moduleDevice: before.exeDevice,
    moduleInode: before.exeInode,
    moduleSha256: createHash("sha256").update(bytes).digest("hex"),
  }
}

interface CompiledModuleFile {
  readonly size: number
  readonly slice: (start: number, end: number) => { readonly bytes: () => Promise<Uint8Array> }
}

// trustedFile is exclusively an in-process test seam for bounding the Bun read.
export async function readCompiledModuleBytes(
  modulePath: string,
  trustedFile: (path: string) => CompiledModuleFile = (path) => Bun.file(path),
) {
  requireCompiledModulePath(modulePath)
  if (process.platform !== "linux") throw new TypeError("producer-provenance-compiled-platform")
  const file = trustedFile(modulePath)
  const size = file.size
  if (!Number.isSafeInteger(size) || size < 1 || size > MAX_COMPILED_MODULE_BYTES) {
    throw new RangeError("producer-provenance-compiled-module-bounded")
  }
  // The extra byte detects growth without an unbounded read. For the supported
  // immutable Bun storage this slice covers the entire module, byte for byte.
  const bytes = await file.slice(0, MAX_COMPILED_MODULE_BYTES + 1).bytes()
  if (bytes.byteLength !== size || bytes.byteLength > MAX_COMPILED_MODULE_BYTES) {
    throw new RangeError("producer-provenance-compiled-module-bounded")
  }
  return bytes
}

function requireCompiledModulePath(modulePath: string) {
  if (
    !modulePath.startsWith("/$bunfs/root/") ||
    /[\\\x00-\x20\x7f?#]/.test(modulePath) ||
    modulePath
      .slice("/$bunfs/root/".length)
      .split("/")
      .some((part) => !part || part === "." || part === "..")
  )
    throw new TypeError("producer-provenance-compiled-module-path")
}
