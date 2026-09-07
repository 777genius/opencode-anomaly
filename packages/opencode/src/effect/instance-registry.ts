import { diagnosticIdentity, unitDiagnostic, unitDiagnosticEnabled } from "@/util/windows-unit-diagnostic"

const disposers = new Set<(directory: string) => Promise<void>>()

export function registerDisposer(disposer: (directory: string) => Promise<void>) {
  if (unitDiagnosticEnabled)
    unitDiagnostic("other", "instance-registry")(`disposer.${diagnosticIdentity(disposer)}.registered`)
  disposers.add(disposer)
  return () => {
    disposers.delete(disposer)
  }
}

export async function disposeInstance(directory: string) {
  await Promise.allSettled(
    [...disposers].map((disposer) => {
      if (!unitDiagnosticEnabled) return disposer(directory)
      const mark = unitDiagnostic("other", "instance-registry")
      const id = diagnosticIdentity(disposer)
      mark(`disposer.${id}.start`)
      // Observe the original promise; preserve allSettled's rejection handling.
      try {
        const promise = disposer(directory)
        void promise.then(
          () => mark(`disposer.${id}.success`),
          () => mark(`disposer.${id}.failure`),
        )
        return promise
      } catch (error) {
        mark(`disposer.${id}.failure`)
        throw error
      }
    }),
  )
}
