/** @file Policy installations and completed assessments remain owned by Firewall. */

/** Successful owner edits retain metadata even when no check uses the installation. */
export interface FirewallActivation {
  readonly time: number;
  readonly activeRevision: string;
  readonly addedRuleIds: readonly string[];
  readonly removedRuleIds: readonly string[];
}
/** The exact completed assessment saved with its applied disposition. */
export interface FirewallCompletion {
  readonly checkId: string;
  readonly direction: "incoming" | "outgoing";
  readonly canonicalCandidate: Uint8Array;
  readonly canonicalInput: Uint8Array;
  readonly canonicalResult: Uint8Array;
  readonly admit: boolean;
}
/** The disposition retained for a completed local assessment. */
export type FirewallCommit = "admitted" | "withheld";
