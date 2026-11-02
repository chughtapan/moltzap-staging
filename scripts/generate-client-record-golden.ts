/**
 * @file Reproduce canonical conversation records with public deterministic test
 * keys: pnpm nx run workspace:generate-client-record-golden.
 */

import { FileSystem } from "@effect/platform";
import { NodeFileSystem } from "@effect/platform-node";
import { Effect, Redacted, Schema } from "effect";
import { fileURLToPath } from "node:url";
import {
  AgentCardIssuedAt as agentCardIssuedAt,
  issueAgentCard,
} from "../packages/registry/dist/issuance.js";
import {
  ActionCertifiedRecord,
  ActionCore,
  CertifiedRecord,
  CompletedReanchor,
  deriveConversationId,
  DirectPacket,
  type EvidenceStatement,
  hashAction,
  hashAnchor,
  hashMembershipDescriptor,
  hashMessageIntent,
  hashRecord,
  InitialAnchorBody,
  MembershipDescriptor,
  MessageIntent,
  MsgId,
  quorumThreshold,
  ReanchorBody,
  RecordCore,
  type RecordHash,
  signEvidenceMessage,
} from "../packages/social-harness/dist/communication/records/index.js";
import { RouterInstanceId } from "../packages/social-harness/dist/communication/router/api/index.js";
import { encodeCanonical } from "../packages/social-harness/dist/encoding/canonical-json.js";
import {
  AgentIdentityCard,
  AgentId,
  AgentName,
  AgentSigningAuthority,
  AgentPublicKey,
  PrincipalId,
  SignedMessage,
  WIRE_VERSION,
} from "../packages/social-harness/dist/index.js";

const sharnessVersion = WIRE_VERSION;
const utf8 = new TextDecoder();

interface MessageInput {
  readonly anchor: ActionCertifiedRecord["routerAnchor"];
  readonly previousRecordHash: RecordHash | null;
  readonly byte: number;
  readonly name: string;
}

/** Public repeated-byte seeds are exclusively synthetic test keys. */
function testAuthority(byte: number) {
  const der = Buffer.concat([
    Buffer.from("302e020100300506032b657004220420", "hex"),
    Buffer.alloc(32, byte),
  ]);
  return AgentSigningAuthority.fromPkcs8(
    Redacted.make(
      `-----BEGIN PRIVATE KEY-----\n${der.toString("base64")}\n-----END PRIVATE KEY-----\n`,
    ),
  );
}

/** Repeated bytes identify synthetic members independently of their signing keys. */
function agentIdOf(byte: number) {
  return Schema.decodeUnknown(AgentId)(
    `agt_${Buffer.alloc(16, byte).toString("base64url")}`,
  );
}

/** A stable card binds one deterministic member seed to the fixed Registry key. */
function deterministicIdentity(byte: number, registry: AgentSigningAuthority) {
  return Effect.gen(function* () {
    const authority = yield* testAuthority(byte + 10);
    const card = yield* issueAgentCard({
      agentId: yield* agentIdOf(byte),
      principalId: yield* Schema.decodeUnknown(PrincipalId)(
        `prn_${Buffer.alloc(16, byte).toString("base64url")}`,
      ),
      agentName: yield* Schema.decodeUnknown(AgentName)(
        `golden-member-${byte}`,
      ),
      publicKey: AgentSigningAuthority.publicKey(authority),
      issuedAt: yield* Schema.decodeUnknown(agentCardIssuedAt)(
        "2026-08-27T12:00:00Z",
      ),
      registrySigningAuthority: registry,
    });
    return { card, authority };
  });
}

/** Both unanimity and Byzantine-threshold profiles use reproducible signatures. */
function deterministicFixture(memberCount: number) {
  return Effect.gen(function* () {
    const registry = yield* testAuthority(1);
    const identities = yield* Effect.forEach(
      [1, 2, 3, 4].slice(0, memberCount),
      (byte) => deterministicIdentity(byte, registry),
      { concurrency: 1 },
    );
    const descriptor = yield* Schema.decodeUnknown(MembershipDescriptor)({
      sharnessVersion,
      kind: "membership_descriptor",
      conversationId: yield* deriveConversationId([
        yield* agentIdOf(1),
        yield* agentIdOf(2),
        ...identities.slice(2).map(({ card }) => card.agentId),
      ]),
      members: yield* Effect.forEach(
        identities,
        ({ card }) => Schema.encode(AgentIdentityCard)(card),
        { concurrency: 1 },
      ),
    });
    return {
      identities,
      descriptor,
      membershipHash: yield* hashMembershipDescriptor(descriptor),
      registrySignerPublicKey: yield* Schema.encode(AgentPublicKey)(
        AgentSigningAuthority.publicKey(registry),
      ),
    };
  });
}

type Fixture = Effect.Effect.Success<ReturnType<typeof deterministicFixture>>;

/** Canonical signer order always includes the fixture's first member, its author. */
function signSet(
  fixture: Fixture,
  statement: (signerAgentId: AgentId) => EvidenceStatement,
  minimumSigners?: number,
) {
  const count = minimumSigners ?? quorumThreshold(fixture.identities.length);
  return Effect.forEach(
    fixture.identities.slice(0, count),
    ({ card, authority }) =>
      signEvidenceMessage({
        statement: statement(card.agentId),
        agentCard: card,
        signingAuthority: authority,
      }).pipe(
        Effect.flatMap((message) => Schema.encode(SignedMessage)(message)),
      ),
    { concurrency: 1 },
  );
}

/** The same intent representation is shared by first and successor messages. */
function messageIntent(fixture: Fixture, input: MessageInput) {
  return Effect.gen(function* () {
    const [author] = fixture.identities;
    if (author === undefined) {
      return yield* Effect.dieMessage("missing golden author");
    }
    return yield* Schema.decodeUnknown(MessageIntent)({
      sharnessVersion,
      kind: "message_intent",
      conversationId: fixture.descriptor.conversationId,
      membershipHash: fixture.membershipHash,
      authorAgentId: author.card.agentId,
      msgId: yield* Schema.decodeUnknown(MsgId)(
        `msg_${Buffer.alloc(32, input.byte).toString("base64url")}`,
      ),
      content: [{ type: "text", text: input.name }],
    });
  });
}

/** A first message embeds membership; successors bind its retained hashes. */
function messageAction(fixture: Fixture, input: MessageInput) {
  return Effect.gen(function* () {
    const intent = yield* messageIntent(fixture, input);
    const common = {
      sharnessVersion,
      conversationId: fixture.descriptor.conversationId,
      previousRecordHash: input.previousRecordHash,
      messageIntent: intent,
      messageIntentHash: yield* hashMessageIntent(intent),
    };
    return yield* Schema.decodeUnknown(ActionCore)(
      input.previousRecordHash === null
        ? {
            ...common,
            kind: "FIRST_MESSAGE",
            membership: fixture.descriptor,
            anchor: input.anchor,
          }
        : {
            ...common,
            kind: "MESSAGE",
            membershipHash: fixture.membershipHash,
            anchorHash: yield* anchorHash(input.anchor),
          },
    );
  });
}

/** A completed anchor retains its verified hash with its certificate. */
function anchorHash(anchor: ActionCertifiedRecord["routerAnchor"]) {
  return anchor.kind === "initial_anchor_body"
    ? hashAnchor(anchor)
    : Effect.succeed(anchor.anchorHash);
}

/** Each form carries its own Registry key and membership for offline verification. */
function form(
  name: string,
  packet: DirectPacket,
  core: RecordCore,
  fixture: Fixture,
) {
  return Effect.gen(function* () {
    return {
      name,
      kind: packet.kind,
      membership: fixture.descriptor,
      registrySignerPublicKey: fixture.registrySignerPublicKey,
      bytes: utf8.decode(yield* encodeCanonical(DirectPacket, packet)),
      recordHash: yield* hashRecord(core),
      actionHash: yield* hashAction(core.action),
    };
  });
}

/** Independently signed action and durability evidence exclude each other from hashes. */
function certifiedPair(
  fixture: Fixture,
  action: ActionCore,
  input: MessageInput,
) {
  return Effect.gen(function* () {
    const core = yield* Schema.decodeUnknown(RecordCore)({
      sharnessVersion,
      kind: "record_core",
      ...(action.kind === "FIRST_MESSAGE"
        ? { membership: action.membership }
        : { membershipHash: action.membershipHash }),
      anchorHash: yield* anchorHash(input.anchor),
      action,
      actionHash: yield* hashAction(action),
    });
    const recordHash = yield* hashRecord(core);
    const actionCertified = yield* actionCertifiedRecord(fixture, core, input);
    const certified = yield* Schema.decodeUnknown(CertifiedRecord)({
      sharnessVersion,
      kind: "certified_record",
      actionCertifiedRecord: actionCertified,
      durabilityCertificate: {
        sharnessVersion,
        kind: "durability_certificate",
        recordHash,
        votes: yield* signSet(fixture, (signerAgentId) => ({
          sharnessVersion,
          kind: "durability_vote",
          signerAgentId,
          recordHash,
          conversationId: fixture.descriptor.conversationId,
          membershipHash: fixture.membershipHash,
        })),
      },
    });
    return { core, recordHash, actionCertified, certified };
  });
}

/** First-message action approval is unanimous even when storage requires fewer members. */
function actionCertifiedRecord(
  fixture: Fixture,
  core: RecordCore,
  input: MessageInput,
) {
  return Effect.gen(function* () {
    return yield* Schema.decodeUnknown(ActionCertifiedRecord)({
      sharnessVersion,
      kind: "action_certified_record",
      recordHash: yield* hashRecord(core),
      recordCore: core,
      routerAnchor: input.anchor,
      actionCertificate: {
        sharnessVersion,
        kind: "action_certificate",
        actionHash: core.actionHash,
        signatures: yield* signSet(
          fixture,
          (signerAgentId) => ({
            sharnessVersion,
            kind: "action_signature",
            signerAgentId,
            actionHash: core.actionHash,
          }),
          core.action.kind === "FIRST_MESSAGE"
            ? fixture.identities.length
            : quorumThreshold(fixture.identities.length),
        ),
      },
    });
  });
}

/** Both wrappers expose the same stable record identity. */
function messageForms(fixture: Fixture, input: MessageInput) {
  return Effect.gen(function* () {
    const action = yield* messageAction(fixture, input);
    const pair = yield* certifiedPair(fixture, action, input);
    return {
      recordHash: pair.recordHash,
      forms: yield* Effect.all([
        form(
          `${input.name} action-certified`,
          pair.actionCertified,
          pair.core,
          fixture,
        ),
        form(`${input.name} certified`, pair.certified, pair.core, fixture),
      ]),
    };
  });
}

/** The re-anchor quorum binds the selected predecessor under the next Router instance. */
function completedAnchor(
  fixture: Fixture,
  anchor: InitialAnchorBody,
  selectedRecordHash: RecordHash,
) {
  return Effect.gen(function* () {
    const body = yield* Schema.decodeUnknown(ReanchorBody)({
      sharnessVersion,
      kind: "reanchor_body",
      conversationId: fixture.descriptor.conversationId,
      membershipHash: fixture.membershipHash,
      previousAnchorHash: yield* hashAnchor(anchor),
      selectedRecordHash,
      routerInstanceId: yield* Schema.decodeUnknown(RouterInstanceId)(
        `rti_${Buffer.alloc(16, 42).toString("base64url")}`,
      ),
    });
    const hash = yield* hashAnchor(body);
    return yield* Schema.decodeUnknown(CompletedReanchor)({
      sharnessVersion,
      kind: "completed_reanchor",
      anchorHash: hash,
      reanchor: body,
      certificate: {
        sharnessVersion,
        kind: "reanchor_certificate",
        anchorHash: hash,
        votes: yield* signSet(fixture, (signerAgentId) => ({
          sharnessVersion,
          kind: "reanchor_vote",
          signerAgentId,
          anchorHash: hash,
          reanchor: body,
        })),
      },
    });
  });
}

/** The two profiles include complete ancestry; N4 also covers a Router restart. */
function membershipForms(memberCount: number) {
  return Effect.gen(function* () {
    const fixture = yield* deterministicFixture(memberCount);
    const anchor = yield* Schema.decodeUnknown(InitialAnchorBody)({
      sharnessVersion,
      kind: "initial_anchor_body",
      conversationId: fixture.descriptor.conversationId,
      membershipHash: fixture.membershipHash,
      routerInstanceId: `rti_${Buffer.alloc(16, 41).toString("base64url")}`,
    });
    const first = yield* messageForms(fixture, {
      anchor,
      previousRecordHash: null,
      byte: 50,
      name: `N${memberCount} FIRST_MESSAGE`,
    });
    const message = yield* messageForms(fixture, {
      anchor,
      previousRecordHash: first.recordHash,
      byte: 51,
      name: `N${memberCount} MESSAGE`,
    });
    if (memberCount === 2) {
      return [...first.forms, ...message.forms];
    }
    const completed = yield* completedAnchor(
      fixture,
      anchor,
      message.recordHash,
    );
    const reanchored = yield* messageForms(fixture, {
      anchor: completed,
      previousRecordHash: message.recordHash,
      byte: 52,
      name: `N${memberCount} MESSAGE completed_reanchor`,
    });
    return [...first.forms, ...message.forms, ...reanchored.forms];
  });
}

const generate = Effect.gen(function* () {
  const forms = yield* Effect.forEach(
    [2, 4],
    (count) => membershipForms(count),
    {
      concurrency: 1,
    },
  );
  const fs = yield* FileSystem.FileSystem;
  yield* fs.writeFileString(
    fileURLToPath(
      new URL(
        "../packages/social-harness/src/communication/records/record-golden.json",
        import.meta.url,
      ),
    ),
    `${JSON.stringify({ forms: forms.flat() }, null, 2)}\n`,
  );
});

await Effect.runPromise(generate.pipe(Effect.provide(NodeFileSystem.layer)));
