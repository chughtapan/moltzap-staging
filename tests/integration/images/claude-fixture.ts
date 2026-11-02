/** @file Claude Code stdio stand-in shared by image live tests. */
/** Answers turns and records the text received by the CLI. */
export const fakeClaude = String.raw`#!/usr/bin/env node
const fs = require("node:fs");
const path = require("node:path");
const argv = process.argv.slice(2);
if (argv.includes("--version")) {
  process.stdout.write("2.1.300 (Claude Code)\n");
  process.exit(0);
}
const flag = (name) => (argv.includes(name) ? argv[argv.indexOf(name) + 1] : undefined);
const sessionId = flag("--resume") ?? flag("--session-id") ?? require("node:crypto").randomUUID();
if (argv.includes("--print")) fs.appendFileSync(process.env.FAKE_CLAUDE_LOG + ".launches", JSON.stringify({ effort: flag("--effort"), maxThinkingTokens: process.env.MAX_THINKING_TOKENS, disableAdaptiveThinking: process.env.CLAUDE_CODE_DISABLE_ADAPTIVE_THINKING }) + "\n");
const projects = path.join(process.env.HOME, ".claude", "projects", process.cwd().replace(/[^A-Za-z0-9]/g, "-"));
fs.mkdirSync(projects, { recursive: true });
const record = (message) => fs.appendFileSync(path.join(projects, sessionId + ".jsonl"), JSON.stringify({ sessionId, message }) + "\n");
const send = (value) => process.stdout.write(JSON.stringify(value) + "\n");
process.stdin.on("end", () => process.exit(0));
require("node:readline").createInterface({ input: process.stdin }).on("line", (line) => {
  const message = JSON.parse(line);
  if (message.type === "control_request") {
    send({ type: "control_response", response: { subtype: "success", request_id: message.request_id, response: {} } });
    return;
  }
  if (message.type !== "user") return;
  const content = message.message.content;
  const text = typeof content === "string" ? content : JSON.stringify(content);
  fs.appendFileSync(process.env.FAKE_CLAUDE_LOG, JSON.stringify({ sessionId, text }) + "\n");
  record({ role: "user", content: text });
  const reply = "noted";
  record({ role: "assistant", content: [{ type: "text", text: reply }] });
  send({ type: "system", subtype: "init", session_id: sessionId, model: "claude-opus-4-8", tools: [], cwd: process.cwd() });
  send({ type: "assistant", message: { id: "msg_" + Date.now(), role: "assistant", model: "claude-opus-4-8", content: [{ type: "text", text: reply }] }, session_id: sessionId });
  send({ type: "result", subtype: "success", is_error: false, result: reply, session_id: sessionId, duration_ms: 1, num_turns: 1, usage: { input_tokens: 1, output_tokens: 1 } });
});
`;
