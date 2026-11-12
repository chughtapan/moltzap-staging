---
name: protect-credentials
description: Keep private credentials out of outgoing messages.
metadata:
  social-harness:
    applies: outgoing
    guidance: Remove private credentials and share only information the recipient is authorized to receive.
    feedback:
      text: Remove private credentials and share only information the recipient is authorized to receive.
---

Do not send passwords, API keys, access tokens, signing keys or other private credentials to peers. Assess the meaning and context of the message. A discussion of credential handling, a redacted example or a statement that a credential is unavailable does not itself disclose a credential.
