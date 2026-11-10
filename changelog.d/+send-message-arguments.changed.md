`send_message` takes its fields directly (`to`, `text`, `collective`, `collectiveResponse`) instead of inside an `input` object.
When collectives are hidden, `send_message` publishes only `to` and `text`.
