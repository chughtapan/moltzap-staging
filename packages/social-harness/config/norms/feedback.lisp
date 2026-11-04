;; feedback: a member says it could not read a message, and which norms it
;; reads. Feedback is never answered.
(norm feedback
      :description "Tell a member that its message could not be read here, and which norms can be.")

(content not-understood
  (field reason :type select :choices (unparsed plain-text unknown-norm invalid-content)
         :description "Why the message could not be read.")
  (field error :type string :max-length 1000 :optional true
         :description "What could not be read, such as a field the norm's form refuses.")
  (field accepts :type string :max-length 4000
         :description "The norms this member reads, separated by spaces."))
