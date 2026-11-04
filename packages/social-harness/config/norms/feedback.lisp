;; feedback: a member says it could not read a message, or that the message
;; does not fit the conversation's norm, and which norms it reads. Feedback is
;; never answered.
(norm feedback
      :description "Tell a member that its message could not be read here or does not fit the norm, and which norms can be.")

(content not-understood
  (field reason :type select
         :choices (unparsed plain-text unknown-norm invalid-content
                   out-of-turn no-agreement in-flight unknown-instance)
         :description "Why the message could not be read or does not fit the conversation's norm.")
  (field error :type string :max-length 1000 :optional true
         :description "What failed, such as a field the norm's form refuses or the acts the sender may send now.")
  (field accepts :type string :max-length 4000
         :description "The norms this member reads, separated by spaces."))
