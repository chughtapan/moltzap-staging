;; feedback: a member says it could not read a message, that the message does
;; not fit the conversation's norm, or that it breaks one of the norm's English
;; rules, and which norms it reads. An act answered this way changes nothing.
;; Feedback is never answered.
(norm feedback
      :description "Tell a member that its message could not be read here, does not fit the norm or breaks one of its rules, and which norms can be.")

(content not-understood
  (field reason :type string :max-length 40
         :description "Why the message could not be read, does not fit the conversation's norm, or breaks one of its rules. The norms specification lists the reasons.")
  (field error :type string :max-length 1000 :optional true
         :description "What failed, such as a field the norm's form refuses, the acts the sender may send now, or the rule the act breaks.")
  (field accepts :type string :max-length 4000
         :description "The norms this member reads, separated by spaces."))
