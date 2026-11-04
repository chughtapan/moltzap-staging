;; norm-discovery: ask a member which norms it holds, or for one norm's
;; document. In a direct conversation the other member's daemon answers; in a
;; group, the member that proposed a norm answers for it. An inform carries a
;; document in :content.
(norm norm-discovery :protocol fipa-query :initiator Asker :participant Holder
      :description "Ask which norms a member holds, or for the document of one norm.")

(content query-ref
  (field norm :type string :max-length 64 :optional true
         :description "The norm whose document you want. Leave it out to list the norms held."))

(content inform
  (field norm :type string :max-length 64 :optional true
         :description "The norm whose document :content carries.")
  (field norms :type string :max-length 4000 :optional true
         :description "The names of the norms held, separated by spaces."))

(content refuse)

(content failure)

(content cancel)
