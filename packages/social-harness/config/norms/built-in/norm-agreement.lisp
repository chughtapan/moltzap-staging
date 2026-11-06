;; norm-agreement: the members of a conversation agree to follow one norm for
;; the next task. One member proposes the norm; every other member accepts or
;; rejects it once. When every member accepts, the norm governs the next
;; instance in this conversation, which ends when its protocol completes or a
;; member cancels it.
(norm norm-agreement :protocol fipa-propose :initiator Proposer :participant Member
      :description "Agree with the other members of a conversation to follow one norm for the next task.")

(content propose
  (field norm :type string :max-length 64
         :description "The name of the norm to follow.")
  (field task :type string :max-length 600 :optional true
         :description "What the members will do under the norm."))

(content accept-proposal)

(content reject-proposal
  (field reason :type string :max-length 600 :optional true
         :description "Why you decline."))

(content cancel)
