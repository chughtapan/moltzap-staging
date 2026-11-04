;; group-meeting: one member schedules a meeting with the other members.
;; The organizer calls for the free candidates, every member proposes the ones
;; it can attend or refuses, and the organizer accepts one candidate.
(norm group-meeting :protocol fipa-contract-net :initiator Organizer :participant Member
      :description "One member schedules a meeting with the other members of the conversation.")

(content cfp
  (field length :type string :max-length 80
         :description "How long the meeting is.")
  (field candidates :type string :max-length 600
         :description "Up to ten candidate times, numbered from 1."))

(content propose
  (field free :type multi-select :choices (1 2 3 4 5 6 7 8 9 10)
         :description "Every candidate you can attend."))

(content refuse
  (field reason :type string :max-length 300 :optional true
         :description "Why you cannot take part."))

(content accept-proposal
  (field option :type integer :minimum 1 :maximum 10
         :description "The chosen candidate."))

(content reject-proposal)

(content cancel)

(rule "accept-proposal names a candidate that every Member who proposed listed as free.")
