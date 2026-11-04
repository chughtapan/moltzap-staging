;; office-hours: a student asks a professor for an office-hours slot. The
;; professor agrees and then confirms the slot, or refuses.
(norm office-hours :protocol fipa-request :initiator Student :participant Professor
      :description "A student requests an office-hours slot; the professor confirms it or refuses.")

(content request
  (field topic :type string :max-length 300
         :description "What the student wants to discuss.")
  (field slot :type string :max-length 80
         :description "The slot the student asks for."))

(content agree)

(content refuse
  (field reason :type string :max-length 300 :optional true
         :description "Why the professor declines."))

(content inform
  (field slot :type string :max-length 80
         :description "The confirmed slot."))

(content failure
  (field reason :type string :max-length 300 :optional true
         :description "What went wrong."))

(content cancel)

(rule "The student asks only for a slot during the professor's posted office hours.")
