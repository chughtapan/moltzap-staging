;; office-hours: a student asks a professor to meet about a topic. The
;; professor offers two or three slots; the student picks one or none. After
;; none, the professor offers once more or escalates to the professor in
;; person. A picked slot is confirmed, or held tentatively until the student
;; checks back.
;;
;; Each act starts and ends facts, and the facts that hold decide which acts
;; may follow:
;;   open          a request is in progress
;;   offer-due     the professor offers slots
;;   offered       slots are out; the student picks one or none
;;   declined      the student declined the offered slots
;;   decision-due  the professor confirms the picked slot or holds it
;;   held          a slot is held; the student checks back
;;   checked-back  the student has checked back on a held slot
(norm office-hours :initiator Student :participant Professor
      :description "A student asks to meet about a topic; the professor offers slots, the student picks one or none, and the professor confirms, holds or escalates.")

(content request :by Student :unless open :initiates (open offer-due)
  (field topic :type string :max-length 300
         :description "What the student wants to discuss."))

;; At most two rounds of offers in one request.
(content propose :by Professor :when offer-due :max 2
         :initiates offered :terminates (offer-due decision-due checked-back declined)
  (field option1 :type string :max-length 80 :description "A slot the professor offers.")
  (field option2 :type string :max-length 80 :description "A second slot.")
  (field option3 :type string :max-length 80 :optional true
         :description "A third slot."))

(content accept-proposal :by Student :when offered
         :initiates decision-due :terminates offered
  (field option :type integer :minimum 1 :maximum 3
         :description "The number of the slot the student picks."))

;; None of the above.
(content reject-proposal :by Student :when offered
         :initiates (offer-due declined) :terminates offered
  (field reason :type string :max-length 300 :optional true
         :description "Why none of the slots works."))

(content confirm :by Professor :when decision-due
         :terminates (open decision-due offer-due checked-back)
  (field slot :type string :max-length 80 :description "The booked slot."))

;; A tentative hold: the student checks back after the given minutes.
(content agree :by Professor :when decision-due :unless checked-back
         :initiates held :terminates decision-due
  (field slot :type string :max-length 80 :description "The slot held tentatively.")
  (field recheck-after :type integer :minimum 1 :maximum 1440
         :description "Minutes after which the student checks back."))

;; The student checks back on a held slot. The professor then confirms it,
;; refuses it, or offers again while a round is left.
(content query-if :by Student :when held
         :initiates (decision-due offer-due checked-back) :terminates held)

;; The held slot is no longer available.
(content refuse :by Professor :when checked-back
         :terminates (open decision-due offer-due checked-back)
  (field reason :type string :max-length 300 :optional true
         :description "Why the held slot cannot be booked."))

;; Escalation to the professor in person once the student has declined the
;; offered slots, which ends the request.
(content failure :by Professor :when declined
         :terminates (open offer-due declined)
  (field reason :type string :max-length 300
         :description "Why the request goes to the professor, such as no offered slot working."))

(content cancel)

(rule "The student's request names only what the student wants to discuss; the professor proposes the times.")
(rule "The professor offers only free slots within the posted office hours.")
