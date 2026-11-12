;; office-hours: a student asks a professor to meet about a topic. The
;; professor offers a few slots, and the student names every one it can make,
;; or none. The professor books nothing yet: it holds the request and tells the
;; student when to check back, so it hears from every student before it
;; books. When the student checks back, the professor confirms one of the
;; student's slots, offers others while a round is left, or refuses. When the
;; student can make none of the slots, the professor offers once more or
;; escalates to the professor in person.
;;
;; Each act starts and ends facts, and the facts that hold decide which acts
;; may follow:
;;   open          a request is in progress
;;   offer-due     the professor offers slots
;;   offered       slots are out; the student names the ones it can make, or none
;;   declined      the student can make none of the offered slots
;;   hold-due      the student named its slots; the professor holds the request
;;   held          the request is held; the student checks back
;;   decision-due  the student checked back; the professor confirms, offers
;;                 again or refuses
(norm office-hours :initiator Student :participant Professor
      :description "A student asks to meet about a topic; the professor offers slots, the student names the ones it can make, and the professor holds the request until the student checks back, then confirms one, offers again or refuses.")

(content request :by Student :unless open :initiates (open offer-due)
  (field topic :type string :max-length 300
         :description "What the student wants to discuss."))

;; At most two rounds of offers in one request.
(content propose :by Professor :when offer-due :max 2
         :initiates offered :terminates (offer-due decision-due declined)
  (field option1 :type string :max-length 80 :description "A slot the professor offers.")
  (field option2 :type string :max-length 80 :description "A second slot.")
  (field option3 :type string :max-length 80 :optional true
         :description "A third slot."))

(content accept-proposal :by Student :when offered
         :initiates hold-due :terminates offered
  (field available :type multi-select :choices (1 2 3)
         :description "The number of every offered slot the student can make."))

;; None of the above.
(content reject-proposal :by Student :when offered
         :initiates (offer-due declined) :terminates offered
  (field reason :type string :max-length 300 :optional true
         :description "Why none of the slots works."))

;; The professor books nothing yet; the student checks back after the given
;; minutes.
(content agree :by Professor :when hold-due
         :initiates held :terminates hold-due
  (field recheck-after :type integer :minimum 1 :maximum 1440
         :description "Minutes after which the student checks back."))

;; The student checks back. The professor then confirms one of the student's
;; slots, offers again while a round is left, or refuses.
(content query-if :by Student :when held
         :initiates (decision-due offer-due) :terminates held)

(content confirm :by Professor :when decision-due
         :terminates (open decision-due offer-due)
  (field slot :type string :max-length 80
         :description "The booked slot, one the student named."))

;; None of the student's slots is still free.
(content refuse :by Professor :when decision-due
         :terminates (open decision-due offer-due)
  (field reason :type string :max-length 300 :optional true
         :description "Why none of the student's slots can be booked."))

;; Escalation to the professor in person once the student can make none of
;; the offered slots, which ends the request.
(content failure :by Professor :when declined
         :terminates (open offer-due declined)
  (field reason :type string :max-length 300
         :description "Why the request goes to the professor, such as no offered slot working."))

(content cancel)

(rule :act request "The topic names only what the student wants to discuss. It gives no times, dates or availability, because the professor proposes the times.")
