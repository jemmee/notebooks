#!/usr/local/bin/sbcl --script

;;;; brew install sbcl
;;;;
;;;; sbcl --script bloom_filter_test.lisp

;;;; 1. Setup Namespace
(defpackage :bloom-demo
  (:use :cl))
(in-package :bloom-demo)

;;;; 2. Data Structure Definition
(defstruct bloom-filter
  bits       ; The bit-vector
  size       ; Size of the bit-vector (m)
  hash-count ; Number of hash functions (k)
  )

;;;; 3. Core Logic
(defun create-bloom (m k)
  "Creates a Bloom Filter with size M and K hash functions."
  (make-bloom-filter :bits (make-array m :element-type 'bit :initial-element 0)
                     :size m
                     :hash-count k))

(defun get-hash (item seed max-size)
  "Simulates multiple hash functions using a seed and sxhash."
  (mod (sxhash (format nil "~A-~A" item seed)) max-size))

(defun bloom-add (bf item)
  "Adds an item to the Bloom Filter by setting K bits to 1."
  (dotimes (i (bloom-filter-hash-count bf))
    (let ((idx (get-hash item i (bloom-filter-size bf))))
      (setf (bit (bloom-filter-bits bf) idx) 1))))

(defun bloom-exists-p (bf item)
  "Returns T if item might be in the set, NIL if definitely not."
  (dotimes (i (bloom-filter-hash-count bf))
    (let ((idx (get-hash item i (bloom-filter-size bf))))
      (when (= (bit (bloom-filter-bits bf) idx) 0)
        (return-from bloom-exists-p nil))))
  t)

;;;; 4. Execution Demo
(defun run-demo ()
  (let ((bf (create-bloom 20 3))) ; Small size to demonstrate collisions/overlap
    (format t "==========================================~%")
    (format t "BLOOM FILTER DEMO (Size: 20, Hashes: 3)~%")
    (format t "==========================================~%~%")
    
    (format t "--- Adding items ---~%")
    (dolist (items '("Ark" "Manna" "Staff" "Covenant" "Myrrh"))
      (bloom-add bf items)
      (format t "Added: ~A~%" items))

    (format t "~%Bit Array State: ~A~%" (bloom-filter-bits bf))

    (format t "~%--- Testing Membership ---~%")
    (dolist (test '("Manna" "Staff" "Goliath" "Trumpet"))
      (let ((exists (bloom-exists-p bf test)))
        (format t "Checking '~A': ~A~%" 
                (format nil "~12A" test) 
                (if exists "PROBABLY PRESENT (OR FALSE POSITIVE)" "DEFINITELY ABSENT"))))
    (format t "~%")))

;; Start the demo automatically
(run-demo)