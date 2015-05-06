(ns mesomatic.allocator
  "Resource Allocation")

(defprotocol IAllocator
  "Allocator abstraction to allow for pluggable
   allocation techniques."
  (allocate [this offers tasks]
    "Given a list of offers and tasks to start, figure out
     the best possible arrangement. Will yield a list of tasks
     containing an offerid to select or a nil if no way to fully
     fulfill requirements was found. Allocation should not yield partial
     results.

     Resources taken into account are:

     - cpus
     - mem
     - port ranges

     tasks is a list of mesomatic.types/TaskInfo
     offers is a list of mesomatic.types/Offer"))

(defn get-scalar
  [rlist name]
  (let [rmap (zipmap (map :name rlist)
                     (map :scalar rlist))]
    (get rmap name)))

(defn get-ranges
  [rlist name]
  (let [rmap (zipmap (map :name rlist)
                     (map :ranges rlist))]
    (get rmap name)))

(defn get-ports
  [rlist]
  (reduce
   + 0
   (for [{:keys [begin end] :as range} (get-ranges rlist "ports")
         :when range]
     (inc (- end begin)))))

(defn resource-factor
  "Compute an integral resource factor to help
   sort offers"
  [{:keys [resources]}]
  (+ (* (get-scalar resources "mem") 1000)
     (get-scalar resources "cpus")))

(defn resource-cmp
  "Comparator for resources. Will sort in decreasing order."
  [r1 r2]
  (pos?
   (- (resource-factor r1)
      (resource-factor r2))))

(defn offer-matches?
  "Predicate to validate an offer can satisfy a task's requirements"
  [offer task]
  (and ;; Check for valid executor
   (>= (get-scalar (:resources offer) "cpus")
       (get-scalar (:resources task)  "cpus"))
   (>= (get-scalar (:resources offer) "mem")
       (get-scalar (:resources task)  "mem"))
   (>= (get-ports (:resources offer))
       (get-ports (:resources task)))))

(defn accept-ports
  "Eat up as many ports as specified in the first port
   range. Notice that only the first port range is considered
   for now."
  [resources ports]
  (vec
   (for [{:keys [name ranges] :as resource} resources]
     (if (= name "ports")
       (let [offer-begin (-> ports first :begin)
             [{:keys [begin end]}] ranges]
         (assoc resource :ranges
                [{:begin (+ begin offer-begin)
                  :end   (+ end offer-begin)}]))
       resource))))

(defn map-ports
  [port-mappings ports]
  (let [begin (-> ports first :begin)]
    (vec
     (for [[{:keys [host-port container-port protocol]} i]
           (partition 2 (interleave port-mappings
                                    (range begin Long/MAX_VALUE)))]
       {:host-port (or host-port i)
        :container-port container-port
        :protocol protocol}))))

(defn accept-offer
  "Associate a task with an offer's corresponding slave.
   When allocating multiple instances of a task (for daemons) and
   the task-id field is a vector get the appropriate member of the vector"
  [offer task pos]
  (let [ports   (get-ranges (:resources offer) "ports")
        get-pos (fn [ids] (if (vector? ids) (nth ids pos) ids))]
    (-> task
        (assoc :slave-id (:slave-id offer))
        (assoc :offer-id (:id offer))
        (update :task-id get-pos pos)
        (update :resources accept-ports ports)
        (cond-> (= (-> task :container :type) :container-type-docker)
          (update-in [:container :docker :port-mappings] map-ports ports)))))

(defn adjust-ports
  "Adjust port range. Notice that only the first range
   is considered."
  [ranges want]
  (let [[{:keys [begin end]}] ranges]
    [{:begin (+ begin want) :end end}]))

(defn adjustor
  "Yield a function which will adjust a resource record
   when appropriate."
  [cpus mem ports]
  (fn [{:keys [type] :as record}]
    (cond
      (= type "mem")  (update record :scalar - mem)
      (= type "cpus") (update record :scalar - cpus)
      (= type "ports") (update record :ranges adjust-ports ports)
      :else record)))

(defn adjust-offer
  "If an offer has been accepted, decrease its available resources."
  [offer task]
  (let [cpus      (get-scalar (:resources task) "cpus")
        mem       (get-scalar (:resources task) "mem")
        ports     (get-ports (:resources task))
        resources (:resources offer)]
    (assoc offer :resources (map (adjustor cpus mem ports) resources))))

(defn allocate-task-naively
  "Allocate a task's worth of necessary resources.
   Tasks may ask for a specific count of instances
   and a maximum collocation factor to avoid behind
   hosted on a single slave.

   Allocation technique
   ====================

   Let's assume the following workload and available offers:

     [ T1(1,1,1,1) T2(4,8,2,1) T3(1,1,4,2) ]
     [ O1(4,8) O2(8,16) O3(2,2) ]

   We first sort our offers:

     [ T2(4,8,2,1) T3(1,1,4,2) T1(1,1,1,1) ]
     [ O1(8,16) O2(4,8) O3(2,2) ]

   We can now step through tasks and allocate appropriately:

     [ T3(1,1,4,2) T1(1,1,1,1) ]
     [ O1(4,8) O3(2,2) ]
     [ T1:(O1,O2) ]

     [ T1(1,1,1,1) ]
     [ O1(2,6) ]
     [ T2:(O1,O2) T3(O1,O3) ]

     [ ]
     [ O1(1,5) ]
     [ T2:(O1,O2) T3(O1,O3) T1(O1) ]"

  [acc {:keys [count] :or {count 1} :as task}]
  (loop [[offer & offers :as untouched] (:offers acc)
         payloads                       []
         adjusted                       nil
         [global local]                 [0 0]]
    (cond
      ;; Cannot properly allocate this task
      ;; Fail altogether.
      (nil? offer)
      (reduced (assoc acc :failed? true))

      ;; We're done with this task.
      (>= global count)
      (-> acc
          (assoc :offers (into (vec adjusted) untouched))
          (update :payloads into payloads))

      ;; Too many collocated tasks, skip to next offer.
      (and (:maxcol task) (>= local (:maxcol task)))
      (recur offers payloads (conj adjusted offer) [global 0])

      ;; We have a match, record it.
      (offer-matches? offer task)
      (recur (conj offers   (adjust-offer offer task))
             (conj payloads (accept-offer offer task global))
             adjusted
             [(inc global) (inc local)])

      ;; No match, let's move on.
      :else
      (recur offers payloads (conj adjusted offer) [global local]))))

(defn allocate-naively
  "Cycle through all tasks, sorted by biggest to smallest
   in terms of resource needs and try to satisfy requirements."
  [offers tasks]
  (let [res (reduce allocate-task-naively
                    {:offers (sort (comparator resource-cmp) offers)}
                    (sort (comparator resource-cmp) tasks))]
    (when-not (:failed? res)
      (:payloads res))))

(defn naive-allocator
  "Conform to IAllocator by handing off decisions to allocate-naively"
  []
  (reify IAllocator
    (allocate [this offers tasks]
      (allocate-naively offers tasks))))
