(ns mesomatic.types
  "Utility functions to convert to and from mesos types."
  (:import org.apache.mesos.Protos$Status
           org.apache.mesos.Protos$FrameworkID
           org.apache.mesos.Protos$OfferID
           org.apache.mesos.Protos$SlaveID
           org.apache.mesos.Protos$TaskID
           org.apache.mesos.Protos$ExecutorID
           org.apache.mesos.Protos$ContainerID
           org.apache.mesos.Protos$FrameworkInfo
           org.apache.mesos.Protos$FrameworkInfo$Capability
           org.apache.mesos.Protos$FrameworkInfo$Capability$Type
           org.apache.mesos.Protos$HealthCheck
           org.apache.mesos.Protos$HealthCheck$HTTP
           org.apache.mesos.Protos$CommandInfo
           org.apache.mesos.Protos$CommandInfo$URI
           org.apache.mesos.Protos$ExecutorInfo
           org.apache.mesos.Protos$MasterInfo
           org.apache.mesos.Protos$SlaveInfo
           org.apache.mesos.Protos$Value
           org.apache.mesos.Protos$Value$Type
           org.apache.mesos.Protos$Value$Scalar
           org.apache.mesos.Protos$Value$Range
           org.apache.mesos.Protos$Value$Ranges
           org.apache.mesos.Protos$Value$Set
           org.apache.mesos.Protos$Value$Text
           org.apache.mesos.Protos$Attribute
           org.apache.mesos.Protos$Resource
           org.apache.mesos.Protos$ResourceStatistics
           org.apache.mesos.Protos$ResourceUsage
           org.apache.mesos.Protos$PerfStatistics
           org.apache.mesos.Protos$Request
           org.apache.mesos.Protos$Offer
           org.apache.mesos.Protos$Offer$Operation
           org.apache.mesos.Protos$Offer$Operation$Launch
           org.apache.mesos.Protos$Offer$Operation$Reserve
           org.apache.mesos.Protos$Offer$Operation$Unreserve
           org.apache.mesos.Protos$Offer$Operation$Create
           org.apache.mesos.Protos$Offer$Operation$Destroy
           org.apache.mesos.Protos$Offer$Operation$Type
           org.apache.mesos.Protos$TaskInfo
           org.apache.mesos.Protos$TaskState
           org.apache.mesos.Protos$TaskStatus
           org.apache.mesos.Protos$TaskStatus$Source
           org.apache.mesos.Protos$TaskStatus$Reason
           org.apache.mesos.Protos$Filters
           org.apache.mesos.Protos$Environment
           org.apache.mesos.Protos$Environment$Variable
           org.apache.mesos.Protos$Parameter
           org.apache.mesos.Protos$Parameters
           org.apache.mesos.Protos$Credential
           org.apache.mesos.Protos$Credentials
           org.apache.mesos.Protos$RateLimit
           org.apache.mesos.Protos$RateLimits
           org.apache.mesos.Protos$TimeInfo
           org.apache.mesos.Protos$DurationInfo
           org.apache.mesos.Protos$Address
           org.apache.mesos.Protos$URL
           org.apache.mesos.Protos$Unavailability
           org.apache.mesos.Protos$MachineID
           org.apache.mesos.Protos$MachineInfo
           org.apache.mesos.Protos$MachineInfo$Mode
           org.apache.mesos.Protos$Volume
           org.apache.mesos.Protos$Volume$Mode
           org.apache.mesos.Protos$ContainerInfo
           org.apache.mesos.Protos$ContainerInfo$Type
           org.apache.mesos.Protos$ContainerInfo$DockerInfo
           org.apache.mesos.Protos$ContainerInfo$DockerInfo$Network
           org.apache.mesos.Protos$ContainerInfo$DockerInfo$PortMapping
           org.apache.mesos.Protos$Port
           org.apache.mesos.Protos$Ports
           org.apache.mesos.Protos$DiscoveryInfo
           org.apache.mesos.Protos$DiscoveryInfo$Visibility
           org.apache.mesos.Protos$Label
           org.apache.mesos.Protos$Labels
           ))

;; Our two exported signatures: data->pb and pb->data

(defprotocol Serializable
  "Interface to convert from clojure data to mesos protobuf
   payloads."
  (data->pb [this]))

(defmulti pb->data
  "Open protocol to convert from mesos protobuf to clojure"
  class)

(declare ->pb)


;; Status
;; ======
;;
;; Enums are a bit of a special case since we don't wrap
;; them in a record, instead just yield a keyword which
;; will can be converted back with the extend-protocol
;; trick later on.

(defmethod pb->data Protos$Status
  [^Protos$Status status]
  (cond
    (= status Protos$Status/DRIVER_RUNNING)     :driver-running
    (= status Protos$Status/DRIVER_NOT_STARTED) :driver-not-started
    (= status Protos$Status/DRIVER_ABORTED)     :driver-aborted
    (= status Protos$Status/DRIVER_STOPPED)     :driver-stopped
    :else status))

;; FrameworkID
;; ===========
;;
;; All payloads will follow this idiom of defining
;; a record which is serializable and have deserialization
;; yield an instance of that protocol.
(defrecord FrameworkID [value]
  Serializable
  (data->pb [this]
    (-> (Protos$FrameworkID/newBuilder)
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$FrameworkID
  [^Protos$FrameworkID id]
  (FrameworkID. (.getValue id)))

;; OfferID
;; =======

(defrecord OfferID [value]
  Serializable
  (data->pb [this]
    (-> (Protos$OfferID/newBuilder)
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$OfferID
  [^Protos$OfferID id]
  (OfferID. (.getValue id)))

;; SlaveID
;; =======

(defrecord SlaveID [value]
  Serializable
  (data->pb [this]
    (-> (Protos$SlaveID/newBuilder)
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$SlaveID
  [^Protos$SlaveID id]
  (SlaveID. (.getValue id)))

;; TaskID
;; ======

(defrecord TaskID [value]
  Serializable
  (data->pb [this]
    (-> (Protos$TaskID/newBuilder)
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$TaskID
  [^Protos$TaskID id]
  (TaskID. (.getValue id)))

;; ExecutorID
;; ==========

(defrecord ExecutorID [value]
  Serializable
  (data->pb [this]
    (-> (Protos$ExecutorID/newBuilder)
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$ExecutorID
  [^Protos$ExecutorID id]
  (ExecutorID. (.getValue id)))

;; ContainerID
;; ===========

(defrecord ContainerID [value]
  Serializable
  (data->pb [this]
    (-> (Protos$ContainerID/newBuilder)
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$ContainerID
  [^Protos$ContainerID id]
  (ContainerID. (.getValue id)))

;; TimeInfo
;; ========

(defrecord TimeInfo [nanoseconds]
  Serializable
  (data->pb [this]
    (-> (Protos$TimeInfo/newBuilder)
        (.setNanoseconds (long nanoseconds))
        (.build))))

(defmethod pb->data Protos$TimeInfo
  [^Protos$TimeInfo time]
  (TimeInfo. (.getNanoseconds time)))

;; DurationInfo
;; ============

(defrecord DurationInfo [nanoseconds]
  Serializable
  (data->pb [this]
    (-> (Protos$DurationInfo/newBuilder)
        (.setNanoseconds (long nanoseconds))
        (.build))))

(defmethod pb->data Protos$DurationInfo
  [^Protos$DurationInfo duration]
  (DurationInfo. (.getNanoseconds duration)))

;; Address
;; =======

(defrecord Address [hostname ip port]
  Serializable
  (data->pb [this]
    (-> (Protos$Address/newBuilder)
        (.setPort (int port))
        (cond->
            hostname (.setHostname (str hostname))
            ip       (.setIp (str ip)))
        (.build))))

(defmethod pb->data Protos$Address
  [^Protos$Address address]
  (Address. (.getHostname address) (.getIp address) (.getPort address)))

;; URL
;; ===

(defrecord URL [scheme address path query fragment]
  Serializable
  (data->pb [this]
    (-> (Protos$URL/newBuilder)
        (.setScheme (str scheme))
        (.setAddress (->pb :Address address))
        (.addAllQueries (mapv (partial ->pb :Parameter) query))
        (cond-> fragment (.setFragment (str fragment)))
        (.build))))

(defmethod pb->data Protos$URL
  [^Protos$URL url]
  (URL. (.getScheme url)
        (pb->data (.getAddress url))
        (.getPath url)
        (mapv pb->data (.getQueriesList url))
        (.getFragment url)))

;; Unavailability
;; ==============

(defrecord Unavailability [start duration]
  Serializable
  (data->pb [this]
    (-> (Protos$Unavailability/newBuilder)
        (.setStart (->pb :TimeInfo start))
        (cond-> duration (.setDuration (->pb :Duration duration)))
        (.build))))

(defmethod pb->data Protos$Unavailability
  [^Protos$Unavailability una]
  (Unavailability. (.getStart una) (.getDuration una)))


;; MachineID
;; =========

(defrecord MachineID [hostname ip]
  Serializable
  (data->pb [this]
    (-> (Protos$MachineID/newBuilder)
        (cond-> hostname (.setHostname (str hostname))
                ip       (.setIp (str ip)))
        (.build))))

(defmethod pb->data Protos$MachineID
  [^Protos$MachineID mid]
  (MachineID. (.getHostname mid) (.getIp mid)))

;; MachineInfo
;; ===========

(defmethod pb->data Protos$MachineInfo$Mode
  [^Protos$MachineInfo$Mode mode]
  (cond
    (= Protos$MachineInfo$Mode/UP mode)       :machine-mode-up
    (= Protos$MachineInfo$Mode/DRAINING mode) :machine-mode-draining
    (= Protos$MachineInfo$Mode/DOWN mode)     :machine-mode-down))

(defrecord MachineInfo [id mode unavailability]
  Serializable
  (data->pb [this]
    (-> (Protos$MachineInfo/newBuilder)
        (.setId (->pb :MachineID id))
        (cond->
            mode           (.setMode (data->pb mode))
            unavailability (.setUnavailability
                            (->pb :Unavailability unavailability))))))

(defmethod pb->data Protos$MachineInfo
  [^Protos$MachineInfo minfo]
  (MachineInfo. (pb->data (.getId minfo))
                (pb->data (.getMode minfo))
                (pb->data (.getUnavailability minfo))))

;; FrameworkInfo
;; =============

(defrecord FrameworkCapability [type]
  Serializable
  (data->pb [this]
    (-> (Protos$FrameworkInfo$Capability/newBuilder)
        (.setType (data->pb type))
        (.build))))

(defmethod pb->data Protos$FrameworkInfo$Capability
  [^Protos$FrameworkInfo$Capability capa]
  (FrameworkCapability. (pb->data (.getType capa))))

(defmethod pb->data Protos$FrameworkInfo$Capability$Type
  [^Protos$FrameworkInfo$Capability$Type type]
  (cond
    (= Protos$FrameworkInfo$Capability$Type/TASK_KILLING_STATE type)
    :framework-capability-task-killing-state
    (= Protos$FrameworkInfo$Capability$Type/GPU_RESOURCES type)
    :framework-capability-gpu-resources
    (= Protos$FrameworkInfo$Capability$Type/REVOCABLE_RESOURCES type)
    :framework-capability-revocable-resource))

(defrecord FrameworkInfo [user name id failover-timeout checkpoint role
                          hostname principal webui-url capabilities labels]
  Serializable
  (data->pb [this]
    (-> (Protos$FrameworkInfo/newBuilder)
        (.setUser (str (or user "")))
        (.setName (str name))
        (cond->
            id               (.setId (->pb :FrameworkID id))
            failover-timeout (.setFailoverTimeout (double failover-timeout))
            (not (nil? checkpoint)) (.setCheckpoint (boolean checkpoint))
            role             (.setRole (str role))
            hostname         (.setHostname (str hostname))
            principal        (.setPrincipal (str principal))
            webui-url        (.setWebuiUrl (str webui-url))
            labels           (.setLabels (->pb :Labels labels)))
        (.addAllCapabilities (mapv (partial ->pb :FrameworkCapability)
                                   capabilities))
        (.build))))

(defmethod pb->data Protos$FrameworkInfo
  [^Protos$FrameworkInfo info]
  (FrameworkInfo.
   (.getUser info)
   (.getName info)
   (when-let [id (.getId info)] (pb->data id))
   (.getFailoverTimeout info)
   (.getCheckpoint info)
   (.getRole info)
   (.getHostname info)
   (.getPrincipal info)
   (.getWebuiUrl info)
   (when-let [labels (.getLabels info)] (pb->data labels))
   (mapv pb->data (.getCapabilitiesList info))))

;; HealthCheck
;; ===========

(defrecord HealthCheckHTTP [port path statuses]
  Serializable
  (data->pb [this]
    (-> (Protos$HealthCheck$HTTP/newBuilder)
        (.setPort (int port))
        (cond-> path (.setPath (str path)))
        (.addAllStatuses (mapv int statuses))
        (.build))))

(defmethod pb->data Protos$HealthCheck$HTTP
  [^Protos$HealthCheck$HTTP http]
  (HealthCheckHTTP.
   (.getPort http)
   (.getPath http)
   (.getStatusesList http)))

(defrecord HealthCheck [http delay-seconds interval-seconds
                        timeout-seconds consecutive-failures
                        grace-period-seconds command]
  Serializable
  (data->pb [this]
    (-> (Protos$HealthCheck/newBuilder)
        (cond->
            http                 (.setHttp (->pb :HealthCheckHTTP http))
            delay-seconds        (.setDelaySeconds (double delay-seconds))
            interval-seconds     (.setIntervalSeconds (double interval-seconds))
            timeout-seconds      (.setTimeoutSeconds (double timeout-seconds))
            consecutive-failures (.setConsecutiveFailures
                                  (int consecutive-failures))
            grace-period-seconds (.setGracePeriodSeconds
                                  (double grace-period-seconds))
            command              (.setCommand (->pb :CommandInfo command)))
        (.build))))

(defmethod pb->data Protos$HealthCheck
  [^Protos$HealthCheck check]
  (HealthCheck.
   (when-let [http (.getHttp check)] (pb->data http))
   (.getDelaySeconds check)
   (.getIntervalSeconds check)
   (.getTimeoutSeconds check)
   (.getConsecutiveFailures check)
   (.getGracePeriodSeconds check)
   (when-let [cmd (.getCommand check)] (pb->data cmd))))

;; CommandInfo
;; ===========

(defrecord URI [value executable extract cache]
  Serializable
  (data->pb [this]
    (-> (Protos$CommandInfo$URI/newBuilder)
        (.setValue (str value))
        (cond->
            (not (nil? executable)) (.setExecutable (boolean executable))
            (not (nil? extract))    (.setExtract (boolean extract))
            (not (nil? cache))      (.setCache (boolean cache)))
        (.build))))

(defmethod pb->data Protos$CommandInfo$URI
  [^Protos$CommandInfo$URI uri]
  (URI. (.getValue uri) (.getExecutable uri) (.getExtract uri) (.getCache uri)))

(defrecord CommandInfo [uris environment shell value arguments user]
  Serializable
  (data->pb [this]
    (-> (Protos$CommandInfo/newBuilder)
        (cond->
            environment        (.setEnvironment (->pb :Environment environment))
            (not (nil? shell)) (.setShell (boolean shell))
            value              (.setValue (str value))
            user               (.setUser (str user)))
        (.addAllArguments (map str arguments))
        (.addAllUris (map (partial ->pb :URI) uris))
        (.build))))

(defmethod pb->data Protos$CommandInfo
  [^Protos$CommandInfo info]
  (CommandInfo.
   (map pb->data (.getUrisList info))
   (when-let [env (.getEnvironment info)] (pb->data env))
   (.getShell info)
   (.getValue info)
   (.getArgumentsList info)
   (.getUser info)))

;; ExecutorInfo
;; ============

(defrecord ExecutorInfo [executor-id framework-id command container
                         resources name source data discovery]
  Serializable
  (data->pb [this]
    (-> (Protos$ExecutorInfo/newBuilder)
        (.setExecutorId (->pb :ExecutorID executor-id))
        (.setCommand (->pb :CommandInfo command))
        (cond->
            framework-id (.setFrameworkId (->pb :FrameworkID framework-id))
            container    (.setContainer (->pb :ContainerInfo container))
            name         (.setName (str name))
            source       (.setSource (str source))
            data         (.setData data)
            discovery    (.setDiscovery discovery))
        (.addAllResources (mapv (partial ->pb :Resource) resources))
        (.build))))

(defmethod pb->data Protos$ExecutorInfo
  [^Protos$ExecutorInfo info]
  (ExecutorInfo.
   (pb->data (.getExecutorId info))
   (when-let [id (.getFrameworkId info)] (pb->data id))
   (pb->data (.getCommand info))
   (when-let [cnt (.getContainer info)] (pb->data cnt))
   (map pb->data (.getResourcesList info))
   (.getName info)
   (.getSource info)
   (.getData info)
   (when-let [disco (.getDiscovery info)] (pb->data disco))))

;; MasterInfo
;; ==========

(defrecord MasterInfo [id ip port pid hostname version address]
  Serializable
  (data->pb [this]
    (-> (Protos$MasterInfo/newBuilder)
        (.setId (str id))
        (.setIp (int ip))
        (.setPort (int port))
        (cond->
            pid      (.setPid (str pid))
            hostname (.setHostname (str hostname))
            version  (.setVersion (str version))
            address  (.setAddress (->pb :Address address)))
        (.build))))

(defmethod pb->data Protos$MasterInfo
  [^Protos$MasterInfo info]
  (MasterInfo.
   (.getId info)
   (.getIp info)
   (.getPort info)
   (.getPid info)
   (.getHostname info)
   (.getVersion info)
   (when-let [address (.getAddress info)] (pb->data address))))

;; SlaveInfo
;; =========

(defrecord SlaveInfo [hostname port resources attributes id checkpoint]
  Serializable
  (data->pb [this]
    (-> (Protos$SlaveInfo/newBuilder)
        (.setHostname (str hostname))
        (cond->
            port                    (.setPort (int port))
            id                      (.setId (->pb :SlaveID id))
            (not (nil? checkpoint)) (.setCheckPoint (boolean checkpoint)))
        (.addAllResources (mapv (partial ->pb :Resource) resources))
        (.addAllAttributes (mapv (partial ->pb :Attribute) attributes))
        (.build))))

(defmethod pb->data Protos$SlaveInfo
  [^Protos$SlaveInfo info]
  (SlaveInfo.
   (.getHostname info)
   (.getPort info)
   (mapv pb->data (.getResourcesList info))
   (mapv pb->data (.getAttributesList info))
   (when-let [id (.getId info)] (pb->data id))
   (.getCheckpoint info)))

;; Value
;; =====

(defmethod pb->data Protos$Value$Type
  [^Protos$Value$Type type]
  (cond
    (= type Protos$Value$Type/SCALAR) :value-scalar
    (= type Protos$Value$Type/RANGES) :value-ranges
    (= type Protos$Value$Type/SET)    :value-set
    (= type Protos$Value$Type/TEXT)   :value-text
    :else type))

(defmethod pb->data Protos$Value$Scalar
  [^Protos$Value$Scalar scalar]
  (.getValue scalar))

(defrecord ValueRange [begin end]
  Serializable
  (data->pb [this]
    (-> (Protos$Value$Range/newBuilder)
        (.setBegin (long begin))
        (.setEnd (long end))
        (.build))))

(defmethod pb->data Protos$Value$Range
  [^Protos$Value$Range range]
  (ValueRange. (.getBegin range) (.getEnd range)))

(defrecord ValueRanges [ranges]
  Serializable
  (data->pb [this]
    (-> (Protos$Value$Ranges/newBuilder)
        (.addAllRange (mapv (partial ->pb :ValueRange) ranges))
        (.build))))

(defmethod pb->data Protos$Value$Ranges
  [^Protos$Value$Ranges ranges]
  (ValueRanges. (.getRangeList ranges)))

(defmethod pb->data Protos$Value$Set
  [^Protos$Value$Set value-set]
  (set (.getItemList value-set)))

(defmethod pb->data Protos$Value$Text
  [^Protos$Value$Text x]
  (.getValue x))

(defrecord Value [type scalar ranges set text]
  Serializable
  (data->pb [this]
    (-> (Protos$Value/newBuilder)
        (.setType (data->pb type))
        (cond->
            scalar (.setScalar (data->pb scalar))
            ranges (.setRanges (data->pb (ValueRanges. ranges)))
            set    (.setSet (data->pb set))
            text   (.setText (data->pb text)))
        (.build))))

(defmethod pb->data Protos$Value
  [^Protos$Value v]
  (Value.
   (pb->data (.getType v))
   (pb->data (.getScalar v))
   (when-let [ranges (.getRanges v)]
     (map pb->data (.getRangeList ranges)))
   (when-let [s (.getSet v)] (pb->data s))
   (when-let [t (.getText v)] (pb->data t))))

;; Attribute
;; =========

(defrecord Attribute [name type scalar ranges set text]
  Serializable
  (data->pb [this]
    (-> (Protos$Attribute/newBuilder)
        (.setName name)
        (.setType (data->pb type))
        (cond->
            scalar (.setScalar (data->pb scalar))
            ranges (.setRanges (data->pb (ValueRanges. ranges)))
            set    (.setSet (data->pb set))
            text   (.setText (data->pb text)))
        (.build))))

(defmethod pb->data Protos$Attribute
  [^Protos$Attribute v]
  (Attribute.
   (.getName v)
   (pb->data (.getType v))
   (pb->data (.getScalar v))
   (when-let [ranges (.getRanges v)]
     (map pb->data (.getRangeList ranges)))
   (when-let [s (.getSet v)] (pb->data s))
   (when-let [t (.getText v)] (pb->data t))))

;; Resource
;; ========

(defrecord Resource [name type scalar ranges set role]
  Serializable
  (data->pb [this]
    (-> (Protos$Resource/newBuilder)
        (.setName name)
        (.setType (data->pb type))
        (cond->
            scalar (.setScalar (data->pb scalar))
            ranges (.setRanges (data->pb (ValueRanges. ranges)))
            (seq set)    (.setSet (data->pb set))
            role   (.setRole role))
        (.build))))

(defmethod pb->data Protos$Resource
  [^Protos$Resource v]
  (Resource.
   (.getName v)
   (pb->data (.getType v))
   (pb->data (.getScalar v))
   (when-let [ranges (.getRanges v)]
     (mapv pb->data (.getRangeList ranges)))
   (when-let [s (.getSet v)] (pb->data s))
   (.getRole v)))

;; ResourceStatistics
;; ==================

(defrecord ResourceStatistics [timestamp cpus-user-time-secs
                               cpus-system-time-secs cpus-limit
                               cpus-nr-periods cpus-nr-throttled
                               cpus-throttled-time-secs
                               mem-rss-bytes mem-limit-bytes
                               mem-file-bytes mem-anon-bytes
                               mem-mapped-file-bytes perf
                               net-rx-packets net-rx-bytes
                               net-rx-errors net-rx-dropped
                               net-tx-packets net-tx-bytes
                               net-tx-errors net-tx-dropped
                               net-tcp-rtt-microsecs-p50
                               net-tcp-rtt-microsecs-p90
                               net-tcp-rtt-microsecs-p95
                               net-tcp-rtt-microsecs-p99]
  Serializable
  (data->pb [this]
    (-> (Protos$ResourceStatistics/newBuilder)
        (.setTimestamp (double timestamp))
        (cond->
            cpus-user-time-secs       (.setCpusUserTimeSecs
                                       (double cpus-user-time-secs))
            cpus-system-time-secs     (.setCpusSystemTimeSecs
                                       (double cpus-system-time-secs))
            cpus-limit                (.setCpusLimit (double cpus-limit))
            cpus-nr-periods           (.setCpusNrPeriods
                                       (int cpus-nr-periods))
            cpus-nr-throttled         (.setCpusNrThrottled
                                       (int cpus-nr-throttled))
            cpus-throttled-time-secs  (.setCpusThrottledTimeSecs
                                       (double cpus-throttled-time-secs))
            mem-rss-bytes              (.setMemRssBytes (long mem-rss-bytes))
            mem-limit-bytes            (.setMemLimitBytes
                                        (long mem-limit-bytes))
            mem-file-bytes            (.setMemFileBytes
                                        (long mem-file-bytes))
            mem-anon-bytes            (.setMemAnonBytes
                                        (long mem-anon-bytes))
            mem-mapped-file-bytes     (.setMemMappedFileBytes
                                       (long mem-mapped-file-bytes))
            perf                      (.setPerf (pb->data perf))
            net-rx-packets            (.setNetRxPackets (long net-rx-packets))
            net-rx-bytes              (.setNetRxBytes (long net-rx-bytes))
            net-rx-errors             (.setNetRxErrors (long net-rx-errors))
            net-rx-dropped            (.setNetRxDropped (long net-rx-dropped))
            net-tx-packets            (.setNetRxPackets (long net-tx-packets))
            net-tx-bytes              (.setNetRxBytes (long net-tx-bytes))
            net-tx-errors             (.setNetRxErrors (long net-tx-errors))
            net-tx-dropped            (.setNetRxDropped (long net-tx-dropped))
            net-tcp-rtt-microsecs-p50 (.setNetTcpRttMicrosecsP50
                                       (double net-tcp-rtt-microsecs-p50))
            net-tcp-rtt-microsecs-p90 (.setNetTcpRttMicrosecsP90
                                       (double net-tcp-rtt-microsecs-p90))
            net-tcp-rtt-microsecs-p95 (.setNetTcpRttMicrosecsP95
                                       (double net-tcp-rtt-microsecs-p95))
            net-tcp-rtt-microsecs-p99 (.setNetTcpRttMicrosecsP99
                                       (double net-tcp-rtt-microsecs-p99)))
        (.build))))

(defmethod pb->data Protos$ResourceStatistics
  [^Protos$ResourceStatistics s]
  (ResourceStatistics.
   (.getTimestamp s)
   (.getCpusUserTimeSecs s)
   (.getCpusSystemTimeSecs s)
   (.getCpusLimit s)
   (.getCpusNrPeriods s)
   (.getCpusNrThrottled s)
   (.getCpusThrottledTimeSecs s)
   (.getMemRssBytes s)
   (.getMemLimitBytes s)
   (.getMemFileBytes s)
   (.getMemAnonbytes s)
   (.getMemMappedfileBytes s)
   (when-let [p (.getPerf s)] (pb->data p))
   (.getNetRxPackets s)
   (.getNetRxBytes s)
   (.getNetRxErrors s)
   (.getNetRxDropped s)
   (.getNetTxPackets s)
   (.getNetTxBytes s)
   (.getNetTxErrors s)
   (.getNetTxDropped s)
   (.getNetTcpRttMicrosecsP50 s)
   (.getNetTcpRttMicrosecsP90 s)
   (.getNetTcpRttMicrosecsP95 s)
   (.getNetTcpRttMicrosecsP99 s)))

;; ResourceUsage
;; =============

(defrecord ResourceUsage [slave-id framework-id executor-id executor-name
                          task-id statistics]
  Serializable
  (data->pb [this]
    (-> (Protos$ResourceUsage/newBuilder)
        (.setSlaveId (data->pb slave-id))
        (.setFrameworkId (data->pb framework-id))
        (cond->
            executor-id   (.setExecutorId (data->pb executor-id))
            executor-name (.setExecutorName (str executor-name))
            task-id       (.setTaskid (data->pb task-id))
            statistics    (.setStatistics (data->pb statistics)))
        (.build))))

(defmethod pb->data Protos$ResourceUsage
  [^Protos$ResourceUsage usage]
  (ResourceUsage.
   (pb->data (.getSlaveId usage))
   (pb->data (.getFrameworkId usage))
   (when-let [id (.getExecutorId usage)] (pb->data id))
   (.getExecutorName usage)
   (when-let [id (.getTaskId usage)] (pb->data id))
   (when-let [stats (.getResourceStatistics usage)] (pb->data stats))))

;; PerfStatistics
;; ==============

(defrecord PerfStatistics [timestamp duration cycles
                           stalled-cycles-frontend
                           stalled-cycles-backend
                           instructions cache-references
                           cache-misses branches branch-misses
                           bus-cycles ref-cycles cpu-clock task-clock
                           page-faults minor-faults major-faults
                           context-switches cpu-migrations
                           alignment-faults emulation-faults
                           l1-dcache-loads l1-dcache-load-misses
                           l1-dcache-stores l1-dcache-store-misses
                           l1-dcache-prefetches l1-dcache-prefetch-misses
                           l1-icache-loads l1-icache-load-misses
                           l1-icache-prefetches l1-icache-prefetch-misses
                           llc-loads llc-load-misses
                           llc-stores llc-store-misses
                           llc-prefetches llc-prefetch-misses
                           dtlb-loads dtlb-load-misses
                           dtlb-stores dtlb-store-misses
                           dtlb-prefetches dtlb-prefetch-misses
                           itlb-loads itlb-load-misses
                           branch-loads branch-load-misses
                           node-loads node-load-misses
                           node-stores node-store-misses
                           node-prefetches node-prefetch-misses]

  Serializable
  (data->pb [this]
    (let [b (-> (Protos$PerfStatistics/newBuilder)
                (.setTimestamp (double timestamp))
                (.setDuration (double duration)))]

      (when cycles
        (.setCycles b (long cycles)))
      (when stalled-cycles-frontend
        (.setStalledCyclesFrontend b (long stalled-cycles-frontend)))
      (when stalled-cycles-backend
        (.setStalledCyclesBackend b (long stalled-cycles-backend)))
      (when instructions
        (.setInstructions b (long instructions)))
      (when cache-references
        (.setCacheReferences b (long cache-references)))
      (when cache-misses
        (.setCacheMisses b (long cache-misses)))
      (when branches
        (.setBranches b (long branches)))
      (when branch-misses
        (.setBranchMisses b (long branch-misses)))
      (when bus-cycles
        (.setBusCycles b (long bus-cycles)))
      (when ref-cycles
        (.setRefCycles b (long ref-cycles)))
      (when cpu-clock
        (.setCpuClock b (double cpu-clock)))
      (when task-clock
        (.setTaskClock b (double task-clock)))
      (when page-faults
        (.setPageFaults b (long page-faults)))
      (when major-faults
        (.setMajorFaults b (long major-faults)))
      (when context-switches
        (.setContextSwitches b (long context-switches)))
      (when  cpu-migrations
        (.setCpuMigrations b (long cpu-migrations)))
      (when alignment-faults
        (.setAlignmentFaults b (long alignment-faults)))
      (when emulation-faults
        (.setEmulationFaults b (long emulation-faults)))
      (when l1-dcache-loads
        (.setL1DcacheLoads b (long l1-dcache-loads)))
      (when l1-dcache-load-misses
        (.setL1DcacheLoadMisses b (long l1-dcache-load-misses)))
      (when l1-dcache-stores
        (.setL1DcacheStores b (long l1-dcache-stores)))
      (when l1-dcache-store-misses
        (.setL1DcacheStoreMisses b (long l1-dcache-store-misses)))
      (when l1-dcache-prefetches
        (.setL1DcachePrefetches b (long l1-dcache-prefetches)))
      (when l1-dcache-prefetch-misses
        (.setL1DcachePrefetchMisses b (long l1-dcache-prefetch-misses)))
      (when l1-icache-loads
        (.setL1IcacheLoads b (long l1-icache-loads)))
      (when l1-icache-load-misses
        (.setL1IcacheLoadMisses b (long l1-icache-load-misses)))
      (when l1-icache-prefetches
        (.setL1IcachePrefetches b (long l1-icache-prefetches)))
      (when l1-icache-prefetch-misses
        (.setL1IcachePrefetchMisses b (long l1-icache-prefetch-misses)))
      (when llc-loads
        (.setLlcLoads b (long llc-loads)))
      (when llc-load-misses
        (.setLlcLoadMisses b (long llc-load-misses)))
      (when llc-stores
        (.setLlcStores b (long llc-stores)))
      (when llc-store-misses
        (.setLlcStoreMisses b (long llc-store-misses)))
      (when llc-prefetches
        (.setLlcPrefetches b (long llc-prefetches)))
      (when llc-prefetch-misses
        (.setLlcPrefetchMisses b (long llc-prefetch-misses)))
      (when dtlb-loads
        (.setDtlbLoads b (long dtlb-loads)))
      (when dtlb-load-misses
        (.setDtlbLoadMisses b (long dtlb-load-misses)))
      (when dtlb-stores
        (.setDtlbStores b (long dtlb-stores)))
      (when dtlb-store-misses
        (.setDtlbStoreMisses b (long dtlb-store-misses)))
      (when dtlb-prefetches
        (.setDtlbPrefetches b (long dtlb-prefetches)))
      (when dtlb-prefetch-misses
        (.setDtlbPrefetchMisses b (long dtlb-prefetch-misses)))
      (when itlb-loads
        (.setItlbLoads b (long itlb-loads)))
      (when itlb-load-misses
        (.setItlbLoadMisses b (long itlb-load-misses)))
      (when branch-loads
        (.setBranchLoads b (long branch-loads)))
      (when branch-load-misses
        (.setBranchLoadMisses b (long branch-load-misses)))
      (when node-loads
        (.setNodeLoads b (long node-loads)))
      (when node-load-misses
        (.setNodeLoadMisses b (long node-load-misses)))
      (when node-stores
        (.setNodeStores b (long node-stores)))
      (when node-store-misses
        (.setNodeStoreMisses b (long node-store-misses)))
      (when node-prefetches
        (.setNodePrefetches b (long node-prefetches)))
      (when node-prefetch-misses
        (.setNodePrefetchMisses b (long node-prefetch-misses)))
      (.build b))))

(defmethod pb->data Protos$PerfStatistics
  [^Protos$PerfStatistics s]
  (PerfStatistics.
   (.getTimestamp s)
   (.getDuration s)
   (.getCycles s)
   (.getStalledCyclesFrontend s)
   (.getStalledCyclesBackend s)
   (.getInstructions s)
   (.getCacheReferences s)
   (.getCacheMisses s)
   (.getBranches s)
   (.getBranchMisses s)
   (.getBusCycles s)
   (.getRefCycles s)
   (.getCpuClock s)
   (.getTaskClock s)
   (.getPageFaults s)
   (.getMajorFaults s)
   (.getMinorFaults s)
   (.getContextSwitches s)
   (.getCpuMigrations s)
   (.getAlignmentFaults s)
   (.getEmulationFaults s)
   (.getL1DcacheLoads s)
   (.getL1DcacheLoadMisses s)
   (.getL1DcacheStores s)
   (.getL1DcacheStoreMisses s)
   (.getL1DcacheLoadPrefetches s)
   (.getL1DcacheLoadPrefetchMisses s)
   (.getL1IcacheLoads s)
   (.getL1IcacheLoadMisses s)
   (.getL1IcachePrefetches s)
   (.getL1IcachePrefetchMisses s)
   (.getLlcLoads s)
   (.getLlcLoadMisses s)
   (.getLlcStores s)
   (.getLlcStoreMisses s)
   (.getLlcPrefetches s)
   (.getLlcPrefetchMisses s)
   (.getDtlbLoads s)
   (.getDtlbLoadMisses s)
   (.getDtlbStores s)
   (.getDtlbStoreMisses s)
   (.getDtlbPrefetches s)
   (.getDtlbPrefetchMisses s)
   (.getItlbLoads s)
   (.getItlbLoadMisses s)
   (.getBranchLoads s)
   (.getBranchLoadMisses s)
   (.getNodeLoads s)
   (.getNodeLoadMisses s)
   (.getNodeStores s)
   (.getNodeStoreMisses s)
   (.getNodePrefetches s)
   (.getNodePrefetchMisses s)))

;; Request
;; =======

(defrecord Request [slave-id resources]
  Serializable
  (data->pb [this]
    (-> (Protos$Request/newBuilder)
        (cond-> slave-id (.setSlaveId (->pb :SlaveID slave-id)))
        (.addAllResources (map (partial ->pb :Resource) resources))
        (.build))))

(defmethod pb->data Protos$Request
  [^Protos$Request req]
  (Request.
   (when-let [id (.getSlaveId req)] (pb->data id))
   (map pb->data (.getResourcesList req))))

;; Offer
;; =====

(defrecord Offer [id framework-id slave-id hostname
                  resources attributes executor-ids]
  Serializable
  (data->pb [this]
    (-> (Protos$Offer/newBuilder)
        (.setId (->pb :OfferID id))
        (.setFrameworkId (->pb :FrameworkID framework-id))
        (.setSlaveId (->pb :SlaveID slave-id))
        (.setHostname (str hostname))
        (.addAllResources (mapv (partial ->pb :Resource) resources))
        (.addAllAttributes (mapv (partial ->pb :Attribute) attributes))
        (.addAllExecutorIds (mapv (partial ->pb :ExecutorID) attributes))
        (.build))))

(defmethod pb->data Protos$Offer
  [^Protos$Offer offer]
  (Offer.
   (pb->data (.getId offer))
   (pb->data (.getFrameworkId offer))
   (pb->data (.getSlaveId offer))
   (.getHostname offer)
   (mapv pb->data (.getResourcesList offer))
   (mapv pb->data (.getAttributesList offer))
   (mapv pb->data (.getExecutorIdsList offer))))

;; Operation
;; =========

(defmethod pb->data Protos$Offer$Operation$Type
  [^Protos$Offer$Operation$Type type]
  (cond
    (= type Protos$Offer$Operation$Type/LAUNCH)    :operation-launch
    (= type Protos$Offer$Operation$Type/RESERVE)   :operation-reserve
    (= type Protos$Offer$Operation$Type/UNRESERVE) :operation-unreserve
    (= type Protos$Offer$Operation$Type/CREATE)    :operation-create
    (= type Protos$Offer$Operation$Type/DESTROY)   :operation-destroy
    :else type))

(defrecord Operation [type tasks resources volumes]
  Serializable
  (data->pb [this]
    (case (:type this)
      :operation-launch
        (let [launch (Protos$Offer$Operation$Launch/newBuilder)]
          (.addAllTaskInfos
            launch (mapv (partial ->pb :TaskInfo) tasks))
          (-> (Protos$Offer$Operation/newBuilder)
              (.setType (Protos$Offer$Operation$Type/LAUNCH))
              (.setLaunch launch)
              (.build)))
      :operation-reserve
        (let [reserve (Protos$Offer$Operation$Reserve/newBuilder)]
          (.addAllResrouces
            reserve (mapv (partial ->pb :Resource) resources))
          (-> (Protos$Offer$Operation/newBuilder)
              (.setType (Protos$Offer$Operation$Type/RESERVE))
              (.setLaunch reserve)
              (.build)))
      :operation-unreserve
        (let [unreserve (Protos$Offer$Operation$Unreserve/newBuilder)]
          (.addAllResrouces
            unreserve (mapv (partial ->pb :Resource) resources))
          (-> (Protos$Offer$Operation/newBuilder)
              (.setType (Protos$Offer$Operation$Type/UNRESERVE))
              (.setLaunch unreserve)
              (.build)))
      :operation-create
        (let [create (Protos$Offer$Operation$Create/newBuilder)]
          (.addAllVolumes
            create (mapv (partial ->pb :Volume) volumes))
          (-> (Protos$Offer$Operation/newBuilder)
              (.setType (Protos$Offer$Operation$Type/CREATE))
              (.setLaunch create)
              (.build)))
      :operation-destroy
        (let [destroy (Protos$Offer$Operation$Destroy/newBuilder)]
          (.addAllVolumes
            destroy (mapv (partial ->pb :Volume) volumes))
          (-> (Protos$Offer$Operation/newBuilder)
              (.setType (Protos$Offer$Operation$Type/DESTROY))
              (.setLaunch destroy)
              (.build))))))

(defmethod pb->data Protos$Offer$Operation
  [^Protos$Offer$Operation op]
  (let [type (pb->data (.getType op))]
    (case type
      :operation-launch
        (Operation. type
                    (map pb->data (.getTaskInfosList (.getLaunch op)))
                    nil
                    nil)
      :operation-reserve
        (Operation. type
                    nil
                    (map pb->data (.getResourcesList (.getReserve op)))
                    nil)
      :operation-unreserve
        (Operation. type
                    nil
                    (map pb->data (.getResourcesList (.getUnreserve op)))
                    nil)
      :operation-create
        (Operation. type
                    (map pb->data (.getVolumesList (.getCreate op)))
                    nil
                    nil)
      :operation-destroy
        (Operation. type
                    (map pb->data (.getVolumesList (.getDestroy op)))
                    nil
                    nil))))

;; Ports
;; =====

(defrecord Port [number name protocol visibility labels]
  Serializable
  (data->pb [this]
    (-> (Protos$Port/newBuilder)
        (.setNumber (int number))
        (cond-> name       (.setName (str name))
                protocol   (.setProtocol (str protocol))
                visibility (.setVisibility (pb->data visibility))
                labels     (.setLabels (->pb :Labels labels)))
        (.build))))

(defmethod pb->data Protos$Port
  [^Protos$Port port]
  (Port. (.getNumber port)
         (.getName port)
         (.getProtocol port)
         (when-let [vis (.getVisibility port)] (pb->data vis))
         (when-let [labels (.getLabels port)] (pb->data labels))))

(defrecord Ports [ports]
  Serializable
  (data->pb [this]
    (-> (Protos$Ports/newBuilder)
        (.addAllPorts (mapv (partial ->pb :Port ports)))
        (.build))))

(defmethod pb->data Protos$Ports
  [^Protos$Ports ports]
  (Ports. (mapv pb->data (.getPortsList ports))))

;; DiscoveryInfo
;; =============

(defrecord DiscoveryInfo [visibility name environment location
                          version ports labels]
  Serializable
  (data->pb [this]
    (-> (Protos$DiscoveryInfo/newBuilder)
        (.setVisibility visibility)
        (cond-> name (.setName (str name))
                environment (.setEnvionment (str environment))
                location    (.setLocation (str location))
                ports       (.setPorts (->pb :Ports ports))
                version     (.setVersion (str version))
                labels      (.setLabels (->pb :Labels labels))))))

(defmethod pb->data Protos$DiscoveryInfo
  [^Protos$DiscoveryInfo di]
  (DiscoveryInfo. (pb->data (.getVisibility di))
                  (.getName di)
                  (.getEnvironment di)
                  (.getLocation di)
                  (.getVersion di)
                  (when-let [ports (.getPorts di)] (pb->data ports))
                  (when-let [labels (.getLabels di)] (pb->data labels))))

(defmethod pb->data Protos$DiscoveryInfo$Visibility
  [^Protos$DiscoveryInfo$Visibility vis]
  (cond
    (= Protos$DiscoveryInfo$Visibility/FRAMEWORK vis)
    :discovery-visibility-framework
    (= Protos$DiscoveryInfo$Visibility/CLUSTER vis)
    :discovery-visibility-cluster
    (= Protos$DiscoveryInfo$Visibility/EXTERNAL vis)
    :discovery-visibility-external))

;; TaskInfo
;; ========

(defrecord TaskInfo [name task-id slave-id resources executor command
                     container data health-check labels discovery]
    Serializable
    (data->pb [this]
      (-> (Protos$TaskInfo/newBuilder)
          (.setName (str name))
          (.setTaskId (->pb :TaskID task-id))
          (.setSlaveId (->pb :SlaveID slave-id))
          (cond->
              executor     (.setExecutor (->pb :ExecutorInfo executor))
              command      (.setCommand (->pb :CommandInfo command))
              container    (.setContainer (->pb :ContainerInfo container))
              data         (.setData data)
              health-check (.setHealthCheck (->pb :HealthCheck health-check))
              labels       (.setLabels (->pb :Labels labels))
              discovery    (.setDiscovery (-> :DiscoveryInfo discovery)))
          (.addAllResources (mapv (partial ->pb :Resource) resources))
          (.build))))

(defmethod pb->data Protos$TaskInfo
  [^Protos$TaskInfo info]
  (TaskInfo.
   (.getName info)
   (pb->data (.getTaskId info))
   (pb->data (.getSlaveId info))
   (mapv pb->data (.getResourcesList info))
   (when-let [i (.getExecutor info)] (pb->data i))
   (when-let [c (.getCommand info)] (pb->data c))
   (when-let [c (.getContainer info)] (pb->data c))
   (.getData info)
   (when-let [hc (.getHealthCheck info)] (pb->data hc))
   (when-let [l (.getLabels info)] (pb->data l))
   (when-let [d (.getDiscovery info)] (pb->data d))))

;; TaskState
;; =========

(defmethod pb->data Protos$TaskState
  [^Protos$TaskState status]
  (cond
    (= status Protos$TaskState/TASK_STAGING)  :task-staging
    (= status Protos$TaskState/TASK_STARTING) :task-starting
    (= status Protos$TaskState/TASK_RUNNING)  :task-running
    (= status Protos$TaskState/TASK_FINISHED) :task-finished
    (= status Protos$TaskState/TASK_FAILED)   :task-failed
    (= status Protos$TaskState/TASK_KILLED)   :task-killed
    (= status Protos$TaskState/TASK_LOST)     :task-lost
    (= status Protos$TaskState/TASK_ERROR)    :task-error
    :else status))

;; TaskStatus
;; ==========

(defmethod pb->data Protos$TaskStatus$Source
  [^Protos$TaskStatus$Source status]
  (cond
    (= status Protos$TaskStatus$Source/SOURCE_MASTER)   :source-master
    (= status Protos$TaskStatus$Source/SOURCE_SLAVE)    :source-slave
    (= status Protos$TaskStatus$Source/SOURCE_EXECUTOR) :source-executor
    :else status))

(defmethod pb->data Protos$TaskStatus$Reason
  [^Protos$TaskStatus$Reason status]

  (cond
    (= status Protos$TaskStatus$Reason/REASON_COMMAND_EXECUTOR_FAILED)
    :reason-command-executor-failed
    (= status Protos$TaskStatus$Reason/REASON_CONTAINER_LAUNCH_FAILED)
    :reason-container-launch-failed
    (= status Protos$TaskStatus$Reason/REASON_CONTAINER_LIMITATION)
    :reason-container-limitation
    (= status Protos$TaskStatus$Reason/REASON_CONTAINER_LIMITATION_DISK)
    :reason-container-limitation-disk
    (= status Protos$TaskStatus$Reason/REASON_CONTAINER_LIMITATION_MEMORY)
    :reason-container-limitation-memory
    (= status Protos$TaskStatus$Reason/REASON_CONTAINER_PREEMPTED)
    :reason-container-preempted
    (= status Protos$TaskStatus$Reason/REASON_CONTAINER_UPDATE_FAILED)
    :reason-container-update-failed
    (= status Protos$TaskStatus$Reason/REASON_EXECUTOR_REGISTRATION_TIMEOUT)
    :reason-executor-registration-timeout
    (= status Protos$TaskStatus$Reason/REASON_EXECUTOR_REREGISTRATION_TIMEOUT)
    :reason-executor-reregistration-timeout
    (= status Protos$TaskStatus$Reason/REASON_EXECUTOR_TERMINATED)
    :reason-executor-terminated
    (= status Protos$TaskStatus$Reason/REASON_EXECUTOR_UNREGISTERED)
    :reason-executor-unregistered
    (= status Protos$TaskStatus$Reason/REASON_FRAMEWORK_REMOVED)
    :reason-framework-removed
    (= status Protos$TaskStatus$Reason/REASON_GC_ERROR)
    :reason-gc-error
    (= status Protos$TaskStatus$Reason/REASON_INVALID_FRAMEWORKID)
    :reason-invalid-frameworkid
    (= status Protos$TaskStatus$Reason/REASON_INVALID_OFFERS)
    :reason-invalid-offers
    (= status Protos$TaskStatus$Reason/REASON_MASTER_DISCONNECTED)
    :reason-master-disconnected
    (= status Protos$TaskStatus$Reason/REASON_RECONCILIATION)
    :reason-reconciliation
    (= status Protos$TaskStatus$Reason/REASON_RESOURCES_UNKNOWN)
    :reason-resources-unknown
    (= status Protos$TaskStatus$Reason/REASON_SLAVE_DISCONNECTED)
    :reason-slave-disconnected
    (= status Protos$TaskStatus$Reason/REASON_SLAVE_REMOVED)
    :reason-slave-removed
    (= status Protos$TaskStatus$Reason/REASON_SLAVE_RESTARTED)
    :reason-slave-restarted
    (= status Protos$TaskStatus$Reason/REASON_SLAVE_UNKNOWN)
    :reason-slave-unknown
    (= status Protos$TaskStatus$Reason/REASON_TASK_INVALID)
    :reason-task-invalid
    (= status Protos$TaskStatus$Reason/REASON_TASK_UNAUTHORIZED)
    :reason-task-unauthorized
    (= status Protos$TaskStatus$Reason/REASON_TASK_UNKNOWN)
    :reason-task-unknown
    :else status))

(defrecord TaskStatus [task-id state message source reason
                       data slave-id executor-id timestamp
                       uuid healthy]
  Serializable
  (data->pb [this]
    (-> (Protos$TaskStatus/newBuilder)
        (.setTaskId (->pb :TaskID task-id))
        (.setState (data->pb state))
        (cond->
            message     (.setMessage (str message))
            source      (.setSource (data->pb source))
            reason      (.setReason (data->pb reason))
            data        (.setData data)
            slave-id    (.setSlaveId (->pb :SlaveID slave-id))
            executor-id (.setExecutorId (->pb :ExecutorID executor-id))
            timestamp   (.setTimestamp (double timestamp))
            uuid        (.setUuid uuid)
            (not (nil? healthy)) (.setHealthy (boolean healthy)))
        (.build))))

(defmethod pb->data Protos$TaskStatus
  [^Protos$TaskStatus status]
  (TaskStatus.
   (pb->data (.getTaskId status))
   (pb->data (.getState status))
   (.getMessage status)
   (pb->data (.getSource status))
   (pb->data (.getReason status))
   (.getData status)
   (pb->data (.getSlaveId status))
   (pb->data (.getExecutorId status))
   (.getTimestamp status)
   (.getUuid status)
   (.getHealthy status)))

;; Filters
;; =======

(defrecord Filters [refuse-seconds]
  Serializable
  (data->pb [this]
    (-> (Protos$Filters/newBuilder)
        (cond-> refuse-seconds (.setRefuseSeconds (double refuse-seconds)))
        (.build))))

(defmethod pb->data Protos$Filters
  [^Protos$Filters filters]
  (Filters. (.getRefuseSeconds filters)))

;; Environment
;; ===========

(defrecord EnvironmentVariable [name value]
  Serializable
  (data->pb [this]
    (-> (Protos$Environment$Variable/newBuilder)
        (.setName (str name))
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$Environment$Variable
  [^Protos$Environment$Variable var]
  (EnvironmentVariable. (.getName var) (.getValue var)))

(defrecord Environment [variables]
  Serializable
  (data->pb [this]
    (-> (Protos$Environment/newBuilder)
        (.addAllVariables (mapv (partial ->pb :EnvironmentVariable) variables))
        (.build))))

;; Parameter
;; =========

(defrecord Parameter [key value]
  Serializable
  (data->pb [this]
    (-> (Protos$Parameter/newBuilder)
        (.setKey (str key))
        (.setValue (str value))
        (.build))))

(defmethod pb->data Protos$Parameter
  [^Protos$Parameter p]
  (Parameter. (.getKey p) (.getValue p)))

(defrecord Parameters [parameter]
  Serializable
  (data->pb [this]
    (-> (Protos$Parameters/newBuilder)
        (.addAllParameters (mapv (partial ->pb :Parameter) parameter))
        (.build))))

(defmethod pb->data Protos$Parameters
  [^Protos$Parameters p]
  (Parameters. (mapv pb->data (.getParameterList p))))

;; Credential
;; ==========

(defrecord Credential [principal secret]
  Serializable
  (data->pb [this]
    (-> (Protos$Credential/newBuilder)
        (.setPrincipal (str key))
        (cond-> secret (.setSecret (str secret)))
        (.build))))

(defmethod pb->data Protos$Credential
  [^Protos$Credential c]
  (Credential. (.getPrincipal c) (.getSecret c)))

(defrecord Credentials [credentials]
  Serializable
  (data->pb [this]
    (-> (Protos$Credentials/newBuilder)
        (.addAllCredentials (mapv (partial ->pb :Credential) credentials))
        (.build))))

(defmethod pb->data Protos$Credentials
  [^Protos$Credentials c]
  (Credentials. (mapv pb->data (.getCredentialsList c))))

;; RateLimit
;; =========

(defrecord RateLimit [qps principal capacity]
  Serializable
  (data->pb [this]
    (-> (Protos$RateLimit/newBuilder)
        (.setPrincipal (str principal))
        (cond->
            qps      (.setQps (double qps))
            capacity (.setCapacity (long capacity)))
        (.build))))

(defmethod pb->data Protos$RateLimit
  [^Protos$RateLimit rl]
  (RateLimit.
   (.getQps rl)
   (.getPrincipal rl)
   (.getCapacity rl)))

(defrecord RateLimits [limits aggregate-default-qps
                       aggregate-default-capacity]
  Serializable
  (data->pb [this]
    (-> (Protos$RateLimits/newBuilder)
        (cond->
            aggregate-default-qps      (.setAggregateDefaultQps
                                        (double aggregate-default-qps))
            aggregate-default-capacity (.setAggregateDefaultCapacity
                                        (long aggregate-default-capacity)))
        (.addAllLimits (mapv (partial ->pb :RateLimit) limits))
        (.build))))

(defmethod pb->data Protos$RateLimits
  [^Protos$RateLimits rl]
  (RateLimits.
   (mapv pb->data (.getLimitsList rl))
   (.getAggregateDefaultQps rl)
   (.getAggregateDefaultCapacity rl)))

;; Volume
;; ======

(defmethod pb->data Protos$Volume$Mode
  [^Protos$Volume$Mode mode]
  (cond
    (= mode Protos$Volume$Mode/RW) :volume-rw
    (= mode Protos$Volume$Mode/RO) :volume-ro
    :else mode))

(defrecord Volume [container-path host-path mode]
  Serializable
  (data->pb [this]
    (-> (Protos$Volume/newBuilder)
        (.setContainerPath (str container-path))
        (.setMode (data->pb mode))
        (cond-> host-path (.setHostPath (str host-path))))))

;; ContainerInfo
;; =============

(defmethod pb->data Protos$ContainerInfo$Type
  [^Protos$ContainerInfo$Type type]
  (cond
    (= type Protos$ContainerInfo$Type/DOCKER) :container-type-docker
    (= type Protos$ContainerInfo$Type/MESOS)  :conatiner-type-mesos
    :else type))

(defrecord PortMapping [host-port container-port protocol]
    Serializable
    (data->pb [this]
      (-> (Protos$ContainerInfo$DockerInfo$PortMapping/newBuilder)
          (.setHostPort (int host-port))
          (.setContainerPort (int container-port))
          (cond-> protocol (.setProtocol protocol))
          (.build))))

(defmethod pb->data Protos$ContainerInfo$DockerInfo$PortMapping
  [^Protos$ContainerInfo$DockerInfo$PortMapping pm]
  (PortMapping.
   (.getHostPort pm)
   (.getContainerPort pm)
   (.getProtocol pm)))

(defmethod pb->data Protos$ContainerInfo$DockerInfo$Network
  [^Protos$ContainerInfo$DockerInfo$Network network]
  (cond
    (= network Protos$ContainerInfo$DockerInfo$Network/HOST)
    :docker-network-host
    (= network Protos$ContainerInfo$DockerInfo$Network/BRIDGE)
    :docker-network-bridge
    (= network Protos$ContainerInfo$DockerInfo$Network/NONE)
    :docker-network-none
    :else network))

(defrecord DockerInfo [image network port-mappings
                       privileged parameters]
  Serializable
  (data->pb [this]
    (-> (Protos$ContainerInfo$DockerInfo/newBuilder)
        (.setImage image)
        (cond->
            network    (.setNetwork (data->pb network))
            (not (nil? privileged)) (.setPrivileged (boolean privileged)))
        (.addAllPortMappings (mapv (partial ->pb :PortMapping) port-mappings))
        (.addAllParameters (mapv (partial ->pb :Parameter) parameters))
        (.build))))

(defrecord ContainerInfo [type volumes hostname docker]
  Serializable
  (data->pb [this]
    (-> (Protos$ContainerInfo/newBuilder)
        (.setType (data->pb type))
        (cond->
            hostname (.setHostname (str hostname))
            docker   (.setDocker (->pb :DockerInfo docker)))
        (.addAllVolumes (mapv (partial ->pb :Volume) volumes))
        (.build))))

(defrecord Label [key value]
  Serializable
  (data->pb [this]
    (-> (Protos$Label/newBuilder)
        (.setKey (str key))
        (cond-> value (.setValue (str value)))
        (.build))))

(defmethod pb->data Protos$Label
  [^Protos$Label label]
  (Label. (.getKey label) (.getValue label)))

(defrecord Labels [labels]
  Serializable
  (data->pb [this]
    (-> (Protos$Labels/newBuilder)
        (.addAllLabels (mapv (partial ->pb :Label) labels))
        (.build))))

(defmethod pb->data Protos$Labels
  [^Protos$Labels labels]
  (Labels. (mapv pb->data (.getLabelsList labels))))


;; Safe for the common stuff in extend-protocol, this marks the end of
;; messages defined in mesos.proto

;; Handle serialization of Status, Value.Type and TaskState from
;; keywords, as well as Scalars from their value.

(extend-protocol Serializable
  java.lang.Integer
  (data->pb [this]
    (-> (Protos$Value$Scalar/newBuilder)
        (.setValue (double this))
        (.build)))
  java.lang.Long
  (data->pb [this]
    (-> (Protos$Value$Scalar/newBuilder)
        (.setValue (double this))
        (.build)))
  java.lang.Double
  (data->pb [this]
    (-> (Protos$Value$Scalar/newBuilder)
        (.setValue this)
        (.build)))
  clojure.lang.PersistentHashSet
  (data->pb [this]
    (-> (Protos$Value$Set/newBuilder)
        (.addAllItem (seq this))
        (.build)))
  java.lang.String
  (data->pb [this]
    (-> (Protos$Value$Text/newBuilder) (.setValue this) (.build)))
  clojure.lang.Keyword
  (data->pb [this]
    (case this
      :driver-not-started       Protos$Status/DRIVER_NOT_STARTED
      :driver-running           Protos$Status/DRIVER_RUNNING
      :driver-aborted           Protos$Status/DRIVER_ABORTED
      :driver-stopped           Protos$Status/DRIVER_STOPPED
      :operation-launch         Protos$Offer$Operation$Type/LAUNCH
      :operation-reserve        Protos$Offer$Operation$Type/RESERVE
      :operation-unreserve      Protos$Offer$Operation$Type/UNRESERVE
      :operation-create         Protos$Offer$Operation$Type/CREATE
      :operation-destroy        Protos$Offer$Operation$Type/DESTROY
      :task-staging             Protos$TaskState/TASK_STAGING
      :task-starting            Protos$TaskState/TASK_STARTING
      :task-running             Protos$TaskState/TASK_RUNNING
      :task-finished            Protos$TaskState/TASK_FINISHED
      :task-failed              Protos$TaskState/TASK_FAILED
      :task-killed              Protos$TaskState/TASK_KILLED
      :task-lost                Protos$TaskState/TASK_LOST
      :task-error               Protos$TaskState/TASK_ERROR
      :value-scalar             Protos$Value$Type/SCALAR
      :value-ranges             Protos$Value$Type/RANGES
      :value-set                Protos$Value$Type/SET
      :value-text               Protos$Value$Type/TEXT
      :volume-rw                Protos$Volume$Mode/RW
      :volume-ro                Protos$Volume$Mode/RO
      :docker-network-host      Protos$ContainerInfo$DockerInfo$Network/HOST
      :docker-network-bridge    Protos$ContainerInfo$DockerInfo$Network/BRIDGE
      :docker-network-none      Protos$ContainerInfo$DockerInfo$Network/NONE
      :container-type-docker    Protos$ContainerInfo$Type/DOCKER
      :container-type-mesos     Protos$ContainerInfo$Type/MESOS
      :machine-mode-up          Protos$MachineInfo$Mode/UP
      :machine-mode-draining    Protos$MachineInfo$Mode/DRAINING
      :machine-mode-down        Protos$MachineInfo$Mode/DOWN
      :source-master            Protos$TaskStatus$Source/SOURCE_MASTER
      :source-slave             Protos$TaskStatus$Source/SOURCE_SLAVE
      :source-executor          Protos$TaskStatus$Source/SOURCE_EXECUTOR

      ;; These are too wide and mess up indenting!
      :framework-capability-revocable-resource
      Protos$FrameworkInfo$Capability$Type/REVOCABLE_RESOURCES
      :framework-capability-task-killing-state
      Protos$FrameworkInfo$Capability$Type/TASK_KILLING_STATE
      :framework-capability-gpu-resources
      Protos$FrameworkInfo$Capability$Type/GPU_RESOURCES

      :discovery-visibility-framework
      Protos$DiscoveryInfo$Visibility/FRAMEWORK
      :discovery-visibility-cluster
      Protos$DiscoveryInfo$Visibility/CLUSTER
      :discovery-visibility-external
      Protos$DiscoveryInfo$Visibility/EXTERNAL


      :reason-command-executor-failed
      Protos$TaskStatus$Reason/REASON_COMMAND_EXECUTOR_FAILED
      :reason-container-launch-failed
      Protos$TaskStatus$Reason/REASON_CONTAINER_LAUNCH_FAILED
      :reason-container-limitation
      Protos$TaskStatus$Reason/REASON_CONTAINER_LIMITATION
      :reason-container-limitation-disk
      Protos$TaskStatus$Reason/REASON_CONTAINER_LIMITATION_DISK
      :reason-container-limitation-memory
      Protos$TaskStatus$Reason/REASON_CONTAINER_LIMITATION_MEMORY
      :reason-container-preempted
      Protos$TaskStatus$Reason/REASON_CONTAINER_PREEMPTED
      :reason-container-update-failed
      Protos$TaskStatus$Reason/REASON_CONTAINER_UPDATE_FAILED
      :reason-executor-registration-timeout
      Protos$TaskStatus$Reason/REASON_EXECUTOR_REGISTRATION_TIMEOUT
      :reason-executor-reregistration-timeout
      Protos$TaskStatus$Reason/REASON_EXECUTOR_REREGISTRATION_TIMEOUT
      :reason-executor-terminated
      Protos$TaskStatus$Reason/REASON_EXECUTOR_TERMINATED
      :reason-executor-unregistered
      Protos$TaskStatus$Reason/REASON_EXECUTOR_UNREGISTERED
      :reason-framework-removed
      Protos$TaskStatus$Reason/REASON_FRAMEWORK_REMOVED
      :reason-gc-error
      Protos$TaskStatus$Reason/REASON_GC_ERROR
      :reason-invalid-frameworkid
      Protos$TaskStatus$Reason/REASON_INVALID_FRAMEWORKID
      :reason-invalid-offers
      Protos$TaskStatus$Reason/REASON_INVALID_OFFERS
      :reason-master-disconnected
      Protos$TaskStatus$Reason/REASON_MASTER_DISCONNECTED
      :reason-reconciliation
      Protos$TaskStatus$Reason/REASON_RECONCILIATION
      :reason-resources-unknown
      Protos$TaskStatus$Reason/REASON_RESOURCES_UNKNOWN
      :reason-slave-disconnected
      Protos$TaskStatus$Reason/REASON_SLAVE_DISCONNECTED
      :reason-slave-removed
      Protos$TaskStatus$Reason/REASON_SLAVE_REMOVED
      :reason-slave-restarted
      Protos$TaskStatus$Reason/REASON_SLAVE_RESTARTED
      :reason-slave-unknown
      Protos$TaskStatus$Reason/REASON_SLAVE_UNKNOWN
      :reason-task-invalid
      Protos$TaskStatus$Reason/REASON_TASK_INVALID
      :reason-task-unauthorized
      Protos$TaskStatus$Reason/REASON_TASK_UNAUTHORIZED
      :reason-task-unknown
      Protos$TaskStatus$Reason/REASON_TASK_UNKNOWN

      ;; default
      nil)))

;; By default, yield the original payload.

(defmethod pb->data :default
  [this]
  this)

(defn ->pb
  [map-type this]

  (data->pb
   (if (record? this)
     this
     (cond
       (= :FrameworkID map-type) (map->FrameworkID this)
       (= :FrameworkCapability map-type) (map->FrameworkCapability this)
       (= :OfferID map-type) (map->OfferID this)
       (= :SlaveID map-type) (map->SlaveID this)
       (= :TaskID map-type)   (map->TaskID this)
       (= :ExecutorID map-type) (map->ExecutorID this)
       (= :ContainerID map-type) (map->ContainerID this)
       (= :FrameworkInfo map-type) (map->FrameworkInfo this)
       (= :HealthCheckHTTP map-type) (map->HealthCheckHTTP this)
       (= :HealthCheck map-type) (map->HealthCheck this)
       (= :URI map-type) (map->URI this)
       (= :CommandInfo map-type) (map->CommandInfo this)
       (= :ExecutorInfo map-type) (map->ExecutorInfo this)
       (= :MasterInfo map-type)   (map->MasterInfo this)
       (= :SlaveInfo map-type)    (map->SlaveInfo this)
       (= :ValueRange map-type)   (map->ValueRange this)
       (= :ValueRanges map-type)  (map->ValueRanges this)
       (= :Value map-type)        (map->Value this)
       (= :Attribute map-type)    (map->Attribute this)
       (= :Resource map-type)     (map->Resource this)
       (= :Labels map-type)       (map->Labels this)
       (= :Label map-type)        (map->Label this)
       (= :Request map-type)      (map->Request this)
       (= :Offer map-type)                (map->Offer this)
       (= :Operation map-type)            (map->Operation this)
       (= :TaskInfo map-type)             (map->TaskInfo this)
       (= :TaskStatus map-type)           (map->TaskStatus this)
       (= :Filters map-type)              (map->Filters this)
       (= :EnvironmentVariable map-type)  (map->EnvironmentVariable this)
       (= :Environment map-type)          (map->Environment this)
       (= :Parameter map-type)            (map->Parameter this)
       (= :Parameters map-type)           (map->Parameter this)
       (= :Credential map-type)           (map->Credential this)
       (= :Credentials map-type)          (map->Credentials this)
       (= :RateLimit map-type)            (map->RateLimit this)
       (= :RateLimits map-type)           (map->RateLimits this)
       (= :Volume map-type)               (map->Volume this)
       (= :PortMapping map-type)          (map->PortMapping this)
       (= :DockerInfo map-type)           (map->DockerInfo this)
       (= :ContainerInfo map-type)        (map->ContainerInfo this)))))
