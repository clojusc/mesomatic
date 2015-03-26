(ns mesomatic.async.scheduler
  (:require [clojure.core.async  :refer [put! close! chan]]
            [mesomatic.scheduler :as s]))

(defn scheduler
  ([ch close?]
   (s/scheduler
    (registered
     [this driver framework-id master-info]
     (put! ch {:type         :registered
               :driver       driver
               :framework-id framework-id
               :master-info  master-info}))
    (reregistered
     [this driver master-info]
     (put! ch {:type        :reregistered
               :driver      :driver
               :master-info master-info}))
    (disconnected
     [this driver]
     (put! ch {:type :disconnected :driver driver}))
    (resource-offers
     [this driver offers]
     (put! ch {:type :resource-offers :driver driver :offers offers}))
    (offer-rescinded
     [this driver offer-id]
     (put! ch {:type :offer-rescinded :driver driver :offer-id offer-id}))
    (status-update
     [this driver status]
     (put! ch {:type :status-update :driver driver :status status}))
    (framework-message
     [this driver executor-id slave-id data]
     (put! ch {:type        :framework-message
               :driver      driver
               :executor-id executor-id
               :slave-id    slave-id
               :data        data}))
    (slave-lost
     [this driver slave-id]
     (put! ch {:type     :slave-lost
               :driver   driver
               :slave-id slave-id}))
    (executor-lost
     [this driver executor-id slave-id status]
     (put! ch {:type        :executor-lost
               :driver      driver
               :executor-id executor-id
               :slave-id    slave-id
               :status      status}))
    (error
     [this driver message]
     (put! ch {:type    :error
               :driver  driver
               :message message}))))
  ([ch]
   (scheduler ch true))
  ([]
   (let [ch (chan)]
     (scheduler ch true)
     ch)))
