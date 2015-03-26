(ns mesomatic.async.executor
  (:require [clojure.core.async :refer [put! close! chan]]
            [mesomatic.executor :as e]))

(defn executor
  ([ch close?]
   (e/executor
    (disconnected
     [this driver]
     (put! ch {:type :disconnected :driver driver}))
    (error
     [this driver message]
     (put! ch {:type :error :driver driver :message message}))
    (framework-message
     [this driver data]
     (put! ch {:type :framework-message :driver driver :data data}))
    (kill-task
     [this driver task-id]
     (put! ch {:type :kill-task :driver driver :task-id task-id}))
    (launch-task
     [this driver task]
     (put! ch {:type :launch-task :driver driver :task task}))
    (registered
     [this driver executor-info framework-info slave-info]
     (put! ch {:type           :registered
               :driver         driver
               :executor-info  executor-info
               :framework-info framework-info
               :slave-info     slave-info}))
    (reregistered
     [this driver slave-info]
     (put! ch {:type :reregistered :slave-info slave-info}))
    (shutdown
     [this driver]
     (put! ch {:type :shutdown :driver driver})
     (when close? (close! ch)))))
  ([ch]
   (executor ch true))
  ([]
   (let [ch (chan)]
     (executor ch true)
     ch)))
