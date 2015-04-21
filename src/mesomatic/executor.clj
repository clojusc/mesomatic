(ns mesomatic.executor
  (:require [mesomatic.types :refer [data->pb pb->data]])
  (:import org.apache.mesos.MesosExecutorDriver))

(defprotocol Executor
  (disconnected      [this driver])
  (error             [this driver message])
  (framework-message [this driver data])
  (kill-task         [this driver task-id])
  (launch-task       [this driver task])
  (registered        [this driver executor-info framework-info slave-info])
  (reregistered      [this driver slave-info])
  (shutdown          [this driver]))

(defprotocol ExecutorDriver
  (abort!                  [this])
  (join!                   [this])
  (run-driver!             [this])
  (send-framework-message! [this data])
  (send-status-update!     [this status])
  (start!                  [this])
  (stop!                   [this]))

(defn wrap-executor
  [impl]
  (reify
    org.apache.mesos.Executor
    (disconnected [this driver]
      (disconnected impl driver))
    (error [this driver message]
      (error impl driver message))
    (frameworkMessage [this driver data]
      (framework-message impl driver data))
    (killTask [this driver task-id]
      (kill-task impl driver (data->pb task-id)))
    (launchTask [this driver task]
      (launch-task impl driver (data->pb task)))
    (registered [this driver executor-info framework-info slave-info]
      (registered impl
                  driver
                  (data->pb executor-info)
                  (data->pb framework-info)
                  (data->pb slave-info)))
    (reregistered [this driver slave-info]
      (reregistered impl
                    driver
                    (data->pb slave-info)))
    (shutdown [this driver]
      (shutdown impl driver))))

(defmacro executor
  [& body]
  `(wrap-executor (reify Executor ~@body)))

(defn executor-driver
  [executor]
  (let [d (MesosExecutorDriver. executor)]
    (reify ExecutorDriver
      (abort! [this]
        (pb->data (.abort d)))
      (join! [this]
        (pb->data (.join d)))
      (run-driver! [this]
        (pb->data (.run d)))
      (send-framework-message! [this data]
        (pb->data (.sendFrameworkMessage d data)))
      (send-status-update! [this status]
        (pb->data (.sendStatusUpdate d (data->pb status))))
      (start! [this]
        (pb->data (.start d)))
      (stop! [this]
        (pb->data (.stop d))))))
