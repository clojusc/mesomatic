(ns mesomatic.scheduler
  (:require [mesomatic.types :refer [data->pb pb->data ->pb]])
  (:import org.apache.mesos.MesosSchedulerDriver))

(defprotocol Scheduler
  (registered        [this driver framework-id master-info])
  (reregistered      [this driver master-info])
  (disconnected      [this driver])
  (resource-offers   [this driver offers])
  (offer-rescinded   [this driver offer-id])
  (status-update     [this driver status])
  (framework-message [this driver executor-id slave-id data])
  (slave-lost        [this driver slave-id])
  (executor-lost     [this driver executor-id slave-id status])
  (error             [this driver message]))

(defprotocol SchedulerDriver
  (abort!                  [this])
  (decline-offer           [this offer-id] [this offer-id filters])
  (join!                   [this])
  (kill-task!              [this task-id])
  (launch-tasks!           [this offer-id tasks] [this offer-id tasks filters])
  (reconcile-tasks         [this statuses])
  (request-resources       [this requests])
  (revive-offers           [this])
  (run-driver!             [this])
  (send-framework-message! [this executor-id slave-id data])
  (start!                  [this])
  (stop!                   [this] [this failover?]))

(defn wrap-scheduler
  [implementation]
  (reify
    org.apache.mesos.Scheduler
    (registered [this driver framework-id master-info]
      (registered implementation
                  driver
                  (pb->data framework-id)
                  (pb->data master-info)))
    (reregistered [this driver master-info]
      (reregistered implementation driver (pb->data master-info)))
    (disconnected [this driver]
      (disconnected implementation driver))
    (resourceOffers [this driver offers]
      (resource-offers implementation driver (mapv pb->data offers)))
    (offerRescinded [this driver offer-id]
      (offer-rescinded implementation driver (pb->data offer-id)))
    (statusUpdate [this driver status]
      (status-update implementation driver (pb->data status)))
    (frameworkMessage [this driver executor-id slave-id data]
      (framework-message implementation
                         driver
                         (pb->data executor-id)
                         (pb->data slave-id)
                         data))
    (slaveLost [this driver slave-id]
      (slave-lost implementation driver (pb->data slave-id)))
    (executorLost [this driver executor-id slave-id status]
      (executor-lost implementation
                     driver
                     (pb->data executor-id)
                     (pb->data slave-id)
                     (pb->data status)))
    (error [this driver message]
      (error implementation driver message))))

(defmacro scheduler
  [& body]
  `(wrap-scheduler (reify Scheduler ~@body)))

(defn scheduler-driver
  ([scheduler framework master]
   (scheduler-driver scheduler framework master nil))
  ([scheduler framework master credential]
   (let [d (if credential
             (MesosSchedulerDriver. scheduler
                                    (->pb :FrameworkInfo framework)
                                    master
                                    (->pb :Credential credential))
             (MesosSchedulerDriver. scheduler
                                    (->pb :FrameworkInfo framework)
                                    master))]
     (wrap-driver d))))
