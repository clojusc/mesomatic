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
  (abort!                    [this])
  (acknowledge-status-update [this status])
  (accept-offers             [this offer-ids operations]
                             [this offer-ids operations filters])
  (decline-offer             [this offer-id] [this offer-id filters])
  (join!                     [this])
  (kill-task!                [this task-id])
  (launch-tasks!             [this offer-id tasks] [this offer-id tasks filters])
  (reconcile-tasks           [this statuses])
  (request-resources         [this requests])
  (revive-offers             [this])
  (run-driver!               [this])
  (send-framework-message!   [this executor-id slave-id data])
  (start!                    [this])
  (stop!                     [this] [this failover?])
  (suppress-offers           [this]))

(defn wrap-driver [d]
  (reify SchedulerDriver
    (abort! [this]
      (pb->data (.abort d)))
    (acknowledge-status-update [this status]
      (pb->data (.acknowledgeStatusUpdate d (->pb :TaskStatus status))))
    (accept-offers [this offer-ids operations]
      (pb->data (.acceptOffers d
                               (mapv (partial ->pb :OfferID) offer-ids)
                               (mapv (partial ->pb :Operation) operations)
                               (->pb :Filters {:refuse-seconds 1}))))
    (accept-offers [this offer-ids operations filters]
      (pb->data (.acceptOffers d
                               (mapv (partial ->pb :OfferID) offer-ids)
                               (mapv (partial ->pb :Operation) operations)
                               (mapv (partial ->pb :Filters) filters))))

    (decline-offer [this offer-id]
      (pb->data (.declineOffer d (->pb :OfferID offer-id))))
    (decline-offer [this offer-id filters]
      (pb->data (.declineOffer d
                               (->pb :OfferID offer-id)
                               (->pb :Filters filters))))
    (join! [this]
      (pb->data (.join d)))
    (kill-task! [this task-id]
      (pb->data (.killTask d (->pb :TaskID task-id))))
    (launch-tasks! [this offer-id tasks]
      (pb->data (.launchTasks d
                              (if (sequential? offer-id)
                                (mapv (partial ->pb :OfferID) offer-id)
                                (vector (->pb :OfferID offer-id)))
                              (mapv (partial ->pb :TaskInfo) tasks))))
    (launch-tasks! [this offer-id tasks filters]
      (pb->data (.launchTasks d
                              (if (sequential? offer-id)
                                (mapv (partial ->pb :OfferID) offer-id)
                                (vector (->pb :OfferID offer-id)))
                              (mapv (partial ->pb :TaskInfo) tasks)
                              (->pb :Filters filters))))
    (reconcile-tasks [this statuses]
      (pb->data
        (.reconcileTasks d (mapv (partial ->pb :TaskStatus) statuses))))
    (request-resources [this requests]
      (pb->data
        (.requestResources d (mapv (partial ->pb :Request) requests))))
    (revive-offers [this]
      (pb->data (.reviveOffers d)))
    (run-driver! [this]
      (pb->data (.run d)))
    (send-framework-message! [this executor-id slave-id data]
      (pb->data (.sendFrameworkMessage d
                                       (->pb :ExecutorID executor-id)
                                       (->pb :SlaveID slave-id)
                                       data)))
    (start! [this]
      (pb->data (.start d)))
    (stop! [this]
      (pb->data (.stop d)))
    (stop! [this failover?]
      (pb->data (.stop this (boolean failover?))))
    (suppress-offers [this]
      (pb->data (.suppressOffers d)))))

(defn wrap-scheduler
  [implementation]
  (reify
    org.apache.mesos.Scheduler
    (registered [this driver framework-id master-info]
      (registered implementation
                  (wrap-driver driver)
                  (pb->data framework-id)
                  (pb->data master-info)))
    (reregistered [this driver master-info]
      (reregistered implementation (wrap-driver driver) (pb->data master-info)))
    (disconnected [this driver]
      (disconnected implementation (wrap-driver driver)))
    (resourceOffers [this driver offers]
      (resource-offers implementation (wrap-driver driver) (mapv pb->data offers)))
    (offerRescinded [this driver offer-id]
      (offer-rescinded implementation (wrap-driver driver) (pb->data offer-id)))
    (statusUpdate [this driver status]
      (status-update implementation (wrap-driver driver) (pb->data status)))
    (frameworkMessage [this driver executor-id slave-id data]
      (framework-message implementation
                         (wrap-driver driver)
                         (pb->data executor-id)
                         (pb->data slave-id)
                         data))
    (slaveLost [this driver slave-id]
      (slave-lost implementation (wrap-driver driver) (pb->data slave-id)))
    (executorLost [this driver executor-id slave-id status]
      (executor-lost implementation
                     (wrap-driver driver)
                     (pb->data executor-id)
                     (pb->data slave-id)
                     (pb->data status)))
    (error [this driver message]
      (error implementation (wrap-driver driver) message))))

(defmacro scheduler
  [& body]
  `(wrap-scheduler (reify Scheduler ~@body)))

(defn driver-builder
  ([scheduler framework master]
   (MesosSchedulerDriver. scheduler
     (->pb :FrameworkInfo framework)
     master))
  ([scheduler framework master credential]
   (MesosSchedulerDriver. scheduler
     (->pb :FrameworkInfo framework)
     master
     (->pb :Credential credential)))
  ([scheduler framework master credential implicit-acknowledgements?]
    (if (nil? credential)
      (MesosSchedulerDriver. scheduler
        (->pb :FrameworkInfo framework)
        master
        implicit-acknowledgements?)
      (MesosSchedulerDriver. scheduler
        (->pb :FrameworkInfo framework)
        master
        (->pb :Credential credential)
        implicit-acknowledgements?))))

(defn scheduler-driver
  [& args]
  (->> args
       (apply driver-builder)
       (wrap-driver)))
