(ns mesomatic.allocator-test
  (:require [clojure.test        :refer :all]
            [mesomatic.allocator :refer :all]))

(deftest allocator-test
  (let [offers [{:id {:value "20150506-090221-16777343-5050-15248-O36"},
                   :framework-id {:value "20150506-090221-16777343-5050-15248-0015"},
                   :slave-id {:value "20150505-120153-16777343-5050-32275-S0"},
                   :hostname "localhost",
                   :resources [{:name "mem", :type :value-scalar, :scalar 6918.0}
                               {:name "cpus", :type :value-scalar, :scalar 8.0}
                               {:name "disk", :type :value-scalar, :scalar 24989.0}
                               {:name "ports",
                                :type :value-ranges,
                                :ranges [{:begin 31000, :end 32000}]}],
                   :attributes [],
                   :executor-ids []},

                  {:id {:value "a4ba1178-6d54-47ca-abec-b7455f372b36"},
                   :framework-id {:value "20150506-090221-16777343-5050-15248-0015"},
                   :slave-id {:value "8ce8c437-e756-4e17-8711-b3082e3aaec0"},
                   :hostname "localhost",
                   :resources [{:name "mem", :type :value-scalar, :scalar 6918.0}
                               {:name "cpus", :type :value-scalar, :scalar 8.0}
                               {:name "disk", :type :value-scalar, :scalar 24989.0}
                               {:name "ports",
                                :type :value-ranges,
                                :ranges [{:begin 31000, :end 32000}]}],
                   :attributes [],
                   :executor-ids []}]

        task-info {:name "bundesrat-daemon-bar",
                   :task-id [{:value "0d6d34f6-0dba-4614-abb9-8e5779aafb7b"}
                             {:value "65a5e645-b92f-4bcb-9421-1c08eaad1d8f"}],
                   :resources [{:type :value-scalar, :name "mem", :scalar 512.0}
                               {:type :value-scalar, :name "cpus", :scalar 0.5}
                               {:type :value-ranges, :name "ports", :ranges [{:begin 0, :end 0}]}],
                   :container {:type :container-type-docker,
                               :docker {:image "dockerfile/nginx",
                                        :port-mappings [{:container-port 80, :protocol nil}],
                                        :network :docker-network-bridge}},
                   :command {:shell false},
                   :count 2,
                   :maxcol 1}]

    (testing "cannot satisfy workload"
      (is (= nil (allocate-naively (take 1 offers) [task-info])))
      (is (= nil (allocate-naively offers (repeat 100 task-info)))))

    (testing "successful allocation"
      (is (= 2 (count (allocate-naively offers [task-info]))))
      (is (= 2 (->> (allocate-naively offers [task-info])
                    (group-by :offer-id)
                    keys
                    count)))
      (is (= 1 (->> (allocate-naively offers [(assoc task-info :maxcol 2)])
                    (group-by :offer-id)
                    keys
                    count))))))
