(ns handy-kafka.core
  (:require  [clojure.core.async :refer [go]]
             [clojure.walk :refer [keywordize-keys]]
             [franzy.clients.consumer.client :as consumer]
             [franzy.clients.consumer.protocols :refer :all]
             [franzy.clients.consumer.defaults :as cd]
             [franzy.serialization.json.deserializers :as json-deserializers]
             [franzy.serialization.deserializers :as deserializers]

             [franzy.serialization.json.serializers :as json-serializers]
             [franzy.serialization.serializers :as serializers]
             [franzy.clients.producer.client :as producer]
             [franzy.clients.producer.protocols :refer :all]))

;;; consumer

(defonce setup-kafka-cosume-state
         (let [state (atom #{})]
           (fn [action]
             (case action
               :all (fn [] (identity @state))
               :state (fn [key] (.contains @state key))
               :stop (fn [key] (swap! state disj key))
               :stop-all (fn [] (do (reset! state #{})
                                    (println "All services stoped.")))
               :mark-available (fn [key] (swap! state conj key))
               (throw (Exception. "Need specify an action."))))))

(def stop-service (setup-kafka-cosume-state :stop))
(def stop-all-services (setup-kafka-cosume-state :stop-all))
(def mark-service-available (setup-kafka-cosume-state :mark-available))
(def available? (setup-kafka-cosume-state :state))
(def all-services (setup-kafka-cosume-state :all))


(def default-consumer-config {"bootstrap.servers"  "127.0.0.1:9092"
                              "group.id"           "franzy.consumer"
                              "auto.offset.reset"  "earliest"})

(defn default-error-fn [& body]
  (let [m (group-by #(isa? (class %) Exception) body)]
    (println "Exception environment reference:")
    (prn (get m false))
    (throw (first (get m true)))))

(defn mount-a-consumer-service
  "service-key: used to stop the service
   partition: specify the kafka topic and parition
   handler-fn: the fn to handler the msg
   useage:
   (mount-a-consumer-service :abc prn {:topic-partitions [{:topic \"api-request\" :partition 0}]} )
   to start consume. "
  [service-key handler-fn {:keys [error-fn config-map topic-partitions]
                           :or   {error-fn default-error-fn config-map default-consumer-config}}]
  (do (mark-service-available service-key)
      (let [cc config-map
            key-deserializer (deserializers/keyword-deserializer)
            value-deserializer (json-deserializers/json-deserializer)
            options (cd/make-default-consumer-options)
            topic-partitions topic-partitions]
        (go
          (with-open [c (consumer/make-consumer cc key-deserializer value-deserializer options)]
            (assign-partitions! c topic-partitions)
            (doseq [topic-partition topic-partitions]
              (next-offset c topic-partition))
            (loop []
              (let [cr (poll! c)]
                (doseq [msg cr]
                  (try
                    (handler-fn (keywordize-keys msg))
                    (catch Exception e
                      (error-fn e msg))
                    (finally
                      (commit-offsets-async! c {(select-keys msg [:topic :partiton]) (:offset msg)}))))
                (when (available? service-key)
                  (recur)))))))))

;;; producer

(def default-producer-config {:bootstrap.servers ["127.0.0.1:9092"]
                              :client.id         "franzy.producer"})

(defn kafka-produce
  "The :partition and :key is for produce key,  :partition have higher priority than :key, that is the key value would
   be :
  (or (:partition msg)
      (:key msg))
   In practice, we would set integer value for :partition ,eg: 0.
   usages:
   (kafka-produce default-producer-config \"api-request\" 0 {:text \"this is test text\"}) "
  ([producer-config topic value]
   (kafka-produce producer-config topic nil nil value))
  ([producer-config topic partition value]
   (kafka-produce producer-config topic partition nil value))
  ([producer-config topic partition key value]
   (let [pc producer-config
         key-serializer (serializers/keyword-serializer)
         value-serializer (json-serializers/json-serializer)]

     (with-open [p (producer/make-producer pc key-serializer value-serializer)]
       (let [producer-record-map {:topic     topic
                                  :partition partition
                                  :key       key
                                  :value     value}]
         (send-async! p producer-record-map))))))

