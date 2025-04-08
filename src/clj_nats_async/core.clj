(ns clj-nats-async.core
  (:require [clojure.string :as str]
            [clojure.tools.logging :as log]
            [clojure.edn :as edn]
            [manifold.stream :as s])
  (:import [io.nats.client Options$Builder Nats Connection MessageHandler Message]))

(def connection? (partial instance? Connection))

(defn- parse-urls [urls]
  (->> urls
       (map #(str/split % #","))
       (flatten)))

(defn- opts->servers [[f :as opts]]
  (->> (cond
         (map? f) (:urls f)
         (sequential? opts) opts
         (string? f) (parse-urls f)
         :else (throw (ex-info "Unknown options format" {:options opts})))
       (into-array String)))

(defn- apply-conf [opts conf]
  (let [appliers {:secure? (fn [o _] (.secure o))
                  :token #(.token %1 (.toCharArray %2))
                  :auth-handler #(.authHandler %1 %2)
                  :credential-path #(.credentialPath %1 %2)
                  :verbose? (fn [o _] (.verbose o))}]
    (reduce-kv (fn [o k v]
                 (let [a (get appliers k)]
                   (cond-> o
                     a (a v))))
               opts
               conf)))

(defn ^Connection create-nats
  "Creates a Nats connection, returning a Nats object.  Opts is a map containing
   the `:urls` and possible other values.  For backwards compatibility, this can
   also be a list of urls, or a comma separated url string."
  [& opts]
  (let [server-opts (-> (Options$Builder.)
                        (.servers (opts->servers opts))
                        (apply-conf (first opts))
                        (.build))]
    (Nats/connect server-opts)))

(defprotocol INatsMessage
  (msg-body [_]))

(defrecord NatsMessage [nats-message]
  INatsMessage
  (msg-body [_]
    (edn/read-string
     (String. (.getData nats-message)
              "UTF-8"))))

(defn- ->message-handler [stream]
  (reify
    MessageHandler
    (onMessage [_ m]
      (s/put! stream (map->NatsMessage {:nats-message m})))))

(defn ^:private create-nats-subscription
  [nats subject {:keys [queue] :as opts} stream]
  (cond-> (.createDispatcher nats)
    queue (.subscribe subject queue (->message-handler stream))
    (not queue) (.subscribe subject (->message-handler stream))))

(defn- unsubscribe [sub]
  (fn []
    (log/debug "Closing NATS subscription:" sub)
    (.unsubscribe (.getDispatcher sub) sub)))

(defn subscribe
  "returns a a Manifold source-only stream of INatsMessages from a NATS subject.
   close the stream to dispose of the subscription"
  ([nats subject] (subscribe nats subject {}))
  ([nats subject opts]
   (let [stream (s/stream)
         source (s/source-only stream)
         nats-subscription (create-nats-subscription nats subject opts stream)]
     (s/on-closed stream #(unsubscribe nats-subscription))
     source)))

(defn publish
  "publish a message
  - subject-or-fn : either a string specifying a fixed subject or a
                     (fn [item] ...) which extracts a subject from an item"
  ([nats subject-or-fn] (publish nats subject-or-fn "" {}))
  ([nats subject-or-fn body] (publish nats subject-or-fn body {}))
  ([nats subject-or-fn body {:keys [reply] :as opts}]
   (let [is-subject-fn? (or (var? subject-or-fn) (fn? subject-or-fn))
         subject (if is-subject-fn? (subject-or-fn body) subject-or-fn)]
     (if subject
       (.publish nats subject reply (.getBytes (pr-str body) "UTF-8"))
       (log/warn (ex-info
                  (str "no subject "
                       (if is-subject-fn? "extracted" "given"))
                  {:body body}))))))

(defn publisher
  "returns a Manifold sink-only stream which publishes items put on the stream
   to NATS"
  ([nats subject-or-fn]
   (let [stream (s/stream)]
     (s/consume (fn [body]
                  (publish nats subject-or-fn body))
                stream)
     (s/sink-only stream))))

(defn pubsub
  "returns a Manifold source+sink stream for a single NATS subject.
   the source returns INatsMessages, while the sink accepts
   strings"
  ([nats subject] (pubsub nats subject {}))
  ([nats subject opts]
   (let [pub-stream (s/stream)
         sub-stream (s/stream)

         nats-subscription (create-nats-subscription nats subject opts sub-stream)]

     (s/consume (fn [body] (publish nats subject body)) pub-stream)

     (s/on-closed sub-stream #(unsubscribe nats-subscription))

     (s/splice pub-stream sub-stream))))
