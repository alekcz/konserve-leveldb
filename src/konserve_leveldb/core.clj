(ns konserve-leveldb.core
  "Address globally aggregated immutable key-value stores(s)."
  (:require [clojure.core.async :as async]
            [konserve.serializers :as ser]
            [konserve.compressor :as comp]
            [konserve.encryptor :as encr]
            [hasch.core :as hasch]
            [konserve-leveldb.io :as io]
            [clj-leveldb :as ldb]
            [konserve.protocols :refer [PEDNAsyncKeyValueStore
                                        -exists? -get -get-meta
                                        -update-in -assoc-in -dissoc
                                        PBinaryAsyncKeyValueStore
                                        -bassoc -bget
                                        -serialize -deserialize
                                        PKeyIterable
                                        -keys]]
            [konserve.storage-layout :refer [SplitLayout]])
  (:import  [java.io ByteArrayOutputStream]
            [org.iq80.leveldb DB]))

(set! *warn-on-reflection* 1)

(def store-layout 1)

(defn str-uuid 
  [key] 
  (str (hasch/uuid key))) 

(defn prep-ex 
  [^String message ^Exception e]
  ; (.printStackTrace e)
  (ex-info message {:error (.getMessage e) :cause (.getCause e) :trace (.getStackTrace e)}))

(defn prep-stream 
  [stream]
  { :input-stream stream
    :size nil})

(defrecord LevelDBStore [db path default-serializer serializers compressor encryptor read-handlers write-handlers locks]
  PEDNAsyncKeyValueStore
  (-exists? 
    [this key] 
      (let [res-ch (async/chan 1)]
        (async/thread
          (try
            (async/put! res-ch (io/it-exists? db (str-uuid key)))
            (catch Exception e (async/put! res-ch (prep-ex "Failed to determine if item exists" e)))))
        res-ch))

  (-get 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-it-only db (str-uuid key))]
            (if (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve value from store" e)))))
      res-ch))

  (-get-meta 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-meta db (str-uuid key))]
            (if (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)] 
                (async/put! res-ch data))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve metadata from store" e)))))
      res-ch))

  (-update-in 
    [this key-vec meta-up-fn up-fn args]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[fkey & rkey] key-vec
                [[mheader ometa'] [vheader oval']] (io/get-it db (str-uuid fkey))
                old-val [(when ometa'
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers ometa')))
                         (when oval'
                            (let [vserializer (ser/byte->serializer  (get vheader 1))
                                  vcompressor (comp/byte->compressor (get vheader 2))
                                  vencryptor  (encr/byte->encryptor  (get vheader 3))
                                  reader (-> vserializer vencryptor vcompressor)]
                              (-deserialize reader read-handlers oval')))]            
                [nmeta nval] [(meta-up-fn (first old-val)) 
                              (if rkey (apply update-in (second old-val) rkey up-fn args) (apply up-fn (second old-val) args))]
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when nmeta 
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers nmeta))
            (when nval 
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers nval))    
            (io/update-it db (str-uuid fkey) [(.toByteArray mbaos) (.toByteArray vbaos)])
            (async/put! res-ch [(second old-val) nval]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to update/write value in store" e)))))
        res-ch))

  (-assoc-in [this key-vec meta val] (-update-in this key-vec meta (fn [_] val) []))

  (-dissoc 
    [this key] 
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (io/delete-it db (str-uuid key))
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to delete key-value pair from store" e)))))
        res-ch))

  PBinaryAsyncKeyValueStore
  (-bget 
    [this key locked-cb]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[header res] (io/get-it-only db (str-uuid key))]
            (if (some? res) 
              (let [rserializer (ser/byte->serializer (get header 1))
                    rcompressor (comp/byte->compressor (get header 2))
                    rencryptor  (encr/byte->encryptor  (get header 3))
                    reader (-> rserializer rencryptor rcompressor)
                    data (-deserialize reader read-handlers res)]
                (async/put! res-ch (locked-cb (prep-stream data))))
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve binary value from store" e)))))
      res-ch))

  (-bassoc 
    [this key meta-up-fn input]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [[[mheader old-meta'] [_ old-val]] (io/get-it db (str-uuid key))
                old-meta (when old-meta' 
                            (let [mserializer (ser/byte->serializer  (get mheader 1))
                                  mcompressor (comp/byte->compressor (get mheader 2))
                                  mencryptor  (encr/byte->encryptor  (get mheader 3))
                                  reader (-> mserializer mencryptor mcompressor)]
                              (-deserialize reader read-handlers old-meta')))           
                new-meta (meta-up-fn old-meta) 
                serializer (get serializers default-serializer)
                writer (-> serializer compressor encryptor)
                ^ByteArrayOutputStream mbaos (ByteArrayOutputStream.)
                ^ByteArrayOutputStream vbaos (ByteArrayOutputStream.)]
            (when new-meta 
              (.write mbaos ^byte store-layout)
              (.write mbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write mbaos ^byte (comp/compressor->byte compressor))
              (.write mbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer mbaos write-handlers new-meta))
            (when input
              (.write vbaos ^byte store-layout)
              (.write vbaos ^byte (ser/serializer-class->byte (type serializer)))
              (.write vbaos ^byte (comp/compressor->byte compressor))
              (.write vbaos ^byte (encr/encryptor->byte encryptor))
              (-serialize writer vbaos write-handlers input))  
            (io/update-it db (str-uuid key) [(.toByteArray mbaos) (.toByteArray vbaos)])
            (async/put! res-ch [old-val input]))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write binary value in store" e)))))
        res-ch))

  PKeyIterable
  (-keys 
    [_]
    (let [res-ch (async/chan)]
      (async/thread
        (try
          (let [key-stream (io/get-keys db)
                keys' (when key-stream
                        (for [[header k] key-stream]
                          (let [rserializer (ser/byte->serializer (get header 1))
                                rcompressor (comp/byte->compressor (get header 2))
                                rencryptor  (encr/byte->encryptor  (get header 3))
                                reader (-> rserializer rencryptor rcompressor)]
                            (-deserialize reader read-handlers k))))
                keys (doall (filter some? (map :key keys')))]
            (doall
              (map #(async/put! res-ch %) keys))
            (async/close! res-ch)) 
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve keys from store" e)))))
        res-ch))
        
  SplitLayout      
  (-get-raw-meta [this key]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-get-meta db (str-uuid key))]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve raw metadata from store" e)))))
      res-ch))
  (-put-raw-meta [this key binary]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (io/raw-update-meta db (str-uuid key) binary)
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write raw metadata to store" e)))))
      res-ch))
  (-get-raw-value [this key]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (let [res (io/raw-get-it-only db (str-uuid key))]
            (if res
              (async/put! res-ch res)
              (async/close! res-ch)))
          (catch Exception e (async/put! res-ch (prep-ex "Failed to retrieve raw value from store" e)))))
      res-ch))
  (-put-raw-value [this key binary]
    (let [res-ch (async/chan 1)]
      (async/thread
        (try
          (io/raw-update-it-only db (str-uuid key) binary)
          (async/close! res-ch)
          (catch Exception e (async/put! res-ch (prep-ex "Failed to write raw value to store" e)))))
      res-ch)))

(defn new-leveldb-store
  [path & {:keys [leveldb-opts default-serializer serializers compressor encryptor read-handlers write-handlers]
                :or {leveldb-opts {}
                     default-serializer :FressianSerializer
                     compressor comp/lz4-compressor
                     encryptor encr/null-encryptor
                     read-handlers (atom {})
                     write-handlers (atom {})}}]
  (let [res-ch (async/chan 1)]
    (async/thread 
      (try
        (let [db (ldb/create-db path leveldb-opts)]
          (async/put! res-ch
            (map->LevelDBStore {:db db
                                :path path
                                :default-serializer default-serializer
                                :serializers (merge ser/key->serializer serializers)
                                :compressor compressor
                                :encryptor encryptor
                                :read-handlers read-handlers
                                :write-handlers write-handlers
                                :locks (atom {})})))
        (catch Exception e (async/put! res-ch (prep-ex "Failed to dbect to store" e)))))          
    res-ch))

(defn release
  "Close the underlying LevelDB store. This cleanup is necessary as only one
  instance of LevelDB is allowed to work on a store at a time."
  [store]
  (.close ^DB (:db store)))

(defn delete-store [store]
  (let [res-ch (async/chan 1)]
    (async/thread
      (try
        (release (:db store))
        (ldb/destroy-db (:path store))
        (async/close! res-ch)
        (catch Exception e (async/put! res-ch (prep-ex "Failed to delete store" e)))))          
    res-ch))