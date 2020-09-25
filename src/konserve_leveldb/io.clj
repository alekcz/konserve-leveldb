(ns konserve-leveldb.io
  "IO function for interacting with database"
  (:require [clj-leveldb :as ldb])
  (:import  [java.io ByteArrayInputStream]))

(set! *warn-on-reflection* 1)

(defn split-header [bytes]
  (when bytes
    (let [data  (->> bytes vec (split-at 4))
          streamer (fn [header data] (list (byte-array header) (-> data byte-array (ByteArrayInputStream.))))]
      (apply streamer data))))

(defn id->meta [id]
  (str id "/meta"))

(defn id->data [id]
  (str id "/data"))

(defn it-exists? 
  [db id]
  (some? (ldb/get db (id->meta id))))
  
(defn get-it 
  [db id]
  (let [meta (ldb/get db (id->meta id))
        data (ldb/get db (id->data id))]
    [(split-header meta) (split-header data)]))

(defn get-it-only 
  [db id]
  (split-header (ldb/get db (id->data id))))  

(defn get-meta
  [db id]
  (split-header (ldb/get db (id->meta id))))  

(defn delete-it 
  [db id]
  (ldb/delete db (id->meta id) (id->data id)))

(defn update-it 
  [db id data]
  (ldb/put db (id->meta id) (first data) (id->data id) (second data)))
  
(defn get-keys 
  [db]
  (map #(split-header (second %)) (ldb/iterator db)))

(defn raw-get-it-only 
  [db id]
  (ldb/get db (id->data id)))

(defn raw-get-meta 
  [db id]
  (ldb/get db (id->meta id)))
  
(defn raw-update-it-only 
  [db id data]
  (when data
    (ldb/put db (id->data id) data)))

(defn raw-update-meta
  [db id meta]
  (when meta
    (ldb/put db (id->meta id) meta)))  