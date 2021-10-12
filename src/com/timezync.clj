(ns com.timezync
  (:require [clojure.data.csv :as csv]
            [clojure.java.io :as io])
  (:gen-class))

(def rating-map
  {:1 -2
   :2 -1
   :3 1
   :4 2
   :5 3})

(defn read-csv[file-name]
  (try (with-open [reader (io/reader file-name)]
         (doall
           (csv/read-csv reader)))
       (catch Exception e (prn "caught exception: " (.getMessage e)))))

(defn csv-data->maps [csv-data]
  (map zipmap
       (->> (first csv-data)
            (map keyword)
            repeat)
       (rest csv-data)))

(defn filter-records-on-task-id [csv-data task-id]
  (group-by :PersonID (filter #(= task-id (:TaskID %))
                              csv-data)))

(defn segregate-rating [v]
  (apply + (map #(get rating-map
                      (keyword (get % :Rating))) v)))

(defn sort-by-values [results]
  (into (sorted-map-by (fn [key1 key2]
                         (compare [(get results key2) key2]
                                  [(get results key1) key1])))
        results))

(defn accumulate-on-peron-id [person-map]
  (sort-by-values (reduce-kv (fn [m k v]
                               (assoc m k (segregate-rating v)))
                             {}
                             person-map)))

(defn -main [file-name task-id]
  (let [group-by-person-id (-> (read-csv file-name)
                               (csv-data->maps)
                               (filter-records-on-task-id task-id))
        record (if (empty? group-by-person-id) "No Task Id present in the file"
                                               (accumulate-on-peron-id group-by-person-id))]
    (println record)))