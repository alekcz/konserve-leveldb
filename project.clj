(defproject alekcz/konserve-leveldb "0.1.0-SNAPSHOT"
  :description "A leveldb backend for konserve using clj-leveldb."
  :url "https://github.com/alekcz/konserve-leveldb"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :aot :all
  :dependencies [[org.clojure/clojure "1.10.2-alpha1"]
                 [factual/clj-leveldb "0.1.1"]
                 [byte-streams "0.2.2"] ;required for clj-leveldb to work https://github.com/ztellman/clj-tuple/issues/18
                 [io.replikativ/konserve "0.6.0-alpha1"]]
  :repl-options {:init-ns konserve-leveldb.core}
  :plugins [[lein-cloverage "1.2.0"]]
  :profiles { :dev {:dependencies [[metosin/malli "0.0.1-20200404.091302-14"]]}})
