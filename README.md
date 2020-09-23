# konserve-leveldb

A leveldb backend for [konserve](https://github.com/replikativ/konserve) implemented with [clj-leveldb](https://github.com/Factual/clj-leveldb). 


# Status

![build](https://github.com/alekcz/konserve-leveldb/workflows/build/badge.svg?branch=master) [![codecov](https://codecov.io/gh/alekcz/konserve-leveldb/branch/master/graph/badge.svg)](https://codecov.io/gh/alekcz/konserve-leveldb) 

## Usage

[![Clojars Project](https://img.shields.io/clojars/v/alekcz/konserve-leveldb.svg)](http://clojars.org/alekcz/konserve-leveldb)

`[alekcz/konserve-leveldb "0.1.0-SNAPSHOT"]`

The purpose of konserve is to have a unified associative key-value interface for
edn datastructures and binary blobs. Use the standard interface functions of konserve.

```clojure
(require '[konserve-leveldb.core :refer :all]
         '[clojure.core.async :refer [<!!] :as async]
         '[konserve.core :as k])
  
  (def leveldb-store (<!! (new-leveldb-store "/path/to/db")))

  (<!! (k/exists? leveldb-store  "cecilia"))
  (<!! (k/get-in leveldb-store ["cecilia"]))
  (<!! (k/assoc-in leveldb-store ["cecilia"] 28))
  (<!! (k/update-in leveldb-store ["cecilia"] inc))
  (<!! (k/get-in leveldb-store ["cecilia"]))

  (defrecord Test [a])
  (<!! (k/assoc-in leveldb-store ["agatha"] (Test. 35)))
  (<!! (k/get-in leveldb-store ["agatha"]))
```

## License

Copyright © 2020 Alexander Oloo

This program and the accompanying materials are made available under the
terms of the Eclipse Public License 2.0 which is available at
http://www.eclipse.org/legal/epl-2.0.

This Source Code may also be made available under the following Secondary
Licenses when the conditions for such availability set forth in the Eclipse
Public License, v. 2.0 are satisfied: GNU General Public License as published by
the Free Software Foundation, either version 2 of the License, or (at your
option) any later version, with the GNU Classpath Exception which is available
at https://www.gnu.org/software/classpath/license.html.