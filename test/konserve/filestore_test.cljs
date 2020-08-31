(ns konserve.filestore-test
  #_(:require [konserve.core :as k]
              [cljs.test :refer-macros [deftest is testing async use-fixtures]]
              [cljs.core.async :as async :refer (take! <! >! put! take! close! chan poll!)]
              [konserve.filestore :refer [new-fs-store delete-store list-keys]]
              [cljs.nodejs :as node])
  #_(:require-macros [cljs.core.async.macros :refer [go]]))

#_(comment
    (defonce fs (node/require "fs"))

    (defonce stream (node/require "stream"))

    (enable-console-print!)

    (use-fixtures :once
      {:before
       (fn []
         (async done
                (go (delete-store "/tmp/konserve-fs-nodejs-test")
                    (def store (<! (new-fs-store "/tmp/konserve-fs-nodejs-test")))
                    (done))))})

    (deftest filestore-test
      (testing "Test the file store functionality."
        (async done
               (go
                 (let [folder "/tmp/konserve-fs-nodejs-test"
                       _      (<! (k/bassoc store :binbar (js/Buffer.from (clj->js (range 10)))))
                       binbar (atom nil)
                       _      (<! (k/bget store :binbar #(let [ch (chan)
                                                               rs (:read-stream %)]
                                                           (.on rs "data" (fn [chunk]
                                                                            (let [x chunk]
                                                                              (reset! binbar x))))
                                                           (.on rs "close" (fn [_]
                                                                             (put! ch true)
                                                                             (close! ch)))
                                                           (.on rs "error" (fn [err] (prn err)))
                                                           ch)))]
                   (is (= (<! (k/get-in store [:foo]))
                          nil))
                   (<! (k/assoc-in store [:foo] :bar))
                   (is (= (<! (k/get-in store [:foo]))
                          :bar))
                   (is (= (<! (list-keys store {}))
                          #{{:key :foo, :format :edn} {:key :binbar, :format :binary}}))
                   (<! (k/dissoc store :foo))
                   (is (= (<! (k/get-in store [:foo]))
                          nil))
                   (is (= (<! (list-keys store {}))
                          #{{:key :binbar, :format :binary}}))
                   (is (= (.toString @binbar) (.toString (js/Buffer.from (clj->js (range 10))))))
                   (delete-store folder)
                   (let [store (<! (new-fs-store folder))]
                     (is (= (<! (list-keys store {}))
                            #{})))
                   (done)))))))
