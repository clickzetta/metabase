(ns metabase.driver.clickzetta
  "Clickzetta Driver."
  (:require
   [clojure.java.jdbc :as jdbc]
   [clojure.set :as set]
   [clojure.string :as str]
   [java-time.api :as t]
   [medley.core :as m]
   [metabase.driver :as driver]
   [metabase.driver.common :as driver.common]
   [metabase.driver.sql :as driver.sql]
   [metabase.driver.sql-jdbc :as sql-jdbc]
   [metabase.driver.sql-jdbc.common :as sql-jdbc.common]
    [metabase.mbql.util :as mbql.u]
   [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
   [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
   [metabase.driver.sql-jdbc.execute.legacy-impl :as sql-jdbc.legacy]
   [metabase.driver.sql-jdbc.sync :as sql-jdbc.sync]
   [metabase.driver.sql-jdbc.sync.common :as sql-jdbc.sync.common]
   [metabase.driver.sql-jdbc.sync.describe-table
    :as sql-jdbc.describe-table]
    [metabase.db.spec :as db.spec]
   [metabase.driver.sql.query-processor :as sql.qp]
   [metabase.driver.sql.util :as sql.u]
   [metabase.driver.sql.util.unprepare :as unprepare]
   [metabase.driver.sync :as driver.s]
   [metabase.lib.metadata :as lib.metadata]
   [metabase.models.secret :as secret]
   [metabase.query-processor.error-type :as qp.error-type]
   [metabase.query-processor.store :as qp.store]
   [metabase.query-processor.timezone :as qp.timezone]
   [metabase.query-processor.util.add-alias-info :as add]
   [metabase.util :as u]
   [metabase.util.date-2 :as u.date]
   [metabase.util.honey-sql-2 :as h2x]
   [metabase.util.i18n :refer [trs tru]]
   [metabase.util.log :as log]
   [ring.util.codec :as codec])
  (:import
   (java.io File)
   (java.sql Connection DatabaseMetaData ResultSet Types)
   (java.time OffsetDateTime ZonedDateTime)))

(set! *warn-on-reflection* true)

(driver/register! :clickzetta, :parent #{:sql-jdbc ::sql-jdbc.legacy/use-legacy-classes-for-read-and-set})

(doseq [[feature supported?] {:datetime-diff                          false
                              :now                                    true
                              :convert-timezone                       false
                              :connection-impersonation               false
                              :connection-impersonation-requires-role false}]
  (defmethod driver/database-supports? [:clickzetta feature] [_driver _feature _db] supported?))

(defmethod driver/humanize-connection-error-message :clickzetta
  [_ message]
  (log/spy :error (type message))
  (condp re-matches message
    #"(?s).*Object does not exist.*$"
    :database-name-incorrect

    ; default - the Clickzetta errors have a \n in them
    message))

(defmethod driver/db-start-of-week :clickzetta
  [_]
  :sunday)

(defmethod sql-jdbc.sync/database-type->base-type :clickzetta
  [_ base-type]
  ({
    :DECIMAL                    :type/Decimal
    :INT                        :type/Integer
    :BIGINT                     :type/BigInteger
    :SMALLINT                   :type/Integer
    :TINYINT                    :type/Integer
    :FLOAT                      :type/Float
    :DOUBLE                     :type/Float
    :VARCHAR                    :type/Text
    :CHAR                       :type/Text
    :STRING                     :type/Text
    :BOOLEAN                    :type/Boolean
    :DATE                       :type/Date
    :TIMESTAMP_LTZ              :type/DateTime
    :MAP                        :type/*
    :STRUCT                     :type/*
    :ARRAY                      :type/*} base-type))

(defmethod driver/describe-database :clickzetta
  [driver database]
  {:tables
   (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (set
       (for [{schema :schema_name, table-name :table_name} (jdbc/query {:connection conn} ["show tables"])]
         {:name   table-name
          :schema schema}))))})


(defn- valid-describe-table-row? [{:keys [column_name data_type]}]
  (every? (every-pred (complement str/blank?)
                      (complement #(str/starts-with? % "#")))
          [column_name data_type]))

(defn- dash-to-underscore [s]
  (when s
    (str/replace s #"\"" "`")))

(defmethod driver/describe-table :clickzetta
  [driver database {table-name :name, schema :schema}]
  {:name   table-name
   :schema schema
   :fields
   (sql-jdbc.execute/do-with-connection-with-options
    driver
    database
    nil
    (fn [^Connection conn]
      (let [results (jdbc/query {:connection conn} [(format
                                                     "describe %s.%s"
                                                                       (dash-to-underscore schema)
                                                                       (dash-to-underscore table-name))])]
        (set
         (for [[idx {col-name :column_name, data-type :data_type, :as result}] (m/indexed results)
               :when (valid-describe-table-row? result)]
           {:name              col-name
            :database-type     (u/upper-case-en(first(str/split (first(str/split data-type #" ")) #"\(")))
            :base-type         (sql-jdbc.sync/database-type->base-type :clickzetta (keyword (u/upper-case-en(first(str/split (first(str/split data-type #" ")) #"\(")))))
            :database-position idx})))))})

(defmethod db.spec/spec :clickzetta
  [_ {:keys [instance service workspace]
      :as   opts}]
  (merge
   {:classname                     "com.clickzetta.client.jdbc.ClickZettaDriver"
    :subprotocol                   "clickzetta"
    :subname                       (str "//" instance "." service "/" workspace "/")
    }
   (dissoc opts :instance :service :workspace :port)))


(defmethod sql-jdbc.conn/connection-details->spec :clickzetta
  [_ {:keys [user password virtualCluster schema instance service workspace]}]

    (sql-jdbc.common/handle-additional-options {:classname                     "com.clickzetta.client.jdbc.ClickZettaDriver"
                                                   :subprotocol                   "clickzetta"
                                                   :subname                       (str "//" instance "." service "/" workspace)
                                                   }
                                               {:additional-options (str "user=" user "&password=" password "&virtualCluster=" virtualCluster "&schema=" schema)}))



(defmethod sql.qp/honey-sql-version :clickzetta
  [_driver]
  2)

(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :seconds]      [_ _ expr] [:to_unix_timestamp expr])
(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :milliseconds] [_ _ expr] [:to_unix_timestamp expr 3])
(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :microseconds] [_ _ expr] [:to_unix_timestamp expr 6])

(defmethod sql.qp/current-datetime-honeysql-form :clickzetta
  [_]
  (h2x/with-database-type-info :%current_timestamp :TIMESTAMP_LTZ))

(defmethod sql.qp/add-interval-honeysql-form :clickzetta
  [_ hsql-form amount unit]
  [:dateadd
   [:raw (name unit)]
   [:raw (int amount)]
   (h2x/->timestamp hsql-form)])

(defn- date-trunc [unit expr] [:date_trunc unit (h2x/->timestamp expr)])

(defmethod sql.qp/date [:clickzetta :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:clickzetta :minute]          [_ _ expr] (date-trunc :minute expr))
(defmethod sql.qp/date [:clickzetta :hour]            [_ _ expr] (date-trunc :hour expr))
(defmethod sql.qp/date [:clickzetta :day]             [_ _ expr] (date-trunc :day expr))
(defmethod sql.qp/date [:clickzetta :month]           [_ _ expr] (date-trunc :month expr))
(defmethod sql.qp/date [:clickzetta :quarter]         [_ _ expr] (date-trunc :quarter expr))
(defmethod sql.qp/date [:clickzetta :year]            [_ _ expr] (date-trunc :year expr))

;; these don't need to be adjusted for start of week, since we're Setting the WEEK_START connection parameter
(defmethod sql.qp/date [:clickzetta :week]
  [_driver _unit expr]
  (date-trunc :week expr))

(defmethod sql.qp/date [:clickzetta :week-of-year-iso]
  [_ _ expr]
  (:weekofyear expr))

(defmethod sql.qp/date [:clickzetta :day-of-week]
  [_driver _unit expr]
  (:weekday expr))

(defmethod sql.qp/quote-style :clickzetta
  [_driver]
  :mysql)

(defmethod driver/execute-reducible-query :clickzetta
  [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [database (lib.metadata/database (qp.store/metadata-provider))]
    (def schema_select (str "`" (get-in database [:details :schema] "public") "`."))
     (def replace_sql (str/replace sql schema_select ""))
      (log/info "Executing Clickzetta query" replace_sql)
    (let [inner-query (-> (assoc inner-query
                                   :query  replace_sql
                                   :max-rows (mbql.u/query->max-rows-limit outer-query))
                            (dissoc :params))
            query       (assoc outer-query :native inner-query)]
        ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond))))