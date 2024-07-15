(ns metabase.driver.clickzetta
  "ClickZetta Lakehouse Driver."
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
    [metabase.mbql.util :as mbql.u]
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
    [metabase.api.common :as api :refer [*current-user*]]
    [ring.util.codec :as codec])
  (:import
    (java.io File)
    (java.sql Connection DatabaseMetaData ResultSet Types)
    (java.time OffsetDateTime ZonedDateTime)))

(set! *warn-on-reflection* true)

(driver/register! :clickzetta, :parent #{:sql-jdbc ::sql-jdbc.legacy/use-legacy-classes-for-read-and-set})

(doseq [[feature supported?] {:datetime-diff                          true
                              :now                                    true
                              :convert-timezone                       true
                              :connection-impersonation               false
                              :connection-impersonation-requires-role false}]
  (defmethod driver/database-supports? [:clickzetta feature] [_driver _feature _db] supported?))

(defmethod driver/humanize-connection-error-message :clickzetta
  [_ message]
  (log/spy :error (type message))
  (condp re-matches message
    #"(?s).*Object does not exist.*$"
    :database-name-incorrect

    ; default - the ClickZetta Lakehouse errors have a \n in them
    message))
(defmethod sql-jdbc.conn/data-warehouse-connection-pool-properties :clickzetta
  [driver database]
  (merge
   ((get-method sql-jdbc.conn/data-warehouse-connection-pool-properties :sql-jdbc) driver database)
   {"preferredTestQuery" "SELECT 1"}))

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
     :MAP                        :type/Dictionary
     :STRUCT                     :type/*
     :ARRAY                      :type/Array
     :BINARY                     :type/*} base-type))


(defn- describe-schema [driver conn workspace schema]
  (log/info "Get tables of schema : " schema)
  (when (not-empty schema)
    (let [sql (str "SHOW TABLES IN " schema)]
      (log/info "Get tables by : " sql)
      (into #{} (map (fn [{table :table_name}]
                       {:name        table
                        :schema      schema}))
            (jdbc/reducible-query {:connection conn} sql)))))


(def ^:private excluded-schemas
  "The set of schemas that should be excluded when querying all schemas."
  #{"information_schema"})

(defn- all-schemas [driver conn workspace]
  (let [sql (str "SHOW SCHEMAS")]
    (log/info "Get all schemas: " sql)
    (into []
          (map (fn [{:keys [schema_name]}]
                 (when-not (contains? excluded-schemas schema_name)
                   (describe-schema driver conn workspace schema_name))))
          (jdbc/reducible-query {:connection conn} sql))))


(defmethod driver/describe-database :clickzetta
  [driver {{:keys [workspace schema] :as _details} :details :as database}]
  (sql-jdbc.execute/do-with-connection-with-options
   driver
   database
   nil
   (fn [^Connection conn]
     (let [schemas (if schema #{(describe-schema driver conn workspace schema)}
                     (all-schemas driver conn workspace))]
       (log/info "Get all schemas done: " schemas)
       {:tables (reduce set/union #{} schemas)}))))


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
            :base-type         (sql-jdbc.sync/database-type->base-type :clickzetta (keyword (u/upper-case-en(first(str/split (first(str/split (first(str/split data-type #" ")) #"\(")) #"\<")))))
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
  [_ {:keys [user password virtualCluster schema instance service workspace additional]}]
  (let [additional-options-base (str "user=" user "&password=" password "&virtualCluster=" virtualCluster "&schema=" schema)
        additional-options (if (seq schema) (str additional-options-base "&schema=" schema) additional-options-base)
        additional-options-with-additional-params (if (seq additional) (str additional-options "&" additional) additional-options)]
    (sql-jdbc.common/handle-additional-options {:classname                     "com.clickzetta.client.jdbc.ClickZettaDriver"
                                                :subprotocol                   "clickzetta"
                                                :subname                       (str "//" instance "." service "/" workspace)
                                                }
                                               {:additional-options additional-options-with-additional-params})))



(defmethod sql.qp/honey-sql-version :clickzetta
  [_driver]
  2)

(defmethod sql.qp/current-datetime-honeysql-form :clickzetta
  [_]
  (h2x/with-database-type-info :%current_timestamp :TIMESTAMP_LTZ))

(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :seconds]
  [_ _ expr]
  (h2x/->timestamp [:from_unixtime expr]))

(defn- date-format [format-str expr]
  [:date_format expr (h2x/literal format-str)])

(defn- str-to-date [format-str expr]
  (h2x/->timestamp [:from_unixtime [:unix_timestamp expr (h2x/literal format-str)]]))

(defn- trunc-with-format [format-str expr]
  (str-to-date format-str (date-format format-str expr)))

(defmethod sql.qp/date [:clickzetta :default]         [_ _ expr] (h2x/->timestamp expr))
(defmethod sql.qp/date [:clickzetta :minute]          [_ _ expr] (trunc-with-format "yyyy-MM-dd HH:mm" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:clickzetta :minute-of-hour]  [_ _ expr] [:minute (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :hour]            [_ _ expr] (trunc-with-format "yyyy-MM-dd HH" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:clickzetta :hour-of-day]     [_ _ expr] [:hour (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :day]             [_ _ expr] (trunc-with-format "yyyy-MM-dd" (h2x/->timestamp expr)))
(defmethod sql.qp/date [:clickzetta :day-of-month]    [_ _ expr] [:dayofmonth (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :day-of-year]     [_ _ expr] (h2x/->integer (date-format "D" (h2x/->timestamp expr))))
(defmethod sql.qp/date [:clickzetta :month]           [_ _ expr] [:date_trunc (h2x/literal :MM) (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :month-of-year]   [_ _ expr] [:month (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :quarter-of-year] [_ _ expr] [:quarter (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :year]            [_ _ expr] [:date_trunc  (h2x/literal :year) (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta :week]            [_ _ expr] [:date_trunc (h2x/literal :week) (h2x/->timestamp expr)])
(defmethod sql.qp/date [:clickzetta ::quarter]        [_ _ expr] [:date_trunc (h2x/literal ::quarter) (h2x/->timestamp expr)])


(defmethod sql.qp/date [:clickzetta :day-of-week]
  [_driver _unit expr]
  [:dayofweek (h2x/->timestamp expr)])


(defmethod sql.qp/date [:clickzetta :week-of-year-iso]
  [_driver _unit expr]
  [:weekofyear (h2x/->timestamp expr)])

(defmethod sql.qp/->honeysql [:clickzetta :replace]
  [driver [_ arg pattern replacement]]
  [:regexp_replace
   (sql.qp/->honeysql driver arg)
   (sql.qp/->honeysql driver pattern)
   (sql.qp/->honeysql driver replacement)])

(defmethod sql.qp/->honeysql [:clickzetta :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) [:raw (int 0)]])

(defmethod sql.qp/->honeysql [:clickzetta :median]
  [driver [_ arg]]
  [:percentile (sql.qp/->honeysql driver arg) [:raw (float 0.5)]])

(defmethod sql.qp/->honeysql [:clickzetta :percentile]
  [driver [_ arg p]]
  [:percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)])

(defmethod sql.qp/add-interval-honeysql-form :clickzetta
  [_ hsql-form amount unit]
  [:dateadd
   [:raw (name unit)]
   [:raw (int amount)]
   (h2x/->timestamp hsql-form)])

(defmethod sql.qp/datetime-diff [:clickzetta :year]
  [driver _unit x y]
  [:timestampdiff [:raw (name "year")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :quarter]
  [driver _unit x y]
  [:timestampdiff [:raw (name "quarter")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :month]
  [_driver _unit x y]
  [:timestampdiff [:raw (name "month")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :week]
  [_driver _unit x y]
  [:timestampdiff [:raw (name "week")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :day]
  [_driver _unit x y]
  [:timestampdiff [:raw (name "day")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :hour]
  [driver _unit x y]
  [:timestampdiff [:raw (name "hour")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :minute]
  [driver _unit x y]
  [:timestampdiff [:raw (name "minute")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :second]
  [_driver _unit x y]
  [:timestampdiff [:raw (name "second")] x y])

(defmethod sql.qp/datetime-diff [:clickzetta :millisecond]
  [_driver _unit x y]
  [:timestampdiff [:raw (name "millisecond")] x y])

(defmethod sql.qp/quote-style :clickzetta
  [_driver]
  :mysql)

(defmethod sql.qp/->honeysql [:clickzetta :power]
  [driver [_ arg power]]
  [:pow
   (sql.qp/->honeysql driver arg)
   (sql.qp/->honeysql driver power)])

(defmethod sql.qp/->honeysql [:clickzetta :log]
  [driver [_ arg]]
  [:log10
   (sql.qp/->honeysql driver arg)])

(defmethod sql.qp/->honeysql [:clickzetta :convert-timezone]
  [driver [_ arg target-timezone source-timezone]]
  [:convert_timezone
   source-timezone
   target-timezone
   (sql.qp/->honeysql driver arg)])

(defmethod driver/execute-reducible-query :clickzetta
  [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [database (lib.metadata/database (qp.store/metadata-provider))]
    (let [schema_select (str "`" (get-in database [:details :schema] "public") "`.")]
      (let [replace_sql (str/replace sql schema_select "")]
        (let [
               common-name (:common_name @*current-user*)
               query-tag-str (format "set query_tag='%s';" common-name)
               real-query (str query-tag-str replace_sql)]
          (log/info "Executing ClickZetta Lakehouse query" replace_sql)
          (let [inner-query (-> (assoc inner-query
                                       :query  replace_sql
                                       :max-rows (mbql.u/query->max-rows-limit outer-query)))
                query       (assoc outer-query :native inner-query)]
            ((get-method driver/execute-reducible-query :sql-jdbc) driver query context respond)))))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickzetta Types/DATE]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getDate rs i)]
      (t/zoned-date-time (t/local-date t) (t/local-time 0) (t/zone-id "UTC")))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickzetta Types/TIMESTAMP]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/zoned-date-time (t/local-date-time t) (t/zone-id "UTC")))))

(defmethod sql-jdbc.execute/read-column-thunk [:clickzetta Types/TIMESTAMP_WITH_TIMEZONE]
  [_ ^ResultSet rs _rsmeta ^Integer i]
  (fn []
    (when-let [t (.getTimestamp rs i)]
      (t/zoned-date-time (t/local-date-time t) (t/zone-id "UTC")))))
