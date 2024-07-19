(ns metabase.driver.clickzetta
  "ClickZetta Lakehouse Driver."
  (:require
    [buddy.core.codecs :as codecs]
    [clojure.java.jdbc :as jdbc]
    [clojure.set :as set]
    [clojure.string :as str]
    [honey.sql :as sql]
    [honey.sql.helpers :as sql.helpers]
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
    [metabase.query-processor.util :as qp.util]
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
    (java.sql Connection DatabaseMetaData ResultSet Time Types)
    (java.time LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime)
    (java.time.format DateTimeFormatter)
    (java.time.temporal ChronoField Temporal)))

(set! *warn-on-reflection* true)

(driver/register! :clickzetta, :parent #{:sql-jdbc ::sql-jdbc.legacy/use-legacy-classes-for-read-and-set})

(doseq [[feature supported?] {:datetime-diff                          true
                              :now                                    true
                              :convert-timezone                       true
                              :connection-impersonation               false
                              :connection-impersonation-requires-role false
                              :foreign-keys                           false}]
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
   {"testConnectionOnCheckout" false
    "testConnectionOnCheckin" false}))

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

(defmethod driver/can-connect? :clickzetta
  [driver { :as details}]
  true)

;----------------------------------------------------------------------------

;(defmethod sql.qp/honey-sql-version :clickzetta
;  [_driver]
;  2)
;
;(defmethod sql.qp/current-datetime-honeysql-form :clickzetta
;  [_]
;  (h2x/with-database-type-info :%current_timestamp :TIMESTAMP_LTZ))
;
;(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :seconds]
;  [_ _ expr]
;  (h2x/->timestamp [:from_unixtime expr]))
;
;(defn- date-format [format-str expr]
;  [:date_format expr (h2x/literal format-str)])
;
;(defn- str-to-date [format-str expr]
;  (h2x/->timestamp [:from_unixtime [:unix_timestamp expr (h2x/literal format-str)]]))
;
;(defn- trunc-with-format [format-str expr]
;  (str-to-date format-str (date-format format-str expr)))
;
;(defmethod sql.qp/date [:clickzetta :default]         [_ _ expr] (h2x/->timestamp expr))
;(defmethod sql.qp/date [:clickzetta :minute]          [_ _ expr] (trunc-with-format "yyyy-MM-dd HH:mm" (h2x/->timestamp expr)))
;(defmethod sql.qp/date [:clickzetta :minute-of-hour]  [_ _ expr] [:minute (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta :hour]            [_ _ expr] (trunc-with-format "yyyy-MM-dd HH" (h2x/->timestamp expr)))
;(defmethod sql.qp/date [:clickzetta :hour-of-day]     [_ _ expr] [:hour (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta :day]             [_ _ expr] (trunc-with-format "yyyy-MM-dd" (h2x/->timestamp expr)))
;(defmethod sql.qp/date [:clickzetta :day-of-month]    [_ _ expr] [:dayofmonth (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta :day-of-year]     [_ _ expr] (h2x/->integer (date-format "D" (h2x/->timestamp expr))))
;(defmethod sql.qp/date [:clickzetta :month]           [_ _ expr] [:date_trunc (h2x/literal :MM) (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta :month-of-year]   [_ _ expr] [:month (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta :quarter-of-year] [_ _ expr] [:quarter (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta :year]            [_ _ expr] [:date_trunc  (h2x/literal :year) (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta :week]            [_ _ expr] [:date_trunc (h2x/literal :week) (h2x/->timestamp expr)])
;(defmethod sql.qp/date [:clickzetta ::quarter]        [_ _ expr] [:date_trunc (h2x/literal ::quarter) (h2x/->timestamp expr)])
;
;
;(defmethod sql.qp/date [:clickzetta :day-of-week]
;  [_driver _unit expr]
;  [:dayofweek (h2x/->timestamp expr)])
;
;
;(defmethod sql.qp/date [:clickzetta :week-of-year-iso]
;  [_driver _unit expr]
;  [:weekofyear (h2x/->timestamp expr)])
;
;(defmethod sql.qp/->honeysql [:clickzetta :replace]
;  [driver [_ arg pattern replacement]]
;  [:regexp_replace
;   (sql.qp/->honeysql driver arg)
;   (sql.qp/->honeysql driver pattern)
;   (sql.qp/->honeysql driver replacement)])
;
;(defmethod sql.qp/->honeysql [:clickzetta :regex-match-first]
;  [driver [_ arg pattern]]
;  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern) [:raw (int 0)]])
;
;(defmethod sql.qp/->honeysql [:clickzetta :median]
;  [driver [_ arg]]
;  [:percentile (sql.qp/->honeysql driver arg) [:raw (float 0.5)]])
;
;(defmethod sql.qp/->honeysql [:clickzetta :percentile]
;  [driver [_ arg p]]
;  [:percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)])
;
;(defmethod sql.qp/add-interval-honeysql-form :clickzetta
;  [_ hsql-form amount unit]
;  [:dateadd
;   [:raw (name unit)]
;   [:raw (int amount)]
;   (h2x/->timestamp hsql-form)])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :year]
;  [driver _unit x y]
;  [:timestampdiff [:raw (name "year")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :quarter]
;  [driver _unit x y]
;  [:timestampdiff [:raw (name "quarter")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :month]
;  [_driver _unit x y]
;  [:timestampdiff [:raw (name "month")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :week]
;  [_driver _unit x y]
;  [:timestampdiff [:raw (name "week")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :day]
;  [_driver _unit x y]
;  [:timestampdiff [:raw (name "day")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :hour]
;  [driver _unit x y]
;  [:timestampdiff [:raw (name "hour")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :minute]
;  [driver _unit x y]
;  [:timestampdiff [:raw (name "minute")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :second]
;  [_driver _unit x y]
;  [:timestampdiff [:raw (name "second")] x y])
;
;(defmethod sql.qp/datetime-diff [:clickzetta :millisecond]
;  [_driver _unit x y]
;  [:timestampdiff [:raw (name "millisecond")] x y])
;
;(defmethod sql.qp/quote-style :clickzetta
;  [_driver]
;  :mysql)
;
;(defmethod sql.qp/->honeysql [:clickzetta :power]
;  [driver [_ arg power]]
;  [:pow
;   (sql.qp/->honeysql driver arg)
;   (sql.qp/->honeysql driver power)])
;
;(defmethod sql.qp/->honeysql [:clickzetta :log]
;  [driver [_ arg]]
;  [:log10
;   (sql.qp/->honeysql driver arg)])
;
;(defmethod sql.qp/->honeysql [:clickzetta :convert-timezone]
;  [driver [_ arg target-timezone source-timezone]]
;  [:convert_timezone
;   source-timezone
;   target-timezone
;   (sql.qp/->honeysql driver arg)])


;-------------------------------------------------------------------
(defmethod driver/execute-reducible-query :clickzetta
  [driver {{sql :query, :keys [params], :as inner-query} :native, :as outer-query} context respond]
  (let [database (lib.metadata/database (qp.store/metadata-provider))]
    (let [schema_select (str "`" (get-in database [:details :schema] "public") "`.")]
      (let [replace_sql (str/replace sql schema_select "")]
        (let [
               common-name (:common_name @*current-user*)
               query-tag-str (format "set query_tag='%s';" common-name)
               real-query (str query-tag-str replace_sql)]
          (log/info "Executing ClickZetta Lakehouse query" real-query)
          (let [remark (qp.util/query->remark driver outer-query)
                query-with-comment    (str "-- " remark "\n" sql)
                inner-query (-> (assoc inner-query
                                       :query  query-with-comment
                                       :max-rows (mbql.u/query->max-rows-limit outer-query)))
                query       (assoc outer-query :native inner-query)]
            (log/info "Indeed Executing ClickZetta Lakehouse query" query)
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


;------------------------------------------------------------------------
(defmethod driver/db-start-of-week :clickzetta
  [_]
  :monday)

(defmethod sql.qp/cast-temporal-string [:clickzetta :Coercion/YYYYMMDDHHMMSSString->Temporal]
  [_ _coercion-strategy expr]
  [:date_parse expr (h2x/literal "%Y%m%d%H%i%s")])

(defmethod sql.qp/cast-temporal-byte [:clickzetta :Coercion/YYYYMMDDHHMMSSBytes->Temporal]
  [driver _coercion-strategy expr]
  (sql.qp/cast-temporal-string driver :Coercion/YYYYMMDDHHMMSSString->Temporal
                               [:from_utf8 expr]))

(defmethod sql.qp/->honeysql [:clickzetta Boolean]
  [_ bool]
  [:raw (if bool "TRUE" "FALSE")])

(defmethod sql.qp/->honeysql [:clickzetta :time]
  [_ [_ t]]
  (h2x/cast :time (u.date/format-sql (t/local-time t))))

(defmethod sql.qp/->float :clickzetta
  [_ value]
  (h2x/cast :double value))

(defmethod sql.qp/->honeysql [:clickzetta :regex-match-first]
  [driver [_ arg pattern]]
  [:regexp_extract (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver pattern)])

(defmethod sql.qp/->honeysql [:clickzetta :median]
  [driver [_ arg]]
  [:approx_percentile (sql.qp/->honeysql driver arg) 0.5])

(defmethod sql.qp/->honeysql [:clickzetta :percentile]
  [driver [_ arg p]]
  [:approx_percentile (sql.qp/->honeysql driver arg) (sql.qp/->honeysql driver p)])

;;; Presto mod is a function like mod(x, y) rather than an operator like x mod y
(defn- format-mod
  [_fn [x y]]
  (let [[x-sql & x-args] (sql/format-expr x {:nested true})
        [y-sql & y-args] (sql/format-expr y {:nested true})]
    (into [(format "mod(%s, %s)" x-sql y-sql)]
          cat
          [x-args
           y-args])))

(sql/register-fn! ::mod #'format-mod)

(def ^:dynamic ^:private *param-splice-style*
  "How we should splice params into SQL (i.e. 'unprepare' the SQL). Either `:friendly` (the default) or `:paranoid`.
  `:friendly` makes a best-effort attempt to escape strings and generate SQL that is nice to look at, but should not
  be considered safe against all SQL injection -- use this for 'convert to SQL' functionality. `:paranoid` hex-encodes
  strings so SQL injection is impossible; this isn't nice to look at, so use this for actually running a query."
  :friendly)

(defmethod unprepare/unprepare-value [:clickzetta String]
  [_ ^String s]
  (case *param-splice-style*
    :friendly (str \' (sql.u/escape-sql s :ansi) \')
    :paranoid (format "from_utf8(from_hex('%s'))" (codecs/bytes->hex (.getBytes s "UTF-8")))))

;; See https://prestodb.io/docs/current/functions/datetime.html

;; This is only needed for test purposes, because some of the sample data still uses legacy types
(defmethod unprepare/unprepare-value [:clickzetta Time]
  [driver t]
  (unprepare/unprepare-value driver (t/local-time t)))

(defmethod unprepare/unprepare-value [:clickzetta OffsetDateTime]
  [_ t]
  (format "timestamp '%s %s %s'" (t/local-date t) (t/local-time t) (t/zone-offset t)))

(defmethod unprepare/unprepare-value [:clickzetta ZonedDateTime]
  [_ t]
  (format "timestamp '%s %s %s'" (t/local-date t) (t/local-time t) (t/zone-id t)))

;;; `:sql-driver` methods

(defn- format-row-number-over
  [_tag [subquery]]
  (let [[subquery-sql & subquery-args] (sql/format-expr subquery)]
    (into [(format "row_number() OVER %s" subquery-sql)]
          subquery-args)))

(sql/register-fn! ::row-number-over #'format-row-number-over)

(defmethod sql.qp/apply-top-level-clause [:clickzetta :page]
  [_driver _top-level-clause honeysql-query {{:keys [items page]} :page}]
  {:pre [(pos-int? items) (pos-int? page)]}
  (let [offset (* (dec page) items)]
    (if (zero? offset)
      ;; if there's no offset we can simply use limit
      (sql.helpers/limit honeysql-query [:inline items])
      ;; if we need to do an offset we have to do nesting to generate a row number and where on that
      (let [over-clause [::row-number-over (select-keys honeysql-query [:order-by])]]
        (-> (apply sql.helpers/select (map last (:select honeysql-query)))
            (sql.helpers/from [(sql.helpers/select honeysql-query [over-clause :__rownum__])])
            (sql.helpers/where [:> :__rownum__ [:inline offset]])
            (sql.helpers/limit [:inline items]))))))

(defmethod sql.qp/current-datetime-honeysql-form :clickzetta
  [_driver]
  (h2x/with-database-type-info :%now "timestamp with time zone"))

(defn- date-diff [unit a b] [:date_diff (h2x/literal unit) a b])
(defn- date-trunc [unit x] [:date_trunc (h2x/literal unit) x])

(defmethod sql.qp/date [:clickzetta :default]         [_ _ expr] expr)
(defmethod sql.qp/date [:clickzetta :minute]          [_ _ expr] (date-trunc :minute expr))
(defmethod sql.qp/date [:clickzetta :minute-of-hour]  [_ _ expr] [:minute expr])
(defmethod sql.qp/date [:clickzetta :hour]            [_ _ expr] (date-trunc :hour expr))
(defmethod sql.qp/date [:clickzetta :hour-of-day]     [_ _ expr] [:hour expr])
(defmethod sql.qp/date [:clickzetta :day]             [_ _ expr] (date-trunc :day expr))
(defmethod sql.qp/date [:clickzetta :day-of-month]    [_ _ expr] [:day expr])
(defmethod sql.qp/date [:clickzetta :day-of-year]     [_ _ expr] [:day_of_year expr])

(defmethod sql.qp/date [:clickzetta :day-of-week]
  [driver _ expr]
  (sql.qp/adjust-day-of-week driver [:day_of_week expr]))

(defmethod sql.qp/date [:clickzetta :week]
  [driver _ expr]
  (sql.qp/adjust-start-of-week driver (partial date-trunc :week) expr))

(defmethod sql.qp/date [:clickzetta :month]           [_ _ expr] (date-trunc :month expr))
(defmethod sql.qp/date [:clickzetta :month-of-year]   [_ _ expr] [:month expr])
(defmethod sql.qp/date [:clickzetta :quarter]         [_ _ expr] (date-trunc :quarter expr))
(defmethod sql.qp/date [:clickzetta :quarter-of-year] [_ _ expr] [:quarter expr])
(defmethod sql.qp/date [:clickzetta :year]            [_ _ expr] (date-trunc :year expr))

(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :seconds]
  [_driver _seconds-or-milliseconds expr]
  [:from_unixtime expr])

(defn ->date
  "Same as [[h2x/->date]], but truncates `x` to the date in the results time zone."
  [x]
  (h2x/->date (h2x/at-time-zone x (qp.timezone/results-timezone-id))))

(defmethod sql.qp/datetime-diff [:clickzetta :year]    [_driver _unit x y] (date-diff :year (->date x) (->date y)))
(defmethod sql.qp/datetime-diff [:clickzetta :quarter] [_driver _unit x y] (date-diff :quarter (->date x) (->date y)))
(defmethod sql.qp/datetime-diff [:clickzetta :month]   [_driver _unit x y] (date-diff :month (->date x) (->date y)))
(defmethod sql.qp/datetime-diff [:clickzetta :week]    [_driver _unit x y] (date-diff :week (->date x) (->date y)))
(defmethod sql.qp/datetime-diff [:clickzetta :day]     [_driver _unit x y] (date-diff :day (->date x) (->date y)))
(defmethod sql.qp/datetime-diff [:clickzetta :hour]    [_driver _unit x y] (date-diff :hour x y))
(defmethod sql.qp/datetime-diff [:clickzetta :minute]  [_driver _unit x y] (date-diff :minute x y))
(defmethod sql.qp/datetime-diff [:clickzetta :second]  [_driver _unit x y] (date-diff :second x y))

(defmethod driver/db-default-timezone :clickzetta
  [driver database]
  (sql-jdbc.execute/do-with-connection-with-options
   driver database nil
   (fn [^java.sql.Connection conn]
     ;; TODO -- this is the session timezone, right? As opposed to the default timezone? Ick. Not sure how to get the
     ;; default timezone if session timezone is unspecified.
     (with-open [stmt (.prepareStatement conn "SELECT current_timezone()")
                 rset (.executeQuery stmt)]
       (when (.next rset)
         (.getString rset 1))))))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                          Custom HoneySQL Clause Impls                                          |
;;; +----------------------------------------------------------------------------------------------------------------+

(def ^:private ^:const timestamp-with-time-zone-db-type "timestamp with time zone")

(defmethod sql.qp/->honeysql [:clickzetta :log]
  [driver [_ field]]
  ;; recent Presto versions have a `log10` function (not `log`)
  [:log10 (sql.qp/->honeysql driver field)])

(defmethod sql.qp/->honeysql [:clickzetta :count-where]
  [driver [_ pred]]
  ;; Presto will use the precision given here in the final expression, which chops off digits
  ;; need to explicitly provide two digits after the decimal
  (sql.qp/->honeysql driver [:sum-where 1.00M pred]))

(defmethod sql.qp/->honeysql [:clickzetta :time]
  [_ [_ t]]
  ;; make time in UTC to avoid any interpretation by Presto in the connection (i.e. report) time zone
  (h2x/cast "time with time zone" (u.date/format-sql (t/offset-time (t/local-time t) 0))))

(defmethod sql.qp/->honeysql [:clickzetta ZonedDateTime]
  [_ ^ZonedDateTime t]
  ;; use the Presto cast to `timestamp with time zone` operation to interpret in the correct TZ, regardless of
  ;; connection zone
  (h2x/cast timestamp-with-time-zone-db-type (u.date/format-sql t)))

(defmethod sql.qp/->honeysql [:clickzetta OffsetDateTime]
  [_ ^OffsetDateTime t]
  ;; use the Presto cast to `timestamp with time zone` operation to interpret in the correct TZ, regardless of
  ;; connection zone
  (h2x/cast timestamp-with-time-zone-db-type (u.date/format-sql t)))

(defn- in-report-zone
  "Returns a HoneySQL form to interpret the `expr` (a temporal value) in the current report time zone, via Presto's
  `AT TIME ZONE` operator. See https://prestodb.io/docs/current/functions/datetime.html"
  [expr]
  (let [report-zone (qp.timezone/report-timezone-id-if-supported :clickzetta (lib.metadata/database (qp.store/metadata-provider)))
        ;; if the expression itself has type info, use that, or else use a parent expression's type info if defined
        type-info   (h2x/type-info expr)
        db-type     (h2x/type-info->db-type type-info)]
    (if (and ;; AT TIME ZONE is only valid on these Presto types; if applied to something else (ex: `date`), then
         ;; an error will be thrown by the query analyzer
         (contains? #{"timestamp" "timestamp with time zone" "time" "time with time zone"} db-type)
         ;; if one has already been set, don't do so again
         (not (::in-report-zone? (meta expr)))
         report-zone)
      (-> (h2x/with-database-type-info (h2x/at-time-zone expr report-zone) timestamp-with-time-zone-db-type)
          (vary-meta assoc ::in-report-zone? true))
      expr)))

;; most date extraction and bucketing functions need to account for report timezone

(defmethod sql.qp/date [:clickzetta :default]
  [_driver _unit expr]
  expr)

(defmethod sql.qp/date [:clickzetta :minute]
  [_driver _unit expr]
  [:date_trunc (h2x/literal :minute) (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :minute-of-hour]
  [_driver _unit expr]
  [:minute (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :hour]
  [_driver _unit expr]
  [:date_trunc (h2x/literal :hour) (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :hour-of-day]
  [_driver _unit expr]
  [:hour (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :day]
  [_driver _unit expr]
  [:date (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :day-of-week]
  [_driver _unit expr]
  (sql.qp/adjust-day-of-week :clickzetta [:day_of_week (in-report-zone expr)]))

(defmethod sql.qp/date [:clickzetta :day-of-month]
  [_driver _unit expr]
  [:day (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :day-of-year]
  [_driver _unit expr]
  [:day_of_year (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :week]
  [_driver _unit expr]
  (letfn [(truncate [x]
                    [:date_trunc (h2x/literal :week) x])]
    (sql.qp/adjust-start-of-week :clickzetta truncate (in-report-zone expr))))

(defmethod sql.qp/date [:clickzetta :month]
  [_driver _unit expr]
  [:date_trunc (h2x/literal :month) (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :month-of-year]
  [_driver _unit expr]
  [:month (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :quarter]
  [_driver _unit expr]
  [:date_trunc (h2x/literal :quarter) (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :quarter-of-year]
  [_driver _unit expr]
  [:quarter (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :year]
  [_driver _unit expr]
  [:date_trunc (h2x/literal :year) (in-report-zone expr)])

(defmethod sql.qp/date [:clickzetta :year-of-era]
  [_driver _unit expr]
  [:year (in-report-zone expr)])

(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :seconds]
  [_driver _unit expr]
  (let [report-zone (qp.timezone/report-timezone-id-if-supported :clickzetta (lib.metadata/database (qp.store/metadata-provider)))]
    [:from_unixtime expr (h2x/literal (or report-zone "UTC"))]))

(defn- date-add [unit amount expr]
  (let [amount (if (number? amount)
                 [:inline amount]
                 amount)]
    (cond-> [:date_add (h2x/literal unit) amount expr]
      (h2x/database-type expr)
      (h2x/with-database-type-info (h2x/database-type expr)))))

(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :milliseconds]
  [_driver _unit expr]
  ;; from_unixtime doesn't support milliseconds directly, but we can add them back in
  (let [report-zone (qp.timezone/report-timezone-id-if-supported :clickzetta (lib.metadata/database (qp.store/metadata-provider)))
        millis      [::mod expr [:inline 1000]]
        expr        [:from_unixtime [:/ expr [:inline 1000]] (h2x/literal (or report-zone "UTC"))]]
    (date-add :millisecond millis expr)))

(defmethod sql.qp/unix-timestamp->honeysql [:clickzetta :microseconds]
  [driver _seconds-or-milliseconds expr]
  ;; Presto can't even represent microseconds, so convert to millis and call that version
  (sql.qp/unix-timestamp->honeysql driver :milliseconds [:/ expr [:inline 1000]]))
