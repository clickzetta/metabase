(ns metabase.driver.clickzetta-test
  (:require
    [clojure.string :as str]
    [clojure.test :refer :all]
    [honey.sql :as sql]
    [java-time.api :as t]
    [metabase.api.database :as api.database]
    [metabase.db.metadata-queries :as metadata-queries]
    [metabase.driver :as driver]
    [metabase.driver.clickzetta :as clickzetta]
    [metabase.driver.sql-jdbc.connection :as sql-jdbc.conn]
    [metabase.driver.sql-jdbc.execute :as sql-jdbc.execute]
    [metabase.driver.sql.query-processor :as sql.qp]
    [metabase.models.database :refer [Database]]
    [metabase.models.field :refer [Field]]
    [metabase.models.table :as table :refer [Table]]
    [metabase.query-processor :as qp]
    [metabase.query-processor.compile :as qp.compile]
    [metabase.sync :as sync]
    [metabase.test :as mt]
    [metabase.test.data.clickzetta :as data.clickzetta]
    [metabase.test.fixtures :as fixtures]
    [toucan2.core :as t2]
    [toucan2.tools.with-temp :as t2.with-temp])
  (:import
    (java.io File)))

(set! *warn-on-reflection* true)

(use-fixtures :once (fixtures/initialize :db))

(deftest describe-database-test
  (mt/test-driver :clickzetta
                  (is (= {:tables #{{:name "test_data_categories" :schema "metabase"}
                                    {:name "test_data_checkins" :schema "metabase"}
                                    {:name "test_data_users" :schema "metabase"}
                                    {:name "test_data_venues" :schema "metabase"}}}
                         (-> (driver/describe-database :clickzetta (mt/db))
                             (update :tables (comp set (partial filter (comp #{"test_data_categories"
                                                                               "test_data_venues"
                                                                               "test_data_checkins"
                                                                               "test_data_users"}
                                                                             :name)))))))))

(deftest describe-table-test
  (mt/test-driver :clickzetta
                  (is (= {:name   "test_data_venues"
                          :schema "metabase"
                          :fields #{{:name          "name",
                                     ;; for HTTP based Presto driver, this is coming back as varchar(255)
                                     ;; however, for whatever reason, the DESCRIBE statement results do not return the length
                                     :database-type "string"
                                     :base-type     :type/Text
                                     :database-position 1}
                                    {:name          "latitude"
                                     :database-type "double"
                                     :base-type     :type/Float
                                     :database-position 3}
                                    {:name          "longitude"
                                     :database-type "double"
                                     :base-type     :type/Float
                                     :database-position 4}
                                    {:name          "price"
                                     :database-type "int"
                                     :base-type     :type/Integer
                                     :database-position 5}
                                    {:name          "category_id"
                                     :database-type "int"
                                     :base-type     :type/Integer
                                     :database-position 2}
                                    {:name          "id"
                                     :database-type "int"
                                     :base-type     :type/Integer
                                     :database-position 0}}}
                         (driver/describe-table :clickzetta (mt/db) (t2/select-one 'Table :id (mt/id :venues)))))))

(deftest table-rows-sample-test
  (mt/test-driver :clickzetta
                  (is (= [[1 "Red Medicine"]
                          [2 "Stout Burgers & Beers"]
                          [3 "The Apple Pan"]
                          [4 "Wurstküche"]
                          [5 "Brite Spot Family Restaurant"]]
                         (->> (metadata-queries/table-rows-sample (t2/select-one Table :id (mt/id :venues))
                                                                  [(t2/select-one Field :id (mt/id :venues :id))
                                                                   (t2/select-one Field :id (mt/id :venues :name))]
                                                                  (constantly conj))
                              (sort-by first)
                              (take 5))))))

(deftest ^:parallel page-test
  (testing ":page clause"
           (let [honeysql (sql.qp/apply-top-level-clause :clickzetta :page
                                                         {:select   [[:metabase.categories.name :name] [:metabase.categories.id :id]]
                                                          :from     [:metabase.categories]
                                                          :order-by [[:metabase.categories.id :asc]]}
                                                         {:page {:page  2
                                                                 :items 5}})]
             (is (= [["SELECT"
                      "  \"name\","
                      "  \"id\""
                      "FROM"
                      "  ("
                      "    SELECT"
                      "      \"metabase\".\"categories\".\"name\" AS \"name\","
                      "      \"metabase\".\"categories\".\"id\" AS \"id\","
                      "      row_number() OVER ("
                      "        ORDER BY"
                      "          \"metabase\".\"categories\".\"id\" ASC"
                      "      ) AS \"__rownum__\""
                      "    FROM"
                      "      \"metabase\".\"categories\""
                      "    ORDER BY"
                      "      \"metabase\".\"categories\".\"id\" ASC"
                      "  )"
                      "WHERE"
                      "  \"__rownum__\" > 5"
                      "LIMIT"
                      "  5"]]
                    (-> (sql.qp/format-honeysql :clickzetta honeysql)
                        (update 0 #(str/split-lines (driver/prettify-native-form :clickzetta %)))))))))

(deftest ^:parallel db-default-timezone-test
  (mt/test-driver :clickzetta
                  (is (= "UTC"
                         (driver/db-default-timezone :clickzetta (mt/db))))))

(deftest template-tag-timezone-test
  (mt/test-driver :clickzetta
    (testing "Make sure date params work correctly when report timezones are set (#10487)"
     (mt/with-temporary-setting-values [report-timezone "Asia/Hong_Kong"]
       ;; the `read-column-thunk` for `Types/TIMESTAMP` always returns an `OffsetDateTime`, not a `LocalDateTime`, as
       ;; the original Presto version of this test expected; therefore, convert the `ZonedDateTime` corresponding to
       ;; midnight on this date (at the report TZ) to `OffsetDateTime` for comparison's sake
       (is (= [[(-> (t/zoned-date-time 2014 8 2 0 0 0 0 (t/zone-id "Asia/Hong_Kong"))
                    t/offset-date-time
                    (t/with-offset-same-instant (t/zone-offset 0)))
                (t/local-date 2014 8 2)]]
              (mt/rows
               (qp/process-query
                {:database     (mt/id)
                 :type         :native
                 :middleware   {:format-rows? false} ; turn off formatting so we can check the raw local date objs
                 :native       {:query         "SELECT {{date}}, cast({{date}} AS date)"
                                :template-tags {:date {:name "date" :display_name "Date" :type "date"}}}
                 :parameters   [{:type   "date/single"
                                 :target ["variable" ["template-tag" "date"]]
                                 :value  "2014-08-02"}]}))))))))

(deftest ^:parallel splice-strings-test
  (mt/test-driver :clickzetta
                  (let [query (mt/mbql-query venues
                                             {:aggregation [[:count]]
                                              :filter      [:= $name "wow"]})]
                    (testing "The native query returned in query results should use user-friendly splicing"
                             (is (= (str "SELECT COUNT(*) AS \"count\" "
                                         "FROM \"metabase\".\"test_data_venues\" "
                                         "WHERE \"metabase\".\"test_data_venues\".\"name\" = 'wow'")
                                    (:query (qp.compile/compile-and-splice-parameters query))
                                    (-> (qp/process-query query) :data :native_form :query)))))))


(deftest ^:parallel honeysql-tests
  (mt/test-driver :clickzetta
                  (mt/with-metadata-provider (mt/id)
                                             (testing "Complex HoneySQL conversions work as expected"
                                                      (testing "unix-timestamp with microsecond precision"
                                                               (is (= [["DATE_ADD("
                                                                        "  'millisecond',"
                                                                        "  mod((1623963256123456 / 1000), 1000),"
                                                                        "  FROM_UNIXTIME((1623963256123456 / 1000) / 1000, 'UTC')"
                                                                        ")"]]
                                                                      (-> (sql/format-expr (sql.qp/unix-timestamp->honeysql :clickzetta :microseconds [:raw 1623963256123456]))
                                                                          (update 0 #(str/split-lines (driver/prettify-native-form :clickzetta %)))))))))))

(defn- clone-db-details
  "Clones the details of the current DB ensuring fresh copies for the secrets
  (keystore and truststore)."
  []
  (-> (:details (mt/db))
      (dissoc :ssl-keystore-id :ssl-keystore-password-id
                               :ssl-truststore-id :ssl-truststore-password-id)
      (merge (select-keys (data.clickzetta/dbdef->connection-details (:name (mt/db)))
                          [:ssl-keystore-path :ssl-keystore-password-value
                                              :ssl-truststore-path :ssl-truststore-password-value]))))

(defn- execute-ddl! [ddl-statements]
  (mt/with-driver :clickzetta
                  (sql-jdbc.execute/do-with-connection-with-options
                   :clickzetta
                   (sql-jdbc.conn/connection-details->spec :clickzetta (clone-db-details))
                   {:write? true}
                   (fn [^java.sql.Connection conn]
                     (doseq [ddl-stmt ddl-statements]
                       (with-open [stmt (.prepareStatement conn ddl-stmt)]
                         (.executeUpdate stmt)))))))

(deftest specific-schema-sync-test
  (mt/test-driver :clickzetta
                  (testing "When a specific schema is designated, only that one is synced"
                           (let [s           "specific_schema"
                                 t           "specific_table"
                                 db-details  (clone-db-details)
                                 with-schema (assoc db-details :schema s)]
                             (execute-ddl! [(format "DROP TABLE IF EXISTS %s.%s" s t)
                                            (format "DROP SCHEMA IF EXISTS %s" s)
                                            (format "CREATE SCHEMA %s" s)
                                            (format "CREATE TABLE %s.%s (pk INTEGER, val1 VARCHAR(512))" s t)])
                             (t2.with-temp/with-temp [Database db {:engine :clickzetta, :name "Temp Presto JDBC Schema DB", :details with-schema}]
                                                     (mt/with-db db
                                                                 ;; same as test_data, but with schema, so should NOT pick up venues, users, etc.
                                                                 (sync/sync-database! db)
                                                                 (is (= [{:name t, :schema s, :db_id (mt/id)}]
                                                                        (map #(select-keys % [:name :schema :db_id]) (t2/select Table :db_id (mt/id)))))))
                             (execute-ddl! [(format "DROP TABLE %s.%s" s t)
                                            (format "DROP SCHEMA %s" s)])))))
