package ioe.online.house;

import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.SqlDialect;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.catalog.hive.HiveCatalog;

public class BlinkFromHive {
    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
        TableEnvironment tableEnv = TableEnvironment.create(settings);

        HiveCatalog hiveCatalog = new HiveCatalog("hive","default",
                "src/main/resources");
        tableEnv.registerCatalog("hive",hiveCatalog);
        tableEnv.useCatalog("hive");
        tableEnv.getConfig().setSqlDialect(SqlDialect.HIVE);

        tableEnv.executeSql("show databases");
    }
}
