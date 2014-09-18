package pro.foundev;
/*
 * Copyright 2014 Foundational Development
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import org.junit.Test;

import java.io.File;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class CassLoaderTest {

    //move all to subclass and or singleton
    private Session session;
    private Cluster cluster;
    private Lock lock = new ReentrantLock();

    private Session getOrCreateSession() {
        lock.lock();
        try {
            if (session == null) {
                cluster = Cluster.
                        builder()
                        .addContactPoint("localhost")
                        .build();
                session = cluster.newSession();
                session.execute("DROP KEYSPACE IF EXISTS cass_loader_test");
                session.execute("CREATE KEYSPACE IF NOT EXISTS cass_loader_test with replication = { 'class':'SimpleStrategy'," +
                        "'replication_factor':1}");
                session.execute("DROP TABLE IF EXISTS cass_loader_test.users");
                session.execute("CREATE TABLE IF NOT EXISTS cass_loader_test.users "+
                "(id uuid, first_name text, last_name text, date_added timestamp, login_count int, "+
                "percent_success double, total_owned decimal, primary key(id))");
            }
        } finally {
            lock.unlock();
        }
        return session;
    }

    private long rowCountFor(String tableName) {
        return getOrCreateSession().execute("SELECT COUNT(*) FROM cass_loader_test." + tableName).one().getLong(0);
    }

    @Test
    public void itParsesXmlFileWithOneTableAndLoadsToCassandra() {
        File loaderFile = new File(String.valueOf(getClass().getResource("cassandra_load.yaml")));
        int numberOfRows = 100;
        CassLoader loader = new CassLoader(loaderFile, numberOfRows);
        long rowCountBefore = rowCountFor("users");
        loader.run();
        long rowCountAfter = rowCountFor("users");
        assertThat(rowCountBefore+numberOfRows, is(equalTo(rowCountAfter)));
    }
}
