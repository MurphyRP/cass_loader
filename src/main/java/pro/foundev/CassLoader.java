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

package pro.foundev;

import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.github.javafaker.Faker;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;

import javax.xml.transform.Result;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.security.SecureRandom;
import java.util.*;

public class CassLoader implements Runnable {

    private final Session session;
    private final File loaderFile;
    private final int numberOfRows;
    private final Yaml yaml;
    private final Faker faker;
    private final SecureRandom random;

    public CassLoader(Session session, File loaderFile, int numberOfRows) {
        this.session = session;
        this.loaderFile = loaderFile;
        this.numberOfRows = numberOfRows;
        this.faker = new Faker();
        this.random = new SecureRandom();
        Constructor constructor = new Constructor(Keyspace.class);
        TypeDescription keyspaceDescription = new TypeDescription(Keyspace.class);
        keyspaceDescription.putListPropertyType("tables", Table.class);
        keyspaceDescription.putMapPropertyType("columns", String.class, String.class);
        constructor.addTypeDescription(keyspaceDescription);
        yaml = new Yaml(constructor);
    }

    private Object getFakeValue(String instanceType) {
        if("int".equals(instanceType)) {
            return random.nextInt();
        }if("positive_int".equals(instanceType)) {
            int randomInt = random.nextInt();
            return Math.abs(randomInt);
        }if("double".equals(instanceType)) {
            return random.nextDouble();
        }if("decimal".equals(instanceType)) {
            return new BigDecimal(random.nextDouble());
        }if("timestamp".equals(instanceType)) {
            long milliseconds = -946771200000L + (Math.abs(random.nextLong()) % (70L * 365 * 24 * 60 * 60 * 1000));
            return new Date(milliseconds);
        }if("text".equals(instanceType)) {
            return faker.lorem().paragraph();
        }if("first_name".equals(instanceType)) {
            return faker.name().firstName();
        }if("last_name".equals(instanceType)) {
            return faker.name().lastName();
        }if("uuid".equals(instanceType)){
            return UUID.randomUUID();
        }
        throw new RuntimeException("unsupported type: " + instanceType + "; Look in CassLoader.java to add more options");
    }

    @Override
    public void run() {
        try {

            final Keyspace keyspace = (Keyspace) yaml.load(new FileInputStream(loaderFile));
            List<ResultSetFuture> futures = new ArrayList<>();
            for (Table table : keyspace.getTables()) {
                for (int i = 0; i < numberOfRows; i++) {
                    Insert insert = QueryBuilder.insertInto(keyspace.getName(), table.getName());
                    for (Map.Entry<String, String> column : table.getColumns().entrySet()) {
                        Object columnValue = getFakeValue(column.getValue());
                        insert.value(column.getKey(), columnValue);
                    }
                    futures.add(session.executeAsync(insert));
                }
            }
            for(ResultSetFuture future: futures){
                future.getUninterruptibly();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}

