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

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

import java.io.File;
import java.io.FileNotFoundException;

public class Main {
    public static int main(String[] args){
        File yaml = new File(args[0]);
        if(!yaml.exists()){
            try {
                throw new FileNotFoundException(args[0]);
            } catch (FileNotFoundException e) {
                e.printStackTrace();
                return 2;
            }
        }
        int rows = Integer.parseInt(args[1]);
        String host = args[2];
        Cluster cluster = Cluster.builder().addContactPoint(host).build();
        Session session = cluster.newSession();

       try {
           CassLoader cassLoader = new CassLoader(session, yaml, rows);
           cassLoader.run();
       }finally {
           if(session!=null)
               session.close();
           if(cluster!=null)
               cluster.close();
       }
       return 0;
    }
}
