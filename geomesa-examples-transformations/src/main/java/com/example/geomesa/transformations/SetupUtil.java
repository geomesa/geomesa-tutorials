package com.example.geomesa.transformations;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

import java.util.HashMap;
import java.util.Map;

/**
 * Copyright 2014 Commonwealth Computer Research, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public class SetupUtil {

    static String INSTANCE_ID = "instanceId";
    static String ZOOKEEPERS = "zookeepers";
    static String USER = "user";
    static String PASSWORD = "password";
    static String AUTHS = "auths";
    static String TABLE_NAME = "tableName";

    // sub-set of parameters that are used to create the Accumulo DataStore
    static String[] ACCUMULO_CONNECTION_PARAMS = new String[] {INSTANCE_ID,
                                                               ZOOKEEPERS,
                                                               USER,
                                                               PASSWORD,
                                                               AUTHS,
                                                               TABLE_NAME};

    /**
     * Creates a common set of command-line options for the parser.  Each option
     * is described separately.
     */
    static Options getCommonRequiredOptions() {
        Options options = new Options();

        Option instanceIdOpt = OptionBuilder.withArgName(INSTANCE_ID)
                                            .hasArg()
                                            .isRequired()
                                            .withDescription(
                                                    "the ID (name) of the Accumulo instance, e.g:  mycloud")
                                            .create(INSTANCE_ID);
        options.addOption(instanceIdOpt);

        Option zookeepersOpt = OptionBuilder.withArgName(ZOOKEEPERS)
                                            .hasArg()
                                            .isRequired()
                                            .withDescription(
                                                    "the comma-separated list of Zookeeper nodes that support your Accumulo instance, e.g.:  zoo1:2181,zoo2:2181,zoo3:2181")
                                            .create(ZOOKEEPERS);
        options.addOption(zookeepersOpt);

        Option userOpt = OptionBuilder.withArgName(USER)
                                      .hasArg()
                                      .isRequired()
                                      .withDescription(
                                              "the Accumulo user that will own the connection, e.g.:  root")
                                      .create(USER);
        options.addOption(userOpt);

        Option passwordOpt = OptionBuilder.withArgName(PASSWORD)
                                          .hasArg()
                                          .isRequired()
                                          .withDescription(
                                                  "the password for the Accumulo user that will own the connection, e.g.:  toor")
                                          .create(PASSWORD);
        options.addOption(passwordOpt);

        Option authsOpt = OptionBuilder.withArgName(AUTHS)
                                       .hasArg()
                                       .withDescription(
                                               "the (optional) list of comma-separated Accumulo authorizations that should be applied to all data written or read by this Accumulo user; note that this is NOT the list of low-level database permissions such as 'Table.READ', but more a series of text tokens that decorate cell data, e.g.:  Accounting,Purchasing,Testing")
                                       .create(AUTHS);
        options.addOption(authsOpt);

        Option tableNameOpt = OptionBuilder.withArgName(TABLE_NAME)
                                           .hasArg()
                                           .isRequired()
                                           .withDescription(
                                                   "the name of the Accumulo table to use -- or create, if it does not already exist -- to contain the new data")
                                           .create(TABLE_NAME);
        options.addOption(tableNameOpt);

        return options;
    }

    static Map<String, String> getAccumuloDataStoreConf(CommandLine cmd) {
        Map<String, String> dsConf = new HashMap<String, String>();
        for (String param : ACCUMULO_CONNECTION_PARAMS) {
            dsConf.put(param, cmd.getOptionValue(param));
        }
        if (dsConf.get(AUTHS) == null) {
            dsConf.put(AUTHS, "");
        }

        System.out.println("Connecting to " + dsConf.get(TABLE_NAME) + " at " + dsConf.get(
                INSTANCE_ID) + ":" + dsConf.get(ZOOKEEPERS));
        return dsConf;
    }
}