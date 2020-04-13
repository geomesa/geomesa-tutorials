/*
 * Copyright (c) 2013-2018 Commonwealth Computer Research, Inc.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Apache License, Version 2.0 which
 * accompanies this distribution and is available at
 * http://www.opensource.org/licenses/apache2.0.php.
 */

package org.geomesa.example.quickstart;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.geotools.data.DataAccessFactory.Param;

import java.util.HashMap;
import java.util.Map;

public class CommandLineDataStore {

    private CommandLineDataStore() {}

    public static Options createOptions(Param[] parameters) {
        Options options = new Options();
        for (Param p: parameters) {
            if (!p.isDeprecated()) {
                Option opt = Option.builder(null)
                                   .longOpt(p.getName())
                                   .argName(p.getName())
                                   .hasArg()
                                   .desc(p.getDescription().toString())
                                   .required(p.isRequired())
                                   .build();
                options.addOption(opt);
            }
        }
        return options;
    }

    public static CommandLine parseArgs(Class<?> caller, Options options, String[] args) throws ParseException {
        try {
            return new DefaultParser().parse(options, args);
        } catch (ParseException e) {
            System.err.println(e.getMessage());
            HelpFormatter formatter = new HelpFormatter();
            formatter.printHelp(caller.getName(), options);
            throw e;
        }
    }

    public static Map<String, String> getDataStoreParams(CommandLine command, Options options) {
        Map<String, String> params = new HashMap<>();
        for (Option opt: options.getOptions()) {
            String value = command.getOptionValue(opt.getLongOpt());
            if (value != null) {
                params.put(opt.getArgName(), value);
            }
        }

        return params;
    }
}
