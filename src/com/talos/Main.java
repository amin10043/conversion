package com.talos;

import java.util.ArrayList;
import java.util.List;

public class Main {

    public static void main(String[] args) {
        String DEFAULT_HOST_NAME = "localhost";
        String DEFAULT_PORT = "5432";
        String DEFAULT_DB_NAME = "mangodb";
        String DEFAULT_USERNAME = "siiladbadmin";
        String DEFAULT_PASSWORD = "";
        String DEFAULT_URL = "http://nominatim.openstreetmap.org/reverse?format=json&";
        String DEFAULT_URL_POSTFIX = "&zoom=18&addressdetails=1";
        CliArgs cliArgs = new CliArgs(args);
        String hostName = cliArgs.switchValue("-host");
        Long port = cliArgs.switchLongValue("-port");
        String dbName = cliArgs.switchValue("-db");
        String dbUserName = cliArgs.switchValue("-u");
        String dbPassword = cliArgs.switchValue("-p");
        String updateZip = cliArgs.switchValue("-zip");
        String updateCounty = cliArgs.switchValue("-county");

        try {
            String HOST_NAME = hostName != null ? hostName : DEFAULT_HOST_NAME;
            String PORT = port != null ? port.toString() : DEFAULT_PORT;
            String DB_NAME = dbName != null ? dbName : DEFAULT_DB_NAME;
            String USERNAME = dbUserName != null ? dbUserName : DEFAULT_USERNAME;
            String PASSWORD = dbPassword != null ? dbPassword : DEFAULT_PASSWORD;
            Boolean isUpdateZip = updateZip != null;
            Boolean isUpdateCounty = updateCounty != null;
            Conversion conversion = new Conversion(HOST_NAME, PORT, DB_NAME, USERNAME, PASSWORD, DEFAULT_URL, DEFAULT_URL_POSTFIX,
                    isUpdateZip, isUpdateCounty);
            conversion.doConversion();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
