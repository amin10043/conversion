package com.talos;

import org.json.simple.JSONObject;
import org.json.simple.parser.ParseException;
import org.json.simple.parser.JSONParser;

import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.net.URL;
import java.net.URLConnection;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Amin on 12/2/16.
 */
public class Conversion {

    List<Long> cityIds = new ArrayList<>();
    String HOST_NAME;
    String PORT;
    String DB_NAME;
    String USERNAME;
    String PASSWORD;
    String URL;
    String URL_POSTFIX;
    Boolean isUpdateZip;
    Boolean isUpdateCounty;

    Conversion(String HOST_NAME, String PORT, String DB_NAME, String USERNAME, String PASSWORD, String URL, String URL_POSTFIX,
               Boolean isUpdateZip, Boolean isUpdateCounty) {
        this.HOST_NAME = HOST_NAME;
        this.PORT = PORT;
        this.DB_NAME = DB_NAME;
        this.USERNAME = USERNAME;
        this.PASSWORD = PASSWORD;
        this.URL = URL;
        this.URL_POSTFIX = URL_POSTFIX;
        this.isUpdateZip = isUpdateZip;
        this.isUpdateCounty = isUpdateCounty;
    }

    void doConversion() throws ParseException, SQLException, IOException {
        Connection connection = getConnection();
        List<Property> properties = new ArrayList<>();
        if (isUpdateZip)
            properties.addAll(findAllPropertiesWithEmptyZip(connection));
        if (isUpdateCounty)
            properties.addAll(findAllPropertiesWithCountyInCity(connection));
        doConversion(properties, connection);
    }

    private void doConversion(List<Property> properties, Connection connection) throws ParseException, SQLException, IOException {

        JSONParser parser = new JSONParser();
        if (connection != null) {
            StringBuilder stringBuilder;
            for (Property property : properties) {
                stringBuilder = new StringBuilder(URL);
                stringBuilder.append("lat=");
                stringBuilder.append(property.getLatitude());
                stringBuilder.append("&lon=");
                stringBuilder.append(property.getLongitude());
                stringBuilder.append(URL_POSTFIX);
                String res = getOpenStreetConnection(stringBuilder.toString());
                Object obj = parser.parse(res);
                String postalCode = null;
                String county = null;
                String state = null;
                if (property.getUpdatePostalCode() &&
                        obj != null && ((JSONObject) obj).get("address") != null &&
                        ((JSONObject) ((JSONObject) obj).get("address")).get("postcode") != null ) {
                    postalCode = ((JSONObject) (((JSONObject) obj).get("address"))).get("postcode").toString();
                    updateZipCodeProperty(connection, property, postalCode);
                }
                if (property.getUpdateCounty() &&
                        obj != null && ((JSONObject) obj).get("address") != null &&
                        ((JSONObject) ((JSONObject) obj).get("address")).get("county") != null && !cityIds.contains(property.getCityId()) &&
                        ((JSONObject) (((JSONObject) obj).get("address"))).get("state") != null) {
                    county = ((JSONObject) (((JSONObject) obj).get("address"))).get("county").toString();
                    state = ((JSONObject) (((JSONObject) obj).get("address"))).get("state").toString();
                    String cityName = getCityNameById(property.getCityId(), connection);
                    updateCityForBuilding(connection, property, county, cityName, state);
                    System.out.println(county);
                    cityIds.add(property.getCityId());
                } else {
                    System.out.println(property.getId() + ",");
                }
            }
        }
        disconnect(connection);
    }

    private Connection getConnection() {
        Connection connection = null;
        try {
            Class.forName("org.postgresql.Driver");
            connection = DriverManager.getConnection(
                    "jdbc:postgresql://" + HOST_NAME + ":" + PORT + "/" + DB_NAME  ,USERNAME, PASSWORD);

        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
        return connection;
    }

    private void disconnect(Connection connection) {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private List<Property> findAllPropertiesWithEmptyZip(Connection conn) throws SQLException {
        List<Property> propertyList = new ArrayList<>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select id,latitude, longitude  from work.building_20170123 WHERE postal_code is NULL AND latitude IS NOT NULL AND longitude IS NOT NULL" +
                " and id  in (" +
                        "40028" +
                ")" +
                " order by id");
        Property property;
        while ( rs.next() )
        {
            property = new Property();
            property.setId(rs.getLong("id"));
            property.setLongitude(rs.getBigDecimal("longitude"));
            property.setLatitude(rs.getBigDecimal("latitude"));
            property.setUpdateCounty(false);
            property.setUpdatePostalCode(true);
            propertyList.add(property);
        }
        rs.close();
        st.close();
        return propertyList;
    }

    private List<Property> findAllPropertiesWithCountyInCity(Connection conn) throws SQLException {
        List<Property> propertyList = new ArrayList<>();
        Statement st = conn.createStatement();
        ResultSet rs = st.executeQuery("select id,latitude, longitude, city_id  from work.building_20170123 " +
                " WHERE city_id is NOT NULL AND latitude IS NOT NULL AND longitude IS NOT NULL " +
                " AND id in (40028) " +
                " order by id");
        Property property;
        while ( rs.next() )
        {
            property = new Property();
            property.setId(rs.getLong("id"));
            property.setLongitude(rs.getBigDecimal("longitude"));
            property.setLatitude(rs.getBigDecimal("latitude"));
            property.setCityId(rs.getLong("city_id"));
            property.setUpdateCounty(true);
            property.setUpdatePostalCode(false);
            propertyList.add(property);
        }
        rs.close();
        st.close();
        return propertyList;
    }

    private String getOpenStreetConnection(String url) throws IOException {

        String response = "";
        java.net.URL u = new URL(url);

        URLConnection urlConn = u.openConnection();
        urlConn.setDoInput(true);
        urlConn.setDoOutput(true);
        urlConn.setUseCaches(false);

        DataOutputStream outgoing = new DataOutputStream(urlConn.getOutputStream());

        outgoing.flush();
        outgoing.close();

        BufferedReader incoming
                = new BufferedReader(new InputStreamReader(urlConn.getInputStream()));
        String str;
        while ((str = incoming.readLine()) != null)
            response += str;
        incoming.close();
        return response;
    }

    private void updateZipCodeProperty(Connection conn, Property property, String postalCode) throws SQLException {
        if (postalCode != null && property != null && property.getId() != null) {
            if (postalCode.contains(":"))
                postalCode = postalCode.substring(0, postalCode.indexOf(":"));
            if (postalCode.contains(";"))
                postalCode = postalCode.substring(0, postalCode.indexOf(";"));
            if (postalCode.contains("CA "))
                postalCode = postalCode.substring(postalCode.indexOf("CA ") + 3);
            try {
                Long postalCodeLong;
                if(!postalCode.contains("-"))
                    postalCodeLong = Long.valueOf(postalCode);
                Statement st = conn.createStatement();
                String query = "UPDATE work.building_20170123 set postal_code ='" +
                        postalCode +
                        "' WHERE id = " +
                        property.getId();
                System.out.println(property.getId() + ",");
                st.executeUpdate(query);
            } catch (NumberFormatException e) {
                System.out.println("Error : Postal Code for Building " + property.getId() + " is not in correct format : " + postalCode);
            }
        } else {
            System.out.println("postalCode for building " + property.getId() + " get null from url");
        }
    }

    private void updateCityForBuilding(Connection conn, Property property, String countyName, String cityName, String stateName) throws SQLException {
        if (countyName != null && !"".equals(countyName) &&
                cityName != null && !"".equals(cityName) &&
                stateName != null && !"".equals(stateName) &&
                property != null && property.getId() != null &&
                property.getCityId() != null) {
            try {
                Long cityId = getCityIdByCountyNameCityNameStateName(conn, countyName, cityName, stateName);
                if (cityId == 0L) {
                    System.out.println("Building " + property.getId() + " is not updated, cityId is zero for CountyName = " +
                            countyName + " and cityName = " + cityName);
                } else if (!property.cityId.equals(cityId)) {
                    Statement st = conn.createStatement();
                    String query = "UPDATE work.building_20170123 set city_id =" +
                            cityId +
                            " WHERE id = " +
                            property.getId();
//                    st.executeUpdate(query);
                    System.out.println(query);
                    System.out.println(property.getId() + ",");
                } else {
                    System.out.println(property.getId() + ",");
                }
            } catch (NumberFormatException e) {
                System.out.println("Error : County code for Building " + property.getId() + " is not in correct ");
            }
        } else {
            System.out.println("County or city or state get null from url for Building : " + property.getId());
        }
    }

    private String getCityNameById(Long cityId, Connection connection) throws SQLException {
        String cityName = "";
        if (cityId != null) {
            Statement st = connection.createStatement();
            ResultSet rs = null;
            String query = "";
            try {
                query = "select name as cityName from city WHERE id =" + cityId;
                rs = st.executeQuery(query);
            } catch (ArrayIndexOutOfBoundsException ex) {
                System.out.println("Error on " +  query);
            }

            if (rs != null && rs.next()) {
                cityName = rs.getString("cityName");
            } else {
                System.out.println("City with Id : " + cityId + " not found in our database");
            }
            if (rs != null)
                rs.close();
            st.close();
        }
        return cityName;
    }

    private Long getCityIdByCountyNameCityNameStateName(Connection connection, String countyName, String cityName, String stateName) throws SQLException {
        Long cityId = 0L;
        Statement st = connection.createStatement();
        ResultSet rs = null;
        String query = "select city.id from city" +
                "  INNER JOIN county on city.county_id = county.id" +
                "  INNER JOIN countrystate on city.countrystate_id = countrystate.id" +
                "  WHERE city.name like '" + cityName + "'" +
                "   and county.name like '%" + countyName + "%'" +
                "   and countrystate.name like '%" + stateName + "%'";
        try {
        rs = st.executeQuery(query);
        } catch (ArrayIndexOutOfBoundsException ex) {
            System.out.println("Error on " + query);
        }
        if (rs != null && rs.next()) {
            cityId = rs.getLong("id");
        } else {
            query = " select city.id from city  " +
                    " INNER JOIN countrystate on city.countrystate_id = countrystate.id " +
                    " where city.county_id IS NULL  AND " +
                    "  city.name like '" + cityName + "' AND " +
                    "  countrystate.name like '%" + stateName + "%'";
            try {
                rs = st.executeQuery(query);
            } catch (ArrayIndexOutOfBoundsException ex) {
                System.out.println("Error on " + query);
            }
            if (rs != null && rs.next()) {
                cityId = rs.getLong("id");
            } else
                System.out.println("City with Name : " + cityName + " And countyName : " + countyName + " not found in our database");
        }
        if (rs != null)
            rs.close();
        st.close();
        return cityId;
    }

    private class Property {
        Long id;
        String postalCode;
        BigDecimal longitude;
        BigDecimal latitude;
        Long cityId;
        Boolean updateCounty;
        Boolean updatePostalCode;

        public Long getId() {
            return id;
        }

        public void setId(Long id) {
            this.id = id;
        }

        public String getPostalCode() {
            return postalCode;
        }

        public void setPostalCode(String postalCode) {
            this.postalCode = postalCode;
        }

        public BigDecimal getLongitude() {
            return longitude;
        }

        public void setLongitude(BigDecimal longitude) {
            this.longitude = longitude;
        }

        public BigDecimal getLatitude() {
            return latitude;
        }

        public void setLatitude(BigDecimal latitude) {
            this.latitude = latitude;
        }

        public Long getCityId() {
            return cityId;
        }

        public void setCityId(Long cityId) {
            this.cityId = cityId;
        }

        public Boolean getUpdateCounty() {
            return updateCounty;
        }

        public void setUpdateCounty(Boolean updateCounty) {
            this.updateCounty = updateCounty;
        }

        public Boolean getUpdatePostalCode() {
            return updatePostalCode;
        }

        public void setUpdatePostalCode(Boolean updatePostalCode) {
            this.updatePostalCode = updatePostalCode;
        }
    }
}
