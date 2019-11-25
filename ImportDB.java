package de.erv.vgercls.importtool;

import static de.itergo.commons.base.Collections.newArrayList;
import static de.itergo.commons.base.Collections.newHashMap;
import static de.itergo.commons.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import de.erv.util.DesEncrypter;
import de.erv.vgercls.importtool.util.Constants;
import de.itergo.commons.base.Counters;
import de.itergo.commons.base.LoggerFactory;
import de.itergo.commons.base.ObjectStorage;

/**
 * Finds a contact_id for a certain search string with values like FIRSTNAME SURNAME and so on. Replaces special
 * characters like ï¿½ ï¿½ etc. Converts them to UPPER Before calling getClaimtypeNo, paymentExists or getContactNo i
 * have to initialize it by calling init. For performance reason the conn is left open until it will be closed with a
 * call to closeTheLot(); This call has to be made otherwise the con stm and rs are left open.
 *
 * @author gusiewert
 */
public class ImportDB {

    private final static Logger logger = LoggerFactory.makeLogger();

    /** data to access the db */
    private final static String PROP_NAME_DRIVER = "driver", PROP_NAME_DRIVERTYPE = "drivertype", PROP_NAME_URL = "url",
            PROP_NAME_PORT = "port", PROP_NAME_DBNAME = "dbname", PROP_NAME_USER = "user", PROP_NAME_PW = "pw",
            PROP_NAME_DRIVER2 = "driverprod", PROP_NAME_DRIVERTYPE2 = "drivertypeprod", PROP_NAME_URL2 = "urlprod",
            PROP_NAME_PORT2 = "portprod", PROP_NAME_DBNAME2 = "dbnameprod", PROP_NAME_USER2 = "userprod",
            PROP_NAME_PW2 = "pwprod";
    /** Reserve table for italy */
    private final static String PROP_NAME_RESERVE_TBL = "reservetable", PROP_NAME_RESERVE_TBL_PK = "reservetblpk",
            PROP_NAME_RESERVE_TBL_RESERVE = "reservetblreserve", PROP_NAME_RESERVE_TBL_DATE_OF_ENTRY = "reservetblmap2",
            PROP_NAME_RESERVE_TBL_AMOUNT_COLUMN = "reservetblamountcolumn",
            PROP_NAME_RESERVE_TBL_AMOUNT = "reservetblamount",
            PROP_NAME_RESERVE_TBL_RESERVE_COLUMN = "reservetblreservecolumn",
            PROP_NAME_RESERVE_TBL_COLUMN = "reservetblcolumn", PROP_NAME_RESERVE_TBL_NAME = "tblName",
            PROP_NAME_RESERVE_TBL_MAP = "reservetblmap";

    /** Result file for italy reserve */
    private final static String PROP_NAME_RESERVE_FILE_PATH = "filepathreserveresult",
            PROP_NAME_DATE_FORMAT_RESULT_FILE = "dateformatresultfile",
            PROP_NAME_DATE_FORMAT_RESULT_FILE_CONTENT = "dateformatresultfilecontent",
            PROP_NAME_NAME_RESULT_FILE = "nameresultfile", PROP_NAME_UPDATE_RESERVE = "updatereserve",
            PROP_NAME_UPDATE_AMOUNT_RESERVE = "updateamountreserve", PROP_NAME_INSERT_NEW_RESERVE = "insertnewreserve",
            PROP_NAME_VALUE_RESERVE = "valuereserve", PROP_NAME_VALUE_AMOUNT = "valueamount",
            PROP_NAME_ERROR = "errorreserve", PROP_NAME_TIME = "time";

    private final static String PROP_NAME_FIELD_TYPE_OF_CLAIM = "fieldtypeofclaim";

    private String reservetable, reservetblpk, reservetblreserve, reservetblDateOfEntry, reservetblreservecolumn,
            insertItalyReserve, reservetblamountcolumn, reservetblamount, columnPk;
    private List<String> fieldsList;
    private final static String SEL_CLAIMNO = "SELECT FALLNR FROM FL_CLAIMTYPE WHERE VRGNR=? AND CLAIM_TYPE=?";
    private final static String SEL_PAYMENT =
            "SELECT AMOUNT FROM FL_PAYMENT WHERE ACTIVITY_NUMBER=? AND CLAIM_NUMBER=?";
    /** Check if the first claymtype has a payment */
    private final static String SEL_CLAIMTYPE =
            "SELECT CLAIM_TYPE,FL_PAYMENT.AMOUNT FROM FL_CLAIMTYPE, FL_PAYMENT WHERE VRGNR=? AND FALLNR='1' AND FL_PAYMENT.ACTIVITY_NUMBER=?"
                    + " AND FL_PAYMENT.CLAIM_NUMBER='1' AND FL_PAYMENT.PAYMENT_NUMBER_CLAIM='1'",
            /** To get the next value for the italy claims */
            SEL_NEXT_VALUE_FOR_ITALY_CLAIM = "SELECT italy_seq.NEXTVAL FROM dual", DUMMY_VERSART = "dummyVersArt",
            /** Select for the status payment */
            SEL_STATUS_PAYMENTS = "SELECT SAP_ZUSTAND FROM FL_PAYMENT WHERE ACTIVITY_NUMBER = ? AND CLAIM_NUMBER = ?",
            SEL_INSURANCE_TYPE =
                    "SELECT MAX(VALUE) FROM OT_LEAFS WHERE MAINUUID IN(SELECT MAINUUID FROM OT_LEAFS WHERE VALUE LIKE ?) "
                            + "AND ELEMENTNAME LIKE 'de.erv.vgercls.bo.schadensvorgang.steuerung.SchadenfalltypSteuerung.versicherungsart.INSURANCE_TYPE'";

    private Connection con, conProd;
    private ResultSet rs, rsPayment, rsClaymtype, rsItaly, rsItalyReserve;
    private PreparedStatement prepStm, prepStmPayment, prepStmClaymtype, prepStmItaly, prepStmItalyReserve,
            prepStmItalyReserveUdateAmount, prepStmItalyReserveInsert, prepStmItalyReserveUpdate,
            prepStmItalyReserveVersArt, prepStmPaymentStatus;
    private String prepSelectForClaim, fieldTypeOfClaim;
    private WriteToFile writeToFile;

    public ImportDB() {
        super();
        ObjectStorage.INSTANCE.fillStorageFromPropertyFile(Constants.STORAGE_ITALY, Constants.PATH_ITALY, null);
    }

    /**
     * Get a connection with the values from the prperty file.
     *
     * @return Connection.
     */
    public Connection getConn() {
        try {
            if (con != null) {
                return con;
            }

            Class.forName(getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DRIVER));
            DesEncrypter dec = new DesEncrypter(Constants.AES_KEY);

            logger.info("DB Connection: " + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DRIVERTYPE)
                    + "@" + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_URL)
                    + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_PORT)
                    + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DBNAME) + " DB Name: "
                    + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DBNAME) + " DB User: "
                    + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_USER));

            con = DriverManager.getConnection(
                    getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DRIVERTYPE) + "@"
                            + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_URL)
                            + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_PORT)
                            + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DBNAME),
                    getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_USER),
                    dec.decrypt(getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_PW)));
            DriverManager.setLogWriter(new java.io.PrintWriter(new java.io.OutputStreamWriter(System.out)));
            return con;
        } catch (Exception e) {
            logger.error("Can't register JDBC driver. Exception: ", e);
        }
        return null;
    }

    /**
     * this is plain stupid but to get it working i get the con to another db
     *
     * @return
     */
    public Connection getConnExt() {
        try {
            if (conProd != null) {
                return conProd;
            }
            Class.forName(getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DRIVER2));
            DesEncrypter dec = new DesEncrypter(Constants.AES_KEY);
            conProd = DriverManager.getConnection(
                    getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DRIVERTYPE2) + "@"
                            + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_URL2)
                            + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_PORT2)
                            + getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_DBNAME2),
                    getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_USER2),
                    dec.decrypt(getStringFromProp(Constants.STORAGE_IMPORTCLIENT, PROP_NAME_PW2)));
            return conProd;
        } catch (Exception e) {
            logger.error("Can't register JDBC driver. Exception: ", e);
        }
        return null;
    }

    /**
     * Returns the contact_id for a given search string with the values in searchStrings.
     *
     * @param search String the search string to use for the search
     * @return String
     */
    public String getContactNo(Map<String, String> searchStrings) throws SQLException {
        logger.info("search string: " + searchStrings);
        String contactId = null;
        try {// has to be initialized before getting here.
            if (con == null) {
                throw new SQLException("You have to call initClaim(String prepStmtSelect) to initialize the system.");
            }

            if (searchStrings.get("LASTNAME") != null) {
                String lastname = searchStrings.get("LASTNAME")
                        .toUpperCase()
                        .replace("Ä", "AE")
                        .replace("Ö", "OE")
                        .replace("Ü", "UE")
                        .replace("ß", "SS");
                logger.info("LASTNAME: " + lastname);
                searchStrings.put("LASTNAME", lastname);
            }
            if (searchStrings.get("FIRSTNAME") != null) {
                String firstname = searchStrings.get("FIRSTNAME")
                        .toUpperCase()
                        .replace("Ä", "AE")
                        .replace("Ö", "OE")
                        .replace("Ü", "UE")
                        .replace("ß", "SS");
                logger.info("FIRSTNAME: " + firstname);
                searchStrings.put("FIRSTNAME", firstname);
            }
            if (searchStrings.get("STREET") != null && searchStrings.get("STREET").length() >= 5) {
                String street = searchStrings.get("STREET")
                        .toUpperCase()
                        .replace("Ä", "AE")
                        .replace("Ö", "OE")
                        .replace("Ü", "UE")
                        .replace("ß", "SS")
                        .substring(0, 5) + "%";
                logger.info("STREET: " + street);
                searchStrings.put("STREET", street);
            }

            prepStm.setString(1, searchStrings.get("LASTNAME"));
            prepStm.setString(2, searchStrings.get("FIRSTNAME"));
            prepStm.setString(3, searchStrings.get("ZIP_CODE"));
            prepStm.setString(4, searchStrings.get("ZIP_CODE"));
            prepStm.setString(5, searchStrings.get("STREET"));

            rs = prepStm.executeQuery();
            while (rs.next()) {
                // if more than one return null.
                /*
                 * if(i > 0){ contactId = null; logger.debug("More than one contact found!"); break; }else{
                 */
                contactId = rs.getString(1);
                logger.info("contact id: " + contactId);
                // }
                break;

            }
        } catch (SQLException e) {
            logger.error("Bad luck running statement Exception: ", e);
        }
        return contactId;
    }

    public void initClaim(String prepStmtSelect) {
        try {
            prepSelectForClaim = prepStmtSelect;
            con = getConn();
            prepStm = con.prepareStatement(prepSelectForClaim);
        } catch (Exception e) {
            logger.error("Bad luck initializing Exception: ", e);
        }
    }

    public String getClaimForItalyDataFixAutomation(String claimNo, String query, Connection conn) {
        try {

            if (prepStm == null) {
                prepStm = conn.prepareStatement(query);
            }

            logger.debug("checking claimtype " + claimNo);
            prepStm.setString(1, claimNo);

            rs = prepStm.executeQuery();

            if (rs.next()) {
                logger.debug("Claimtype no: " + rs.getString("VORGANGSNUMMER"));
                return rs.getString("VORGANGSNUMMER");
            }

        } catch (Exception e) {
            logger.error("Bad luck initializing Exception: ", e);
        }
        return null;
    }

    public Connection initConnection() {

        if (con == null) {
            con = getConn();
        }
        return con;
    }

    public void initClaim() {
        try {

            con = getConn();
            prepStm = con.prepareStatement(prepSelectForClaim);
        } catch (Exception e) {
            logger.error("Bad luck initializing Exception: ", e);
        }
    }

    public void initClaimItaly() {
        try {
            con = getConn();
            prepStmItaly = con.prepareStatement(SEL_NEXT_VALUE_FOR_ITALY_CLAIM);
            prepStmItalyReserveVersArt = con.prepareStatement(SEL_INSURANCE_TYPE);
        } catch (Exception e) {
            logger.error("Bad luck initializing Exception: ", e);
            writeResultsToFile("Can't initialize database connection!");
        }
    }

    public void initClaimType() {
        try {
            con = getConn();
            prepStm = con.prepareStatement(SEL_CLAIMNO);
            prepStmPayment = con.prepareStatement(SEL_PAYMENT);
            prepStmClaymtype = con.prepareStatement(SEL_CLAIMTYPE);
        } catch (Exception e) {
            logger.error("Bad luck initializing Exception: ", e);
        }
    }

    /**
     * Finds with a claim no. and a claimtype the claimtype no. if there is one.
     *
     * @param claimNo String
     * @param claimtype String
     * @return int
     * @throws Exception @{@link Exception}
     */
    public int getClaimtypeNo(String claimNo, String claimtype) throws Exception {
        try {
            logger.info("Get claimtype number.");

            checkNotNull(claimNo, "Vrgnr is null!");

            if (con == null) {
                con = getConn();
            }

            if (prepStm == null) {
                prepStm = con.prepareStatement(SEL_CLAIMNO);
            }

            logger.debug("checking claimtype " + claimtype);
            prepStm.setString(1, claimNo);
            prepStm.setString(2, claimtype);
            rs = prepStm.executeQuery();

            while (rs.next()) {
                logger.debug("Claimtype no: " + rs.getInt(1));
                return rs.getInt(1);
            }
            return 0;
        } catch (Exception e) {
            logger.error("Bad luck running statement Exception: ", e);
        }
        return 0;
    }

    /**
     * Find out if for a given payment no. with claimtype no. and claim no. a payment was already made.
     *
     * @param claimNo String
     * @param claimtypeNo int
     * @return boolean
     */
    public boolean paymentExists(String claimNo, int claimtypeNo) {
        boolean exist = false;
        int paymentNumber = 1;
        try {
            logger.info("Payment exists --> checking payment amounts");
            checkNotNull(claimNo, "Vrgnr is null!");

            if (con == null) {
                con = getConn();
            }

            if (prepStmPayment == null) {
                prepStmPayment = con.prepareStatement(SEL_PAYMENT);
            }

            prepStmPayment.setString(1, claimNo);
            prepStmPayment.setInt(2, claimtypeNo);
            rsPayment = prepStmPayment.executeQuery();

            while (rsPayment.next() && !exist) {
                logger.info("Payment amount for " + paymentNumber + ". payment: " + rsPayment.getBigDecimal(1)
                        + " for activitynumber: " + claimNo + " and claimtypenumber: " + claimtypeNo);
                // there are payments with pos. and neg. amounts
                if (rsPayment.getBigDecimal(1) != null && rsPayment.getBigDecimal(1).compareTo(BigDecimal.ZERO) != 0) {
                    exist = true;
                    logger.info("found payment amount in " + paymentNumber + ". payment!");
                }
                paymentNumber++;
            }
            if (!exist) {
                logger.info("No payment found with amount != 0 for activitynumber: " + claimNo
                        + " and claimtypenumber: " + claimtypeNo);
            }
        } catch (Exception e) {
            logger.error("Bad luck running statement Exception: ", e);
        }
        return exist;
    }

    /**
     * Returns the claimtype if the first claimtype has no entry in the first payment.
     *
     * @param claimNo
     * @return
     */
    public String[] getClaimtypeAndPaymentAmount(String claimNo) {
        try {
            logger.info("Get claimtype and payment amount");
            checkNotNull(claimNo, "Vrgnr is null!");

            if (con == null) {
                con = getConn();
            }

            if (prepStmClaymtype == null) {
                prepStmClaymtype = con.prepareStatement(SEL_CLAIMTYPE);
            }

            String[] res = new String[2];
            logger.debug("checking claimtype " + claimNo);

            prepStmClaymtype.setString(1, claimNo);
            prepStmClaymtype.setString(2, claimNo);
            rsClaymtype = prepStmClaymtype.executeQuery();

            while (rsClaymtype.next()) {
                logger.debug("Claimtype no: " + rsClaymtype.getString(1));
                res[0] = rsClaymtype.getString(1);
                res[1] = rsClaymtype.getString(2);
                return res;
            }
        } catch (Exception e) {
            logger.error("Bad luck running statement Exception: ", e);
        }
        logger.debug("Found nothing with this claim no: " + claimNo);
        return null;
    }

    public String getSequenceValForItaly() throws SQLException {
        logger.info("Get sequence value for Italy");

        if (con == null) {
            con = getConn();
        }

        if (prepStmItaly == null) {
            prepStmItaly = con.prepareStatement(SEL_NEXT_VALUE_FOR_ITALY_CLAIM);
        }

        logger.debug("checking italy ");
        rsItaly = prepStmItaly.executeQuery();

        while (rsItaly.next()) {
            return rsItaly.getString(1);
        }

        return null;
    }

    /**
     * Handles the entry for italy claim. If it is a payment and a entry in the table exists update. If it is a reserve
     * and a entry exists update the entry. If it is a reserve and no entry exists insert the new entry. If it is a
     * reserve payment or the amount is 0 return true. It's not entered in the regular table.
     *
     * @return boolean
     * @throws SQLException
     */
    public boolean isReservePayment(Map<String, Map<String, String>> alldataItem) throws Exception {
        logger.info("Is reserve payment?");

        if (con == null) {
            con = getConn();
        }

        if (fieldTypeOfClaim == null) {
            fieldTypeOfClaim = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_FIELD_TYPE_OF_CLAIM);
        }

        String res = "";
        Map<String, String> dataItem = newHashMap(), data = null;
        int x = 0;
        try {
            while (res != null) {
                res = getStringFromProp(Constants.STORAGE_ITALY,
                        PROP_NAME_RESERVE_TBL_NAME + Integer.valueOf(x++).toString());
                data = alldataItem.get(res);

                if (data != null) {
                    dataItem.putAll(data);
                }

            }
            // find out if payment or reserve
            if (reservetable == null) {
                loadPorpertiesForItalyReserve();
            }

            // check if it exists update

            if (prepStmItalyReserve == null) {
                setUpStatements();
            }
            prepStmItalyReserve.setString(1, dataItem.get(reservetblpk));
            rsItalyReserve = prepStmItalyReserve.executeQuery();

            boolean entryExists = rsItalyReserve.next();

            String rowId = entryExists ? rsItalyReserve.getString(1)
                    : null;
            String resVal = dataItem.get(reservetblreserve), strAmount = dataItem.get(reservetblamount), field = null;

            double reserveValue = resVal != null ? Double.parseDouble(resVal)
                    : 0;
            double amount = strAmount != null ? Double.parseDouble(strAmount)
                    : 0;
            if (reserveValue > 0 || amount == 0 && reserveValue == 0) {
                Counters.INSTANCE.add(Constants.COUNTER_KEY_PROCESSED);
                if (entryExists) {// do update
                    logger.info("Update reserve payment.");
                    prepStmItalyReserveUpdate.setDouble(1, reserveValue);
                    prepStmItalyReserveUpdate.setString(2, rowId);
                    prepStmItalyReserveUpdate.execute();
                    con.commit();

                    logger.info("Update done.");
                    logToFile(1, dataItem.get(reservetblpk), reserveValue, amount);
                } else {// do insert

                    logger.info("Insert reserve payment?");
                    // set vers art
                    dataItem.put(DUMMY_VERSART, getVersArt(dataItem.get(fieldTypeOfClaim)));
                    int len = fieldsList.size();
                    int i = 0;
                    for (i = 0; i < len; i++) {
                        field = fieldsList.get(i);
                        if (reservetblreserve.equals(field) || reservetblamount.equals(field)) {
                            prepStmItalyReserveInsert.setDouble(i + 1, reservetblreserve.equals(field) ? reserveValue
                                    : amount);
                        } else if (reservetblDateOfEntry.equals(field)) {

                            DateFormat formater = new SimpleDateFormat("ddMMyyyy");
                            java.util.Date parsedUtilDate = formater.parse(dataItem.get(field));
                            java.sql.Date sqltDate = new java.sql.Date(parsedUtilDate.getTime());

                            prepStmItalyReserveInsert.setDate(i + 1, sqltDate);
                        } else {
                            prepStmItalyReserveInsert.setString(i + 1, dataItem.get(field));
                        }
                    }

                    prepStmItalyReserveInsert.execute();
                    logger.info("Insert done.");
                    logToFile(2, dataItem.get(reservetblpk), reserveValue, amount);
                }
                return true;
            } else {
                if (entryExists) {// update amount
                    logger.info("Update amount and reserve in reserve payment.");
                    prepStmItalyReserveUdateAmount.setDouble(1, amount);
                    prepStmItalyReserveUdateAmount.setDouble(2, reserveValue);
                    prepStmItalyReserveUdateAmount.setString(3, rowId);
                    prepStmItalyReserveUdateAmount.execute();
                    con.commit();
                    logger.info("Update done.");
                    logToFile(3, dataItem.get(reservetblpk), reserveValue, amount);
                }
            }
        } catch (Exception e) {
            logger.error("Exception: ", e);
            logToFile(4, dataItem.get(reservetblpk), null, null);
            throw e;
        }

        logger.info("No reserve payment!");
        return false;
    }

    /**
     * Check if one of the payments for this claimNo and claimtypeNo has a payment set on AB. If so return true.
     *
     * @return boolean
     */

    public boolean isStatusPayed(String claimNo, int claimtypeNo) throws SQLException {

        boolean payed = false;
        int paymentNumber = 1;
        logger.debug("checking paymentStatus claimNo: " + claimNo + " claymtypeNo: " + claimtypeNo);

        if (prepStmPaymentStatus == null) {
            prepStmPaymentStatus = con.prepareStatement(SEL_STATUS_PAYMENTS);
        }

        prepStmPaymentStatus.setString(1, claimNo);
        prepStmPaymentStatus.setInt(2, claimtypeNo);
        rsPayment = prepStmPaymentStatus.executeQuery();
        while (rsPayment.next() && !payed) {
            logger.info("Payment status of " + paymentNumber + ". payment: " + rsPayment.getString(1)
                    + " for activitynumber: " + claimNo + " and claimtypenumber: " + claimtypeNo);
            if ("AB".equals(rsPayment.getString(1))) {
                logger.info(paymentNumber + ". payment is payed!");
                payed = true;
            }
            paymentNumber++;
        }
        if (!payed) {
            logger.info("No payment found with status 'AB' for activitynumber: " + claimNo + " and claimtypenumber: "
                    + claimtypeNo);
        }
        return payed;
    }

    /**
     * Set up the prepared statements for the Italy claim.
     *
     * @throws SQLException
     */
    private void setUpStatements() throws SQLException {
        logger.info("Reserve Italy set up prep statements.");

        prepStmItalyReserve = con.prepareStatement("SELECT ROWID FROM " + reservetable + " WHERE " + columnPk + "=?");
        prepStmItalyReserveUdateAmount = con.prepareStatement("UPDATE " + reservetable + " SET "
                + reservetblamountcolumn + "=?," + reservetblreservecolumn + "=?" + " WHERE ROWID" + "=?");
        prepStmItalyReserveInsert = con.prepareStatement(insertItalyReserve);
        prepStmItalyReserveUpdate = con.prepareStatement(
                "UPDATE " + reservetable + " SET " + reservetblreservecolumn + "=?" + " WHERE ROWID" + "=?");
        prepStmItalyReserveVersArt = con.prepareStatement(SEL_INSURANCE_TYPE);
        prepStmPaymentStatus = con.prepareStatement(SEL_STATUS_PAYMENTS);

    }

    public String getVersArt(String type) throws SQLException {
        prepStmItalyReserveVersArt.setString(1, type);
        rsItalyReserve = prepStmItalyReserveVersArt.executeQuery();
        boolean entryExists = rsItalyReserve.next();
        String versArt = entryExists ? rsItalyReserve.getString(1)
                : null;
        return versArt;
    }

    /**
     * Get the property values from file and build the insert Statement for the reserve table.
     */
    private void loadPorpertiesForItalyReserve() throws IOException {
        logger.info("Reserve Italy load properties.");

        reservetable = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL);
        reservetblreservecolumn = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_RESERVE_COLUMN);
        reservetblpk = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_PK);
        reservetblreserve = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_RESERVE);
        reservetblDateOfEntry = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_DATE_OF_ENTRY);
        reservetblamountcolumn = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_AMOUNT_COLUMN);
        reservetblamount = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_AMOUNT);

        // set up the prepstm for insert
        StringBuilder strBuff = new StringBuilder(), strBuffValues = new StringBuilder();
        strBuff.append("INSERT INTO ").append(reservetable).append(" (");

        if (fieldsList == null) {
            fieldsList = newArrayList();
        }
        String res = "VERSART", map = DUMMY_VERSART;
        int i = 0;

        while (res != null) {

            if (res != null && !"".equals(res)) {
                strBuff.append(res).append(",");
                strBuffValues.append("?").append(",");
                fieldsList.add(map);
                if (reservetblpk.equals(map)) {
                    columnPk = res;
                }
            }
            res = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_COLUMN + i);
            map = getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_TBL_MAP + i++);
        }
        strBuff.deleteCharAt(strBuff.lastIndexOf(","));
        strBuffValues.deleteCharAt(strBuffValues.lastIndexOf(","));
        strBuff.append(") ").append("VALUES(").append(strBuffValues.toString()).append(")");
        insertItalyReserve = strBuff.toString();

    }

    /**
     * This is used to put the string together that has to be written into the file with the result from the reserve
     * insert.
     *
     * @param from int
     * @param pk String
     * @param reserve Double
     * @param amount Double
     */
    private void logToFile(int from, String pk, Double reserve, Double amount) {
        StringBuilder strBuff = new StringBuilder();
        strBuff.append(getStringFromProp(Constants.STORAGE_ITALY, from == 1 ? PROP_NAME_UPDATE_RESERVE
                : from == 2 ? PROP_NAME_INSERT_NEW_RESERVE
                        : from == 3 ? PROP_NAME_UPDATE_AMOUNT_RESERVE
                                : PROP_NAME_ERROR))
                .append(pk)
                .append(" ");

        if (from < 4) {
            strBuff.append(getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_VALUE_RESERVE))
                    .append(reserve)
                    .append(" ")
                    .append(getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_VALUE_AMOUNT))
                    .append(amount);
        }
        strBuff.append(" ")
                .append(getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_TIME))
                .append(new SimpleDateFormat(
                        getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_DATE_FORMAT_RESULT_FILE_CONTENT))
                                .format(new java.util.Date()));

        writeResultsToFile(strBuff.toString());
    }

    /**
     * Writes to the file. Uses the path, name and the current date for the name of the file. All the info comes from
     * the property file.
     *
     * @param content String
     */
    private void writeResultsToFile(String content) {

        if (writeToFile == null) {
            writeToFile = new WriteToFile();
        }

        writeToFile.writeToFile(getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_RESERVE_FILE_PATH),
                new SimpleDateFormat(getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_DATE_FORMAT_RESULT_FILE))
                        .format(new java.util.Date()) + " "
                        + getStringFromProp(Constants.STORAGE_ITALY, PROP_NAME_NAME_RESULT_FILE) + ".txt",
                content);
    }

    /**
     * Get the String from the property file.
     *
     * @param search String
     * @return String
     */
    private String getStringFromProp(String _storage, String _search) {

        return ObjectStorage.INSTANCE.getProperty(_storage, _search, null, String.class);

    }

    /**
     * Close the objects used for this jdbc action.
     */
    public void closeTheLot() {
        try {
            logger.info("Close the db!");
            if (rs != null) {
                rs.close();
            }
            if (rsClaymtype != null) {
                rsClaymtype.close();
            }
            if (rsItaly != null) {
                rsItaly.close();
            }
            if (rsPayment != null) {
                rsPayment.close();
            }
            if (rsItalyReserve != null) {
                rsItalyReserve.close();
            }
            if (prepStm != null) {
                prepStm.close();
            }
            if (prepStmPayment != null) {
                prepStmPayment.close();
            }
            if (prepStmClaymtype != null) {
                prepStmClaymtype.close();
            }
            if (prepStmItaly != null) {
                prepStmItaly.close();
            }
            if (prepStmItalyReserve != null) {
                prepStmItalyReserve.close();
            }
            if (prepStmItalyReserveInsert != null) {
                prepStmItalyReserveInsert.close();
            }
            if (prepStmItalyReserveUdateAmount != null) {
                prepStmItalyReserveUdateAmount.close();
            }
            if (prepStmItalyReserveUpdate != null) {
                prepStmItalyReserveUpdate.close();
            }
            if (prepStmItalyReserveVersArt != null) {
                prepStmItalyReserveVersArt.close();
            }
            if (prepStmPaymentStatus != null) {
                prepStmPaymentStatus.close();
            }

            if (con != null) {
                con.close();
            }
            if (conProd != null) {
                conProd.close();
            }
        } catch (Exception e) {
            logger.error("Can't close jdbc. con " + con + " prepStm " + prepStm + " rs " + rs);
            logger.error("Can't close jdbc. Exception: ", e);
        }
    }
}
