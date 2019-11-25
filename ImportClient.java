package de.erv.vgercls.importtool;

import static de.itergo.commons.base.Collections.newArrayList;
import static de.itergo.commons.base.Collections.newHashMap;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.RuleBasedCollator;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import org.apache.log4j.Logger;

import de.erv.util.DesEncrypter;
import de.erv.vgercls.importtool.util.Constants;
import de.itergo.commons.base.Counters;
import de.itergo.commons.base.LoggerFactory;
import de.itergo.commons.base.ObjectStorage;
import de.itergo.commons.reflection.ClassCreator;

import de.novum.vger2.api.VgerResponseTask;
import de.novum.vger2.api.processing.XMLBuilder;
import de.novum.vger2.api.processing.steps.StepChange;
import de.novum.vger2.api.processing.steps.StepChanges;
import de.novum.vger2.api.processing.steps.StepCreate;
import de.novum.vger2.api.processing.steps.StepLoad;
import de.novum.vger2.api.processing.steps.StepNewArrayElement;
import de.novum.vger2.api.processing.steps.StepSearch;
import de.novum.vger2.api.processing.tasks.TaskProceed;
import de.novum.vger2.errormsg.ErrorCodes;
import de.novum.vger2.exception.VgerBusinessException;

public class ImportClient extends FileClient {

    private static final Logger logger = LoggerFactory.makeLogger();

    private static final String PROP_NAME_CONTACTSEARCH = "contactsearch";
    /** Names of the items in the property file */
    /** The number of the job that is running */

    private static final String BENUTZER = "benutzer";
    private static final String PASSWORT = "passwort";
    private static final String MANDANT = "mandant";
    private static final String LINKOBJCLAIM = "linkobjclaim";
    private static final String LINKOBJCLAIMPK = "linkobjclaimpk";
    private static final String LINKOBJCLAIMTYPE = "linkedobjclaimtype";
    private static final String LINKOBJCLAIMTYPEFK = "linkedobjclaimtypefk";
    private static final String LINKOBJCLAIMTYPEFKMERCUR = "linkedobjclaimtypefkmercur";
    private static final String LINKOBJCLAIMTYPEPKSTATICFIELD = "linkedobjclaimtypepkstaticfield";
    private static final String LINKOBJCLAIMTYPEPKSTATICVALUE = "linkedobjclaimtypepkstaticvalue";
    private static final String LINKOBJCLAIMTYPESEARCHSTATICFIELD = "linkedobjclaimtypesearchstaticfield";
    private static final String LINKOBJCLAIMTYPEPK = "linkedobjclaimtypepk";
    private static final String LINKOBJCONTACT = "linkedobjcontact";
    private static final String CONTACTSEARCH = "contactsearch";
    private static final String CONTACTCOLUMN = "contactcolumn";
    private static final String LINKOBJPAYMENT = "linkedobjpayment";
    private static final String BOSCHADENSVORGANG = "boschadensvorgang";
    private static final String BOZAHLUNG = "bozahlung";
    private static final String BOPARTNER = "bopartner";
    private static final String PATHERRORFILE = "patherrorfile";
    private static final String ERRORSTART = "errorstarttxt";
    private static final String ERRORDATEFORMAT = "dateformat";
    private static final String TRIGGER = "trigger";
    private static final String STATIC = "static";
    private static final String DATEACTIVEFROM = "dateActiveFrom";
    private static final String DATEFORMATENTRYDATEAPP = "dateformatentrydateapp";
    private static final String DATEFORMAT = "dateformat";
    private static final String FIELDDATEFORMATENTRYDATEAPP = "fielddateformatentrydateapp";
    private static final String MAPCOUNTRY = "mapcountry";
    private static final String TESTMODE = "testmode";

    /** the field where to link */
    private static final String PARTNERNUMMERLINKFORPARTNER = "partnernummerlinkforpartner";
    private static final String FALLPARTNERLINKFORPARTNER = "fallpartnerlinkforpartner";
    private static final String CONTACTFLAGLINKFORPARTNER = "contactflaglinkforpartner";

    /** errror msg no claim no in the db */
    private static final String ERROR_MSG_NO_CLAIM_NUMBER = "errorclaimno";
    /** path for the original error data */
    private static final String PATH_ERRORFILE_ORIGINAL_DATA = "patherrorfileoriginaldata";
    /** gets added to the job name in the file */
    private static final String EXTENSION_ORIG_DATA_FILE_NAME = "extensionorigdatfilename";

    /** Data from property file for the file with the results (success and error) */
    private static final String PATH_RESULT_FILE = "pathresultfile";
    private static final String DATE_FORMAT_RESULT_FILE = "dateformatresultfile";
    private static final String NAME_RESULT_FILE = "nameresultfile";
    private static final String DATE_FORMAT_RESULT_FILE_CONTEND = "dateformatresultfilecontend";
    /** Start the line for success */
    private static final String RESULT_FILE_SUCCESS = "resultfilesuccess";
    /** Start the line for error */
    private static final String RESULT_FILE_ERROR = "resultfileerror";
    /** Start the line for test save */
    private static final String RESULT_FILE_TEST_SAVE = "resultfiletestsave";
    /** The info for the Mercur or Italy number */
    private static final String RESULT_FILE_ERROR_MERC_IT_NO = "resultfileerrormercitalynumber";

    /** Testing flag */

    private boolean testMode;

    /** do not change this string since it is used by ItalyClaim to mark the error msg */
    private static final String ERROR_MSG = "Error Msg:";
    /**
     * The name of the error table don't change this name since it has to equal the name of the table in the property
     * file it's for italy in the moment.
     */
    private static final String ERRORS = "errortablename";

    /** The name of the table with the original data just used for error handling. */
    private static final String ORIGINAL_DATA = "originaldatatablename";

    /** date format for the date active from field */
    private static final String dateFormatDateActiveFrom = "dd.MM.yyyy";
    private static final String separator = System.getProperty("line.separator");
    private boolean goOn;
    private int empty;
    /** Data from the propery file */
    private String job, className, linkObjClaimTypePkStaticField, linkedobjclaimtypepkstaticvalue, dateFormat,
            linkedobjclaimtypesearchstaticfield, schadenfalltyp;

    private SimpleDateFormat df;
    /** What date is this active from */
    /** Map the claim countries */
    private Map<String, String> country, triggerClaimStaticValues, triggerClaimtypeStaticValues,
            triggerPaymentStaticValues, contactSearch, triggerContactStaticValues;
    private List<String> triggerClaim, triggerClaimtype, triggerPayment, triggerContact;

    private List<Map<String, String>> arrListOriginalData;
    /** The search strings to use for the search */
    /** Stores the errors if imports don't go through */
    private StringBuilder strBuffErrors, strBuffErrorFile;
    private int errorCounter;
    /** This is just for the system.exit. Gets set if one or more errors occur. */
    private boolean gotError;
    /** Class to handle the db issues */
    private ImportDB importDB;

    public ImportClient() {
        init();
        // importData();
    }

    /**
     * Initialize the program. Get values from the properties file.
     */
    @SuppressWarnings(value = "unchecked")
    private void init() {
        // get the mapping for country and cliam country. This was used once leave it for now who knows?

        ObjectStorage.INSTANCE.fillStorageFromPropertyFile(Constants.STORAGE_COUNTRY_MAPPING,
                Constants.PATH_COUNTRY_MAPPING, null);
        ObjectStorage.INSTANCE.fillStorageFromPropertyFile(Constants.STORAGE_IMPORTCLIENT,
                Constants.PATH_IMPORTCLIENT_CONFIGURATION, null);
        ObjectStorage.INSTANCE.fillStorageFromPropertyFile(Constants.STORAGE_COUNTRY_MAPPING_RETROFITTED,
                Constants.PATH_COUNTRY_MAPPING_RETROFITTED, null);

        country = (Map) ObjectStorage.INSTANCE.getStorageItem(Constants.STORAGE_COUNTRY_MAPPING);

        final String testFlag = getStringFromProp(TESTMODE);

        if (null != testFlag && "true".equals(testFlag.trim())) {
            testMode = true;
        }

        // the triggers and their static values if they have any
        triggerClaim = newArrayList();
        triggerClaimtype = newArrayList();
        triggerPayment = newArrayList();
        triggerContact = newArrayList();
        triggerClaimStaticValues = newHashMap();
        triggerClaimtypeStaticValues = newHashMap();
        triggerPaymentStaticValues = newHashMap();
        triggerContactStaticValues = newHashMap();

        setTriggerValues(triggerClaim, triggerClaimStaticValues, TRIGGER + LINKOBJCLAIM);
        setTriggerValues(triggerClaimtype, triggerClaimtypeStaticValues, TRIGGER + LINKOBJCLAIMTYPE);
        setTriggerValues(triggerPayment, triggerPaymentStaticValues, TRIGGER + LINKOBJPAYMENT);
        setTriggerValues(triggerContact, triggerContactStaticValues, TRIGGER + LINKOBJCONTACT);

        // get the values for the contact search
        setContactSearchValues();

        linkObjClaimTypePkStaticField = getStringFromProp(LINKOBJCLAIMTYPEPKSTATICFIELD);
        linkedobjclaimtypepkstaticvalue = getStringFromProp(LINKOBJCLAIMTYPEPKSTATICVALUE);

        linkedobjclaimtypesearchstaticfield = getStringFromProp(LINKOBJCLAIMTYPESEARCHSTATICFIELD);

    }

    /**
     * Get the classes we need and call readData to do the import.
     */

    private void importData() {
        try {
            ImportFilter aClass = null;

            className = System.getProperty("java.run.class");
            logger.info("run importData got class " + className);
            if (null == className) {
                return;
            }

            aClass = ClassCreator.createClass(className, ImportFilter.class);

            writeDataToObjects(aClass.readData());
            // for the second run close the con first
            if (null != importDB) {
                importDB.closeTheLot();
                importDB = null;
            }

        } catch (final Exception e) {
            logger.error("error ImportData " + e.getMessage(), e);
            gotError = true;
            writeToFile(getStringFromProp(PATHERRORFILE), getFileName(job), e.getMessage() + e, true);
            // writeToFile(filePath, fileName, fileContent, writeOriginalData)
        } finally {
            /******************************************************************************
             * this has to be closed to make sure all jdbc things are closed properly. *
             ******************************************************************************/
            if (null != importDB) {
                importDB.closeTheLot();
            }
            disconnect();
            if (null != strBuffErrors) {
                logger.info("got error(s)");
            }

            if (gotError) {
                System.exit(1);
            } else {
                System.exit(0);
            }
        }
    }

    /**
     * From Italy is a file coming with the errors that occured loading the data. Read this file first and place the
     * errors into error file
     *
     * @param arrL ArrayList<HashMap<String, ArrayList<HashMap<String, String>>>>
     */
    private void handleErrors(final List<Map<String, List<Map<String, String>>>> arrL) {
        Map<String, String> hmLinkObj = null;
        final String errors = getStringFromProp(ERRORS);
        int i = 0;
        final int size = arrL.size();
        hmLinkObj = getTable(errors, arrL);
        final StringBuilder res = new StringBuilder(), resData = new StringBuilder();
        String errorMsg = null;
        final String claimN = getStringFromProp(LINKOBJCLAIMPK);
        String claimNo = null, mercurVrgno = null;
        final String mercurVr = getStringFromProp(LINKOBJCLAIMTYPEFKMERCUR);
        while (null != hmLinkObj) {
            for (final String item : hmLinkObj.keySet()) {
                // put the claim number at the beginning of the line to make it easy to find.
                if (mercurVr.equals(item)) {
                    mercurVrgno = hmLinkObj.get(item);
                    continue;
                } else if (claimN.equals(item)) {
                    claimNo = hmLinkObj.get(item);
                    continue;
                }
                // get the error message for the item
                if (ERROR_MSG.equals(item)) {
                    errorMsg = hmLinkObj.get(item);
                } else {
                    res.append(' ').append(item).append(" = ").append(hmLinkObj.get(item));
                }
            }
            hmLinkObj = getTable(errors, arrL);
            if (++i == size - 1) {
                resData.append(claimN)
                        .append(" = ")
                        .append(claimNo)
                        .append(' ')
                        .append(mercurVr)
                        .append(" = ")
                        .append(mercurVrgno);
                appendError(resData.toString() + res.toString(), errorMsg, claimNo);
                // for the result file
                appendResultFile(RESULT_FILE_ERROR, claimNo, mercurVrgno);
                res.setLength(0);
                resData.setLength(0);
                i = 0;
            }
        }
    }

    /**
     * Add the original data into a ArrayList.
     *
     * @param arrL ArrayList<HashMap<String, ArrayList<HashMap<String, String>>>>
     */
    private void handleOriginalData(final List<Map<String, List<Map<String, String>>>> arrL) {
        final String originalData = getStringFromProp(ORIGINAL_DATA);
        Map<String, String> hmLinkObj = getTable(originalData, arrL);
        if (null == arrListOriginalData) {
            arrListOriginalData = newArrayList();
        }
        while (null != hmLinkObj) {
            arrListOriginalData.add(hmLinkObj);
            hmLinkObj = getTable(originalData, arrL);
        }
    }

    /**
     * Extract the data from the arrlist and put it into objects.
     *
     * @param data ArrayList<HashMap<String, ArrayList<HashMap<String, String>>>>>
     * @throws Exception Exception
     */
    private void writeDataToObjects(final Map<String, List<Map<String, List<Map<String, String>>>>> data)
            throws Exception {
        // what job
        logger.info("start converting the data ");
        // warum wurde hier ein Iterator genommen wenn eh nur ein element drin ist?!?
        job = data.keySet().iterator().next();
        final List<Map<String, List<Map<String, String>>>> arrL = data.get(job);
        final int arrLSize = arrL.size();
        int count = 0;
        logger.info("objects to process " + arrLSize);
        empty = 0;
        if (0 == arrLSize) {
            logger.info("noting to process");
            return;
        }
        goOn = !data.isEmpty();
        // get the obj to be linked
        Map<String, String> hmLinkObj = null, hmObjLinkExt = null;
        List<Claim> arrLClaim = null;
        List<Claimtype> arrLClaimtype = null;
        final String obj = getStringFromProp(LINKOBJCLAIM);
        String st = null;
        final String objToBeLinkedClaimtype = getStringFromProp(LINKOBJCLAIMTYPE),
                objToBeLinkedPayment = getStringFromProp(LINKOBJPAYMENT);

        Claim claim = null;
        Claimtype claimtype = null;
        Payment payment = null;
        boolean handleMiscellaneous = false;

        while (goOn) {
            logger.debug("go on");
            // get the errors for the moment this is just for italy handle separate
            if (!handleMiscellaneous) {
                handleErrors(arrL);
                handleOriginalData(arrL);
                handleMiscellaneous = true;
            }
            // top item
            // hashmap containing table values
            if (null != obj) {

                hmLinkObj = getTable(obj, arrL);
                // fill claim object here Mercur Erstmeldung
                if (null != hmLinkObj) {
                    st = getSearchString(getStringFromProp(LINKOBJCLAIM), hmLinkObj);
                    claim = new Claim(getSearchString(getStringFromProp(LINKOBJCLAIMPK), hmLinkObj), hmLinkObj);
                    // get claimtype
                    if (null != objToBeLinkedClaimtype) {
                        hmObjLinkExt = getTable(objToBeLinkedClaimtype, arrL);
                        if (null != hmObjLinkExt) {
                            // if there get static value
                            st = getStringFromProp(LINKOBJCLAIMTYPEPKSTATICVALUE);

                            if (null == st) {
                                st = getSearchString(getStringFromProp(LINKOBJCLAIMTYPEPK), hmObjLinkExt);
                            }

                            claim.setClaimtype(claimtype = new Claimtype(hmObjLinkExt, st,
                                    getSearchString(getStringFromProp(LINKOBJCLAIMTYPEFK), hmObjLinkExt),
                                    getSearchString(getStringFromProp(LINKOBJCLAIMTYPEFKMERCUR), hmObjLinkExt)));
                            // Contact
                            hmObjLinkExt = getTable(getStringFromProp(LINKOBJCONTACT), arrL);
                            if (null != hmObjLinkExt) {
                                claimtype.setContact(new Contact(hmObjLinkExt));
                            }
                            // Payment
                            hmObjLinkExt = getTable(getStringFromProp(LINKOBJPAYMENT), arrL);
                            if (null != hmObjLinkExt) {
                                claimtype.setPayment(new Payment(hmObjLinkExt));
                            }

                        }
                    }
                    if (null == arrLClaim) {
                        arrLClaim = newArrayList(arrLSize);
                    }
                    arrLClaim.add(claim);
                }
            }
            if (null == hmLinkObj) {
                // got to be abschlussmeldung claimtype and payment to be filled
                if (null != objToBeLinkedClaimtype) {
                    hmObjLinkExt = getTable(objToBeLinkedClaimtype, arrL);
                    if (null != hmObjLinkExt) {
                        // if there get static value
                        st = getStringFromProp(LINKOBJCLAIMTYPEPKSTATICVALUE);
                        if (null == st) {
                            st = getSearchString(getStringFromProp(LINKOBJCLAIMTYPEPK), hmObjLinkExt);
                        }
                        claimtype = new Claimtype(hmObjLinkExt, st,
                                getSearchString(getStringFromProp(LINKOBJCLAIMTYPEFK), hmObjLinkExt),
                                getSearchString(getStringFromProp(LINKOBJCLAIMTYPEFKMERCUR), hmObjLinkExt));
                        if (null == arrLClaimtype) {
                            arrLClaimtype = newArrayList(arrLSize);
                        }
                        arrLClaimtype.add(claimtype);
                        // payment
                        hmObjLinkExt = getTable(objToBeLinkedPayment, arrL);
                        payment = new Payment(hmObjLinkExt);
                        claimtype.setPayment(payment);
                    }
                }
            }
            // no more data
            if (null == hmLinkObj && null == hmObjLinkExt) {
                if (handleMiscellaneous && 0 == count) {
                    if (null != strBuffErrors) {
                        writeToFile(getStringFromProp(PATHERRORFILE), getFileName(job), strBuffErrors.toString(), true);
                    }
                    logger.info("looks like a empty set!");
                    return;
                }
                handleTableNameError(obj, objToBeLinkedClaimtype, objToBeLinkedPayment, arrL);
                return;
            }
            count++;
        }
        // call here the insert for Claimtype
        if (null != arrLClaimtype) {
            Collections.sort(arrLClaimtype, new StrComparator());
            insertClaimtype(arrLClaimtype);
        }
        // and here for claim
        if (null != arrLClaim) {
            Collections.sort(arrLClaim, new StrComparatorClaim());
            insertClaim(arrLClaim);
        }
        logger.info("+++++ job " + job + " done");
    }

    /**
     * Builds a task from the array list with the change values. Handles the tasks for mercur first and the italy task.
     *
     * @param arrLClaim ArrayList<Claim>
     * @throws Exception Exception
     */
    private void insertClaim(final List<Claim> arrLClaim) {
        logger.info("\n\nstart insertClaim");
        final int arrLSize = arrLClaim.size();
        int start = 0;
        // format for the valid from date
        final Date dateObject = getDateActiveFrom();
        dateFormat = getStringFromProp(DATEFORMAT + className);
        // format for the entry date
        final String staticlinkforpartner = getStringFromProp(PARTNERNUMMERLINKFORPARTNER),
                fallpartnerLinkForPartner = getStringFromProp(FALLPARTNERLINKFORPARTNER),
                contactFlagLinkForPartner = getStringFromProp(CONTACTFLAGLINKFORPARTNER),
                businessObjectName = getStringFromProp(BOSCHADENSVORGANG);
        String mercurno = null, contactId = null, contactIdFallpartner = null;
        final String boPartner = getStringFromProp(BOPARTNER);
        String vrgnr = null, originalNumber = null;
        // arraylists to hold the values for the partners from mercur.
        final List<Map<String, String>> createPartner = newArrayList();
        final Map<String, Map<String, String>> linkIdAndTable = newHashMap();
        if (null != dateFormat) {
            df = new SimpleDateFormat(dateFormat);
        }

        if (null == importDB) {
            importDB = new ImportDB();
            importDB.initClaim(getStringFromProp(PROP_NAME_CONTACTSEARCH));
        }
        TaskProceed task = null;
        Claim claim = null;
        boolean createPartnerFirst = false;
        for (int i = 0; i < arrLSize; i++) {
            if (0 == i) {
                claim = arrLClaim.get(i);
            }
            try {
                start = i;
                // values for the claimtype
                if (null != claim.claimtype) {
                    task = new TaskProceed(businessObjectName, null, dateObject);

                    // TODO: gj: just to prevent the save for this data has to come out when ready.
                    if (testMode) {
                        task.setTestError("SAVE");
                    }

                    final StepCreate stepCreate = new StepCreate();
                    task.addVgerProcessObj(stepCreate);
                    // table values from claim
                    Map<String, String> tableValues = claim.values;
                    // set the Import values to get the data through the permission barrier (the triggers).
                    setTriggers(tableValues, stepCreate, null, null, 0);
                    // the mercur has to be linked very much on top
                    if (null != claim.claimtype.contact) {
                        // link the mercure assistance
                        mercurno = claim.claimtype.contact.values.get(staticlinkforpartner);
                        if (null != mercurno) {
                            stepCreate.addVgerProcessObj(
                                    getStepChange(staticlinkforpartner + "[" + linkObjClaimTypePkStaticField + "=='"
                                            + linkedobjclaimtypepkstaticvalue + "']", mercurno));
                            claim.claimtype.contact.values.remove(staticlinkforpartner);
                        }
                    }
                    // values for the claim
                    stepCreate.addVgerProcessObj(getStepChanges(tableValues, null, null, 0));
                    tableValues = claim.claimtype.values;
                    setTriggers(tableValues, stepCreate, null, null, 1);
                    // add values for claimtype
                    stepCreate.addVgerProcessObj(getStepChanges(tableValues, linkedobjclaimtypepkstaticvalue,
                            linkObjClaimTypePkStaticField, 1));
                    // if more than one contact with this pk link the partner to the one created above
                    boolean goOn = true;
                    vrgnr = claim.pk;
                    if (null != claim.getClaimtype()) {
                        originalNumber = claim.getClaimtype().fkMercur;
                    }
                    while (goOn) {
                        // values for contact
                        if (null != claim.claimtype.contact) {
                            // link the data from the partner delivered
                            tableValues = claim.claimtype.contact.values;
                            // if partner exists get the contact id and link this one as well. Otherwise create a new
                            // partner.
                            contactId = getContactId(tableValues);
                            if (null != contactId) {// prepare the link
                                tableValues = newHashMap(1);
                                tableValues.put(staticlinkforpartner + "[" + linkObjClaimTypePkStaticField + "=='"
                                        + linkedobjclaimtypepkstaticvalue + "']", contactId);
                            }
                            // partner doesn't exist so create a new one for mercur if italy change the already created
                            // by the app
                            if (null != mercurno) {
                                // collect the partner values add them further below. They have to be sorted first the
                                // links than the creates.
                                if (null != contactId) {
                                    linkIdAndTable.put(contactId, tableValues);
                                    if (!createPartnerFirst && null == contactIdFallpartner) {
                                        contactIdFallpartner = contactId;
                                    }
                                } else {// create new partner
                                    setTriggers(tableValues, stepCreate, null, null, 4);
                                    createPartner.add(tableValues);
                                    if (null == contactIdFallpartner && !createPartnerFirst) {
                                        createPartnerFirst = true;
                                    }
                                }
                            } else {// changes for italy
                                if (null == contactId) {// Fallpartner gets set through the FP static value
                                    stepCreate.addVgerProcessObj(getStepChanges(tableValues,
                                            linkedobjclaimtypepkstaticvalue, linkObjClaimTypePkStaticField, 2));
                                } else {
                                    // with this change link not working
                                    stepCreate.addVgerProcessObj(
                                            getStepChange(staticlinkforpartner + "[" + linkObjClaimTypePkStaticField
                                                    + "=='" + linkedobjclaimtypepkstaticvalue + "']", contactId));
                                    // Link Fallpartner
                                    stepCreate
                                            .addVgerProcessObj(getStepChange(
                                                    fallpartnerLinkForPartner + "[" + linkObjClaimTypePkStaticField
                                                            + "=='" + linkedobjclaimtypepkstaticvalue + "']",
                                                    contactId));
                                }
                            }
                        }
                        // values for payment
                        if (null != claim && null != claim.claimtype.payment) {
                            setTriggers(tableValues, stepCreate, null, null, 3);
                            // link the data from the payment
                            tableValues = claim.claimtype.payment.values;
                            stepCreate.addVgerProcessObj(getStepChanges(tableValues, linkedobjclaimtypepkstaticvalue,
                                    linkObjClaimTypePkStaticField, 2));
                        }
                        // check if the next claim got the same claim number.
                        if (arrLSize > i + 1) {
                            claim = arrLClaim.get(i + 1);
                            if (claim.pk.equals(vrgnr)) {
                                claim.claimtype.contact.values.remove(staticlinkforpartner);
                                i++;
                            } else {
                                goOn = false;
                            } // got new claim
                        } else {
                            claim = null;
                            break;
                        }
                    } // end while

                    // create the steps acording to the entries and in the corresponding order first the links than the
                    // create.
                    int sizePartners = 0;
                    for (final String key : linkIdAndTable.keySet()) {
                        stepCreate.addVgerProcessObj(createNewObject(staticlinkforpartner, linkIdAndTable.get(key), 0));
                    }
                    linkIdAndTable.clear();
                    sizePartners = createPartner.size();
                    for (int j = 0; j < sizePartners; j++) {
                        // this is for Fallpartner to get the link contactId just for the first one
                        if (!createPartnerFirst) {
                            createPartner.get(j).remove(contactFlagLinkForPartner);
                        } else {
                            createPartnerFirst = false;
                        }
                        stepCreate.addVgerProcessObj(createNewObject(boPartner, createPartner.get(j), 0));
                    }
                    createPartner.clear();
                    // this is for Fallpartner to get the link contactId
                    if (null != contactIdFallpartner) {
                        stepCreate.addVgerProcessObj(
                                getStepChange(fallpartnerLinkForPartner + "[" + linkObjClaimTypePkStaticField + "=='"
                                        + linkedobjclaimtypepkstaticvalue + "']", contactIdFallpartner));
                        contactIdFallpartner = null;
                    }
                }
                // just for now testing than add again
                // logger.debug("Start:" + separator + XMLBuilder.createNewXML(task));
                logger.info("Inserting number: " + (i + 1) + " -with claim number: " + vrgnr);
                sendTask(task, businessObjectName, null, arrLClaim, start, i, vrgnr, originalNumber);
            } catch (final Exception ex) {// catch the exception and place them into the file.
                final StringBuilder strBuff = new StringBuilder();
                if (null != claim) {
                    strBuff.append(claim.toString());
                    if (null != claim.claimtype) {
                        strBuff.append(claim.claimtype.toString());
                        if (null != claim.claimtype.payment) {
                            strBuff.append(claim.claimtype.payment.toString());
                        }
                    }
                }
                appendError(strBuff.toString(), ex.getMessage(), claim.pk);
                // for the result file
                appendResultFile(RESULT_FILE_ERROR, claim.pk, claim.getClaimtype().fkMercur);
                logger.debug("Error claim number:" + claim.pk);
            }

        }
        // write errors to a file
        if (null != strBuffErrors) {
            writeToFile(getStringFromProp(PATHERRORFILE), getFileName(job), strBuffErrors.toString(), true);
        }
        // log some info about the state of the imports.
        logger.info("-- job " + job + " total amount: " + arrLSize);
        logger.info("-- job " + job + " failed: " + errorCounter);

        logger.info("-- check for processed--");

        final long processed = Counters.INSTANCE.getCurrentValue(Constants.COUNTER_KEY_PROCESSED);
        final long italy = Counters.INSTANCE.getCurrentValue(Constants.COUNTER_KEY_ITALY);

        if (italy > 0 && italy != processed) {
            logger.error("--read " + italy + " from italy file and processed " + processed);
            throw new RuntimeException("check processed failed");

        }

        errorCounter = 0;
    }

    /**
     * The mercur claimtype (Abschlussmeldung). Arr with all tasks comes in. Some tasks might be for the same claim no.
     * Find out if there exists the same claimtype and if there is a payment already done. If yes just add this payment.
     * If not create a new claimtype and add the payment. If there are more payments for one claim with the same
     * claimtype put them together.
     *
     * @param arrLClaimtype ArrayList<Claimtype>
     * @exception Exception
     */
    private void insertClaimtype(final List<Claimtype> arrLClaimtype) {
        // the static fields from the properties file
        String vrgnr = null, originalNumber = null;
        final String boZahlung = getStringFromProp(BOZAHLUNG);
        final int arrLSize = arrLClaimtype.size();
        int claimtypeNo = 0, start = 0;
        List<String> changed = null;
        final Date dateObject = getDateActiveFrom();
        Claimtype claimtype = null;
        Map<String, String> tableValues = null;
        TaskProceed task = null;
        StepSearch stepSearch = null;
        // handles the task of getting the info from the db. Need the initClaimType to start with.
        if (null == importDB) {
            importDB = new ImportDB();
            importDB.initClaimType();
        }
        boolean sameClaim = true;
        outer: for (int i = 0; i < arrLSize; i++) {
            try {
                start = i;
                claimtype = arrLClaimtype.get(i);
                tableValues = claimtype.values;
                // create the steps
                task = new TaskProceed(getStringFromProp(BOSCHADENSVORGANG), null, dateObject);
                stepSearch = new StepSearch();
                stepSearch.setBusinessObjectName(getStringFromProp(BOSCHADENSVORGANG));
                final StepLoad load = new StepLoad();
                load.setLazyLoadingEnabled(false);
                // TODO: gj: test to stop from writig the data
                if (testMode) {
                    task.setTestError("SAVE");
                }

                // bind them together
                task.addVgerProcessObj(stepSearch);
                stepSearch.addVgerProcessObj(load);
                // The object we search for
                stepSearch.setSelectionExpression(claimtype.fk);
                sameClaim = true;

                while (sameClaim) {
                    if (null != tableValues) {

                        tableValues.remove(getStringFromProp(LINKOBJCLAIMTYPEFKMERCUR));
                        // get the value for Schadenfalltyp

                        schadenfalltyp = tableValues.get(linkedobjclaimtypesearchstaticfield).trim();
                        // since the first entry of a claimtype might be overwritten if no payment has been done yet
                        // check if possible.
                        // try to find out if there is a entry in the db with this claimnumber and this claimtype if so
                        // the claimtypeNo is returned

                        claimtypeNo = importDB.getClaimtypeNo(claimtype.fk, schadenfalltyp);

                        originalNumber = claimtype.fkMercur;
                        // if status of one payment is payed just add the payment(s).

                        if (!importDB.isStatusPayed(claimtype.fk, claimtypeNo)) {
                            // set the triggers after we have the claimtype.
                            setTriggers(tableValues, null, stepSearch, null, 1);

                            if (claimtypeNo > 0) {// update existing claimtype
                                // remove fk from the table since the value is already in.
                                tableValues.remove(getStringFromProp(LINKOBJCLAIMTYPEFK));
                                stepSearch.addVgerProcessObj(getStepChanges(tableValues, schadenfalltyp,
                                        linkedobjclaimtypesearchstaticfield, 2));
                            } else {// get the claimtype (if present) and the amount for the first payment.

                                final String[] claimtypeP = importDB.getClaimtypeAndPaymentAmount(claimtype.fk);
                                // this if block is just error handling in case a claim number is not in the db
                                // put it to error file saves us to send it first to server
                                if (null == claimtypeP) {
                                    appendError(claimtype.toString() + claimtype.payment.toString(),
                                            getStringFromProp(ERROR_MSG_NO_CLAIM_NUMBER) + claimtype.fk, claimtype.fk);
                                    // for the result file
                                    appendResultFile(RESULT_FILE_ERROR, claimtype.fk, claimtype.fkMercur);

                                    if (i + 2 > arrLSize) {
                                        break outer;
                                    }

                                    vrgnr = claimtype.fk;
                                    claimtype = arrLClaimtype.get(i + 1);

                                    if (null != claimtype && claimtype.fk.equals(vrgnr)) {
                                        tableValues = claimtype.values;
                                        sameClaim = true;
                                        i++;
                                        continue;
                                    } else {
                                        sameClaim = false;
                                        continue outer;
                                    }
                                }
                                final double amount = null == claimtypeP[1] ? 0
                                        : Double.valueOf(claimtypeP[1]);
                                // if there is a payment in this batch with the same claimtype like the one in the db
                                // create a new claimtype
                                if (hasPaymentWithThisClaimtype(arrLClaimtype, arrLSize, claimtypeP[0], claimtype.fk)) {
                                    stepSearch.addVgerProcessObj(createNewObject(
                                            getStringFromProp("linkedobjclaimtypedisplayedname"), tableValues, 1));
                                } else {// can we update value in the db? If amount is 0 and it has not been changed yet
                                        // do a update.
                                    if (0 == amount && null == changed
                                            || null != changed && changed.indexOf(claimtype.fk) < 0) {
                                        stepSearch.addVgerProcessObj(getStepChanges(tableValues,
                                                linkedobjclaimtypepkstaticvalue, linkObjClaimTypePkStaticField, 2));
                                        // memorize it to change it only once
                                        if (null == changed) {
                                            changed = newArrayList(2);
                                        }
                                        changed.add(claimtype.fk);
                                    } else {// it either has been changed or the payment is > 0 (there was a update in
                                            // the past)
                                        stepSearch.addVgerProcessObj(createNewObject(
                                                getStringFromProp("linkedobjclaimtypedisplayedname"), tableValues, 1));
                                    }
                                }
                            }
                        } // end if statusPayed
                    } // end if
                      // changes for payment. Since one set of data might be just another payment for the same
                      // claimtype check here to add them as new payments and not claimtypes.
                    vrgnr = claimtype.fk;

                    boolean goOn = true, newPayment = false;
                    while (goOn) {
                        // Update on claimttype, even if there is alreada a payed (AB) payment
                        stepSearch.addVgerProcessObj(getStepChanges(claimtype.values, schadenfalltyp,
                                linkedobjclaimtypesearchstaticfield, 2));

                        // new form first one gets inserted in the existing if nothing is in yet. The next in a array as
                        // new payments.
                        if (!newPayment && !importDB.paymentExists(vrgnr, claimtypeNo)) {
                            stepSearch.addVgerProcessObj(getStepChanges(claimtype.payment.values, schadenfalltyp,
                                    linkedobjclaimtypesearchstaticfield, 2));
                            // after the first one have to start array to insert new payments.
                        } else {
                            stepSearch.addVgerProcessObj(
                                    createNewObject(boZahlung + "[" + linkedobjclaimtypesearchstaticfield + "=='"
                                            + schadenfalltyp + "']", claimtype.payment.values, 2));
                        }
                        // check here the next data set if a new claimtype or just a new payment.
                        if (arrLSize > i + 1) {
                            claimtype = arrLClaimtype.get(i + 1);
                            if (claimtype.fk.equals(vrgnr) && claimtype.claimtype.equals(schadenfalltyp)) {
                                newPayment = true;
                                i++;
                            } else {
                                goOn = false;
                                // got new claimtype
                            }
                        } else {
                            claimtype = null;
                            break;
                        }
                    }
                    // if it's the same claim create new claimtypes
                    if (null != claimtype && claimtype.fk.equals(vrgnr)) {
                        tableValues = claimtype.values;
                        sameClaim = true;
                        i++;
                    } else {
                        sameClaim = false;
                    }
                }
                // TODO: gj: this logger has to come out in sendTask is already one just for testing
                // logger.debug("Start:" + separator + XMLBuilder.createNewXML(task));
                // send the task
                logger.info("Inserting number: " + (i + 1) + " -with claim number: " + vrgnr);
                sendTask(task, getStringFromProp(BOSCHADENSVORGANG), arrLClaimtype, null, start, i, vrgnr,
                        originalNumber);

            } catch (final Exception ex) {// catch the exception and place them into the file.
                appendError(claimtype.toString() + claimtype.payment.toString(), ex.getMessage(), claimtype.fk);
                ex.printStackTrace();
                // for the result file
                appendResultFile(RESULT_FILE_ERROR, vrgnr, originalNumber);
                logger.debug("Error claim number:" + vrgnr + "Exception: " + ex);
            }
        }
        // write errors to a file
        if (null != strBuffErrors) {
            writeToFile(getStringFromProp(PATHERRORFILE), getFileName(job), strBuffErrors.toString(), true);
        }
        // give some info to the logger
        logger.info("++++++ job " + job + " done. Total amount: " + arrLSize + " failed: " + errorCounter + " ++++++");
        errorCounter = 0;
    }

    /**
     * Find out if there is a entry in all the payments to be processed with this fk (claimnumber) and the same
     * claimtype.
     *
     * @param arrLClaimtype ArrayList<Claimtype>
     * @param arrLSize int
     * @param strClaimtype String
     * @param fk String
     * @return boolean
     */
    private boolean hasPaymentWithThisClaimtype(
            final List<Claimtype> arrLClaimtype,
            final int arrLSize,
            final String strClaimtype,
            final String fk) {
        Claimtype claimtype = null;
        for (int i = 0; i < arrLSize; i++) {
            claimtype = arrLClaimtype.get(i);
            if (claimtype.fk.equals(fk) && claimtype.claimtype.equals(strClaimtype)) {
                return true;
            }
        }
        return false;
    }

    /**
     * This is used to set up the StepChanges. The strPk is used as the field for the primary key the key is the primary
     * key used for the search.
     *
     * @param tableValues HashMap<String, String>
     * @param key String
     * @param strPk String
     * @return StepChanges
     */
    private StepChanges getStepChanges(
            final Map<String, String> tableValues,
            final String key,
            final String strPk,
            final int type) throws Exception {
        String value = null;
        final StepChanges stepChanges = new StepChanges();
        final String mapC = getStringFromProp(MAPCOUNTRY);
        // don't do it twice change claimtype
        tableValues.remove(strPk);
        for (final String item : tableValues.keySet()) {
            value = tableValues.get(item);

            if (null == value) {
                continue;
            }

            value = value.trim();

            if ("".equals(value)) {
                continue;
            }

            if (1 == type) {
                try {
                    // change format for date
                    if (null != dateFormat && getStringFromProp(FIELDDATEFORMATENTRYDATEAPP).equals(item)) {
                        value = new SimpleDateFormat(getStringFromProp(DATEFORMATENTRYDATEAPP)).format(df.parse(value));
                    }
                } catch (final Exception nfex) {
                    logger.debug("Can't format date: " + value);
                    throw nfex;
                }
            }
            if (0 == type) {
                stepChanges.addVgerProcessObj(getStepChange(item, value));
            } else {// Schadenland change Schadensvorgang.Land
                    // XXX nicht gut aber nur für ein Mapping vorgesehen
                    // if(mapC.equals(item) || ("Schadensvorgang.Schadenfall.Schadenland".equals(item)
                    // &&!"de.erv.vgercls.importtool.ItalyClaim".equalsIgnoreCase(className))){
                if (mapC.equals(item)) {
                    value = country.get(null != value ? value.toLowerCase()
                            : "");
                }

                final String mappingItem = ObjectStorage.INSTANCE
                        .getProperty(Constants.STORAGE_COUNTRY_MAPPING_RETROFITTED, value, null, String.class);

                if ("Schadensvorgang.Schadenfall.Schadenland".equals(item)
                        && "de.erv.vgercls.importtool.MercurClaimFirst".equalsIgnoreCase(className)
                        && mappingItem != null && mappingItem.length() > 0) {
                    logger.info("!!!Sonderlocke!!! Ummappen auf " + mappingItem);
                    value = mappingItem;
                }

                stepChanges.addVgerProcessObj(getStepChange(item + "[" + strPk + "=='" + key + "']", value));
            }
        }
        return stepChanges;
    }

    /**
     * @param selectionExpression String
     * @param tableValues HashMap<String, String>
     * @return StepNewArrayElement
     */
    private StepNewArrayElement createNewObject(
            final String selectionExpression,
            final Map<String, String> tableValues,
            final int type) {
        // do payment
        final StepNewArrayElement stepNewArrayElement = new StepNewArrayElement();
        if (null != tableValues) {
            if (1 == type) {// claimtype
                setTriggers(tableValues, null, null, stepNewArrayElement, 2);
            } else if (2 == type) {
                setTriggers(tableValues, null, null, stepNewArrayElement, 3);
            }
            stepNewArrayElement.setSelectionExpression(selectionExpression);
            StepChange change = null;
            String value = null;
            for (final String item : tableValues.keySet()) {
                value = tableValues.get(item);
                if (null == value) {
                    continue;
                }
                value = value.trim();
                if ("".equals(value)) {
                    continue;
                }
                change = new StepChange(item, value);
                stepNewArrayElement.addVgerProcessObj(change);
            }
        }
        return stepNewArrayElement;
    }

    /**
     * Creates a StepChange
     *
     * @param selExpression String
     * @param value String
     * @return StepChange
     */
    private StepChange getStepChange(final String selExpression, final String value) {
        final StepChange change = new StepChange();
        change.setSelectionExpression(selExpression);
        change.setValue(value);
        return change;
    }

    /**
     * To be able to write the values to some fields i have to set the migration value Import. This shows in the xml
     * like Schadensvorgang.Migrationskennzeichen value Import. Trigger can be as well a value like Schadenfall.
     *
     * @param tableValues HashMap<String, String>
     * @param stepCreate StepCreate
     * @param stepSearch StepSearch
     */
    private void setTriggers(
            final Map<String, String> tableValues,
            final StepCreate stepCreate,
            final StepSearch stepSearch,
            final StepNewArrayElement stepNewArrayElement,
            final int selection) {
        // set the values to get the data through the permission barrier the triggers.
        final Iterator<String> iter = 0 == selection ? triggerClaim.iterator()
                : 1 == selection ? triggerClaimtype.iterator()
                        : 2 == selection ? triggerClaimtype.iterator()
                                : 4 == selection ? triggerContact.iterator()
                                        : triggerPayment.iterator();
        String trig = null, res = null;
        while (iter.hasNext()) {
            trig = iter.next();
            res = tableValues.get(trig);
            tableValues.remove(trig);
            if (0 == selection) {
                if (null == res) {
                    res = triggerClaimStaticValues.get(trig);
                }
                stepCreate.addVgerProcessObj(new StepChange(trig, res));
            } else if (1 == selection) {
                if (null == res) {
                    res = triggerClaimtypeStaticValues.get(trig);
                }
                if (null != stepCreate) {
                    stepCreate.addVgerProcessObj(new StepChange(
                            trig + "[" + linkObjClaimTypePkStaticField + "=='" + linkedobjclaimtypepkstaticvalue + "']",
                            res));
                } else {
                    stepSearch.addVgerProcessObj(new StepChange(
                            trig + "[" + linkedobjclaimtypesearchstaticfield + "=='" + schadenfalltyp + "']", res));
                }
            } else if (2 == selection) {
                if (null == res) {
                    res = triggerClaimtypeStaticValues.get(trig);
                }
                stepNewArrayElement.addVgerProcessObj(new StepChange(trig, res));
            } else if (3 == selection) {

                if (null == res) {
                    res = triggerPaymentStaticValues.get(trig);
                }

                if (null != stepNewArrayElement) {
                    stepNewArrayElement.addVgerProcessObj(new StepChange(trig, res));
                } else {
                    stepCreate.addVgerProcessObj(new StepChange(trig, res));
                }
            } else if (4 == selection) {
                if (null == res) {
                    res = triggerContactStaticValues.get(trig);
                }
                if (null != stepCreate) {
                    stepCreate.addVgerProcessObj(new StepChange(trig, res));
                }
            }
        }
    }

    /**
     * Sends the xml task to be processed.
     *
     * @param task TaskProceed
     * @param businessObjectName String
     * @param errorString String
     * @throws VgerBusinessException see VgerBusinessException
     */
    private void sendTask(
            final TaskProceed task,
            final String businessObjectName,
            final List<Claimtype> arrLClaimtype,
            final List<Claim> arrLClaim,
            final int start,
            final int stop,
            final String primaryKey,
            final String originalKey) {
        try {
            logger.debug("Start:" + separator + XMLBuilder.createNewXML(task));
            if (null == vger) {
                connectToServer();
            }
            Counters.INSTANCE.add(Constants.COUNTER_KEY_PROCESSED);
            final VgerResponseTask result = vger.proceed(task);
            // write the claim number to a file if it succeded
            appendResultFile(RESULT_FILE_SUCCESS, primaryKey, originalKey);
            logger.debug("Result:" + result.getVgerProcessTask());

        } catch (final VgerBusinessException e) {
            // write the claim number to a file if it fails
            // TODO: gj: this has to stay in only the onesfurther below have to be taken out
            // appendResultFile(RESULT_FILE_ERROR,primaryKey);
            final Throwable cause = e.getCause();
            // build here the msg for the error file
            String res = null;
            final StringBuilder strBuff = new StringBuilder(), strBuffAll = new StringBuilder();
            for (int i = start; i <= stop; i++) {
                if (null != arrLClaimtype) {
                    final Claimtype claimType = arrLClaimtype.get(i);
                    res = claimType.toString() + claimType.payment.toString();
                    logger.info("Failed inserting claim number: " + arrLClaimtype.get(i).fk);
                }
                if (null != arrLClaim) {
                    final Claim claim = arrLClaim.get(i);
                    strBuff.append(claim.toString()).append(" ");
                    if (null != claim.claimtype) {
                        strBuff.append(claim.claimtype.toString()).append(" ");
                        strBuff.append(null != claim.claimtype.contact ? claim.claimtype.contact.toString()
                                : "").append(" ");
                        strBuff.append(null != claim.claimtype.payment ? claim.claimtype.payment.toString()
                                : "").append(" ");
                    }
                    res = strBuff.toString();
                    logger.info("Failed inserting claim number: " + claim.pk);
                }
                strBuffAll.append(null != cause ? cause.getMessage()
                        : "not known")
                        .append(" Msg: ")
                        .append(null != cause ? cause.getLocalizedMessage()
                                : "not known")
                        .append(separator);
                appendError(res, strBuffAll.toString(), primaryKey);
                strBuffAll.setLength(0);
                strBuff.setLength(0);
            }
            if (cause instanceof VgerBusinessException
                    && ((VgerBusinessException) cause).getErrorCode() == ErrorCodes.TEST_BUSINESS_EXCEPTION_SAVE) {
                logger.debug("Test save Businessobject " + businessObjectName
                        + " wird angelegt wenn nicht in test save mode!");
                appendResultFile(RESULT_FILE_TEST_SAVE, primaryKey, originalKey);
            } else {
                logger.debug("Error inserting " + businessObjectName + " " + e);
                appendResultFile(RESULT_FILE_ERROR, primaryKey, originalKey);
            }
        }
        logger.debug("\n");
    }

    /**
     * Get the connection to V'ger.
     */
    private void connectToServer() {
        try {
            logger.info("connect to server");
            final String mandant = getStringFromProp(MANDANT);
            connect();
            final DesEncrypter dec = new DesEncrypter(Constants.AES_KEY);
            login(mandant, getStringFromProp(BENUTZER), dec.decrypt(getStringFromProp(PASSWORT)));
            logger.info("connected to server");
        } catch (final Exception e) {
            logger.error("error ImportData " + e.getMessage(), e);
            /******************************************************************************
             * this has to be closed to make sure all jdbc things are closed properly. *
             ******************************************************************************/
            if (null != importDB) {
                importDB.closeTheLot();
            }
            disconnect();
        }
    }

    /**
     * Appends the errors to the strBuffErrors that holds all the errors.
     *
     * @param res String
     * @param cause String
     */
    private void appendError(final String res, final String cause, final String primaryKey) {
        if (null == strBuffErrors) {
            strBuffErrors = new StringBuilder();
        }
        strBuffErrors.append(separator)
                .append(++errorCounter)
                .append(".) ")
                .append(getStringFromProp(ERRORSTART))
                .append(" ")
                .append(new SimpleDateFormat(getStringFromProp(ERRORDATEFORMAT)).format(new java.util.Date()))
                .append(": ")
                .append(res)
                .append(separator)
                .append("    Error Msg: ")
                .append(cause);
        // place the data into error file
        createErrorFileOriginalData(primaryKey);

        if (!gotError) {
            gotError = true;
        }
    }

    /**
     * Picks with the primary key the line that represents the data from the arrList.
     *
     * @param primaryKey String
     */
    private void createErrorFileOriginalData(final String primaryKey) {
        if (null == arrListOriginalData) {
            return;
        }
        if (null == strBuffErrorFile) {
            strBuffErrorFile = new StringBuilder();
        }
        String res = null;
        final int size = arrListOriginalData.size();
        for (int i = 0; i < size; i++) {
            res = arrListOriginalData.get(i).get(primaryKey);
            if (null != res) {
                strBuffErrorFile.append(res).append(separator);
                arrListOriginalData.remove(i);
                break;
            }
        }
    }

    /**
     * Builds with the values wanted as search string a hash map. Uses the select from property file and this hash map
     * to get from the ImportDB the contact id. Returns the contact id if only one contact with this search string is
     * found. Null if more than one or none are found.
     *
     * @param tableValues HashMap<String, String>
     * @return String
     */
    private String getContactId(final Map<String, String> tableValues) throws SQLException {
        String value = null, valueContact = null;
        final Map<String, String> searchStrings = newHashMap();

        for (final String key : contactSearch.keySet()) {
            value = contactSearch.get(key);
            valueContact = tableValues.get(key);
            searchStrings.put(value, valueContact);
        }
        return importDB.getContactNo(searchStrings);
    }

    /**
     * Get the String from the property file.
     *
     * @param search String
     * @return String
     */
    private String getStringFromProp(final String _search) {

        return ObjectStorage.INSTANCE.getProperty(Constants.STORAGE_IMPORTCLIENT, _search, null, String.class);

    }

    /**
     * Returns a Hashmap with the values for this table with tableName. Removes than the table from the arraylist.
     * Checks as well if there are still tables left if not set the goOn flag to false.
     *
     * @param tableName String name for the table i need.
     * @param data ArrayList<HashMap<String, ArrayList<HashMap<String, String>>>>
     * @return HashMap<String, String>
     */
    private Map<String, String> getTable(
            final String tableName,
            final List<Map<String, List<Map<String, String>>>> data) {

        final int sizeArrL = data.size();

        for (int j = 0; j < sizeArrL; j++) {

            final Map<String, List<Map<String, String>>> tables = data.get(j);

            if (tables.containsKey(tableName)) {

                if (tables.get(tableName).isEmpty()) {
                    if (++empty == sizeArrL) {
                        goOn = false;
                    }
                    return null;
                }

                final Map<String, String> table = tables.get(tableName).get(0);
                tables.get(tableName).remove(table);

                if (tables.get(tableName).isEmpty()) {
                    empty++;
                }

                if (empty == sizeArrL) {
                    goOn = false;
                }
                return table;
            }
        }
        return null;
    }

    /**
     * returns for the object the search string
     *
     * @param obj what object is to be scanned
     * @param values HashMap
     * @return String
     */
    private String getSearchString(final String obj, final Map<String, String> values) {
        return null == values.get(obj) ? null
                : values.get(obj).trim();
    }

    /**
     * The names from the importclient.properties have to match the names from the other property files. Like
     * italien.properties or mercur.properties. If not log a error.
     *
     * @param claim
     * @param claimtype
     * @param payment
     * @param data
     */
    private void handleTableNameError(
            final String claim,
            final String claimtype,
            final String payment,
            final List<Map<String, List<Map<String, String>>>> data) {
        final int sizeArrL = data.size();
        final StringBuilder strBuff = new StringBuilder();
        for (int j = 0; j < sizeArrL; j++) {

            final Iterator<String> iter = data.get(j).keySet().iterator();
            while (iter.hasNext()) {
                strBuff.append(iter.next()).append(", ");
            }
        }
        logger.debug("Table names or amount don't match! " + claim + ", " + claimtype + ", " + payment + " with "
                + strBuff.toString());
    }

    /**
     * Returns a date for the active from. The date is in the properties file.
     *
     * @return Date
     */
    private Date getDateActiveFrom() {
        final DateFormat df = new SimpleDateFormat(dateFormatDateActiveFrom);
        Date dateObject = null;
        try {
            dateObject = df.parse(getStringFromProp(DATEACTIVEFROM));
        } catch (final Exception exception) {
            logger.error("Formatting: " + exception);
        }
        return dateObject;
    }

    /**
     * Sets the triggers from the properties file into Hashmaps.
     *
     * @param triggers ArrayList<String>
     * @param staticValuesHm HashMap<String, String>
     * @param searchStr String
     */
    private void setTriggerValues(
            final List<String> triggers,
            Map<String, String> staticValuesHm,
            final String searchStr) {
        String res = "";
        String staticValue = null;
        int i = 0;
        while (null != res) {
            res = getStringFromProp(searchStr + Integer.valueOf(i).toString());
            if (null == res) {
                break;
            }

            triggers.add(res);
            staticValue = getStringFromProp(searchStr + Integer.valueOf(i++).toString() + STATIC);
            if (null != staticValue) {
                if (null == staticValuesHm) {
                    staticValuesHm = newHashMap();
                }
                staticValuesHm.put(res, staticValue);
            }
        }

    }

    /**
     * Take the values from the property file for the search to find if a contact with certain criteria fits and place
     * them into a hashmap.
     */
    private void setContactSearchValues() {
        String res = "", column = "";
        int i = 0;

        if (null == contactSearch) {
            contactSearch = newHashMap();
        }

        while (null != res) {
            res = getStringFromProp(CONTACTSEARCH + Integer.valueOf(i).toString());
            column = getStringFromProp(CONTACTCOLUMN + Integer.valueOf(i++).toString());
            if (null == res) {
                break;
            }
            contactSearch.put(res, column);
        }
    }

    /**
     * In case of a error write the according data to two files. One is containing the original data the other one the
     * messages from the errors.
     *
     * @param error String. The error message.
     */
    private void writeToFile(
            final String filePath,
            final String fileName,
            final String fileContent,
            final boolean writeOriginalData) {

        PrintWriter file = null;
        try {

            final String pathSuffix = getDirectoryPrefix();

            file = new PrintWriter(new FileOutputStream(filePath + pathSuffix + fileName, true));
            file.println(fileContent);
            file.close();
        } catch (final IOException e) {
            if (null != file) {
                file.close();
            }
            // logger
            logger.info("++++++ could not write to file path " + filePath + " file name: " + fileName);
        } // end catch
        if (writeOriginalData && null != strBuffErrorFile && !"".equals(strBuffErrorFile.toString())) {
            writeToFile(getStringFromProp(PATH_ERRORFILE_ORIGINAL_DATA),
                    getFileName(job + getStringFromProp(EXTENSION_ORIG_DATA_FILE_NAME)), strBuffErrorFile.toString(),
                    false);
        }
    }

    /**
     * The data gets appended to the file not a new one created every time. Changes only if the date is changing so next
     * day.
     *
     * @param successError String
     * @param primaryKey String
     */
    private void appendResultFile(final String successError, final String primaryKey, final String originalKey) {
        writeToFile(getStringFromProp(PATH_RESULT_FILE),
                getStringFormatedDate(getStringFromProp(DATE_FORMAT_RESULT_FILE)) + " " + job + " "
                        + getStringFromProp(NAME_RESULT_FILE),
                getStringFromProp(successError) + primaryKey + " " + getStringFromProp(RESULT_FILE_ERROR_MERC_IT_NO)
                        + " " + originalKey + " "
                        + getStringFormatedDate(getStringFromProp(DATE_FORMAT_RESULT_FILE_CONTEND)),
                false);
    }

    /**
     * Builds a string for a file name. Starts with the date certain format adds the name and ends with txt.
     *
     * @param name String
     * @return String
     */
    private String getFileName(final String name) {
        return getStringFormatedDate(getStringFromProp(ERRORDATEFORMAT)) + " " + name + ".txt";
    }

    private String getStringFormatedDate(final String format) {
        return new SimpleDateFormat(format).format(new java.util.Date());
    }

    /**
     * Sorts the claimtypes.
     *
     * @author gusiewert
     */
    class StrComparator implements Comparator<Claimtype> {
        private final RuleBasedCollator collator = (RuleBasedCollator) RuleBasedCollator.getInstance(Locale.GERMANY);

        @Override
        public int compare(final Claimtype o1, final Claimtype o2) {
            return collator.compare(o1.getFkAndClaimtype(), o2.getFkAndClaimtype());
        }
    }

    /**
     * Sorts the claims
     *
     * @author gusiewert
     */
    class StrComparatorClaim implements Comparator<Claim> {
        private final RuleBasedCollator collator = (RuleBasedCollator) RuleBasedCollator.getInstance(Locale.GERMANY);

        @Override
        public int compare(final Claim o1, final Claim o2) {
            return collator.compare(o1.pk, o2.pk);
        }
    }

    class Claim {
        private Claimtype claimtype;
        private final Map<String, String> values;
        private String pk;

        public Claim(final String pk, final Map<String, String> values) {
            this.pk = pk;
            this.values = values;
        };

        public Map<String, String> getValues() {
            return values;
        }

        public String getPk() {
            return pk;
        }

        public void setPk(final String pk) {
            this.pk = pk;
        }

        public Claimtype getClaimtype() {
            return claimtype;
        }

        public void setClaimtype(final Claimtype claimtype) {
            this.claimtype = claimtype;
        };

        @Override
        public String toString() {
            final StringBuilder strBuff = new StringBuilder();
            for (final String item : values.keySet()) {
                strBuff.append(item).append("=").append(values.get(item)).append(" ");
            }
            return strBuff.toString();
        }
    }

    class Claimtype {
        private Payment payment;
        private Contact contact;
        private Map<String, String> values;
        private String fk;
        private String fkMercur;// this is just for the error file so far
        private String claimtype;

        public Claimtype() {
        }

        public Claimtype(final Map<String, String> values, final String pk, final String fk, final String fkMercur) {
            this.values = values;
            this.fk = fk;
            this.fkMercur = fkMercur;
            claimtype = values.get(getStringFromProp(LINKOBJCLAIMTYPESEARCHSTATICFIELD));
        }

        public Payment getPayment() {
            return payment;
        }

        public void setPayment(final Payment payment) {
            this.payment = payment;
        };

        public Map<String, String> getValues() {
            return values;
        };

        @Override
        public String toString() {
            final StringBuilder strBuff = new StringBuilder();
            strBuff.append(getStringFromProp(LINKOBJCLAIMTYPEFK)).append("=").append(fk).append(" ");
            strBuff.append(getStringFromProp(LINKOBJCLAIMTYPEFKMERCUR)).append("=").append(fkMercur).append(" ");
            for (final String item : values.keySet()) {
                strBuff.append(item).append("=").append(values.get(item)).append(" ");
            }
            return strBuff.toString();
        }

        public void setContact(final Contact contact) {
            this.contact = contact;
        }

        public String getFkAndClaimtype() {
            return fk + claimtype;
        }

    }

    class Payment {
        private final Map<String, String> values;

        public Payment(final Map<String, String> values) {
            this.values = values;
        }

        public Map<String, String> getValues() {
            return values;
        };

        @Override
        public String toString() {
            final StringBuilder strBuff = new StringBuilder();
            for (final String item : values.keySet()) {
                strBuff.append(item).append("=").append(values.get(item)).append(" ");
            }
            return strBuff.toString();
        }
    }

    class Contact {
        private final Map<String, String> values;

        public Contact(final Map<String, String> values) {
            this.values = values;
        }

        public Map<String, String> getValues() {
            return values;
        };

        @Override
        public String toString() {
            final StringBuilder strBuff = new StringBuilder();
            for (final String item : values.keySet()) {
                strBuff.append(item).append("=").append(values.get(item)).append(" ");
            }
            return strBuff.toString();
        }
    }

    /**
     * Start it here.
     *
     * @param args String[]
     */
    public static void main(final String[] args) {
        logger.info(new Date());
        ImportClient ic = new ImportClient();
        ic.getDBDetails();

    }

    public Connection getDBDetails() {
        Connection conn = null;
        if (importDB == null) {
            importDB = new ImportDB();
            conn = importDB.initConnection();
        }
        return conn;
    }

    private String getDirectoryPrefix() {
        return null != className ? getStringFromProp("directoryPrefix." + className)
                : "";
    }

}
