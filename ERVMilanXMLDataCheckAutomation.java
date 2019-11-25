package de.erv.vgercls.importtool;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;

public class ERVMilanXMLDataCheckAutomation {

    private static final String PROPERTY_CLAIMS_NUMBER = "CLAIMS_NUMBER";
    private static final String PROPERTY_MLC_AMOUNT = "MLC_AMOUNT";
    private static InputStreamReader characterStream;

    private static String getOutputFilePath = null;
    private static File fileWithMLCAmtGtZero = null;

    public static File getFileWithMLCAmtGtZero() {
        return fileWithMLCAmtGtZero;
    }

    public static void setFileWithMLCAmtGtZero(File aFileWithMLCAmtGtZero) {
        fileWithMLCAmtGtZero = aFileWithMLCAmtGtZero;
    }

    public static String getGetOutputFilePath() {
        return getOutputFilePath;
    }

    public static void setGetOutputFilePath(String aGetOutputFilePath) {
        getOutputFilePath = aGetOutputFilePath;
    }

    /** Class to handle the db issues */
    private static ImportDB importDB;
    private static Connection con;

    public static void main(String[] args) {

        String inputXmlFilePath = getFilePath();
        ImportClient ic = new ImportClient();
        if (importDB == null) {
            importDB = new ImportDB();
            con = ic.getDBDetails();
        }

        createOutoutDirectoryForToday(inputXmlFilePath);

        List<File> fileList = listFilesForFolder(inputXmlFilePath);
        List<File> xmlFileList = filterXmlFiles(fileList);
        checkXmlfileList(xmlFileList);
        for (File file : xmlFileList) {
            retrieveMLCAmtGtZero(file);
        }

    }

    private static String getDBDetails(String claimNo, String query) {
        String result = null;

        result = importDB.getClaimForItalyDataFixAutomation(claimNo, query, con);

        return result;

    }

    private static String getFilePath() {
        // String filePath = "C:/XMLFiles";
        String filePath = "C:\\CLSERVMILAN";
        return filePath;
    }

    private static List<File> listFilesForFolder(String filePath) {
        List<String> fileNameList = new ArrayList<String>();
        List<File> fileList = new ArrayList<File>();
        File folder = new File(filePath);
        for (File fileName : folder.listFiles()) {
            if (fileName.isDirectory()) {
                continue;
            }
            fileList.add(fileName);
            fileNameList.add(fileName.getName());

        }
        filterXmlFiles(fileList);
        return fileList;
    }

    private static List<File> filterXmlFiles(List<File> inputFileList) {
        List<File> xmlFileList = new ArrayList<File>();
        String fileExtension = null;
        for (File file : inputFileList) {
            String name = file.getName();
            int lastIndexOf = name.lastIndexOf(".");
            if (lastIndexOf == -1) {
                continue;
            }
            fileExtension = name.substring(lastIndexOf);
            // System.out.println(fileExtension);
            if (".xml".equals(fileExtension)) {
                xmlFileList.add(file);
            }

        }
        return xmlFileList;
    }

    private static void checkXmlfileList(List<File> xmlFileList) {
        for (File file : xmlFileList) {
            String name = file.getName();
        }
    }

    private static Document retrieveMLCAmtGtZero(File inputXMLFile) {
        Document finalDocument = null;

        Document claimsNotAlreadExistingDoc = null;

        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();

            System.out.println(inputXMLFile);
            InputStream inputStream = new FileInputStream(inputXMLFile);
            characterStream = new InputStreamReader(inputStream, "UTF-8");
            InputSource is = new InputSource(characterStream);
            is.setEncoding("UTF-8");

            Document inputDocument = builder.parse(is);
            // Parent node of input document
            Element parentNode = inputDocument.getDocumentElement();

            // Build query to check which claim numbers are already inserted before the job crash
            // Individual payments' select query
            String queryPart1 = "SELECT * FROM fl_claim claim, fl_payment payment WHERE claim.mercur_vorgangsnummer=";
            // All payments select
            String allPaymentsQuery =
                    "SELECT * FROM fl_claim claim, fl_payment payment WHERE mercur_vorgangsnummer in(";
            StringBuilder allPayments = new StringBuilder(allPaymentsQuery);

            // create another new xml document with nodes having mlc amount > 0
            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            finalDocument = documentBuilder.newDocument();
            // Parent node retrieved from input document and added to new document
            Element rootname = finalDocument.createElement(parentNode.getNodeName());
            finalDocument.appendChild(rootname);

            claimsNotAlreadExistingDoc = documentBuilder.newDocument();
            Element rootname1 = claimsNotAlreadExistingDoc.createElement(parentNode.getNodeName());
            claimsNotAlreadExistingDoc.appendChild(rootname1);

            // Child nodes retrieved from input document
            NodeList children = parentNode.getChildNodes();
            StringBuilder individualQuery = new StringBuilder();
            String claimNumber = null;

            System.out.println("\n");
            for (int i = 0; i < children.getLength(); i++) {
                // Individual child node got from children nodes from input document
                // These are nodes which are related to one claim detail
                Node child = children.item(i);
                if (child.getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }

                // All the sub nodes in child node are retrieved one by one. These nodes are details containing details
                // about a mercur claim number
                NodeList grandChildren = child.getChildNodes();
                // System.out.println("\nClaim item " + i);

                for (int x = 0; x < grandChildren.getLength(); x++) {
                    Node grandChild = grandChildren.item(x);
                    if (grandChild.getNodeType() != Node.ELEMENT_NODE) {
                        continue;
                    }

                    String key = grandChild.getNodeName();
                    String value = grandChild.getTextContent();
                    if (key.equals(PROPERTY_CLAIMS_NUMBER)) {
                        claimNumber = value;

                        // System.out.println("claims number " + value);

                    }
                    if (key.equals(PROPERTY_MLC_AMOUNT) && Float.parseFloat(value.trim()) > 0) {

                        // System.out.println("mlc amount " + value);
                        // NodeList list = document.getElementsByTagName(child.getNodeName());

                        // If the mlc amount value is > 0, then the entire child node is retrieved and copied to final
                        // document
                        Node element = children.item(i);

                        Node copiedNode = finalDocument.importNode(element, true);
                        Node copiedNode1 = claimsNotAlreadExistingDoc.importNode(element, true);
                        finalDocument.getDocumentElement().appendChild(copiedNode);

                        // qeury is built to check whether claim number is already inserted to the FL_PAYMENT and
                        // FL_CLAIM table.
                        // The query will be available in the console and should be copied to DB tool like TOAD Editor
                        // to check manually.
                        // individualQuery.append(queryPart1).append("'" + claimNumber + "' ").append(
                        // "AND claim.VORGANGSNUMMER = payment.ACTIVITY_NUMBER;");
                        individualQuery.append(queryPart1)
                                .append("?")
                                .append(" AND claim.VORGANGSNUMMER = payment.ACTIVITY_NUMBER");

                        String res = getDBDetails(claimNumber, individualQuery.toString());
                        if (res != null) {
                            System.out
                                    .println("Mercure Claim no " + claimNumber + " exists with vorgangsnummer " + res);
                        } else {
                            System.out.println("Mercure Claim no " + claimNumber + " does not exist");
                            claimsNotAlreadExistingDoc.getDocumentElement().appendChild(copiedNode1);
                        }
                        System.out.println(individualQuery.append(";").toString());
                        // The below builds query to include all claim numbers to check in one stretch.
                        allPayments.append("'").append(claimNumber).append("'").append(",");
                        claimNumber = null;
                        individualQuery.setLength(0);

                    } else {
                        continue;
                    }

                }

            }
            allPayments.append("'")
                    .append(claimNumber)
                    .append("'")
                    .append(") AND claim.VORGANGSNUMMER = payment.ACTIVITY_NUMBER;");
            System.out.println(allPayments.toString());
            prettyPrint(finalDocument, "\\mlcAmtGtZero.xml");
            prettyPrint(claimsNotAlreadExistingDoc, "\\data_interface.xml");

        } catch (ParserConfigurationException | SAXException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return finalDocument;
    }

    // private boolean checkFileSize(File fileName) {
    // if (fileName.exists() && fileName.length() <= 0) {
    // return false;
    // }
    // return true;
    // }

    public static Document prettyPrint(Document xml, String fileName) throws Exception {
        Transformer tf = TransformerFactory.newInstance().newTransformer();
        tf.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        tf.setOutputProperty(OutputKeys.INDENT, "yes");
        String absoluteFileNameNPath = getGetOutputFilePath();
        System.out.println(absoluteFileNameNPath + fileName);
        File mlcAmtGtZeroFile = new File(absoluteFileNameNPath + fileName);
        StreamResult streamResult = new StreamResult(mlcAmtGtZeroFile);
        setFileWithMLCAmtGtZero(mlcAmtGtZeroFile);
        tf.transform(new DOMSource(xml), streamResult);
        return xml;
    }

    private static LocalDate getCurrentDate() {
        LocalDate today = LocalDate.now();
        return today;
    }

    private static File createOutoutDirectoryForToday(String filePath) {
        String dirName = getCurrentDate().toString();

        File dir = new File(filePath + "\\fileOutput\\" + dirName);
        if (!dir.exists()) {
            dir.mkdir();
            System.out.println("Directory created for today");
        } else {
            System.out.println("Directory exists already for today");
        }

        setGetOutputFilePath(dir.getAbsolutePath());
        return dir;
    }

}
