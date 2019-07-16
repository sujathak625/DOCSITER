package com.suja;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParserFactory;
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
import org.xml.sax.helpers.DefaultHandler;

public class ERVMilanXMLDataCheckAutomation {

    private static final String PROPERTY_CLAIMS_NUMBER = "CLAIMS_NUMBER";
    private static final String PROPERTY_MLC_AMOUNT = "MLC_AMOUNT";

    public static void main(String[] args) {

        String inputXmlFilePath = getFilePath();
        List<File> fileList = listFilesForFolder(inputXmlFilePath);

        List<File> xmlFileList = filterXmlFiles(fileList);
        checkXmlfileList(xmlFileList);
        for (File file : xmlFileList) {
            readXMLFileAnddisplayTags(file);
        }

    }

    private static String getFilePath() {
        String filePath = "C:/XMLFiles";
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

    static String getValue(String tag, Element element) {
        NodeList nodes = element.getElementsByTagName(tag).item(0).getChildNodes();
        Node node = (Node) nodes.item(0);
        return node.getNodeValue();
    }

    private static void visit(Node node, int level) {
        NodeList list = node.getChildNodes();
        String nodeName = new String();
        String nodeValue = new String();
        // System.out.println(list);
        for (int i = 0; i < list.getLength(); i++) {
            Node childNode = list.item(i);
            if (childNode.getNodeType() == Node.ELEMENT_NODE) {
                nodeName = childNode.getNodeName().toString();
                System.out.println(nodeName);
                visit(childNode, level + 1);
            }
        }
    }

    private static void readXMLFileAnddisplayTags(File inputXMLFile) {
        SAXParserFactory saxParserFactory = SAXParserFactory.newInstance();
        try {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            DocumentBuilder builder = factory.newDocumentBuilder();
            DefaultHandler handler = new DefaultHandler();
            System.out.println(inputXMLFile);
            InputStream inputStream = new FileInputStream(inputXMLFile);
            InputSource is = new InputSource(new InputStreamReader(inputStream, "UTF-8"));
            is.setEncoding("UTF-8");
            Document document = builder.parse(is);

            Element parentNode = document.getDocumentElement();
            // System.out.println(parentNode.getNodeName());

            // create another xml document with nodes having mlc amount > 0
            DocumentBuilderFactory documentFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder documentBuilder = documentFactory.newDocumentBuilder();
            Document document2 = documentBuilder.newDocument();
            Element rootname = document2.createElement(parentNode.getNodeName());
            document2.appendChild(rootname);

            NodeList children = parentNode.getChildNodes();
            int j = 0;

            System.out.println("\n");
            for (int i = 0; i < children.getLength(); i++) {

                Node child = children.item(i);
                if (child.getNodeType() != Node.ELEMENT_NODE) {
                    continue;
                }
                System.out.println("length " + children.getLength());

                NodeList grandChildren = child.getChildNodes();
                System.out.println("\nClaim item " + i);

                for (int x = 0; x < grandChildren.getLength(); x++) {
                    Node grandChild = grandChildren.item(x);
                    if (grandChild.getNodeType() != Node.ELEMENT_NODE) {
                        continue;
                    }

                    String key = grandChild.getNodeName();
                    String value = grandChild.getTextContent();
                    if (key.equals(PROPERTY_CLAIMS_NUMBER)) {
                        System.out.println("claims number " + value);
                    }
                    if (key.equals(PROPERTY_MLC_AMOUNT) && Float.parseFloat(value.trim()) > 0) {
                        System.out.println("mlc amount " + value);
                        // childNodeValuesMap.put(x, child);
                        NodeList list = document.getElementsByTagName(child.getNodeName());

                        Node element = children.item(i);

                        Node copiedNode = document2.importNode(element, true);
                        document2.getDocumentElement().appendChild(copiedNode);

                    } else {
                        continue;
                    }

                }

            }
            prettyPrint(document2);

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
    }

    private boolean checkFileSize(File fileName) {
        if (fileName.exists() && fileName.length() <= 0) {
            return false;
        }
        return true;
    }

    public static final Document prettyPrint(Document xml) throws Exception {
        Transformer tf = TransformerFactory.newInstance().newTransformer();
        tf.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        tf.setOutputProperty(OutputKeys.ENCODING, "UTF-8");
        tf.setOutputProperty(OutputKeys.INDENT, "yes");
        StreamResult streamResult = new StreamResult(new File("C:\\XMLFiles\\desitnationfile\\newFile.xml"));
        tf.transform(new DOMSource(xml), streamResult);
        return xml;
    }

}
