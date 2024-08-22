package fqlite.analyzer.pblist;

import java.io.File;
import java.io.IOException;
import java.util.List;

import com.dd.plist.BinaryPropertyListParser;
import com.dd.plist.NSDictionary;

import fqlite.base.Job;
import nl.pvanassen.bplist.converter.ConvertToXml;
import nl.pvanassen.bplist.ext.nanoxml.XMLElement;
import nl.pvanassen.bplist.parser.BPListElement;
import nl.pvanassen.bplist.parser.ElementParser;

public class BPListParser {

	private static final ConvertToXml convetToXml = new ConvertToXml();
	private static final ElementParser elementParser = new ElementParser();

	public static String parse(Job job, String path) {

		 //File file = new File(path);
		 
		 try {
			//NSDictionary rootDict = (NSDictionary)BinaryPropertyListParser.parse(file);
			 NSDictionary rootDict = (NSDictionary)BinaryPropertyListParser.parse(job.bincache.read(path)); 
 			 return rootDict.toXMLPropertyList();
		 
		 } catch (IOException e) {
			e.printStackTrace();
		 } catch (Exception e) {
			
			System.out.println("Problem " + path);
			return "<no valid bplist> " + job.bincache.getHexString(path);
		 }
		 
		 
		
		return "<no valid bplist>";
	}

	
	
	
	public static String parseOld(String path) {

		List<BPListElement<?>> elements;

		try {

			elements = elementParser.parseObjectTable(new File(path));
			XMLElement xmlElement = convetToXml.convertToXml(elements);
			System.out.println(xmlElement);		
			return xmlElement.toString();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "<no valid bplist>";
	}

	//public static void main(String [] args){
		
         // String result = parse("/Users/pawlaszc/Desktop/test.bplist");
         // System.out.println("Done:: \n" + result);
	//}

}


