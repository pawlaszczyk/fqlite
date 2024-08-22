package fqlite.analyzer;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.nio.ByteBuffer;

import fqlite.util.Auxiliary;

public class BinaryLoader {

	
	public static String parse2(String path){
		
		String result = "";
		try {
		
			BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(path));
		    ByteBuffer bf = ByteBuffer.wrap(buffer.readAllBytes());
		    bf.position(0);
		    result = Auxiliary.bytesToHex(bf);
		    buffer.close();
		
		}catch(Exception err){
			
			err.printStackTrace();
		}

		return result;
	}
	
	public static String parse(String path){
		
		String result = "";
		try {
		
			BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(path));
		    ByteBuffer bf = ByteBuffer.wrap(buffer.readAllBytes());
		    bf.position(0);
		    result = Auxiliary.bytesToHex(bf); //Auxiliary.hex2ASCII_v2(Auxiliary.bytesToHex(bf));
		    buffer.close();
		
		}catch(Exception err){
			
			System.out.println("ATTENTION! Wrong path to binary :: " + path);
			System.out.println("File does not exist");
			
		}

		return result;
	}
	
	public static String parseASCII(String path){
		
		String result = "";
		try {
		
			BufferedInputStream buffer = new BufferedInputStream(new FileInputStream(path));
		    ByteBuffer bf = ByteBuffer.wrap(buffer.readAllBytes());
		    bf.position(0);
		    result = Auxiliary.hex2ASCII_v2(Auxiliary.bytesToHex(bf));
		    buffer.close();
		
		}catch(Exception err){
			
			System.out.println("ATTENTION! Wrong path to binary :: " + path);
			System.out.println("File does not exist");
			
		}

		return result;
	}
}
