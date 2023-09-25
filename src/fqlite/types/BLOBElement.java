package fqlite.types;

public class BLOBElement {

	public byte[] binary;
	public BLOBTYPE type;
	
	public BLOBElement(byte[] content, BLOBTYPE type){
		this.binary = content;
		this.type = type;
	}
}

