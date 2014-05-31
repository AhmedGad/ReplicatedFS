package Impl;

import java.io.Serializable;

public class FileContent implements Serializable {

	private static final long serialVersionUID = 8969701885826822440L;
	private String fileName;

	public String getFileName() {
		return fileName;
	}

	private StringBuilder data;

	public FileContent(String name) {
		fileName = name;
		data = new StringBuilder();
	}

	public void appendData(String dataChunk) {
		data.append(dataChunk);
	}

	public String getData() {
		return data.toString();
	}
}
