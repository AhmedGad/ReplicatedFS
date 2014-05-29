package Impl;

public class FileContent {
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
