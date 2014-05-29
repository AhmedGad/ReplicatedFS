package Impl;

public class FileContent {
	private StringBuilder data;

	public FileContent() {
		data = new StringBuilder();
	}

	public void appendData(String dataChunk) {
		data.append(dataChunk);
	}

	public String getData() {
		return data.toString();
	}
}
