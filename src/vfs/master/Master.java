package vfs.master;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import vfs.struct.ChunkInfo;
import vfs.struct.FileNode;

public class Master {

	private FileNode root;

	private int nextFileHandleID = 0;
	
	private String serializedJSONFileName = "file_hierachy.json";

	public Master() {
		String s = null;
		try {
			BufferedReader br = new BufferedReader(new FileReader(serializedJSONFileName));
			StringBuilder sb = new StringBuilder();
			String line = br.readLine();
			while (line != null) {
				sb.append(line);
				sb.append(System.lineSeparator());
				line = br.readLine();
			}
			s = sb.toString();
			br.close();
		} catch (FileNotFoundException e) {
			root = new FileNode("vfs", true);
		} catch (IOException e) {
			e.printStackTrace();
		}
		try {
			root = new FileNode();
			root.parseJSON(new JSONObject(s), null);
		} catch (JSONException e) {
			root = new FileNode("vfs", true);
		}
	}
	
	

	public JSONObject open(String path, String mode) {
		String[] names = path.split("[/\\\\]");
		FileNode fileNode = root;
		for (int i = 1; i < names.length; i++) {
			fileNode = fileNode.findChildWithName(names[i]);
			if (fileNode == null)
				return null;
		}
		JSONObject handle = new JSONObject();
		handle.put("handle", nextFileHandleID++);
		handle.put("offset", 0);
		handle.put("mode", -1);
		JSONObject fileInfo = new JSONObject();
		fileInfo.put("fileName", fileNode.fileName);
		fileInfo.put("fileType", fileNode.isDir ? 1 : 0);
		fileInfo.put("remotePath", fileNode.getPath());
		handle.put("fileInfo", fileInfo);

		JSONArray chunkList = new JSONArray();
		for (ChunkInfo chunkInfo : fileNode.fileChunkTable.values()) {
			JSONObject chunk = new JSONObject();
			chunk.put("chunkId", chunkInfo.chunkId);
			chunk.put("slaveIP", chunkInfo.slaveIP);
			chunk.put("port", chunkInfo.port);
			chunk.put("fileIndex", chunkInfo.fileIndex);
			chunk.put("chunkLeft", chunkInfo.chunkLeft);
			chunkList.put(chunk);
		}
		handle.put("chunkList", chunkList);
		return handle;
	}

	private static void testWrite() {
		FileNode test = new FileNode("vfs", true);
		test.child = new FileNode("a", true);
		test.child.brother = new FileNode("b", false);
		test.child.brother.fileChunkTable = new HashMap<Integer, ChunkInfo>();
		test.child.brother.fileChunkTable.put(1, new ChunkInfo(1, "10.60.41.1", 8888, 232, 234));
		test.child.brother.fileChunkTable.put(2, new ChunkInfo(2, "10.65.7.1", 8888, 786, 231));
		test.child.child = new FileNode("c", false);
		test.child.child.fileChunkTable.put(1, new ChunkInfo(1, "10.60.255.1", 8888, 45, 689));
		try {
			FileWriter fileWriter = new FileWriter("file_hierachy.json");
			fileWriter.write(test.toJSON().toString());
			fileWriter.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		Master master = new Master();
		System.out.println(master.open("vfs/b", ""));
	}

}
