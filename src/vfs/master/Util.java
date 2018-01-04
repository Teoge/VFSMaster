package vfs.master;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.json.JSONArray;
import org.json.JSONObject;

public class Util {

	public static void sendProtocol(OutputStream out, int protocol) throws IOException {
		byte[] protocolBuff = new byte[8];
		byte[] protocolBytes = (Integer.toString(protocol)).getBytes();
		for (int i = 0; i < protocolBytes.length; ++i) {
			protocolBuff[i] = protocolBytes[i];
		}
		out.write(protocolBuff, 0, protocolBuff.length);
	}
	
	public static void sendJSON(OutputStream out, JSONObject obj) throws IOException {
		byte[] bytes = obj.toString().getBytes();
		out.write(bytes, 0, bytes.length);
	}
	
	public static void sendJSON(OutputStream out, JSONArray array) throws IOException {
		byte[] bytes = array.toString().getBytes();
		out.write(bytes, 0, bytes.length);
	}
	
	public static boolean receiveOK(InputStream in, int protocol) throws IOException {
		byte[] protocolBuff = new byte[8];
		in.read(protocolBuff, 0, 8);
		if(Integer.parseInt(protocolBuff.toString()) == protocol)
			return true;
		else
			return false;
	}
	
}
