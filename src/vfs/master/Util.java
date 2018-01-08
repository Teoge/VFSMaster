package vfs.master;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.json.JSONArray;
import org.json.JSONObject;

import vfs.struct.VSFProtocols;

public class Util {

	public static void sendProtocol(OutputStream out, int protocol) throws IOException {
		byte[] protocolBuff = new byte[8];
		byte[] protocolBytes = (Integer.toString(protocol)).getBytes();
		for (int i = 0; i < protocolBytes.length; ++i) {
			protocolBuff[i] = protocolBytes[i];
		}
		out.write(protocolBuff, 0, protocolBuff.length);
	}

	public static int receiveProtocol(InputStream in) throws IOException {
		byte[] protocolBuff = new byte[8];
		in.read(protocolBuff, 0, 8);
		int ends = 0;
		for (int i = 0; i < protocolBuff.length; ++i) {
			if (protocolBuff[i] == '\0') {
				ends = i;
				break;
			}
		}
		return Integer.parseInt(new String(protocolBuff, 0, ends));
	}

	public static void sendJSON(OutputStream out, JSONObject obj) throws IOException {
		DataOutputStream output = new DataOutputStream(out);
		byte[] bytes = obj.toString().getBytes();
		output.writeInt(bytes.length);
		output.write(bytes, 0, bytes.length);
	}

	public static void sendJSON(OutputStream out, JSONArray array) throws IOException {
		DataOutputStream output = new DataOutputStream(out);
		byte[] bytes = array.toString().getBytes();
		output.writeInt(bytes.length);
		output.write(bytes, 0, bytes.length);
	}

	public static String receiveString(InputStream in) throws IOException {
		DataInputStream input = new DataInputStream(in);
		int length = input.readInt();
		byte[] bytes = new byte[length];
		readBytes(input, bytes, length);
		return new String(bytes);
	}

	public static void sendInt(OutputStream out, int i) throws IOException {
		DataOutputStream output = new DataOutputStream(out);
		output.writeInt(i);
	}

	public static int receiveInt(InputStream in) throws IOException {
		DataInputStream input = new DataInputStream(in);
		return input.readInt();
	}

	public static void sendSignal(OutputStream out, String str) throws IOException {
		DataOutputStream output = new DataOutputStream(out);
		output.writeUTF(str);
		System.out.println("Master response: " + str);
	}

	public static boolean receiveOK(InputStream in) throws IOException {
		DataInputStream input = new DataInputStream(in);
		String reply = input.readUTF();
		if (reply.equals(VSFProtocols.MESSAGE_OK))
			return true;
		else
			return false;
	}

	private static void readBytes(DataInputStream in, byte[] buf, int len) throws IOException {
		int b = 0;
		while (b < len) {
			b += in.read(buf, b, len - b);
		}
	}

}
