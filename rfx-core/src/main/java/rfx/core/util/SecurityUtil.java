package rfx.core.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Formatter;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

import org.apache.commons.codec.binary.Base64;

public class SecurityUtil {
	public static final long ENCRYPT_XOR = 35;
	public static SecretKeySpec key;
	static {		
		String privateKey = SecurityUtil.class.getName();
		key = new SecretKeySpec(privateKey.getBytes(), "Blowfish");
	}

	public static String encryptBlowfish(String to_encrypt) {
		//System.out.println("encryptBlowfish:"+to_encrypt);	
		if(StringUtil.isEmpty(to_encrypt)){
			return null;
		}
		try {
			Cipher cipher = Cipher.getInstance("Blowfish");
			cipher.init(Cipher.ENCRYPT_MODE, key);
			String s = Base64.encodeBase64String(cipher.doFinal(to_encrypt.getBytes()));
			
			return s;
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println(e.getMessage() + " to_encrypt:"+to_encrypt);
			return "";
		}
	}

	public static String decryptBlowfish(String to_decrypt) {		
		if(StringUtil.isEmpty(to_decrypt)){
			return null;
		}
		try {
			byte[] encryptedData = Base64.decodeBase64(to_decrypt);
			Cipher cipher = Cipher.getInstance("Blowfish");
			cipher.init(Cipher.DECRYPT_MODE, key);
			byte[] decrypted = cipher.doFinal(encryptedData);
			return new String(decrypted);
		} catch (Exception e) {
			//e.printStackTrace();
			System.err.println(e.getMessage() + " to_encrypt:"+to_decrypt);
			return null;
		}
	}

	public static String decryptBeaconValue(String beacon) {
		if (StringUtil.isEmpty(beacon)) {
			return StringPool.BLANK;
		}
		try {			
			char[] beacons = beacon.toCharArray();
			int l = beacons.length;
			if (l % 2 != 0) {
				return StringPool.BLANK;
			}
			StringBuilder s = new StringBuilder(120);
			for (int i = 0; i < l; i++) {
				if (i % 2 == 0) {
					if (beacons[i] == 'z') {
						beacons[i] = '0';
					}
					String n = (beacons[i] + "" + beacons[i + 1]);
					long n1 = Long.parseLong(n, 35);
					char n2 = (char) (n1 ^ ENCRYPT_XOR);
					s.append(n2);
				}
			}
			return s.toString();
		} catch (Exception e) {}
		return StringPool.BLANK;
	}

	public static String encryptBeaconValue(String str) {
		if (StringUtil.isEmpty(str)) {
			return StringPool.BLANK;
		}
		StringBuilder s = new StringBuilder(120);
		// int secondTime = (int) (System.currentTimeMillis()/1000);
		String[] toks = (str).split("");

		for (String tok : toks) {
			if (tok.length() > 0) {
				int i = (int) tok.charAt(0);
				long n = i ^ ENCRYPT_XOR;
				if (n < 35) {
					s.append("z").append(Long.toString(n, 35));
				} else {
					s.append(Long.toString(n, 35));
				}
			}
		}
		return s.toString();
	}

	public static String sha1(String s) {
		try {
			byte[] hash = MessageDigest.getInstance("SHA-1").digest(
					s.getBytes());
			Formatter formatter = new Formatter();
			for (byte b : hash) {
				formatter.format("%02x", b);
			}
			String hashedSha1 = formatter.toString();
			formatter.close();
			return hashedSha1;
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return StringPool.BLANK;
	}
}
