package ordbutils;

import java.util.Base64;

import org.apache.commons.codec.binary.Hex;

public class TestBase64 {

	public static void main(String[] args) {
		// TODO Auto-generated method stubwxp://f2f1uYS4ZT3yYUvTtEz-d2QANAHtzX69ZVXG
//		byte []bb=Base64.getDecoder().decode("f2f11fioWI3oOsmNfarXMzyu");
//		f2f11fioWI3oOsmNfarXMzyu_hAMyW642qk7
//		f2f1uYS4ZT3yYUvTtEz-d2QANAHtzX69ZVXG
//		f2f1dWxLl97WoIovEheHBu0s0KuIkWk7a1s0
//		byte []bb=Base64.getDecoder().decode("d2QANAHtzX69ZVXG");//7764003401edcd7ebd6555c6
		byte []bb=Base64.getDecoder().decode("Bu0s0KuIkWk7a1s0");//06ed2cd0ab8891693b6b5b34
		System.out.println(Hex.encodeHexString(bb));
	}

}
