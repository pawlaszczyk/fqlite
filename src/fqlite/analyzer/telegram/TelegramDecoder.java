package fqlite.analyzer.telegram;

import fqlite.analyzer.fleece.FleeceDecoder;

public class TelegramDecoder {

	
	
	public static String decodeAppConfig(String hexstring){
		
		hexstring = hexstring.replace(" ","");
		hexstring = hexstring.toUpperCase();
		byte[] stream = FleeceDecoder.hexStringToByteArray(hexstring);

		NativeByteBuffer data = new NativeByteBuffer(stream);
	    TLRPC.help_AppConfig appconfig = TLRPC.help_AppConfig.TLdeserialize(data, data.readInt32(false), true);
	    
	    StringBuffer result = new StringBuffer();
	    result.append("disableFree: " + appconfig.disableFree + "\n");
	    result.append("networkType: " + appconfig.networkType + "\n");
	    return result.toString();
	}
	
	
	public static String decodeUserFull(String hexstring){
		
		hexstring = hexstring.replace(" ","");
		hexstring = hexstring.toUpperCase();
		byte[] stream = FleeceDecoder.hexStringToByteArray(hexstring);

		NativeByteBuffer data = new NativeByteBuffer(stream);
	    TLRPC.UserFull user = TLRPC.UserFull.TLdeserialize(data, data.readInt32(false), false);

	    StringBuffer result = new StringBuffer();
	    result.append("id: " + user.id + "\n");
	    result.append("about: " + user.about + "\n");
	    result.append("chats_count: " + user.common_chats_count + "\n");		 
	    result.append("flags: " + user.flags + "\n");
	    result.append("flags2: " + user.flags2 + "\n");
	    result.append("folder_id: " + user.folder_id + "\n");
	  
	    return result.toString();
	}
	

	public static String decodeUser(String hexstring){
		
		hexstring = hexstring.replace(" ","");
		hexstring = hexstring.toUpperCase();
		byte[] stream = FleeceDecoder.hexStringToByteArray(hexstring);

		NativeByteBuffer data = new NativeByteBuffer(stream);
	    TLRPC.User user = TLRPC.User.TLdeserialize(data, data.readInt32(false), false);

	    StringBuffer result = new StringBuffer();
	    result.append("id: " + user.id + "\n");
	    result.append("first Name: " + user.first_name + "\n");
	    result.append("last Name: " + user.last_name + "\n");		 
	    result.append("bot_active_users: " + user.bot_active_users + "\n");
	    result.append("lang_code: " + user.lang_code + "\n");
	    result.append("bot_business: " + user.bot_business + "\n");
	    result.append("fake: " + user.fake + "\n");
	    result.append("usernames: " + user.usernames + "\n");
	    
	    
	    return result.toString();
	}
	
	public static String decodeMessage(String hexstring){
		
		hexstring = hexstring.replace(" ","");
		hexstring = hexstring.toUpperCase();
		byte[] stream = FleeceDecoder.hexStringToByteArray(hexstring);

		NativeByteBuffer data = new NativeByteBuffer(stream);
	    TLRPC.Message message = TLRPC.Message.TLdeserialize(data, data.readInt32(false), false);


	    StringBuffer result = new StringBuffer();
	    result.append("id: " + message.id + "\n");
	    result.append("date: " + message.date + "\n");
	    result.append("message: " + message.message + "\n");
	    result.append("from channel_id: " + message.from_id.channel_id + "\n");
	    result.append("from chat_id: " + message.from_id.chat_id + "\n");
	    result.append("networkType: " + message.from_id.networkType + "\n");
	    result.append("user_id: " + message.from_id.user_id + "\n");
	    result.append("destroyTime: " + message.destroyTime + "\n");
	    
	    return result.toString();
	}
	
	
	public static void main(String[] args) {
		
		String m1 = "42523494 00000000 00000000 06000000 22175159 D743994E 01000000 D2B4C466 7E4E6120 6B6C6172 E280A620 6461206B C3B66E6E 74652069 63682064 69636820 696E2065 696E2070 61617220 47727570 70656E20 65696E6C 6164656E 2C206461 2077C3A4 72652064 65696E20 616B6164 656D6973 63686573 20496E74 65726573 73652076 6F6C6C20 62656672 69656469 6774E280 A62E2EF0 9F9880F0 9F998300 01200000";
 		String m2 = "42523494020100000000000005000000221751594165777E0100000022175159782E2B5300000000EEB0C4663942696E207365697420686575746520646162656920F09F9883204B6F6D6D656E20676C65696368206D616C206B6C696E67656C6E20F09F93B2000001200000";
 		
 		String u1 = "CA4F3183771800021000000097B0FB20000000005A8C01345491EBA906536F7068696100064A616E73656E000D34393137363234383637333034000006F7D18202000000AAA7311B97B0FB2014010808B5F6D658D9B01F1C8ED4514524C725667F000000020000003F708C008634C466";
 		
 		
		System.out.println(decodeMessage(m1));
		System.out.println(decodeMessage(m2));	
		System.out.println(decodeUser(u1));
		
		
	}
	
	
	
	

}
