import java.io.BufferedReader;
import java.io.InputStreamReader;

public class UserJohn {
	public static void main(String[] args) {
		try {
			String topic = "SimpleTopic";
			String username = "john";
			String password = "john123";
			SimpleChat chat = new SimpleChat(topic, username, password);

			BufferedReader commandLine = new java.io.BufferedReader(new InputStreamReader(System.in));
			while (true) {
				String s = commandLine.readLine();
				if (s.equalsIgnoreCase("exit")) {
					chat.close();
					break;
				} else {
					chat.writeMessage(s);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
