import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

public class SocektStart {

    public static void main(String[] args) {
        try {
            ServerSocket socket = new ServerSocket(8021);

            Socket socket1 = socket.accept();

            System.out.println("send  start");
            socket1.getOutputStream().write("error hh\r\n".getBytes());
            System.out.println("send end");

            BufferedReader reader = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
            String rec = reader.readLine();
            System.out.println("rec======="+rec);
            System.out.println("finished");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
