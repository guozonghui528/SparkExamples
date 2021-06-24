import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;

public class SocektStart {

    public static void main(String[] args) {
        try {
            ServerSocket socket = new ServerSocket(8080);

            Socket socket1 = socket.accept();

            System.out.println("send  start");
            int i =1;
            while(true){
                Random random = new Random(10);
//                int i =random.nextInt(9)+1;
                String ip = "172.1.1."+i;
                System.out.println("======"+ip);
                socket1.getOutputStream().write((ip+"\r\n").getBytes());
                i++;
                Thread.currentThread().sleep(4800);
            }

//            System.out.println("send end");
//
//            BufferedReader reader = new BufferedReader(new InputStreamReader(socket1.getInputStream()));
//            String rec = reader.readLine();
//            System.out.println("rec======="+rec);
//            System.out.println("finished");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
