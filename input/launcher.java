import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class launcher {

    public static void main(String[] args) throws IOException {
        // TODO 
        int range = 10000;
        File file = new File("edgeData.txt");
        if(!file.exists()) file.createNewFile();
        FileWriter fw = new FileWriter(file);
        BufferedWriter bw = new BufferedWriter(fw);
        for(int i = 0; i < 10000000; i++) {
            int u = (int) (range*(Math.random()));
            int v = (int) (range*(Math.random()));

            String toWrite = u + " " + v + " \n";
            bw.write(toWrite);

        }
    }




}