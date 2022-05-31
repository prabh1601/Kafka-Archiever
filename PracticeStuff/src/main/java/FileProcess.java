import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

public class FileProcess {
    public static void main(String[] args) throws IOException {
        String fileName = "/mnt/Drive1/JetBrains/Intellij/test.txt";
        FileWriter fw = new FileWriter(fileName);
        File f = new File(fileName);
        if(f.exists()){
            System.out.println("Exists already");
        }
        fw.write("This is testing how this workss\n");
        fw.write("This is testing how this workss\n");
        fw.write("This is testing how this workss\n");
        fw.close();

    }
}
