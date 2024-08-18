package com.springbatch.listener;

import org.springframework.batch.core.annotation.OnSkipInRead;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileWriter;
import java.util.Date;

@Component
public class SkipListener {
    @OnSkipInRead
    public void skipInRead(Throwable th){
        if(th instanceof FlatFileParseException){
            createFile("C:\\Users\\2130525\\Intellijproject\\springbatch\\springbatchtutorial\\chunkjob\\First Chunk Step\\reader\\skipInRead.txt",
                    ((FlatFileParseException)th).getInput()
            );
        }
    }
    public void createFile(String filePath, String data){
        try(FileWriter fileWriter = new FileWriter(new File(filePath), true)){
            fileWriter.write(data+", "+new Date() +"\n");
        }catch(Exception e){
            System.out.println("Exception While File Reading");
        }
    }
}
