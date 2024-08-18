package com.springbatch.writer;

import com.springbatch.model.StudentCsv;
import com.springbatch.model.StudentJdbc;
import com.springbatch.model.StudentJson;
import com.springbatch.model.StudentResponse;
import org.springframework.batch.item.ItemWriter;
import org.springframework.stereotype.Component;

import java.util.List;
@Component
public class SecondItemWriter implements ItemWriter<StudentCsv> {
    @Override
    public void write(List<? extends StudentCsv> list) throws Exception {
        System.out.println("Inside Item Writer");
        list.stream().forEach(System.out::println);
    }
}
