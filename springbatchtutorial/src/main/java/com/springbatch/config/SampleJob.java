package com.springbatch.config;

import com.springbatch.listener.FirstJobListener;
import com.springbatch.listener.SecondJobListener;
import com.springbatch.listener.SkipListener;
import com.springbatch.model.StudentCsv;
import com.springbatch.model.StudentJdbc;
import com.springbatch.model.StudentJson;
import com.springbatch.model.StudentResponse;
import com.springbatch.processor.FirstItemProcessor;
import com.springbatch.reader.FirstItemReader;
import com.springbatch.service.SecondTasklet;
import com.springbatch.service.StudentService;
import com.springbatch.writer.FirstItemWriter;
import com.springbatch.writer.SecondItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.skip.AlwaysSkipItemSkipPolicy;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.adapter.ItemReaderAdapter;
import org.springframework.batch.item.adapter.ItemWriterAdapter;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.*;
import org.springframework.batch.item.file.mapping.BeanWrapperFieldSetMapper;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.json.*;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.Writer;
import java.util.Date;

@Configuration
public class SampleJob {
    @Autowired
    private JobBuilderFactory jobBuilderFactory;
    @Autowired
    private StepBuilderFactory stepBuilderFactory;
    @Autowired
    private SecondTasklet secondTasklet;
    @Autowired
    private FirstJobListener firstJobListener;
    @Autowired
    private SecondJobListener secondJobListener;
    @Autowired
    private FirstItemReader firstItemReader;
    @Autowired
    private FirstItemProcessor firstItemProcessor;
    @Autowired
    private FirstItemWriter firstItemWriter;
    @Autowired
    private SecondItemWriter secondItemWriter;
    @Autowired
    private DataSource dataSource;
    @Autowired
    private StudentService studentService;
    @Autowired
    private SkipListener skipListener;

    @Bean
    public Job firstJob() {
        return jobBuilderFactory.get("First Job")
                .incrementer(new RunIdIncrementer())
                .start(firstStep())
                .next(secondStep())
                .listener(firstJobListener)
                .build();
    }

    private Step firstStep() {
        return stepBuilderFactory.get("First Step")
                .tasklet(firstTask())
                .build();
    }

    private Tasklet firstTask() {
        return new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
                System.out.println("This is First Tasklet Step");
                return RepeatStatus.FINISHED;
            }
        };
    }

    private Step secondStep() {
        return stepBuilderFactory.get("Second Step")
                .tasklet(secondTasklet)
                .build();
    }

    //    private Tasklet secondTask() {
//        return new Tasklet() {
//            @Override
//            public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws Exception {
//                System.out.println("This is Second Tasklet Step");
//                return RepeatStatus.FINISHED;
//            }
//        };
//    }


// ---------------------- Chuk-Oriented Job --------------------------------
    @Bean
    public Job secondJob() {
        return jobBuilderFactory.get("Second Job")
                .incrementer(new RunIdIncrementer())
                .start(firstChunkStep())
                .build();
    }

    private Step firstChunkStep() {
        return stepBuilderFactory.get("First Chunk Step")
                .<StudentCsv, StudentCsv>chunk(3)
//                .reader(jdbcCursorItemReader()) //For JDBC Reader
                .reader(flatFileItemReader(null))//For CSV File Reader
//                .reader(itemReaderAdapter()) //For JSON File Reader
//                .processor(firstItemProcessor)
//                .writer(secondItemWriter)
                .writer(jsonFileItemWriter(null)) //For JSON Writer
//                .writer(flatFileItemWriter(null)) //For CSV File Writer
//                .writer(itemWriterAdapter()) //for Rest API Writer
//                .writer(secondItemWriter)
                .faultTolerant()
                .skip(FlatFileParseException.class)
//                .skipLimit(Integer.MAX_VALUE)
                .skipPolicy(new AlwaysSkipItemSkipPolicy())//Alternate Skip Policy Approach
                .listener(skipListener)
                .build();
    }

    //<-------------------------Reader-------------------------------->
    //CSV File Reader
    @StepScope
    @Bean
    public FlatFileItemReader<StudentCsv> flatFileItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource
    ){
        FlatFileItemReader<StudentCsv> flatFileItemReader = new FlatFileItemReader<>();
        flatFileItemReader.setResource(fileSystemResource);
        flatFileItemReader.setLineMapper(new DefaultLineMapper<StudentCsv>(){
            {
                setLineTokenizer(new DelimitedLineTokenizer(){
                    {
                        setNames("ID","First Name","Last Name", "Email");
                    }
                });
                setFieldSetMapper(new BeanWrapperFieldSetMapper<StudentCsv>(){
                    {
                        setTargetType(StudentCsv.class);
                    }
                });
            }
        });
        flatFileItemReader.setLinesToSkip(1);
        return flatFileItemReader;
    }

    //JSON Reader
    @StepScope
    @Bean
    public JsonItemReader<StudentJson> jsonItemReader(
            @Value("#{jobParameters['inputFile']}") FileSystemResource fileSystemResource
    ) {
//        System.out.println("jsonFileItemWriter is calling..."+fileSystemResource);
        JsonItemReader<StudentJson> jsonJsonItemReader = new JsonItemReader<>();
        jsonJsonItemReader.setResource(fileSystemResource);
        jsonJsonItemReader.setJsonObjectReader(new JacksonJsonObjectReader<>(StudentJson.class));
        return jsonJsonItemReader;
    }

    //JDBC Reader
    public JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader(){
        JdbcCursorItemReader<StudentJdbc> jdbcCursorItemReader = new JdbcCursorItemReader<>();
        jdbcCursorItemReader.setDataSource(dataSource);
        jdbcCursorItemReader.setSql("select id, first_name as firstName, last_name as lastName, email from students");
        jdbcCursorItemReader.setRowMapper(new BeanPropertyRowMapper<StudentJdbc>(){
            {
                setMappedClass(StudentJdbc.class);
            }
        });
        return jdbcCursorItemReader;
    }

    //Reader for REST Api
    public ItemReaderAdapter<StudentResponse> itemReaderAdapter(){
        ItemReaderAdapter<StudentResponse> itemReaderAdapter = new ItemReaderAdapter<>();
        itemReaderAdapter.setTargetObject(studentService);
        itemReaderAdapter.setTargetMethod("getStudent");
        return itemReaderAdapter;
    }
    //<---------------------------Writer----------------------->
    //CSV Item Writer
    @StepScope
    @Bean
    public FlatFileItemWriter<StudentJdbc> flatFileItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource
    ){
        FlatFileItemWriter<StudentJdbc> flatFileItemWriter = new FlatFileItemWriter<>();
        flatFileItemWriter.setResource(fileSystemResource);
        flatFileItemWriter.setHeaderCallback(new FlatFileHeaderCallback() {
            @Override
            public void writeHeader(Writer writer) throws IOException {
                writer.write("ID,First Name,Last Name, Email");
            }
        });
        flatFileItemWriter.setLineAggregator(new DelimitedLineAggregator<StudentJdbc>(){
            {
                setFieldExtractor(new BeanWrapperFieldExtractor<StudentJdbc>(){
                    {
                        setNames(new String[]{
                                "id","firstName","lastName", "email"
                        });
                    }
                });
            }
        });
        flatFileItemWriter.setFooterCallback(new FlatFileFooterCallback() {
            @Override
            public void writeFooter(Writer writer) throws IOException {
                writer.write("Created @ "+new Date());
            }
        });
        return flatFileItemWriter;
    }

    //JSON File Writer
    @StepScope
    @Bean
    public JsonFileItemWriter<StudentCsv> jsonFileItemWriter(
            @Value("#{jobParameters['outputFile']}") FileSystemResource fileSystemResource
    ){
//        System.out.println("jsonFileItemWriter is calling..."+fileSystemResource);
        JsonFileItemWriter<StudentCsv> jsonFileItemWriter = new JsonFileItemWriter<>(
                fileSystemResource,
                new JacksonJsonObjectMarshaller<StudentCsv>()
        );
        return jsonFileItemWriter;
    }

    //JDBC Writer
    @Bean
    public JdbcBatchItemWriter<StudentJson> jdbcBatchItemWriter(){
        JdbcBatchItemWriter<StudentJson> jdbcBatchItemWriter = new JdbcBatchItemWriter<>();
        jdbcBatchItemWriter.setDataSource(dataSource);
        jdbcBatchItemWriter.setSql("INSERT INTO students (id, first_name, last_name, email)"
                +"VALUES(:id, :firstName, :lastName, :email)");
        jdbcBatchItemWriter.setItemSqlParameterSourceProvider(
                new BeanPropertyItemSqlParameterSourceProvider<StudentJson>()
        );
        return jdbcBatchItemWriter;
    }
    //REST Api Writer
    public ItemWriterAdapter<StudentCsv> itemWriterAdapter(){
        ItemWriterAdapter<StudentCsv> itemWriterAdapter = new ItemWriterAdapter<>();
        itemWriterAdapter.setTargetObject(studentService);
        itemWriterAdapter.setTargetMethod("restCallToCreateStudent");
        return itemWriterAdapter;
    }
}
