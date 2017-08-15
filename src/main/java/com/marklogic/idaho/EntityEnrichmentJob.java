package com.marklogic.idaho;

import com.marklogic.client.DatabaseClient;
import com.marklogic.client.helper.DatabaseClientProvider;
import com.marklogic.client.io.StringHandle;
import com.marklogic.client.query.CountedDistinctValue;
import com.marklogic.client.query.QueryDefinition;
import com.marklogic.client.query.StructuredQueryBuilder;
import com.marklogic.spring.batch.item.reader.ValuesItemReader;
import com.marklogic.spring.batch.item.writer.MarkLogicPatchItemWriter;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.JobScope;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;

@EnableBatchProcessing
@Import( { com.marklogic.spring.batch.config.MarkLogicBatchConfiguration.class } )
public class EntityEnrichmentJob {
    
    private Environment env;
    
    private final String JOB_NAME = "entityEnrichmentJob";
    
    @Bean
    public Job job(JobBuilderFactory jobBuilderFactory, Step step) {
        return jobBuilderFactory.get(JOB_NAME).start(step).build();
    }
    
    @Bean
    @JobScope
    public Step step(
        StepBuilderFactory stepBuilderFactory,
        DatabaseClientProvider databaseClientProvider,
        @Value("#{jobParameters['tokenizer_model']}") String tokenizerModel,
        @Value("#{jobParameters['named_entity_model']}") String namedEntityModel,
        @Value("#{jobParameters['collection']}") String collection) {
        
        DatabaseClient databaseClient = databaseClientProvider.getDatabaseClient();
        QueryDefinition qd = new StructuredQueryBuilder().collection(collection);
        ItemReader<CountedDistinctValue> reader =new ValuesItemReader(databaseClient, getQueryOptions(), "uris", qd);

        ItemProcessor<CountedDistinctValue, String[]> processor =
            new EntityEnrichmentItemProcessor(databaseClient, tokenizerModel, namedEntityModel);
        ItemWriter<String[]> writer = new MarkLogicPatchItemWriter(databaseClient);
           
        
        return stepBuilderFactory.get("step1")
                .<CountedDistinctValue, String[]>chunk(10)
                .reader(reader)
                .processor(processor)
                .writer(writer)
                .build();
    }

    public StringHandle getQueryOptions() {
        return new StringHandle("<options xmlns=\"http://marklogic.com/appservices/search\">\n" +
                "    <search-option>unfiltered</search-option>\n" +
                "    <quality-weight>0</quality-weight>\n" +
                "    <values name=\"uris\">\n" +
                "        <uri/>\n" +
                "    </values>\n" +
                "</options>");
    }

}
