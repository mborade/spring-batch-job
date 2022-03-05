/*
 * Copyright 2017 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.spring.configuration;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.sql.DataSource;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.partition.PartitionHandler;
import org.springframework.batch.core.partition.support.Partitioner;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.database.JdbcPagingItemReader;
import org.springframework.batch.item.database.Order;
import org.springframework.batch.item.database.PagingQueryProvider;
import org.springframework.batch.item.database.support.MySqlPagingQueryProvider;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.jdbc.DataSourceBuilder;
import org.springframework.cloud.deployer.resource.support.DelegatingResourceLoader;
import org.springframework.cloud.deployer.spi.task.TaskLauncher;
import org.springframework.cloud.task.batch.partition.CommandLineArgsProvider;
import org.springframework.cloud.task.batch.partition.DeployerPartitionHandler;
import org.springframework.cloud.task.batch.partition.DeployerStepExecutionHandler;
import org.springframework.cloud.task.batch.partition.NoOpEnvironmentVariablesProvider;
import org.springframework.cloud.task.batch.partition.PassThroughCommandLineArgsProvider;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.io.FileSystemResource;

import io.spring.batch.DownloadingStepExecutionListener;
import io.spring.batch.EnrichmentProcessor;
import io.spring.domain.Foo;

/**
 * @author Michael Minella
 */
@Configuration
public class JobConfiguration {

	@Profile("master")
	@Configuration
	public static class MasterConfiguration {
		
		// @Autowired
	    // private DataSource dataSource;

		@Bean
		public Step master(StepBuilderFactory stepBuilderFactory,
				Partitioner partitioner,
				PartitionHandler partitionHandler) {
			return stepBuilderFactory.get("master")
					.partitioner("load", partitioner)
					.partitionHandler(partitionHandler)
					.build();
		}

		@Bean
		public Job job(JobBuilderFactory jobBuilderFactory) throws Exception {
			return jobBuilderFactory.get("jdbctofile")
					.start(master(null, null, null))
					.build();
		}

//		@Bean
//		public Partitioner partitioner(ResourcePatternResolver resourcePatternResolver,
//				@Value("${job.resource-path}") String resourcePath) throws IOException {
//			Resource[] resources = resourcePatternResolver.getResources(resourcePath);
//
//			MultiResourcePartitioner partitioner = new MultiResourcePartitioner();
//			partitioner.setResources(resources);
//
//			return partitioner;
//		}
		
	    @Bean
	    public ColumnRangePartitioner partitioner() 
	    {
			DataSource dataSource = DataSourceBuilder.create()					
									.username("rfamro1")
									// .password("none")
									.driverClassName("com.mysql.jdbc.Driver")
									.url("jdbc:mysql://mysql-rfam-public.ebi.ac.uk:4497/Rfam")
									.build();
	        ColumnRangePartitioner columnRangePartitioner = new ColumnRangePartitioner();
	        columnRangePartitioner.setColumn("ncbi_id");
	        columnRangePartitioner.setDataSource(dataSource);
	        columnRangePartitioner.setTable("taxonomy");
	        return columnRangePartitioner;
	    }

		@Bean
		public PassThroughCommandLineArgsProvider commandLineArgsProvider() {

			List<String> commandLineArgs = new ArrayList<>(4);
			commandLineArgs.add("--spring.profiles.active=worker");
			commandLineArgs.add("--spring.cloud.task.initialize.enable=false");
			commandLineArgs.add("--spring.batch.initializer.enabled=false");
			commandLineArgs.add("--spring.datasource.initialize=false");

			PassThroughCommandLineArgsProvider provider = new PassThroughCommandLineArgsProvider(commandLineArgs);

			return provider;
		}

		@Bean
		public DeployerPartitionHandler partitionHandler(//@Value("${job.worker-app}") String resourceLocation,
				@Value("${spring.application.name}") String applicationName,
				ApplicationContext context,
				TaskLauncher taskLauncher,
				JobExplorer jobExplorer,
				CommandLineArgsProvider commandLineArgsProvider) {
			DeployerPartitionHandler partitionHandler =
					new DeployerPartitionHandler(taskLauncher,
							jobExplorer,
							new DelegatingResourceLoader()
				            .getResource("maven://io.spring.cloud-native-batch:partitionedjob:1.0"),
							"load");
				
			partitionHandler.setCommandLineArgsProvider(commandLineArgsProvider);
			partitionHandler.setEnvironmentVariablesProvider(new NoOpEnvironmentVariablesProvider());
			partitionHandler.setMaxWorkers(2);
			partitionHandler.setApplicationName(applicationName);

			return partitionHandler;
		}
	}

	@Profile("worker")
	@Configuration
	public static class WorkerConfiguration {

		@Bean
		public DeployerStepExecutionHandler stepExecutionHandler(ApplicationContext context, JobExplorer jobExplorer, JobRepository jobRepository) {
			return new DeployerStepExecutionHandler(context, jobExplorer, jobRepository);
		}

//		@Bean
//		public DownloadingStepExecutionListener downloadingStepExecutionListener() {
//			return new DownloadingStepExecutionListener();
//		}

//		@Bean
//		@StepScope
//		public FlatFileItemReader<Foo> reader(@Value("#{stepExecutionContext['localFile']}")String fileName) throws Exception {
//			FlatFileItemReader<Foo> reader = new FlatFileItemReaderBuilder<Foo>()
//					.name("fooReader")
//					.resource(new FileSystemResource(fileName))
//					.delimited()
//					.names(new String[] {"first", "second", "third"})
//					.targetType(Foo.class)
//					.build();
//
//			return reader;
//		}
		
//		@Bean
//		@StepScope
//		public JdbcCursorItemReader<Foo> reader(DataSource dataSource) throws Exception {
//			RowMapper<Foo> rowMapper = new FooRowMapper<Foo>();
//			dataSource = DataSourceBuilder.create()					
//									.username("rfamro")
//									.password("none")
//									.driverClassName("com.mysql.jdbc.Driver")
//									.url("jdbc:mysql://mysql-rfam-public.ebi.ac.uk:4497/Rfam")
//									.build();
//			JdbcCursorItemReader<Foo> reader = new JdbcCursorItemReaderBuilder<Foo>()
//					.name("fooReader")
//					.sql("select ncbi_id, species from taxonomy")
//					.rowMapper(rowMapper)
//					.dataSource(dataSource)
//					.build();
//
//			return reader;
//		}
		
	    @Bean
	    @StepScope
	    public JdbcPagingItemReader<Foo> pagingItemReader(
	            @Value("#{stepExecutionContext['minValue']}") Long minValue,
	            @Value("#{stepExecutionContext['maxValue']}") Long maxValue) 
	    {
	        System.out.println("reading " + minValue + " to " + maxValue);
	 
	        Map<String, Order> sortKeys = new HashMap<>();
	        sortKeys.put("id", Order.ASCENDING);
	         
	        MySqlPagingQueryProvider queryProvider = new MySqlPagingQueryProvider();
	        queryProvider.setSelectClause("id, firstName, lastName, birthdate");
	        queryProvider.setFromClause("from customer");
	        queryProvider.setWhereClause("where id >= " + minValue + " and id < " + maxValue);
	        queryProvider.setSortKeys(sortKeys);
	        
	        
	        
	        DataSource dataSource = DataSourceBuilder.create()					
					.username("rfamro1")
					// .password("none")
					.driverClassName("com.mysql.jdbc.Driver")
					.url("jdbc:mysql://mysql-rfam-public.ebi.ac.uk:4497/Rfam")
					.build();
	        		
	        JdbcPagingItemReader<Foo> reader = new JdbcPagingItemReader<>();
	        reader.setDataSource(dataSource);
	        reader.setFetchSize(1000);
	        reader.setRowMapper(new FooRowMapper<Foo>());
	        reader.setQueryProvider((PagingQueryProvider)queryProvider);
	         
	        return reader;
	    }

		@Bean
		@StepScope
		public EnrichmentProcessor processor() {
			return new EnrichmentProcessor();
		}

//		@Bean
//		public JdbcBatchItemWriter<Foo> writer(DataSource dataSource) {
//
//			return new JdbcBatchItemWriterBuilder<Foo>()
//					.dataSource(dataSource)
//					.beanMapped()
//					.sql("INSERT INTO FOO VALUES (:first, :second, :third, :message)")
//					.build();
//		}
		
		@Bean
		public FlatFileItemWriter<Foo> writer(String filePath) throws Exception {
			FlatFileItemWriter<Foo> writer = new FlatFileItemWriterBuilder<Foo>()
					.name("fooWriter")
					.resource(new FileSystemResource("./file-"+new Random().nextInt()))
					.delimited()
					.names(new String[] {"ncbi_id", "species"})
					.build();

			return writer;
		}
		@Bean
		public Step load(StepBuilderFactory stepBuilderFactory) throws Exception {
			return stepBuilderFactory.get("load")
					.<Foo, Foo>chunk(500)
					.reader(pagingItemReader(null, null))
					.processor(processor())
					.writer(writer(null))
					.build();
		}

//		@Bean
//		@LoadBalanced
//		public RestTemplate restTemplate() {
//			return new RestTemplate();
//		}
	}
}
