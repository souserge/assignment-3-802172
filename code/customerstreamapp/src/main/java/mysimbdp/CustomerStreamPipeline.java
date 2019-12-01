/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package mysimbdp;

import java.util.Optional;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqIO;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>
 * To run the pipeline using managed resource in Google Cloud Platform, you
 * should specify the following command-line options:
 * --project=<YOUR_PROJECT_ID>
 * --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE> --runner=DataflowRunner
 */
public class CustomerStreamPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(CustomerStreamPipeline.class);

  public static void main(String[] args) {
    DoFn<String[], Void> logMessages = new DoFn<String[], Void>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String msg = String.join(",", c.element());
        LOG.info(msg);
      }
    };

    SimpleFunction<RabbitMqMessage, String[]> parseMessages = new SimpleFunction<RabbitMqMessage, String[]>() {
      @Override
      public String[] apply(RabbitMqMessage input) {
        String body = new String(input.getBody());
        String[] row = body.split("\n");
        return row;
      }
    };

    CustomerStreamPipeline.waitForRabbitMQ(2);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

    String uri = CustomerStreamPipeline.getConnectionUri();
    String queue = CustomerStreamPipeline.getQueueName();

    PCollection<RabbitMqMessage> messages = p.apply(RabbitMqIO.read().withUri(uri).withQueue(queue));

    messages.apply(MapElements.via(parseMessages)).apply(ParDo.of(logMessages));

    p.run();
  }

  private static String getConnectionUri() {
    String user = Optional.ofNullable(System.getenv("RABBITMQ_DEFAULT_USER")).orElse("admin");
    String pass = Optional.ofNullable(System.getenv("RABBITMQ_DEFAULT_PASS")).orElse("admin");
    String host = Optional.ofNullable(System.getenv("RABBITMQ_HOST")).orElse("localhost");
    String port = Optional.ofNullable(System.getenv("RABBITMQ_PORT")).orElse("5672");
    String[] params = { "retry_delay=5", "connection_attempts=5" };

    return "amqp://" + user + ":" + pass + "@" + host + ":" + port + '?' + String.join("&", params);
  }

  private static String getQueueName() {
    return Optional.ofNullable(System.getenv("RABBITMQ_STREAMAPP_QUEUE_NAME")).orElse("customerstreamapp");
  }

  private static void waitForRabbitMQ(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      System.out.println("Something went wrong.");
    }
  }
}
