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

import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqIO;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * <p>To run the pipeline using managed resource in Google Cloud
 * Platform, you should specify the following command-line options:
 *   --project=<YOUR_PROJECT_ID>
 *   --stagingLocation=<STAGING_LOCATION_IN_CLOUD_STORAGE>
 *   --runner=DataflowRunner
 */
public class CustomerStreamPipeline {
  private static final Logger LOG = LoggerFactory.getLogger(CustomerStreamPipeline.class);

  public static void main(String[] args) {
    this.waitForRabbitMQ(10);

    Pipeline p = Pipeline.create(
        PipelineOptionsFactory.fromArgs(args).withValidation().create());

    String uri = this.getConnectionUri();
    String queue = this.getQueueName();

    PCollection<RabbitMqMessage> messages = p.apply(
        RabbitMqIO.read().withUri(uri).withQueue(queue));
    
    messages
    .apply(MapElements.via(new SimpleFunction<RabbitMqMessage, String>() {
      @Override
      public String apply(RabbitMqMessage input) {
        return new String(input.getBody());
      }
    }))
    .apply(ParDo.of(new DoFn<String, Void>() {
      @ProcessElement
      public void processElement(ProcessContext c)  {
        LOG.info(c.element());
      }
    }));

    p.run();
  }

  private String getConnectionUri() {
    String user = System.getenv("RABBITMQ_DEFAULT_USER");
    String pass = System.getenv("RABBITMQ_DEFAULT_PASS");
    String host = System.getenv("RABBITMQ_HOST");
    String port = "5672";
    String[] params = {
      "retry_delay=5",
      "connection_attempts=5"
    };

    return "amqp://" + user + ":" + pass + "@" + host + '?' + String.join("&", params);
  }

  private String getQueueName() {
    return "customerstreamapp";
  }

  private void waitForRabbitMQ(int seconds) {
    try {
      TimeUnit.SECONDS.sleep(seconds);
    } catch (InterruptedException e) {
      System.out.println("Something went wrong.");
    }
  }
}
