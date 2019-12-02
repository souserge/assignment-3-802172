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
import java.util.Date;
import java.util.concurrent.TimeUnit;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqIO;
import org.apache.beam.sdk.io.rabbitmq.RabbitMqMessage;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Keys;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.WithTimestamps;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.transforms.windowing.AfterProcessingTime;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.joda.time.Duration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.joda.time.Instant;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;

import java.util.HashMap;
import java.util.Map;

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

    DoFn<KV<String, Long>, Void> logMessages = new DoFn<KV<String, Long>, Void>() {

      @ProcessElement
      public void processElement(ProcessContext c) {
        Date d = c.timestamp().toDate();
        int day = d.getDate();
        int month = d.getMonth();
        int year = d.getYear() - 100;
        KV<String, Long> el = c.element();
        Long count = el.getValue();
        String userid = el.getKey();
        String msg = "On " + day + "/" + month + "/" + year + ", person " + userid + " moved " + count + " times";
        LOG.info(msg);
      }
    };

    SerializableFunction<Map<String, String>, Instant> addTimestamps = new SerializableFunction<Map<String, String>, Instant>() {
      @Override
      public Instant apply(Map<String, String> element) {
        DateTimeFormatter timestampFormatter = new DateTimeFormatterBuilder().appendYear(4, 4).appendMonthOfYear(2)
            .appendDayOfMonth(2).appendHourOfDay(2).appendLiteral(':').appendMinuteOfHour(2).appendLiteral(':')
            .appendSecondOfMinute(2).toFormatter();
        return Instant.parse(element.get("timestamp"), timestampFormatter);
      }
    };

    SimpleFunction<RabbitMqMessage, Map<String, String>> parseMessages = new SimpleFunction<RabbitMqMessage, Map<String, String>>() {
      @Override
      public Map<String, String> apply(RabbitMqMessage input) {
        String body = new String(input.getBody());

        // part_id, ts_date, ts_time, room
        String[] row = body.split("\n");

        if (row.length != 4) {
          LOG.error("Incorrect row:\n" + body);
        }

        Map<String, String> rowMap = new HashMap<String, String>();
        String userid = row[0];
        String timestamp = row[1] + row[2];
        String room = row[3];
        rowMap.put("userid", userid);
        rowMap.put("timestamp", timestamp);
        rowMap.put("room", room);

        return rowMap;
      }
    };

    SimpleFunction<Map<String, String>, KV<String, String>> toKeyValue = new SimpleFunction<Map<String, String>, KV<String, String>>() {
      @Override
      public KV<String, String> apply(Map<String, String> element) {
        return KV.of(element.get("userid"), element.get("room"));
      }
    };

    DoFn<KV<String, Long>, RabbitMqMessage> toRabbitMQ = new DoFn<KV<String, Long>, RabbitMqMessage>() {
      @ProcessElement
      public void processElement(ProcessContext c) {
        String ts = c.timestamp().toString();
        String routingKey = c.element().getKey();
        Long value = c.element().getValue();
        byte[] body = (ts + "," + value.toString()).getBytes();

        // public RabbitMqMessage(
        // String routingKey,
        // byte[] body,
        // String contentType,
        // String contentEncoding,
        // Map<String, Object> headers,
        // Integer deliveryMode,
        // Integer priority,
        // String correlationId,
        // String replyTo,
        // String expiration,
        // String messageId,
        // Date timestamp,
        // String type,
        // String userId,
        // String appId,
        // String clusterId)
        RabbitMqMessage msg = new RabbitMqMessage(routingKey, body, null, null, new HashMap<>(), 1, 1, null, null, null,
            null, new Date(), null, null, null, null);

        c.output(msg);
      }
    };

    Window<String> fixedDailyWindow = Window.<String>into(FixedWindows.of(Duration.standardDays(1)))
        .withAllowedLateness(Duration.standardDays(1000)).accumulatingFiredPanes()
        .triggering(AfterProcessingTime.pastFirstElementInPane().plusDelayOf(Duration.standardSeconds(20)));
    // Begining of the pipeline ---------------

    CustomerStreamPipeline.waitForRabbitMQ(15);

    Pipeline p = Pipeline.create(PipelineOptionsFactory.fromArgs(args).withValidation().create());

    String uri = CustomerStreamPipeline.getConnectionUri();
    String queue = CustomerStreamPipeline.getQueueName();

    PCollection<RabbitMqMessage> messages = p.apply(RabbitMqIO.read().withUri(uri).withQueue(queue));
    PCollection<Map<String, String>> parsed = messages.apply(MapElements.via(parseMessages));

    PCollection<Map<String, String>> withTimestamp = parsed
        .apply(WithTimestamps.of(addTimestamps).withAllowedTimestampSkew(Duration.standardDays(1000)));

    PCollection<KV<String, String>> withTimestampKV = withTimestamp.apply(MapElements.via(toKeyValue));

    PCollection<String> useridOnly = withTimestampKV.apply(Keys.create());

    PCollection<String> windowed = useridOnly.apply(fixedDailyWindow);
    PCollection<KV<String, Long>> counted = windowed.apply(Count.perElement());

    counted.apply(ParDo.of(logMessages));

    PCollection<RabbitMqMessage> groupedMessages = counted.apply(ParDo.of(toRabbitMQ));
    groupedMessages.apply(
        RabbitMqIO.write().withUri(uri).withExchangeDeclare(true).withExchange("movement_notifications", "topic"));

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
