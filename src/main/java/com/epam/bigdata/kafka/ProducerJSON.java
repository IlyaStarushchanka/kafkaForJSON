package com.epam.bigdata.kafka;

import com.epam.bigdata.entity.LogsEntity;
import com.google.common.io.Resources;
import eu.bitwalker.useragentutils.UserAgent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.json.JSONObject;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Stream;

/**
 * Created by Ilya_Starushchanka on 11/1/2016.
 */
public class ProducerJSON {
    public static void main(String[] args) throws Exception {
        // set up the producer
        KafkaProducer<String, String> producer;
        try (InputStream props = Resources.getResource("producer.props").openStream()) {
            Properties properties = new Properties();
            properties.load(props);
            producer = new KafkaProducer<>(properties);
        }

        try(Stream<Path> paths = Files.walk(Paths.get(args[0]))) {
            while (true) {
                paths.forEach(filePath -> {
                    if (Files.isRegularFile(filePath)) {
                        try (Stream<String> lines = Files.lines(filePath, Charset.forName("ISO-8859-1"))) {
                            lines.forEach(line -> {
                                LogsEntity logsEntity = new LogsEntity(line);
                                UserAgent ua = UserAgent.parseUserAgentString(logsEntity.getUserAgent());
                                String device = ua.getBrowser() != null ? ua.getOperatingSystem().getDeviceType().getName() : null;
                                String osName = ua.getBrowser() != null ? ua.getOperatingSystem().getName() : null;
                                String uaFamily = ua.getBrowser() != null ? ua.getBrowser().getGroup().getName() : null;
                                logsEntity.setDevice(device);
                                logsEntity.setOsName(osName);
                                logsEntity.setUaFamily(uaFamily);
                                JSONObject jsonObject = new JSONObject(logsEntity);
                                producer.send(new ProducerRecord<>(args[1], jsonObject.toString()));
                            });
                        } catch (IOException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                });
                Thread.sleep(100);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
