package com.example;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.List;
import java.util.Map;
import java.util.Properties;

public class LambdaHandler implements RequestHandler<Map<String, Object>, String> {
    @Override
    public String handleRequest(Map<String, Object> event, Context context) {
        try {
            context.getLogger().log("Event received: " + event); // Log complet de l'événement

            // Vérification des données de l'événement
            List<Map<String, Object>> records = (List<Map<String, Object>>) event.get("Records");
            if (records == null || records.isEmpty()) {
                context.getLogger().log("No records found in event");
                return "Failure: No records found in event";
            }

            Map<String, Object> s3 = (Map<String, Object>) records.get(0).get("s3");
            if (s3 == null) {
                context.getLogger().log("No S3 data found in event");
                return "Failure: No S3 data found in event";
            }

            Map<String, Object> bucket = (Map<String, Object>) s3.get("bucket");
            String bucketName = bucket != null ? bucket.get("name").toString() : "Unknown";

            Map<String, Object> obj = (Map<String, Object>) s3.get("object");
            String fileName = obj != null ? obj.get("key").toString() : "Unknown";

            context.getLogger().log("Bucket: " + bucketName + ", File: " + fileName);

            // Convertir les données en JSON
            Map<String, Object> jsonData = Map.of(
                    "bucket", bucketName,
                    "file", fileName
            );

            // Envoyer à Kafka
            sendToKafka2(jsonData, context);

            return "Success";
        } catch (Exception e) {
            context.getLogger().log("Error in handleRequest: " + e.getMessage());
            e.printStackTrace(); // Log complet de la pile d'exceptions
            return "Failure: " + e.getMessage();
        }
    }


    private void sendToKafka(Map<String, Object> jsonData, Context context) {
        Properties props = new Properties();
        String kafkaBootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
        String kafkaTopic = System.getenv("KAFKA_TOPIC");

        context.getLogger().log("Kafka Bootstrap Servers: " + kafkaBootstrapServers);
        context.getLogger().log("Kafka Topic: " + kafkaTopic);

        if (kafkaBootstrapServers == null || kafkaBootstrapServers.isEmpty()) {
            context.getLogger().log("KAFKA_BOOTSTRAP_SERVERS environment variable is not set");
            return;
        }
        if (kafkaTopic == null || kafkaTopic.isEmpty()) {
            context.getLogger().log("KAFKA_TOPIC environment variable is not set");
            return;
        }

        props.put("bootstrap.servers", kafkaBootstrapServers);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Convertir les données JSON en chaîne
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(jsonData);

            // Créer un enregistrement Kafka
            ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, "key", jsonString);

            // Envoyer le message
            context.getLogger().log("Sending message to Kafka: " + jsonString);
            RecordMetadata metadata = producer.send(record).get();
            context.getLogger().log("Message envoyé à Kafka. Topic: " + metadata.topic() +
                    ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
        } catch (Exception e) {
            context.getLogger().log("Erreur lors de l'envoi à Kafka : " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }

    private void sendToKafka2(Map<String, Object> jsonData, Context context) {
        // Configuration des propriétés Kafka
        Properties props = new Properties();
        props.put("bootstrap.servers", System.getenv("KAFKA_BOOTSTRAP_SERVERS")); // Serveurs Kafka
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("acks", "all"); // Garantit que le message est bien reçu par tous les ISR (in-sync replicas)
        props.put("request.timeout.ms", "10000"); // Timeout pour les requêtes
        props.put("delivery.timeout.ms", "15000"); // Timeout global pour la livraison des messages
        props.put("retries", 3); // Nombre de tentatives en cas d'échec
        props.put("retry.backoff.ms", "100"); // Temps entre deux réessais

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        try {
            // Convertir les données JSON en chaîne
            ObjectMapper mapper = new ObjectMapper();
            String jsonString = mapper.writeValueAsString(jsonData);

            // Créer un enregistrement Kafka
            String topic = System.getenv("KAFKA_TOPIC");
            ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", jsonString);

            // Envoi asynchrone
            context.getLogger().log("Sending message to Kafka: " + jsonString);
            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    context.getLogger().log("Erreur lors de l'envoi à Kafka : " + exception.getMessage());
                } else {
                    context.getLogger().log("Message envoyé à Kafka. Topic: " + metadata.topic() +
                            ", Partition: " + metadata.partition() + ", Offset: " + metadata.offset());
                }
            });
        } catch (Exception e) {
            context.getLogger().log("Erreur générale dans sendToKafka : " + e.getMessage());
            e.printStackTrace();
        } finally {
            producer.close(); // Toujours fermer le producteur pour libérer les ressources
        }
    }


}