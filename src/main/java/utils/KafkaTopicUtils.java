package utils;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

//Para criar os tópicos de todas as streams de uma maneira mais ordenada
public class KafkaTopicUtils {

    private final AdminClient adminClient;

    //AdminClient com as propriedades fornecidas
    public KafkaTopicUtils(Properties props) {
        Properties adminProps = new Properties();
        adminProps.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, props.getProperty(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG));
        this.adminClient = AdminClient.create(adminProps);
    }

    //Criar um tópico
    public void createTopicIfNotExists(String topicName, int numPartitions, short replicationFactor) {
        try {
            Set<String> existingTopics = adminClient.listTopics().names().get();
            if (!existingTopics.contains(topicName)) {
                NewTopic newTopic = new NewTopic(topicName, numPartitions, replicationFactor);
                adminClient.createTopics(Collections.singleton(newTopic)).all().get();
                System.out.printf("Tópico '%s' criado com sucesso!%n", topicName);
            } else {
                System.out.printf("Tópico '%s' já existe.%n", topicName);
            }
        } catch (ExecutionException | InterruptedException e) {
            System.err.printf("Erro ao criar/verificar o tópico '%s': %s%n", topicName, e.getMessage());
        }
    }

    public void close() {
        if (adminClient != null) {
            adminClient.close();
        }
    }
}