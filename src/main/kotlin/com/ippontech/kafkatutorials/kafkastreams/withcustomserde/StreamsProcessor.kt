package com.ippontech.kafkatutorials.kafkastreams.withcustomserde

import com.ippontech.kafkatutorials.kafkastreams.Person
import com.ippontech.kafkatutorials.kafkastreams.agesTopic
import com.ippontech.kafkatutorials.kafkastreams.personsTopic
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.KeyValue
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.apache.log4j.LogManager
import java.time.LocalDate
import java.time.Period
import java.time.ZoneId
import java.util.*

// $ kafka-topics --zookeeper localhost:2181 --create --topic ages --replication-factor 1 --partitions 4

fun main(args: Array<String>) {
    StreamsProcessor("localhost:9092").process()
}

class StreamsProcessor(val brokers: String) {

    private val logger = LogManager.getLogger(javaClass)

    fun process() {
        val streamsBuilder = StreamsBuilder()

//        val personSerde = PersonSerde()
        val personSerde = Serdes.serdeFrom(PersonSerializer(), PersonDeserializer())

        val personStream: KStream<String, Person> = streamsBuilder
                .stream(personsTopic, Consumed.with(Serdes.String(), personSerde))

        val resStream: KStream<String, String> = personStream.map { _, p ->
            val birthDateLocal = p.birthDate.toInstant().atZone(ZoneId.systemDefault()).toLocalDate()
            val age = Period.between(birthDateLocal, LocalDate.now()).getYears()
            logger.debug("Age: $age")
            KeyValue("${p.firstName} ${p.lastName}", "$age")
        }

        resStream.to(agesTopic, Produced.with(Serdes.String(), Serdes.String()))

        val topology = streamsBuilder.build()

        val props = Properties()
        props["bootstrap.servers"] = brokers
        props["application.id"] = "kafka-tutorial"
        val streams = KafkaStreams(topology, props)
        streams.start()
    }
}
