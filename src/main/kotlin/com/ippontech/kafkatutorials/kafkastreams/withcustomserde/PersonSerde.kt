package com.ippontech.kafkatutorials.kafkastreams.withcustomserde

import com.ippontech.kafkatutorials.kafkastreams.Person
import com.ippontech.kafkatutorials.kafkastreams.jsonMapper
import org.apache.kafka.common.serialization.Deserializer
import org.apache.kafka.common.serialization.Serde
import org.apache.kafka.common.serialization.Serializer

class PersonSerde : Serde<Person> {
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
    override fun close() {}
    override fun deserializer(): Deserializer<Person> = PersonDeserializer()
    override fun serializer(): Serializer<Person> = PersonSerializer()
}

class PersonSerializer : Serializer<Person> {
    override fun serialize(topic: String, data: Person?): ByteArray? {
        if (data == null) return null
        return jsonMapper.writeValueAsBytes(data)
    }

    override fun close() {}
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
}

class PersonDeserializer : Deserializer<Person> {
    override fun deserialize(topic: String, data: ByteArray?): Person? {
        if (data == null) return null
        return jsonMapper.readValue(data, Person::class.java)
    }

    override fun close() {}
    override fun configure(configs: MutableMap<String, *>?, isKey: Boolean) {}
}
