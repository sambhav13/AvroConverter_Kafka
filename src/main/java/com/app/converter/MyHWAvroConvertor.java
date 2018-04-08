package com.app.converter;

import com.hortonworks.registries.schemaregistry.client.SchemaRegistryClient;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroDeserializer;
import com.hortonworks.registries.schemaregistry.serdes.avro.kafka.KafkaAvroSerializer;
import org.apache.avro.generic.GenericContainer;
import org.apache.avro.generic.IndexedRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaAndValue;
import org.apache.kafka.connect.errors.DataException;
import org.apache.kafka.connect.storage.Converter;

import java.util.HashMap;
import java.util.Map;


/**
 * Created by samgupta0 on 4/5/2018.
 */
public class MyHWAvroConvertor implements Converter   {

    KafkaAvroSerializer serializer;
    KafkaAvroDeserializer deserializer;
    private AvroData avroData;



    public final String DEFAULT_SCHEMA_REG_URL = "http://localhost:7788/api/v1";

    public Map<String, Object> createConfig(String schemaRegistryUrl) {
        Map<String, Object> config = new HashMap<>();
        config.put(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), schemaRegistryUrl);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_SIZE.name(), 10L);
        config.put(SchemaRegistryClient.Configuration.CLASSLOADER_CACHE_EXPIRY_INTERVAL_SECS.name(), 5000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_SIZE.name(), 1000L);
        config.put(SchemaRegistryClient.Configuration.SCHEMA_VERSION_CACHE_EXPIRY_INTERVAL_SECS.name(), 60 * 60 * 1000L);
        return config;
    }


    @Override
    public void configure(Map<String, ?> configs, boolean b) {

        String schemaRegistryUrl = System.getProperty(SchemaRegistryClient.Configuration.SCHEMA_REGISTRY_URL.name(), DEFAULT_SCHEMA_REG_URL);
        Map<String, Object> config = createConfig(schemaRegistryUrl);
        //SampleSchemaRegistryClientApp sampleSchemaRegistryClientApp = new SampleSchemaRegistryClientApp(config);

        SchemaRegistryClient schemaRegistryClient = new SchemaRegistryClient(config);
        this.serializer =  new KafkaAvroSerializer(schemaRegistryClient);
        this.deserializer = new KafkaAvroDeserializer(schemaRegistryClient);
	this.deserializer.configure(configs,false);

        avroData = new AvroData(new AvroDataConfig(configs));
    }

    @Override
    public byte[] fromConnectData(String topic, Schema schema, Object o) {

        byte[] result_ser = this.serializer.serialize(topic,o);
        return result_ser;
        //return new byte[0];
    }

    @Override
    public SchemaAndValue toConnectData(String topic, byte[] value) {

        /*Object result_des = deserializer.deserialize(topic, value);
        if(result_des!=null)
            System.out.println("THE DESERIALIZED VALUE WAS OBTAINED");
        GenericRecord deserialized_record = (GenericRecord) result_des;

        SchemaAndValue sc = new SchemaAndValue(deserialized_record.getSchema(),

                toConnectData(deserialized_record.getSchema(),deserialized_record);
        return null;*/
            try {
		System.out.println("Topic-->"+topic);
		System.out.println("value-->"+value);
                Object deserialized_tmp = deserializer.deserialize(topic, value);
		System.out.println("deserialized OBJECT OBTAINED");
		
		if(deserialized_tmp !=null){
		System.out.println("The record is not null");
		System.out.println(deserialized_tmp);
		System.out.println("The deserialized record class is--->"+deserialized_tmp.getClass());
		
		}
                GenericContainer deserialized = (GenericContainer)deserialized_tmp;
            if (deserialized == null) {
                return SchemaAndValue.NULL;
            } else if (deserialized instanceof IndexedRecord) {
                return avroData.toConnectData(deserialized.getSchema(), deserialized);
            }
            throw new DataException("Unsupported type returned during deserialization of topic %s "
                    .format(topic));
        } catch (SerializationException e) {
            throw new DataException("Failed to deserialize data for topic %s to Avro: "
                    .format(topic), e);
        }
    }



}
