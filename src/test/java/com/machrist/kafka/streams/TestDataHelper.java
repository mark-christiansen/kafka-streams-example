package com.machrist.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.streams.KeyValue;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

public class TestDataHelper {

    private ObjectMapper objectMapper = new ObjectMapper();

    public KeyValue<String, String> createFullCustomerEncodedJson() {
        String key = "1";
        ObjectNode customer = createCustomer("1", "Jerry", "Seinfeld", 70,
                createAddress("10", "129 West 81st Street", "Apartment 5A", "", "New York", "NY", "US", "10025"),
                createAddress("11", "2880 Broadway", "", "", "New York", "NY", "US", "10025"));
        String value = customer.toString();
        String encodedValue = new String(Base64.getEncoder().encode(value.getBytes()));
        return new KeyValue<>(key, encodedValue);
    }

    public KeyValue<String, String> createCustomerEncodedJson() {
        String key = "1";
        ObjectNode customer = createCustomer("1", "Jerry", "Seinfeld", 70, null, null);
        String value = customer.toString();
        String encodedValue = new String(Base64.getEncoder().encode(value.getBytes()));
        return new KeyValue<>(key, encodedValue);
    }

    public void assertCustomerEqual(String encodedJson,
                                    GenericRecord customer,
                                    GenericRecord address1,
                                    GenericRecord address2) throws JsonProcessingException {

        String json = new String(Base64.getDecoder().decode(encodedJson));
        JsonNode node = objectMapper.readTree(json);

        assertStringEqual(node.get("Id"), (String) customer.get("id"));
        assertStringEqual(node.get("FirstName"), (String) customer.get("firstName"));
        assertStringEqual(node.get("LastName"), (String) customer.get("lastName"));
        assertIntEqual(node.get("Age"), (Integer) customer.get("age"));
        assertAddressEqual(node.get("Address"), address1);
        assertAddressEqual(node.get("Address2"), address2);
    }

    private void assertAddressEqual(JsonNode jsonAddress, GenericRecord address) {
        if (address == null) {
            assertNull(jsonAddress);
        } else {
            assertStringEqual(jsonAddress.get("Id"), (String) address.get("id"));
            assertStringEqual(jsonAddress.get("Line1"), (String) address.get("addressLine1"));
            assertStringEqual(jsonAddress.get("Line2"), (String) address.get("addressLine2"));
            assertStringEqual(jsonAddress.get("City"), (String) address.get("city"));
            assertStringEqual(jsonAddress.get("State"), (String) address.get("state"));
            assertStringEqual(jsonAddress.get("Zip"), (String) address.get("postalCode"));
        }
    }

    private ObjectNode createCustomer(String id, String firstName, String lastName, int age, ObjectNode address1, ObjectNode address2) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("Id", id);
        objectNode.put("FirstName", firstName);
        objectNode.put("LastName", lastName);
        objectNode.put("Age", age);
        if (address1 != null) {
            objectNode.set("Address", address1);
        }
        if (address2 != null) {
            objectNode.set("Address2", address2);
        }
        return objectNode;
    }

    private ObjectNode createAddress(String id, String line1, String line2, String line3, String city, String state, String country, String postalCode) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("Id", id);
        objectNode.put("Line1", line1);
        objectNode.put("Line2", line2);
        objectNode.put("City", city);
        objectNode.put("State", state);
        objectNode.put("Zip", postalCode);
        return objectNode;
    }

    private void assertStringEqual(JsonNode expected, String actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertEquals(expected.asText(), actual);
        }
    }

    private void assertIntEqual(JsonNode expected, Integer actual) {
        if (expected == null) {
            assertNull(actual);
        } else if (actual == null) {
            fail();
        } else {
            assertEquals(expected.intValue(), actual.intValue());
        }
    }
}
