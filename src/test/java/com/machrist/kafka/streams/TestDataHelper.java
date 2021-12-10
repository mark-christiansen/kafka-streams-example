package com.machrist.kafka.streams;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.apache.kafka.streams.KeyValue;

import java.util.Base64;

import static org.junit.jupiter.api.Assertions.*;

public class TestDataHelper {

    private ObjectMapper objectMapper = new ObjectMapper();

    public KeyValue<String, String> createValidEncodedJson() {
        String key = "1";
        ObjectNode customer = createCustomer("1", "Jerry", "Seinfeld",
                createAddress("10", "129 West 81st Street", "Apartment 5A", "", "New York", "NY", "US", "10025"),
                createAddress("11", "2880 Broadway", "", "", "New York", "NY", "US", "10025"));
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
        assertAddressEqual(node.get("Address1"), address1);
        assertAddressEqual(node.get("Address2"), address2);
    }

    private void assertAddressEqual(JsonNode jsonAddress, GenericRecord address) {
        if (address == null) {
            assertNull(jsonAddress);
        } else {
            assertStringEqual(jsonAddress.get("Id"), (String) address.get("id"));
            assertStringEqual(jsonAddress.get("AddressLine1"), (String) address.get("addressLine1"));
            assertStringEqual(jsonAddress.get("AddressLine2"), (String) address.get("addressLine2"));
            assertStringEqual(jsonAddress.get("AddressLine3"), (String) address.get("addressLine3"));
            assertStringEqual(jsonAddress.get("City"), (String) address.get("city"));
            assertStringEqual(jsonAddress.get("State"), (String) address.get("state"));
            assertStringEqual(jsonAddress.get("Country"), (String) address.get("country"));
            assertStringEqual(jsonAddress.get("PostalCode"), (String) address.get("postalCode"));
        }
    }

    private ObjectNode createCustomer(String id, String firstName, String lastName, ObjectNode address1, ObjectNode address2) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("Id", id);
        objectNode.put("FirstName", firstName);
        objectNode.put("LastName", lastName);
        objectNode.set("Address1", address1);
        objectNode.set("Address2", address2);
        return objectNode;
    }

    private ObjectNode createAddress(String id, String line1, String line2, String line3, String city, String state, String country, String postalCode) {
        ObjectNode objectNode = objectMapper.createObjectNode();
        objectNode.put("Id", id);
        objectNode.put("AddressLine1", line1);
        objectNode.put("AddressLne2", line2);
        objectNode.put("AddressLine3", line3);
        objectNode.put("City", city);
        objectNode.put("State", state);
        objectNode.put("Country", country);
        objectNode.put("PostalCode", postalCode);
        return objectNode;
    }

    private void assertStringEqual(JsonNode expected, String actual) {
        if (expected == null) {
            assertNull(actual);
        } else {
            assertEquals(expected.asText(), actual);
        }
    }
}
