package com.yahoo.ycsb;

import com.google.gson.*;

import java.lang.reflect.Type;

/**
 * Adapter class to convert ByteIterator into and back from Json.
 */
public class ByteIteratorAdapter implements JsonSerializer<ByteIterator>, JsonDeserializer<ByteIterator> {

  private final String typeIdentifier = "type";
  private final String propertyIdentifier = "properties";

  @Override
  public JsonElement serialize(ByteIterator byteIterator,
                               Type type,
                               JsonSerializationContext jsonSerializationContext) {
    JsonObject result = new JsonObject();
    result.add(typeIdentifier, new JsonPrimitive(byteIterator.getClass().getName()));
    result.add(propertyIdentifier, jsonSerializationContext.serialize(byteIterator));

    return result;
  }

  @Override
  public ByteIterator deserialize(JsonElement jsonElement,
                                  Type type,
                                  JsonDeserializationContext jsonDeserializationContext) throws JsonParseException {
    JsonObject jsonObject = jsonElement.getAsJsonObject();
    String typeString = jsonObject.get(typeIdentifier).getAsString();
    JsonElement element = jsonObject.get(propertyIdentifier);

    try {
      return jsonDeserializationContext.deserialize(element, Class.forName(typeString));
    } catch (ClassNotFoundException e) {
      throw new JsonParseException("Could not find class " + typeString, e);
    }
  }
}