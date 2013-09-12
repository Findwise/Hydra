# Hydra API

For anything shared between stages and other components, the hydra-api package is used as a common base.


### RemotePipeline

Interaction with an instance of hydra-core occurs through the RemotePipeline class, and that class only. Some details about data transfer via the RemotePipeline class are worth mentioning, because they affect the way you handle anything that has passed through this class.

When serializing and deserializing a LocalDocument or LocalQuery some limitations/generalizations are imposed by the Json nature of the serialization:

**Arrays, Lists and Sets:** Arrays of any type and Lists and Sets of any generic type become List<Object> (with ArrayList as the implementing class)

**All integer-types:** All Integers, be they of type int, long, byte or short, become long or int depending on size

**All floating-point types:** All floating point types, be they float or double become double

**Complex objects:** In general, these should work (e.g. placing a Document inside another Document or similar), but might be prone to bugs in some cases. All such serializations become of type Map<String, Object> (the implementing class being HashMap). See SerializationUtils below.

### SerializationUtils

If for some reason want to control serialization yourself, or want to explicitly serialize something to Json for other purposes, use the class com.findwise.tools.SerializationUtils from the API package, which exposes two static methods: String toJson(Object o) and Map<String, Object> fromJson(String s)

All serialization is done using Google Gson.

Serializing an object such as:

```java
class Person {
  private String firstname = "Byggare";
  private String lastname = "Bob";
  private int salary = 4711;
}
```

by the statement `toJson(new Person());` will produce a Json String that looks like `{firstname:“Byggare”, lastname:“Bob”, salary:4711}`, e.g. a `Map<String, Object>`, and will be deserialized as such if one were to write the statement `fromJson(toJson(new Person()))`.


### Stage configurations

Configuration and parameters for stages are stored in Hydra and queried for via the RemotePipeline.getProperties() method. If your stage extends AbstractStage (and it should), this will automatically be handled for you. Add to that, it will inject properties into your stage object at runtime, if you have the proper annotations. An example can be seen below.

```java
@Stage
class MyStage {
  @Parameter
  private String myParameter = "defaultValue";
}
```
Here we have a stage called MyStage annotated with the `@Stage` annotation. If such an annotation exists, AbstractStage will scan for fields marked with the `@Parameter` annotation. If there is a parameter stored in Hydra for this particular stage, with this particular name (`myParameter` in this case) it will be set to whatever value Hydra had stored for it.

Instantiation will happen first though, so whatever value you set at instantiation (in this case defaultValue) will be in the field `myParameter` unless `AbstractStage` overwrites it.

The same caveats for anything that is serialized from Hydra to a `RemotePipeline` object still apply (see above). Remember that generics are not available at runtime, and as such there may be runtime errors if there is a type mismatch in a generic type:

```java
@Stage
class MyStage {
  @Parameter
  private Map<String, String> myMap;
}
```
If myMap stored in Hydra is a map, this will work fine at injection time, since Java type erasure turns this into a `Map<?, ?>` anyway. However, should the map contain a mix of String objects and lists or something similar, a ClassCastException will occur when trying to read the value of that map entry.

In short: keep track of your types in the Hydra datastore as well as in your stage, because a mismatch can be unpredictable.
