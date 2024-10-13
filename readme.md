
# Open Feature Store (OFS)

Open Feature Store is a type-enforced feature storage solution using JanusGraph.
OFS aims to parallel how data is represented in Object-Oriented languages to aid in search.


OFS was created to act as a core for my future projects. If others find this project useful, let me know and I'll look at expanding on the idea and making it more accessible. 

## Getting Started

#### Configuring the Graph

OFS requires these properties in your JanusGraph Configuration
```
schema.default=none
graph.set-vertex-id=true
graph.allow-custom-vid-types=true
```

#### Getting the OFS API

Build your OFSConfiguration Params object and pass that into the OpenFeatureStore object along with your JanusGraph interface.

```
OFSConfigurationParams params = OFSConfigurationParams.build()
OpenFeatureStore store = new OpenFeatureStore(graph, params);
```

#### OFS Installation

OFS requires running the installer before use to populate the required JanusGraph properties, indices, etc.

```
OFSInstaller installer = openFeatureStore.getOpenFeatureStoreInstaller()
installer.install();
```

## Design Elements

### Properties

#### Primitive Properties

Primitive Properties are a wrapper around JanusGraph Property Keys. All object fields with the exception of allowableValues are used directly for the JanusGraph PropertyKey. [See the JanusGraph docs on Property Keys for more information.](https://docs.janusgraph.org/v0.3/basics/schema/)

OFS provides support for the field allowableValues. Allowable values are used when setting property values on OFS elements. If the value is not in the allowable values set, it is rejected.

```
new PrimitivePropertyKey<Character>(
  "testCharacterProp",                    // Property Name
  Character.class,                        // Property Type, See JanusGraph supported Types
  Cardinality.SINGLE,                     // Property Cardinality,
  new HashSet<>(Arrays.asList('a', 'b'))  // Property Allowable Values
);
```

#### Reference Properties

Reference Properties are OFS specific and aim to act as a way to link an OFS element to a Feature as a setable property.
For example, one Feature (ex. County) might reference another Feature (ex. State).

Reference Properties support setting the Name, ModelId (ID of the model which Features being referenced must inherit from), Cardinality and Allowable Values.

```
new RefPropertyKey(
  "testpropref",                                  // Property Name
  superModel.getId(),                             // Id of the Model the Property is using
  Cardinality.SINGLE,                             // Property Cardinality,
  new HashSet<>(Arrays.asList(feature.getId()))   // Property Allowable Feature Values
);
```

### Indices

Indices are a wrapper around the JanusGraph index API. [See the JanusGraph docs on Indexing for more information.](https://docs.janusgraph.org/schema/index-management/index-performance/).

```
new Index(
  "testIndex",              // Index Name
  indexProps,               // Map of Property Key -> JanusGraph Mapping Parameter
  IndexElementType.VERTEX,  // Type of Element Index (On Vertex or Edge)
  IndexType.COMPOSITE,      // Type of index (On Vertex or Edge)
  false                     // Unique or Not
);
```

### Schemas

Schemas are OFS objects which define what properties are required for inheritors to provide. 
They also allow for setting default values into properties which will be used if an inheritor does not provide the property.

```
List<Property> defaultProperties = new ArrayList<>();
defaultProperties.add(new Property("prop", "test"));
defaultProperties.add(new Property("proplong", 2L));

Set<String> requiredPropertyKeys = new HashSet<>(Arrays.asList("prop", "proplong", "propNoDefault"));

new ModelSchema(
  requiredPropertyKeys, // Keys the schema will require to implement
  defaultProperties     // Default properties on the Schema inheritors can override
);
```

### Models

Models are OFS objects which wrap a Feature Schema and hold metadata defined by their Model Schema.
Models can inherit from x number of other Models. This allows building of an inheritance tree of Feature Schemas for Features 
implementing those models to be required to implement.

```
// create Feature Schema for the Model
List<Property> fproperties = new ArrayList<>();
fproperties.add(new Property("testprop", "test"));
fproperties.add(new Property("testprop2", 2L));
fproperties.add(new Property("testprop3"));
FeatureSchema featureSchema = new FeatureSchema(fproperties);

// create the model
new Model(
  inheritsFromIds,   // Ids of the Models that this Model inherts from for Feature Schemas
  modelProps,        // Properties the Model has to fit the stated Model Schema
  modelSchemaId,     // ID of the Schema to use for the Model
  featureSchema      // Schema that Features using this Model must implement
);
```

*Note: Typically all models will reference the same Model Schema.*

### Views

Views are OFS objects which allow for building a custom structure to work with Models and other Views.
They enable the ability to only see which Models and Views an application needs for different Use Cases.

```
new View(
  modelIds,         // IDs of the Models this View will reference
  viewIds,          // IDs of the Views this View will reference
  viewProps,        // Properties the View has to fit the stated View Schema
  viewSchemaId      // ID of the Schema to use for the View
);
```

*Note: Typically all views will reference the same View Schema.*

### Features

Features are OFS objects which store properties and inherit from a Model.
Features are required to define all properties of the Models they inherit from if a default property is not provided in the Feature Schema.

```
List<Property> featureProps = new ArrayList<>();
featureProps.add(new Property("testprop", "test"));
featureProps.add(new Property("testprop2", 2L));
featureProps.add(new Property("testprop3", "test"));

new Feature(
  modelId,      // ID of the model this view will inherit properties from
  featureProps  // Properties the Featire has to fit all inherited Model's Feature Schemas
);
```

### State Export/Import

OFS provides an interface through the StateManager to export & import the entire graph. This can be useful to transfer between storage/index JanusGraph backends or as a way to kickstart an install with a pre-configured graph.

```
StateManager manager = openFeatureStore.getStateManager();
Path exportDir = getExportDir();
manager.exportOFSState(exportDir); // Exports graph state to the exportDir
```

## Building OFS

OFS is a Maven project built with Java 17. You can take a look at the build.yml github workflow for specifics
on how it can be built. 

## Limitations

- For eventually consistent backends, delete operations could result in consistency problems. OFS has a resource lock system to prevent
issues like deleting Models while data ingesting is pointing at that Model, but that system may not work with those backends.
You can still use these backends, just keep this limitation in mind.

## Distributed Considerations

### Resource Locking
OFSConfigurationParams contains a parameter for ResourceLock.
This is used to lock resources during operations to prevent issues like deleting a Model while
another thread is attempting to reference that Model on a Feature. OFS by default uses a local cache,
so in the distributed case an implementer needs to provide a distributed implementation of that interface.
For example, wrapping calls to a cache like Redis is ideal.


## TODO

- More extensive usage documentation
- Batch calls for relevant operations (create, cast?)
- Support an OFS graph version (for upgrade & export)
- Iterator solution for property and index lists (for export)
- Remove unneeded calculated data from graph export (index status & instanceof in models)
- More tests for import/export
   - Test for custom GraphStorage system and ImportCacheProvider
- Ability to update Model default Feature schema values
   - The schema required keys should not change but the defaults should be able to

## Ideas

- Threaded state import & export
- Add option to allow for storing fields on element nodes (like features) not present on the schema (with an option to filter them out or not on read and search)
  - keep these in mind during Feature casting to new model


