---
marp: true
theme: gaia
size: 4K
class: default
paginate: true
footer: if(:bird:) @asm0di0 else @asm0dey
title: Kotlin for Apache Spark
backgroundImage: "linear-gradient(to bottom, #000 0%, #1a2028 50%, #293845 100%)"
color: white

---
<!--
_backgroundImage: "linear-gradient(to bottom, #000 0%, #1a2028 50%, #293845 100%)"
_class: lead
_paginate: false
_footer: ""
-->

<style>
footer {
    display: table
}
.hljs-variable { color: lightblue }
.hljs-string { color: lightgreen }
.hljs-params { color: lightpink }
.highlighted-line {
  background-color: #14161a;
  display: block;
}
</style>


# Love to Frankenstein's monster:
# Kotlin for Apache Spark

Pasha Finkelshteyn, JetBrains

---

# Vanity fair

- 14 years in IT
- 11 years in development
    - Developer
    - Team Lead and even CTO (O_o)
- 1 year in data engineering
- And now building tools for data engineers at JetBrains :heart:

![bg right:30% fit](https://plugins.jetbrains.com/files/12494/95821/icon/pluginIcon.svg)[Big Data Tools](https://plugins.jetbrains.com/plugin/12494-big-data-tools)

---
<!-- _class: lead -->
# <!-- fit --> I :heart:![fit drop-shadow bg right:30%](https://upload.wikimedia.org/wikipedia/commons/7/74/Kotlin-logo.svg)

---

# Story of my love with Kotlin

- Used it in production several weeks before first release
- Was first adopter in Sberbank's production
- Giving talks on it
- Trying to use it **EVERYWHERE**

---

# My story with data engineering

Data engineering is all about Java, Python and Scala.

And of course I've started with Scala — it's native!

But soon I realized that lots of things could be easier with Kotlin!

---

# Kotlin key benefits

- JVM
- `null`-aware and null-safe type system
- Extension methods (and values)
- Reified generics
- DSL-building abilities
- Compatible (?) with other JVM ecosystem languages

---

# Note on null-aware type system

![fit drop-shadow](images/types-hier.png)

---

# Note on null-aware type system 

```kotlin {1,4-5}
class Z

fun main(){
    val nullZ: Z? = null
    val z = Z()
    println(nullZ is Z)  // false
    println(z is Z)      // true
    println(nullZ is Z?) // true
    println(z is Z?)     // true
}
```

---

# Note on null-aware type system 

```kotlin {1,4-6}
class Z

fun main(){
    val nullZ: Z? = null
    val z = Z()
    println(nullZ is Z)  // false
    println(z is Z)      // true
    println(nullZ is Z?) // true
    println(z is Z?)     // true
}
```

---

# Note on null-aware type system 

```kotlin {1,4-5,7-9}
class Z

fun main(){
    val nullZ: Z? = null
    val z = Z()
    println(nullZ is Z)  // false
    println(z is Z)      // true
    println(nullZ is Z?) // true
    println(z is Z?)     // true
}
```

---

# Extension methods

```kotlin
fun Iterable<Int>.sum() = reduce { a, b -> a + b }
```
This :arrow_up: already exists in stdlib

---

# Reified generics

* Generics on JVM **can't** be reified. Ever.
* But there are *inline* methods in Kotlin. They will be inlined at compile time:
    ```kotlin
    inline fun runIt(func: () -> Unit) = func()
    ```
* And if method is inlined we can reify generic at call site!
    ```kotlin
    inline fun <reified T> callIt(func: () -> T): T = func()
    ```

---

# DSL-building

Extension functions in conjunction with functional arguments allow us to following magic:

```kotlin
fun html(init: HTML.() -> Unit): HTML {
    val result = HTML()
    HTML.init() // or return HTML().apply { init () }
    return result
}
fun HTML.h1(text: String) = addElement("<h1>$text<h1>")
html { h1("Example") }
```

---

![bg](https://source.unsplash.com/9pw4TKvT3po)

# And the journey begins

---

# I mean

![bg](https://source.unsplash.com/2UDlp4foic4)

---

# <!-- fit --> But why Frankenstein's?
# <!-- fit --> And why monster?

1. It is alike my surname
1. We need to crossbreed Kotlin and Scala to produce something with best parts of both worlds!
1. And at the start of experiment we don't have any idea on its behavior!

![bg right:40%](images/frankenstein.jpg)

---

# First sketch

![bg right:40%](https://source.unsplash.com/FrQKfzoTgsw)

```kotlin
val spark = SparkSession.orCreate
listOf("a" to "and", "b" to "beetle")
    .map(MapFunction { it }, Encoders.bean())
    .show
```

## Fails

Encoder can't (de)serialize

---

# Primitive encoders

```kotlin
@JvmField val ENCODERS = mapOf<KClass<out Any>, Encoder<out Any>>(
    Boolean::class to Encoders.BOOLEAN(),
    Byte::class to Encoders.BYTE(),
    Short::class to Encoders.SHORT(),
    Int::class to Encoders.INT(),
    Long::class to Encoders.LONG(),
    Float::class to Encoders.FLOAT(),
    Double::class to Encoders.DOUBLE(),
    String::class to Encoders.STRING(),
    BigDecimal::class to Encoders.DECIMAL(),
    Date::class to Encoders.DATE(),
    Timestamp::class to Encoders.TIMESTAMP(),
    ByteArray::class to Encoders.BINARY()
) 
```

---

# Getting real

```kotlin
inline fun <reified T> encoder(): Encoder<T>? = 
    ENCODERS[T::class] as? Encoder<T>? ?: 
    Encoders.bean(T::class.java) //Encoders.kryo(T::class.java) 
```

 - Will work for correct Java Beans
 - Will work for primitives
 - Won't work for `data` classes

 ---

 # Getting real

```kotlin
inline fun <reified T> SparkSession.toDS(list: List<T>): Dataset<T> = 
    createDataset(list, encoder<T>()) 
```

Gives us ability to perform first piece of magic:

```kotlin
spark.toDS(listOf(1, 2, 3)).show()
```

And it starts to look like DSL!
And we love Spark to be DSL

---

# Type inference!

- Generics are everywhere
- Generics are being erased at runtime
- Need to find some hack

---

# Jackson

```kotlin {2,4,10}
public abstract class TypeReference<T> {
    protected final Type _type;
    protected TypeReference() {
        Type superClass = getClass().getGenericSuperclass();
        if (superClass instanceof Class<?>) {
            // ↑ sanity check, should never happen
            throw new IllegalArgumentException(/* */);
            // ↑ comment that not enough data
        }
        _type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public Type getType() { return _type; }
```

---

# Jackson

```kotlin {5-9}
public abstract class TypeReference<T> {
    protected final Type _type;
    protected TypeReference() {
        Type superClass = getClass().getGenericSuperclass();
        if (superClass instanceof Class<?>) {
            // ↑ sanity check, should never happen
            throw new IllegalArgumentException();
            // ↑ comment that not enough data
        }
        _type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public Type getType() { return _type; }
```

---

# Now let's translate it to Kotlin

```kotlin
abstract class TypeRef<T> protected constructor() {
    var type: ParameterizedType
    init {
        val sC = this::class.java.genericSuperclass
        require(sC !is Class<*>) { "error" }
        // ↑ should never happen
        this.type = sC as ParameterizedType
```

Easy!

---

# And use it…

```kotlin
fun obtainGenericDataSchema(typeImpl: ParameterizedTypeImpl): DataType {
    val z = typeImpl.rawType.kotlin.declaredMemberProperties
    val y = typeImpl.actualTypeArguments
    return StructType(
            KotlinReflectionHelper
                    .dataClassProps(typeImpl.rawType.kotlin)
                    .map {
                        val dt = if(!it.c.isData) 
                            JavaTypeInference.inferDataType(it.c.java)._1 
                                else null
                        StructField(it.name, dt, it.nullable, Metadata.empty())
                    }
                    .toTypedArray()
```

---
<!-- _class: lead -->
![bg right:50%](https://media.giphy.com/media/l3vQXALZIGo6CACVq/source.gif)

# <!-- fit --> And it won't work

Getting a little scary, right?

---

# And it won't work

Because Jackson's hack won't work in Kotlin

By the way, it's the beginning of the story of love to the Monster!

Because it's boring when everything works.

---

# What should I do now? Google!

```kotlin
inline fun <reified T : Any> getKType(): KType = object : SuperTypeTokenHolder<T>() {}.getKTypeImpl()
open class SuperTypeTokenHolder<T>
fun SuperTypeTokenHolder<*>.getKTypeImpl(): KType = javaClass.genericSuperclass.toKType().arguments.single().type!!
fun KClass<*>.toInvariantFlexibleProjection(arguments: List<KTypeProjection> = emptyList()): KTypeProjection {
    val args = if (java.isArray()) listOf(java.componentType.kotlin.toInvariantFlexibleProjection()) else arguments
    return KTypeProjection.invariant(createType(args, nullable = false))
}
fun Type.toKTypeProjection(): KTypeProjection = when (this) {
    is Class<*> -> this.kotlin.toInvariantFlexibleProjection()
    is ParameterizedType -> {
        val erasure = (rawType as Class<*>).kotlin
        erasure.toInvariantFlexibleProjection((erasure.typeParameters.zip(actualTypeArguments).map { (parameter, argument) ->
            val projection = argument.toKTypeProjection()
            projection.takeIf {
                parameter.variance == KVariance.INVARIANT || parameter.variance != projection.variance
            } ?: KTypeProjection.invariant(projection.type!!)
        }))
    }
    is WildcardType -> when {
        lowerBounds.isNotEmpty() -> KTypeProjection.contravariant(lowerBounds.single().toKType())
        upperBounds.isNotEmpty() -> KTypeProjection.covariant(upperBounds.single().toKType())
        else -> KTypeProjection.STAR
    }
    is GenericArrayType -> Array<Any>::class.toInvariantFlexibleProjection(listOf(genericComponentType.toKTypeProjection()))
    is TypeVariable<*> -> TODO() // TODO
    else -> throw IllegalArgumentException("Unsupported type: $this")
}

fun Type.toKType(): KType = toKTypeProjection().type!!
```

---
# Sponsor of this horror above:
# Alexander Udalov (JetBrains, Kotlin)

This is already deprecated

Because `typeOf<T>()` introduced

---

# Sidenote about `typeOf`

- Works in compile time
- Still experimental **
- Much faster then Scala's `TypeTags`s






** do̳͚n'͓̫̞̝̳t̲̜ ͇u̮̳̲͚̯s̯̬̯e̙͈ ̬i̙̟̹̘̳t̙̻͍ ͇̬̫i̜͖͕n ̪̰̝pr̠͕o̺͉̞͖d͖̺̦͚͔u̟̝̜c͍̺t̬͔̯̝̗̜i̮̣͇͔̻̥o̮̙͉̹͎͍n̰̼: p͕̻̥̪̩̻̱̼o̖͕̖̱͎̬̲̺̝͈̝̟w̜̮̺͉̟̱͎̳̫͙͙̩͙͓̞̖̫ͅe̜̰̘͓̮̯r͖͈̮̦̝͖̱͚ ͔̯̭c͚̱͇̦̯ͅo̰̣͔͚͙͙̜̗r̙̫̰͍̝̤͔̻͙̥̤͎̜̩͕̫͇͖r͉̬̭̬̪̲̙͍͖͕͙̘̣͉u͉̥̟̻̜̖̥̮̜̲̲̻̤̥̭̲̟p̞̲̝̫̫̗̬̺͚͖̤t͍̩̼̮̺̯̬ͅs̺͚̙̙̫̝̠̻̯͙̭


---

# New inference

```kotlin {2}
fun schema(type: KType, map: Map<String, KType> = mapOf()): DataType {
    val primitiveSchema = knownDataTypes[type.classifier]
    if (primitiveSchema != null) 
        return KSimpleTypeWrapper(
            primitiveSchema, 
            (type.classifier!! as KClass<*>).java, 
            type.isMarkedNullable
        )
    val klass = type.classifier as? KClass<*> ?: 
        throw IllegalArgumentException("Unsupported type $type")
    val args = type.arguments
```

---

# New inference

```kotlin {3-8}
fun schema(type: KType, map: Map<String, KType> = mapOf()): DataType {
    val primitiveSchema = knownDataTypes[type.classifier]
    if (primitiveSchema != null) 
        return KSimpleTypeWrapper(
            primitiveSchema, 
            (type.classifier!! as KClass<*>).java, 
            type.isMarkedNullable
        )
    val klass = type.classifier as? KClass<*> ?: 
        throw IllegalArgumentException("Unsupported type $type")
    val args = type.arguments
```

---

# New inference

```kotlin {11}
fun schema(type: KType, map: Map<String, KType> = mapOf()): DataType {
    val primitiveSchema = knownDataTypes[type.classifier]
    if (primitiveSchema != null) 
        return KSimpleTypeWrapper(
            primitiveSchema, 
            (type.classifier!! as KClass<*>).java, 
            type.isMarkedNullable
        )
    val klass = type.classifier as? KClass<*> ?: 
        throw IllegalArgumentException("Unsupported type $type")
    val args = type.arguments
```

---

# New inference

```kotlin
    val types = transitiveMerge(
        map,
        klass
            .typeParameters
            .zip(args)
            .map { it.first.name to it.second.type!! }
            .toMap()
    )
```

`transitiveMerge`: remember GenericSignature → RuntimeType

---

# New inference

```kotlin
val structType = StructType(
    klass
        .primaryConstructor!!
        .parameters
        .filter { it.findAnnotation<Transient>() == null }
        .map {
            val projectedType = types[it.type.toString()] ?: it.type
            val propertyDescriptor = 
            PropertyDescriptor(it.name, klass.java, "is" +
                    it.name?.capitalize(), null)
            KStructField(
                propertyDescriptor.readMethod.name, 
                StructField(it.name, schema(projectedType, types), 
                    projectedType.isMarkedNullable, Metadata.empty()))}
```

---

# <!-- fit --> Dark side of Scala :ghost:

- `KStructField`: wrapper over StructField
- `KSimpleTypeWrapper`: wrapper over primitive type
- `KDataTypeWrapper`: wrapper over data class

These wrappers hold real classes, nullability etc.

Because Scala can't infer them!

---
# <!-- fit --> Dark side of Scala :ghost:

1. Add custom handling methods:
    ```kotlin
     def serializerFor(cls: java.lang.Class[_], dt: DataTypeWithClass) = …
     def deserializerFor(cls: java.lang.Class[_], dt: DataTypeWithClass) = …
    ```
---
# <!-- fit --> Dark side of Scala :ghost:

2. Add custom logics when there is predefined schema:
    ```scala
    case _ if predefinedDt.isDefined =>
        predefinedDt.get match {
          case dataType: KDataTypeWrapper =>
            val cls = dataType.cls
            val properties = getJavaBeanReadableProperties(cls)
            val structFields = dataType.dt.fields.map… //boring
            val fields = structFields.map { structField =>
            // recursive here …
        createSerializerForObject(inputObject, fields)
    ```

---
# <!-- fit --> Dark side of Scala :ghost:

- LOTS of debug here
- Scala is very type safe. Bit codegen is not!
- So use 
    - `LogLevel.DEBUG` 
    - `"spark.sql.codegen.comments" true` to view data flow

---
<!-- _class: lead -->
# <!-- fit --> And it's alive!

![bg right ](https://media.giphy.com/media/tze1mGedykiuk/source.gif)

---

# Future plans

Support for:
- Scala
    - 2.11
    - 2.13
    - 3 (?)
- Spark
    - 2.3.3
    - 2.4.0+

---
<!-- _class: lead -->
# <!-- fit --> Give it a try!

## [JetBrains/kotlin-spark-api](https://github.com/JetBrains/kotlin-spark-api)

---

<!-- _class: lead -->
# Thanks!
## Questions?
### if(:bird:) @asm0di0 else @asm0dey
