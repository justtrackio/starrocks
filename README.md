# starrocks

Shaded Java UDAF jar for merging Apache DataSketches theta sketches in StarRocks.

## Artifact

This project publishes a fat jar under the Maven coordinates `io.justtrack:starrocks`.

The public Maven repository URL is:

```text
https://justtrackio.github.io/starrocks/
```

## Consuming the artifact

Gradle:

```groovy
repositories {
    maven {
        url = uri("https://justtrackio.github.io/starrocks/")
    }
}

dependencies {
    implementation("io.justtrack:starrocks:1.0.0")
}
```

Maven:

```xml
<repositories>
  <repository>
    <id>justtrackio-public</id>
    <url>https://justtrackio.github.io/starrocks/</url>
  </repository>
</repositories>

<dependency>
  <groupId>io.justtrack</groupId>
  <artifactId>starrocks</artifactId>
  <version>1.0.0</version>
</dependency>
```

The published artifact is the shaded jar, so consumers do not need to add the DataSketches dependency separately.

## Publishing releases

GitHub Actions publishes the Maven repository to the `gh-pages` branch.

- Push a tag like `v1.0.0` to publish that version.
- Or run the `Publish Maven Repository` workflow manually and provide `release_version`.

The workflow updates the Maven repository contents in `gh-pages`, which GitHub Pages serves over HTTPS.
