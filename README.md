README
======

**TritanDb** is a time-series database for Internet of Things Analytics with a rich graph data model and fast lightweight core.
It can be used to ingest, store and query time-series data in real-time.
TRITAnDb stands for Time-series Rapid Internet of Things Analytics Database.

Features
-------

* Best-of-class compression specifically optimised for time-series data.
* Fast queries and aggregation utilising a specialised immutable data structure called TrTables.
* Fast data ingestion utilising a ring buffer.
* Event sourcing to support journaling.
* Accepts out-of-order events.
* Rich graph data model like RDF are supported.
* Powerful graph query engine with support for SPARQL.
* Optimised for Fog Computing across lightweight Things as well as cloud servers.
* Written in Kotlin and runs on the JVM.


Documentation
-------------
* [Docs](https://eugenesiow.gitbooks.io/tritandb)

Installation
------------

You can build TritanDb using Gradle

    git clone https://github.com/eugenesiow/tritandb-kt.git
    cd tritandb-kt
    ./gradlew build


Questions?
----------

