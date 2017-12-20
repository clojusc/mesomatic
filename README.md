# mesomatic

[![Build Status][travis-badge]][travis][![Clojars Project][clojars-badge]][clojars][![Clojure version][clojure-v]](project.clj)

*A simple and idiomatic Clojure facade around the Mesos JAVA API*

[![][logo]][logo-large]


Mesomatic provides facilities to interact with [Apache Mesos][mesos] from
clojure. It provides a simple and idiomatic facade around the Mesos JAVA API
and facilities to help when writing mesos frameworks.

Mesomatic versions match the API version they target, a trailing minor
indicates the patch release number, for instance version `1.0.1-r0` will
target mesos `1.0.1`.

Note that the clojusc Github org has volunteered to maintain the library
originally created by [pyr][pyr] at `pyr/mesomatic`. The new location,
`clojusc/mesomatic`, is now the offical home for the library.


## Resources

* An excellent [Mesos presentation][mesos-video] from ApacheCon 2014
* [pyr][pyr]'s Euroclojure 2015 [talk on Mesomatic][mesomatic-video]


## Usage

Add this to your leiningen profile:

```clojure
:dependencies [[clojusc/mesomatic "1.0.1-r1"]]
```

If you want to use the [core.async][core-async] facade,
you will need to pull it in as well:

```clojure
:dependencies [[clojusc/mesomatic "1.0.1-r1"]
               [clojusc/mesomatic-async "1.0.1-r1"]]
```


## Examples

Be sure to examine the [example frameworks][examples] built with mesomatic.


## Namespaces

- `mesomatic.types`: contains a facade to and from all protobuf types.
- `mesomatic.scheduler`: facades for schedulers and scheduler-drivers
- `mesomatic.executor`: facades for executors and executor-drivers
- `mesomatic.async.executor`: produce executor callbacks on a channel
- `mesomatic.async.scheduler`: produce scheduler callbacks on a channel
- `mesomatic.helpers`: utility helpers for cluster decisions


## Type conversions

To go to and from protobuf types, mesomatic uses two simple functions:

- `pb->data`: yields a data structure from a mesos type, usually in the form of
              a record.
- `data->pb`: converts a data structure to a mesos type.
- `->pb`: convert a plain map to a mesos type hinted at by a keyword

By yielding records, mesomatic provides elements which are homomorphic to
maps and can easily be converted back to protobuf.


#### Special cases

A few cases do not yield records:

- Scalar values (`Protos.Value.Scalar`) yield doubles.
- All enums yield keywords.
- Set values (`Protos.Value.Set`) yield sets.
- Some types containing a single repeated field are unrolled
  as a seq of their content, such as `Protos.Value.Ranges`.


## Changelog

### 1.0.1

- Target mesos 1.0.1
- Support for GPU resources
- Updates for API changes in Java bindings

This release was built with help from:

- @oubiwann
- @munk
- @mforsyth
- @dgrnbg
- @alexandergunnarson


## Contributor Resources

- http://mesos.apache.org/documentation/latest/upgrades/
- https://github.com/ContainerSolutions/minimesos
- https://github.com/ContainerSolutions/minimesos-docker
- https://github.com/katacoda/minimesos-examples
- https://github.com/mesos/elasticsearch/tree/master/system-test/src/systemTest/java/org/apache/mesos/elasticsearch/systemtest


<!-- Named page links below: /-->

[travis]: https://travis-ci.org/clojusc/mesomatic
[travis-badge]: https://travis-ci.org/clojusc/mesomatic.png?branch=master
[deps]: http://jarkeeper.com/clojusc/mesomatic
[deps-badge]: http://jarkeeper.com/clojusc/mesomatic/status.svg
[logo]: ux-resources/images/mesomatic-logo-x250.png
[logo-large]: ux-resources/images/mesomatic-logo-x1000.png
[tag-badge]: https://img.shields.io/github/tag/clojusc/mesomatic.svg
[tag]: https://github.com/clojusc/mesomatic/tags
[clojure-v]: https://img.shields.io/badge/clojure-1.9.0-blue.svg
[clojars]: https://clojars.org/clojusc/mesomatic
[clojars-badge]: https://img.shields.io/clojars/v/clojusc/mesomatic.svg
[mesos]: http://mesos.apache.org
[pyr]: https://github.com/pyr
[core-async]: https://github.com/clojure/core.async
[examples]: https://github.com/clojusc/mesomatic-examples
[mesos-video]: https://www.youtube.com/watch?v=hTcZGODnyf0
[mesomatic-video]: https://www.youtube.com/watch?v=X-fVA5DxezE
