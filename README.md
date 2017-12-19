mesomatic: the cluster is a library
===================================

[![Build Status](https://secure.travis-ci.org/pyr/mesomatic.png)](http://travis-ci.org/pyr/mesomatic)

Mesomatic provides facilities to interact with [Apache Mesos](http://mesos.apache.org)
from clojure. It provides a simple and idiomatic facade around the Mesos JAVA API and
facilities to help when writing mesos frameworks.

Mesomatic versions match the API version they target, a trailing minor indicates
the patch release number, for instance version `1.0.1-r0` will target mesos `1.0.1`.

Note that the clojusc Github org has volunteered to maintain the library originally
created by [pyr](https://github.com/pyr) at `pyr/mesomatic`. The new location,
`clojusc/mesomatic` is now the offical home for the library.

## Usage

Add this to your leiningen profile.

```clojure
:dependencies [[org.spootnik/mesomatic "1.0.1-r0"]]
```

If you want to use the [core.async](https://github.com/clojure/core.async) facade,
you will need to pull it in as well:

```clojure
:dependencies [[org.spootnik/mesomatic       "1.0.1-r0"]
               [org.spootnik/mesomatic-async "1.0.1-r0"]]
```

## Examples

See example frameworks built with mesomatic at 
https://github.com/clojusc/mesomatic-examples
               
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

### 1.0.1-r0

- Target mesos 1.0.1
- Support for GPU resources

This release was built with help from:

- @oubiwann
- @munk
- @mforsyth
- @dgrnbg
- @alexandergunnarson






