# primitive

Copyright (C) 2020, Donald McLean. All rights reserved.

This program and the accompanying materials are licensed under
the terms of the GNU General Public License version 3.0
as published by the Free Software Foundation.

## Experimental Lift based Neo4J library

### Goals

The primary goal of this effort is to create a library that makes it easier to develop Neo4J applications in Scala.
The Java driver for Neo4J has some peculiarities as to how results are returned and I would rather not have to deal
with them on a regular basis.

The secondary goal of this effort is to wrap all of the calls in an actor based framework. Most of the calls will
return futures rather than direct results.

The third goal is to create an English based DSL for the queries, something that is more human readable. Yes, I realize
that "human readable" is a matter of personal taste, but I'm not planning on changing any of the fundamental elements,
only using English words instead of symbols.

### Modules and Module Loader

