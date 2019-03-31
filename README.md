# cpp-db-lib
In-process database library in C++.

## What is this?
With the capabilities of modern C++ standards, it should be possible to define a persistency layer directly in C++, with no external processes.

Ideally, this should support

- Defining a schema in C++ header files
- Support for relationships between classes, declared and checked at compile time
- Automatic serialization of classes, without extra code
- Checking of constraints after operations
- Collections of objects as members
- Transactions
- Storage should be JSON or JSON-lines based, using the filesystem, for ease of mainteinance.

Moreover, it should be possible to provide easy ways to build JSON APIs out of this, possibly with automatically-generated authentication support, to make it easy to create typical backends for web applications.

This is in a very experimental stage, and mostly exists only in my head. The `legacy` folder contains a previous, failed attempt at building this.
