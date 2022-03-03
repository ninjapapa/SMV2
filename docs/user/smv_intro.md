# SMV Application

## Introduction
SMV is a very simple data application framework that enables the following:

* Ability to group DataFrame operations into modules and organize the modules and their dependency in a hierarchical manner
* No overhead for developers to use DataFrame to manipulate data and create new data sets and variables
* Ability to use the same framework to quickly create ad-hoc analysis and which can easily transformed into a production package
* Easy testing of module levels. Developer shouldn't have to create repetitive and cumbersome scaffolding to test a module
* Intermediate data persistence and versioning. Data versioning is critical for applying test driven development to data applications
* Ability to test/run at the module level and at the application level. Developers are able to test/run modules they currently working on
* Creation of execution graphs at the application level.
* Utilities for managing the end to end dependency graph, persisted data meta info and execution logging

## SmvDataSet

The objective of Spark Modularized View is to modularize data applications. This modularity is captured in `SmvDataSet`. An `SmvDataSet` is a sequence of Spark `DataFrame` operations which takes multiple input `DataFrames` and generates a single `DataFrame`. Since each `SmvDataSet` produces a single `DataFrame`, we can bind an `SmvDataSet's` code with its output. On the other hand, all the `DataFrames` an `SmvDataSet` depends on can be represented by the `SmvDataSet` which generated them. Therefore, the entire data flow can be organized as a collection of `SmvDataSets` which form a directed graph.
