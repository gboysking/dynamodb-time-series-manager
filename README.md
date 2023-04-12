# Time Series Statistics Manager

A simple and efficient library for managing time series statistics using AWS DynamoDB.

## Table of Contents

1. [Introduction](#introduction)
2. [Installation](#installation)
3. [Usage](#usage)
4. [Components of the TimeSeriesStatisticsManager](#components-of-the-timeseriesstatisticsmanager)
5. [Example](#example)
6. [Conclusion](#conclusion)

## Introduction

The TimeSeriesStatisticsManager is a TypeScript library designed to manage time series statistics using AWS DynamoDB. It provides an easy-to-use interface for adding and retrieving statistics, with built-in support for multiple time partitions, such as minutes, hours, days, months, and years.

## Installation

Install the library using npm:

```bash
npm install time-series-statistics-manager
```

## Usage

First, import the _TimeSeriesStatisticsManager_ class and create a new instance with your desired options:

```typescript
import { TimeSeriesStatisticsManager } from 'time-series-statistics-manager';

Create a new instance of the TimeSeriesStatisticsManager:

const manager = new TimeSeriesStatisticsManager({});

//Add a statistic:

await manager.addStatistic("example_topic", Date.now());

//Get statistics:

const stats = await manager.getStatistics("example_topic", startTime, endTime);
```

## Components of the TimeSeriesStatisticsManager

1. *TimeSeriesStatisticsManagerOptions*: An object that you can use to configure the TimeSeriesStatisticsManager. It includes the following optional properties: _table_, _client_, and _timePartitions_.
2. *Statistic*: An object representing a single statistic, which includes _topic_, _period_, _count_, and _time_partition_ properties.
3. *TimePartition*: An object that represents a time partition, which includes _name_, _format_, and _interval_ properties.

## Example

Here's a simple example demonstrating how to use the TimeSeriesStatisticsManager:

```typescript
import { TimeSeriesStatisticsManager } from 'time-series-statistics-manager';

const manager = new TimeSeriesStatisticsManager({});

(async () => {
    await manager.addStatistic("example_topic", Date.now());
    
    const startTime = Date.now() - (60 * 60 * 1000); // 1 hour ago
    const endTime = Date.now();
    
    const stats = await manager.getStatistics("example_topic", startTime, endTime);
    
    console.log(stats);
})();
```

## Conclusion

The TimeSeriesStatisticsManager provides a simple and efficient way to manage time series statistics using AWS DynamoDB. With built-in support for multiple time partitions, it makes it easy to add and retrieve statistics for various time ranges. Give it a try and see how it can help you manage your time series data.
