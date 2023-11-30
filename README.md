![img](docs/assets/img/flow_php_banner_02_2022.png)

Flow is a PHP-based, strongly typed data processing framework with constant memory consumption.

[![Latest Stable Version](https://poser.pugx.org/flow-php/flow/v)](https://packagist.org/packages/flow-php/flow)
[![Latest Unstable Version](https://poser.pugx.org/flow-php/flow/v/unstable)](https://packagist.org/packages/flow-php/flow)
[![License](https://poser.pugx.org/flow-php/flow/license)](https://packagist.org/packages/flow-php/flow)
[![Test Suite](https://github.com/flow-php/flow/actions/workflows/test-suite.yml/badge.svg?branch=1.x)](https://github.com/flow-php/flow/actions/workflows/test-suite.yml)

- 📈 [Project Roadmap](https://github.com/orgs/flow-php/projects/1)
- 📜 [Documentation](docs/introduction.md)
- 🛠️ [Contributing](CONTRIBUTING.md)
- 🚧 [Upgrading](UPGRADE.md)

Supported PHP versions: [![PHP 8.1](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/) [![PHP 8.2](https://img.shields.io/badge/php-~8.2-8892BF.svg)](https://php.net/) [![PHP 8.3](https://img.shields.io/badge/php-~8.3-8892BF.svg)](https://php.net/)

## We Stand Against Terror

<table>
  <thead>
    <tr>
      <td align="center"><a href="https://www.standwithukraine.how/" target="_blank">Stand With Ukraine</a></td>
      <td align="center"><a href="https://www.standwithus.com/">Stand With Us</a></td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td align="center"><img width="256" height="186" alt="Flag of Ukraine" src="https://upload.wikimedia.org/wikipedia/commons/thumb/4/49/Flag_of_Ukraine.svg/256px-Flag_of_Ukraine.svg.png"></td>
      <td align="center"><img width="256" height="186" alt="Flag of Israel" src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d4/Flag_of_Israel.svg/256px-Flag_of_Israel.svg.png"></td>
    </tr>
  </tbody>
</table>

> On Feb. 24, 2022, Russia declared an unprovoked war on Ukraine and launched a full-scale invasion. Russia is currently bombing peaceful Ukrainian cities, including schools and hospitals and attacking civilians who are fleeing conflict zones.

> On Oct. 7, 2023, the national holiday of Simchat Torah, Hamas terrorists initiated an attack on Israel in the early hours, targeting civilians. They unleashed violence that resulted in at least 1,400 casualties and abducted at least 200 individuals, not limited to Israelis.

---

## Introduction

Flow is a first and the most advanced PHP ETL framework. 

📜 [Documentation](docs/introduction.md)

#### What is ETL?

ETL stands for **Extract, Transform, Load** – a process used in database usage and data warehousing. It involves three critical steps:

1. **Extract**: The first step is to extract data from various sources. These sources could be databases, CSV files, online services, or other formats. During extraction, the goal is to retrieve all the necessary data efficiently and accurately.
2. **Transform**: Once the data is extracted, it needs to be transformed. This transformation process involves cleaning the data (removing duplicates, fixing errors), converting it into a suitable format or structure for the purposes of querying and analysis, and applying any business rules or calculations that are needed.
3. **Load**: Finally, the transformed data is loaded into a target database, data warehouse, or a data mart where it can be accessed, queried, and used for business analysis or decision-making processes.

#### Main Use Cases

ETL frameworks are essential in various scenarios, especially in data-driven environments. Some of the key use cases include:
1. **Data Integration**: ETL is fundamental in integrating data from multiple, often disparate, sources. This is crucial for businesses that gather data from various systems and need a unified view.
2. **Business Intelligence (BI)**: For BI processes, ETL is used to collect data from different sources and bring it into a data warehouse, where it can be analyzed to provide business insights.
3. **Data Warehousing**: ETL plays a pivotal role in building and maintaining data warehouses. It helps in structuring large amounts of data into a format that is easy to analyze.
4. **Data Migration**: When organizations change systems or upgrade databases, ETL processes are necessary to migrate data effectively from the old system to the new one.
5. **Data Cleaning and Transformation**: Ensuring data quality is paramount. ETL frameworks are used to clean, standardize, and transform data, thus ensuring high-quality data for analysis.
6. **Historical Data Storage**: ETL is used to extract large volumes of historical data from operational systems and load it into data warehouses for long-term storage, analysis, and reporting.
7. **Reporting and Analysis**: By consolidating data from various sources, ETL frameworks simplify the reporting and analysis process, providing businesses with actionable insights.
8. **Regulatory Compliance**: For compliance with various regulations, organizations use ETL processes to gather, standardize, and store data in a manner that meets regulatory requirements.

## Features of Flow PHP

Flow PHP is a powerful and versatile ETL framework designed to cater to a variety of data processing needs. Below are some of its standout features:

- **Low and Constant Memory Consumption**: Optimized for efficiency, Flow PHP ensures minimal memory usage, maintaining low and constant consumption regardless of data size. This makes it ideal for handling large datasets without compromising performance.
- **Versatile Data Source Interaction**: Whether your data resides in databases, spreadsheets, or online platforms, Flow PHP can seamlessly read from and write to any data source.
- **Rich Collection of Data Transformation Functions**: Transform your data with ease using a wide array of built-in functions. From simple mapping to complex manipulations, the framework covers all your data transformation needs.
- **Direct Access to Remote Filesystems**: Flow PHP provides the ability to interact directly with remote filesystems, facilitating efficient data handling and processing without the need for local storage.
- **Advanced Data Partitioning**: Efficiently partition your data for improved manageability and processing. This feature is crucial for handling large datasets or for processing data in distributed systems.
- **Grouping & Aggregating**: Easily group and aggregate data to extract meaningful insights. This feature is particularly useful for summarizing datasets and performing statistical analysis.
- **Remote File Processing**: Process files stored remotely with the same ease as local files, enabling powerful and flexible data integration from various sources.
- **Join Operations**: Perform join operations between different datasets. This is essential for combining data from multiple sources, providing a more comprehensive view.
- **Efficient Sorting**: Sort your data based on specific criteria or conditions, ensuring that your datasets are organized precisely as needed.
- **ASCII Table Display**: Visualize your datasets as neatly formatted ASCII tables, making it easier to read and analyze data directly from the console.
- **Schema Validation**: Ensure data quality and consistency by validating your datasets against predefined schemas.
- **Window Functions**: Utilize window functions for advanced data analysis, allowing you to perform calculations across sets of rows that are related to the current row.
- **Built-In Caching Mechanism**: Improve performance with built-in caching, reducing processing time and enhancing overall efficiency, especially in repetitive data processing tasks.

Flow PHP’s comprehensive set of features makes it an ideal choice for developers and organizations looking to harness the power of ETL for efficient and effective data processing.

## Usage Example

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\{data_frame, lit, ref, sum, to_output};
use Flow\ETL\Filesystem\SaveMode;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_parquet(__FLOW_DATA__ . '/orders_flow.parquet'))
    ->select('created_at', 'total_price', 'discount')
    ->withEntry('created_at', ref('created_at')->cast('date')->dateFormat('Y/m'))
    ->withEntry('revenue', ref('total_price')->minus(ref('discount')))
    ->select('created_at', 'revenue')
    ->groupBy('created_at')
    ->aggregate(sum(ref('revenue')))
    ->sortBy(ref('created_at')->desc())
    ->withEntry('daily_revenue', ref('revenue_sum')->round(lit(2))->numberFormat(lit(2)))
    ->drop('revenue_sum')
    ->write(to_output(truncate: false))
    ->withEntry('created_at', ref('created_at')->toDate('Y/m'))
    ->mode(SaveMode::Overwrite)
    ->write(to_parquet(__FLOW_OUTPUT__ . '/daily_revenue.parquet'))
    ->run();
```

```console
$ php daily_revenue.php
+------------+---------------+
| created_at | daily_revenue |
+------------+---------------+
|    2023/10 |    206,669.74 |
|    2023/09 |    227,647.47 |
|    2023/08 |    237,027.31 |
|    2023/07 |    240,111.05 |
|    2023/06 |    225,536.35 |
|    2023/05 |    234,624.74 |
|    2023/04 |    231,472.05 |
|    2023/03 |    231,697.36 |
|    2023/02 |    211,048.97 |
|    2023/01 |    225,539.81 |
+------------+---------------+
10 rows
```

## Community Contributions

Flow PHP is not just a tool, but a growing community of developers passionate about data processing and PHP. We strongly believe in the power of collaboration and welcome contributions of all forms. Whether you're fixing a bug, proposing a new feature, or improving our documentation, your input is invaluable to the growth of Flow PHP.

### How You Can Contribute

- **Submitting Bug Reports and Feature Requests**: Encounter an issue or have an idea for an enhancement? Submit an issue on our GitHub repository. Please provide a clear description and, if possible, steps to reproduce the bug or details of the feature request.
- **Code Contributions**: Interested in directly impacting the development of Flow PHP? Check out our issue tracker for areas where you can contribute. From simple fixes to substantial feature additions, every bit of help is appreciated.
- **Improving Documentation**: Good documentation is key to any project's success. If you notice gaps, inaccuracies, or areas that could use better explanations, we encourage you to submit updates.
- **Community Support**: Help out fellow users by answering questions on our community channels, Stack Overflow, or other forums where Flow PHP users gather.
- **Spread the Word**: Share your experiences using Flow PHP, write blog posts, tutorials, or speak at meetups and conferences. Let others know how Flow PHP has helped in your projects!
- **Leave a GitHub Star**: If you find Flow PHP useful, consider giving it a star on GitHub. Your star is a simple yet powerful way to show support and helps others discover our project.

### Contribution Guidelines

To ensure a smooth collaboration process, we've put together guidelines for contributing. 
Please take a moment to read our [Contribution Guidelines](CONTRIBUTING.md) before starting your work. This will help you understand our process and make contributing a breeze.

### Questions?

If you have any questions about contributing, please feel free to reach out to us. We're more than happy to provide guidance and support.

Join us in shaping the future of data processing in PHP — every contribution, big or small, makes a significant difference!

## GitHub Stars

[![Star History Chart](https://api.star-history.com/svg?repos=flow-php/flow&type=Date)](https://star-history.com/#flow-php/flow&Date)

## Sponsors

Flow PHP is sponsored by:

- [Blackfire](https://blackfire.io/) - the best PHP profiling and monitoring tool! 

