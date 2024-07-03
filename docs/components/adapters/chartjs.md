# Chart JS Adapter

- [⬅️️ Back](../../introduction.md)

Flow PHP's Adapter ChartJS is a meticulously designed library intended to seamlessly integrate ChartJS within your ETL (
Extract, Transform, Load) workflows. This adapter is pivotal for developers seeking to effortlessly render and interact
with charts, ensuring a visually engaging and insightful data transformation journey. By utilizing the Adapter ChartJS
library, developers can leverage a robust set of features tailored for precise chart rendering and interaction through
ChartJS, simplifying complex data visualizations and enhancing data presentation efficiency. The Adapter ChartJS library
encapsulates a comprehensive set of functionalities, offering a streamlined API for managing chart tasks, which is
essential in modern data processing and transformation scenarios. This library showcases Flow PHP's commitment to
delivering versatile and efficient data processing solutions, making it an excellent choice for developers dealing with
data visualization in large-scale and data-intensive environments. With Flow PHP's Adapter ChartJS, managing chart
rendering and interaction within your ETL workflows becomes a more refined and efficient endeavor, harmoniously aligning
with the robust and adaptable framework of the Flow PHP ecosystem.

## Installation

``` 
composer require flow-php/etl-adapter-chartjs
```

## Usage

```php
<?php

use function Flow\ETL\Adapter\ChartJS\{bar_chart, line_chart, pie_chart, to_chartjs_file, to_chartjs_var};
use function Flow\ETL\DSL\{df, first, from_array, lit, ref, refs, sum};

$data = [
    ['Date' => '2023-01-01', 'Revenue' => 10000.53, 'CM' => 5000.12, 'Ads Spends' => 2000.78, 'Storage Costs' => 1000.34, 'Shipping Costs' => 1500.45, 'Currency' => 'USD'],
    ['Date' => '2023-01-02', 'Revenue' => 10234.56, 'CM' => 5102.23, 'Ads Spends' => 2050.12, 'Storage Costs' => 1050.78, 'Shipping Costs' => 1550.99, 'Currency' => 'USD'],
    ['Date' => '2023-01-03', 'Revenue' => 11000.98, 'CM' => 5200.32, 'Ads Spends' => 2100.67, 'Storage Costs' => 1100.87, 'Shipping Costs' => 1600.34, 'Currency' => 'USD'],
    ['Date' => '2023-01-04', 'Revenue' => 10890.34, 'CM' => 5300.98, 'Ads Spends' => 2150.56, 'Storage Costs' => 1150.67, 'Shipping Costs' => 1650.87, 'Currency' => 'USD'],
    ['Date' => '2023-01-05', 'Revenue' => 13750.12, 'CM' => 5950.78, 'Ads Spends' => 2750.78, 'Storage Costs' => 1750.78, 'Shipping Costs' => 2250.12, 'Currency' => 'USD'],
    ['Date' => '2023-02-06', 'Revenue' => 14000.23, 'CM' => 6000.89, 'Ads Spends' => 2800.89, 'Storage Costs' => 1800.89, 'Shipping Costs' => 2300.23, 'Currency' => 'USD'],
];

df()
    ->read(from_array($data))
    ->withEntry('Profit', ref('Revenue')->minus(ref('CM'))->minus(ref('Ads Spends'))->minus(ref('Storage Costs'))->minus(ref('Shipping Costs'))->round(lit(2)))
    ->write(
        to_chartjs_file(
            $chart = bar_chart(
                ref('Date'),
                refs(
                    ref('Revenue'),
                    ref('CM'),
                    ref('Ads Spends'),
                    ref('Storage Costs'),
                    ref('Shipping Costs'),
                    ref('Profit'),
                )
            ),
            $output = __DIR__ . '/Output/bar_chart.html'
        )
    )
    ->run();
```