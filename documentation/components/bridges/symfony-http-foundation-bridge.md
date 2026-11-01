---
package: flow-php/symfony-http-foundation-bridge
---

# Symfony Http Foundation Bridge

[PACKAGE_NAV]

[TOC]

The Http Foundation Bridge integrates Symfony Http Foundation with Flow PHP.

`FlowStreamedResponse` is a Symfony Streamed Response that can:

- stream to one of the available formats (CSV, JSON, Parquet, XML)
- apply transformations on the fly
- stream large datasets that usually do not fit in memory
- use ETL to convert data on the fly

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/symfony-http-foundation-bridge.md).

## Usage

Stream a large parquet file, and convert it to CSV format on the fly.
Flow extracts data from datasets in chunks, so FlowStreamedResponse can stream
files that usually do not fit in memory.

```php
<?php

namespace Symfony\Application\Controller;

use Flow\Bridge\Symfony\HttpFoundation\Response\FlowBufferedResponse;
use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Flow\ETL\API\Transformation\AddRowIndex;
use Flow\ETL\API\Transformation\Limit;
use Flow\ETL\API\Transformation\MaskColumns;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\Routing\Attribute\Route;
use function Flow\Bridge\Symfony\HttpFoundation\http_csv_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\Adapter\Parquet\from_parquet;

final class ReportsController extends AbstractController
{
    #[Route('/report/stream', name: 'report_stream')]
    public function streamReport() : FlowStreamedResponse
    {
        return http_stream_open(from_parquet(__DIR__ . '/reports/orders.parquet'))
            ->headers(['X-Custom-Header' => 'Custom Value'])
            ->transform(
                new MaskColumns(['email', 'address']),
                new AddRowIndex()
            )
            ->as('orders.csv')
            ->status(200)
            ->streamedResponse(http_csv_output());
    }

    #[Route('/report', name: 'report')]
    public function bufferReport() : FlowBufferedResponse
    {
        return http_stream_open(from_parquet(__DIR__ . '/reports/orders.parquet'))
            ->transform(
                new Limit(100),
                new MaskColumns(['email', 'address']),
                new AddRowIndex(),
            )
            ->as('orders.csv')
            ->response(http_csv_output());
    }
}
```

## Stream a DataFrame

`http_stream_open()` also takes a DataFrame. The frame runs under its own configuration, so `config()` is refused
for it. Set `analyze()` on the configuration of the frame instead. The response adds its transformations and its
output to the frame. After you build the response, do not use the frame again.

```php
<?php

namespace Symfony\Application\Controller;

use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\Routing\Attribute\Route;
use function Flow\Bridge\Symfony\HttpFoundation\http_csv_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{config_builder, df, lit, ref};

final class OrdersController extends AbstractController
{
    #[Route('/orders/paid', name: 'orders_paid')]
    public function paidOrders() : FlowStreamedResponse
    {
        $orders = df(config_builder())
            ->read(from_parquet(__DIR__ . '/reports/orders.parquet'))
            ->filter(ref('status')->equals(lit('paid')))
            ->select('id', 'total');

        return http_stream_open($orders)
            ->as('paid_orders.csv')
            ->streamedResponse(http_csv_output());
    }
}
```

## Available Outputs

- `Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput`: `http_csv_output()` converts the dataset to CSV format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\JsonOutput`: `http_json_output()` converts the dataset to JSON format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\ParquetOutput`: `http_parquet_output()` converts the dataset to Parquet format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\XMLOutput`: `http_xml_output()` converts the dataset to XML format.

## Modify output on the fly

To change the output on the fly, give FlowStreamedResponse a Transformation. It applies the transformation to the
dataset.

```php
<?php

use Flow\ETL\API\{DataFrame, Transformation};

use function Flow\ETL\DSL\lit;

new class implements Transformation {
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->withColumn('time', lit(\time()));
    }
};
```

The example above adds a new column `time` with the current timestamp to the dataset.

## Collecting Telemetry After Streaming

After a stream of a large dataset, you can collect telemetry data such as the total row count, the execution time,
or the memory usage. Flow has a `StreamCompletionListener` mechanism. It gets a `Report` with statistics about the
streamed data.

To receive a `Report`:
1. Configure `Analyze` in the `Config` with the `config()` method.
2. Set an `onComplete()` callback with the `http_on_complete()` DSL function.

```php
<?php

namespace Symfony\Application\Controller;

use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Flow\ETL\Runtime\Metrics\Report;
use Psr\Log\LoggerInterface;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\Routing\Attribute\Route;
use function Flow\Bridge\Symfony\HttpFoundation\http_json_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_on_complete;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\{analyze, config_builder};

final class ReportsController extends AbstractController
{
    public function __construct(
        private readonly LoggerInterface $logger,
    ) {}

    #[Route('/report/stream', name: 'report_stream')]
    public function streamReport() : FlowStreamedResponse
    {
        return http_stream_open(from_parquet(__DIR__ . '/reports/orders.parquet'))
            ->config(config_builder()->analyze(analyze()))
            ->onComplete(http_on_complete(function (?Report $report) : void {
                if ($report === null) {
                    return;
                }

                $this->logger->info('Stream completed', [
                    'total_rows' => $report->statistics()->totalRows(),
                    'execution_time_seconds' => $report->statistics()->executionTime->inSeconds(),
                    'heap_peak_mb' => $report->statistics()->heap->max()->inMb(),
                    'process_peak_mb' => $report->statistics()->processes->parent->inMb(),
                ]);
            }))
            ->as('orders.json')
            ->streamedResponse(http_json_output());
    }
}
```

### Analyze Options

The `analyze()` function has more options for more detailed statistics:

- `analyze()`: basic statistics (row count, execution time, memory usage)
- `analyze()->withSchema()`: also collects schema information about the dataset
- `analyze()->withColumnStatistics()`: also collects statistics for each column (min, max, null counts)
- `analyze()->withSchema()->withColumnStatistics()`: collects all available statistics

```php ignore
// Collect schema information along with basic statistics
->config(config_builder()->analyze(analyze()->withSchema()))

// Collect everything
->config(config_builder()->analyze(analyze()->withSchema()->withColumnStatistics()))
```

### Report Contents

The `Report` object gives access to:

- `$report->statistics()->totalRows()`: total number of rows streamed
- `$report->statistics()->executionTime->inSeconds()`: execution duration
- `$report->statistics()->executionTime->startedAt`: start timestamp
- `$report->statistics()->executionTime->finishedAt`: end timestamp
- `$report->statistics()->heap->max()`: peak PHP heap plus backend memory, sampled once for each batch
- `$report->statistics()->processes`: OS peak of the process and of each worker: `workerPeak()`, `peakSum()`
- `$report->schema()`: dataset schema (when `withSchema()` is enabled)
- `$report->statistics()->columns`: column statistics (when `withColumnStatistics()` is enabled)
