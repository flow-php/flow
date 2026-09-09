---
package: flow-php/symfony-http-foundation-bridge
---

# Symfony Http Foundation Bridge

[PACKAGE_NAV]

[TOC]

Http Foundation Bridge provides seamless integration between Symfony Http Foundation and Flow PHP.

`FlowStreamedResponse` is a Symfony Streamed Response that can:

- stream to one of the available formats (CSV, JSON, Parquet, XML)
- apply transformations on the fly
- stream large datasets that normally would not fit in memory
- use ETL to convert data on the fly

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/symfony-http-foundation-bridge.md).

## Usage

Stream a large parquet file converting it on the fly to CSV format.
Since Flow is extracting data from datasets in chunk, FlowStreamedResponse allows to stream
files that normally would not fit in memory.

```php
<?php

namespace Symfony\Application\Controller;

use Flow\Bridge\Symfony\HttpFoundation\Response\FlowBufferedResponse;
use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Flow\ETL\Transformation\AddRowIndex;
use Flow\ETL\Transformation\Limit;
use Flow\ETL\Transformation\MaskColumns;
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

## Available Outputs

- `Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput` - `http_csv_output()` - converts dataset to CSV format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\JSONOutput` - `http_json_output()` -converts dataset to JSON format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\ParquetOutput` - `http_parquet_output()` -converts dataset to Parquet format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\XMLOutput` - `http_xml_output()` -converts dataset to XML format.

## Modify output on the fly

Sometimes we need to modify the output on the fly. 
To do that, FlowStreamedResponse allows passing a Transformation that will be applied on the dataset.

```php
new class implements Transformation {
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->withColumn('time', \time());
    }
};
```

Above example will add a new column `time` to the dataset with the current timestamp.

## Collecting Telemetry After Streaming

When streaming large datasets, you may want to collect telemetry data such as total row counts, execution time,
or memory usage after the streaming completes. Flow provides a `StreamClosure` mechanism that gets called
with a `Report` containing statistics about the streamed data.

To receive a `Report`, you need to:
1. Configure `Analyze` in the `Config` using `config()` method
2. Set an `onComplete()` callback using `http_on_complete()` DSL function

```php
<?php

namespace Symfony\Application\Controller;

use Flow\Bridge\Symfony\HttpFoundation\Response\FlowStreamedResponse;
use Flow\ETL\Config;
use Flow\ETL\Dataset\Report;
use Psr\Log\LoggerInterface;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\Routing\Attribute\Route;
use function Flow\Bridge\Symfony\HttpFoundation\http_json_output;
use function Flow\Bridge\Symfony\HttpFoundation\http_on_complete;
use function Flow\Bridge\Symfony\HttpFoundation\http_stream_open;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\analyze;

final class ReportsController extends AbstractController
{
    public function __construct(
        private readonly LoggerInterface $logger,
    ) {}

    #[Route('/report/stream', name: 'report_stream')]
    public function streamReport() : FlowStreamedResponse
    {
        return http_stream_open(from_parquet(__DIR__ . '/reports/orders.parquet'))
            ->config(Config::builder()->analyze(analyze()))
            ->onComplete(http_on_complete(function (?Report $report) : void {
                if ($report === null) {
                    return;
                }

                $this->logger->info('Stream completed', [
                    'total_rows' => $report->statistics()->totalRows(),
                    'execution_time_seconds' => $report->statistics()->executionTime->inSeconds(),
                    'memory_peak_mb' => $report->statistics()->memory->max()->inMb(),
                ]);
            }))
            ->as('orders.json')
            ->streamedResponse(http_json_output());
    }
}
```

### Analyze Options

The `analyze()` function supports additional options for collecting more detailed statistics:

- `analyze()` - Basic statistics (row count, execution time, memory usage)
- `analyze()->withSchema()` - Also collects schema information about the dataset
- `analyze()->withColumnStatistics()` - Also collects per-column statistics (min, max, null counts)
- `analyze()->withSchema()->withColumnStatistics()` - Collects all available statistics

```php ignore
// Collect schema information along with basic statistics
->config(Config::builder()->analyze(analyze()->withSchema()))

// Collect everything
->config(Config::builder()->analyze(analyze()->withSchema()->withColumnStatistics()))
```

### Report Contents

The `Report` object provides access to:

- `$report->statistics()->totalRows()` - Total number of rows streamed
- `$report->statistics()->executionTime->inSeconds()` - Execution duration
- `$report->statistics()->executionTime->startedAt` - Start timestamp
- `$report->statistics()->executionTime->finishedAt` - End timestamp
- `$report->statistics()->memory->max()` - Peak memory usage
- `$report->schema()` - Dataset schema (when `withSchema()` is enabled)
- `$report->statistics()->columns` - Column statistics (when `withColumnStatistics()` is enabled)
