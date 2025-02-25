# Symfony Http Foundation Bridge

- [⬅️️ Back](../../introduction.md)
- [📚API Reference](/documentation/api/bridge/symfony/http-foundation)
- [📁Files](/documentation/api/bridge/symfony/http-foundation/indices/files.html)

Http Foundation Bridge provides seamless integration between Symfony Http Foundation and Flow PHP.

`FlowStreamedResponse` is a Symfony Streamed Response that can:

- stream to one of the available formats (CSV, JSON, Parquet, XML)
- apply transformations on the fly
- stream large datasets that normally would not fit in memory
- use ETL to convert data on the fly

## Installation

```
composer require flow-php/symfony-http-foundation-bridge:1.x-dev
```

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
}
```

Above example will add a new column `time` to the dataset with the current timestamp.
