# Symfony Http Foundation Bridge

- [⬅️️ Back](../../introduction.md)

Http Foundation Bridge provides seamless integration between Symfony Http Foundation and Flow PHP.

`FlowStreamedResponse` is a Symfony Streamed Response that can:

- stream to one of the available formats (CSV, JSON, Parquet, XML)
- apply transformations on the fly
- stream large datasets that normally would not fit in memory
- use ETL to convert data on the fly

## Installation

```
composer require flow-php/symfony-http-foundation-bridge
```

## Usage

Stream a large parquet file converting it on the fly to CSV format.
Since Flow is extracting data from datasets in chunk, FlowStreamedResponse allows to stream
files that normally would not fit in memory.

```php
<?php

declare(strict_types=1);

namespace Symfony\Application\Controller;

use Flow\Bridge\Symfony\HttpFoundation\FlowStreamedResponse;
use Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;
use function Flow\ETL\Adapter\Parquet\ParquetEtractor;

final class ReportsController extends AbstractController
{
    #[Route('/stream/report', name: 'stream-report')]
    public function streamReport() : Response
    {
        return new FlowStreamedResponse(
            new ParquetEtractor(__DIR__ . '/reports/orders.parquet'),
            new CSVOutput(withHeader: true)
        );
    }
}
```

## Available Outputs

- `Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput` - converts dataset to CSV format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\JSONOutput` - converts dataset to JSON format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\ParquetOutput` - converts dataset to Parquet format.
- `Flow\Bridge\Symfony\HttpFoundation\Output\XMLOutput` - converts dataset to XML format.

## Modify output on the fly

Sometimes we need to modify the output on the fly. 
To do that, FlowStreamedResponse allows to pass a Transformation that will be applied on the dataset.

```php
return new FlowStreamedResponse(
    new ParquetEtractor(__DIR__ . '/reports/orders.parquet'),
    new CSVOutput(withHeader: true),
    new class implements Transformation {
        public function transform(DataFrame $dataFrame): DataFrame
        {
            return $dataFrame->withColumn('time', \time());
        }
    }
);
```

Above example will add a new column `time` to the dataset with the current timestamp.

Predefined Transformations: 

- `Flow\Bridge\Symfony\HttpFoundation\Transformation\MaskColumns` - mask columns with `*****` value.

```php
<?php

declare(strict_types=1);

namespace Symfony\Application\Controller;

use Flow\Bridge\Symfony\HttpFoundation\FlowStreamedResponse;
use Flow\Bridge\Symfony\HttpFoundation\Output\CSVOutput;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;
use function Flow\ETL\Adapter\Parquet\ParquetEtractor;

final class ReportsController extends AbstractController
{
    #[Route('/stream/report', name: 'stream-report')]
    public function streamReport() : Response
    {
        return new FlowStreamedResponse(
            new ParquetEtractor(__DIR__ . '/reports/orders.parquet'),
            new CSVOutput(withHeader: true),
            new MaskColumns(['email', 'address'])
        );
    }
}
```