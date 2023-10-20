<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class TextExtractor implements Extractor
{
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBatch = 1000,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        /** @var array<Row> $rows */
        $rows = [];

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            $fileStream = $context->streams()->fs()->open($filePath, Mode::READ);

            $rowData = \fgets($fileStream->resource());

            if ($rowData === false) {
                return;
            }

            while ($rowData !== false) {
                if ($shouldPutInputIntoRows) {
                    $rows[] = ['text' => \rtrim($rowData), '_input_file_uri' => $filePath->uri()];
                } else {
                    $rows[] = ['text' => \rtrim($rowData)];
                }

                if (\count($rows) >= $this->rowsInBatch) {
                    yield array_to_rows($rows, $context->entryFactory());

                    /** @var array<Row> $rows */
                    $rows = [];
                }

                $rowData = \fgets($fileStream->resource());
            }

            if ([] !== $rows) {
                yield array_to_rows($rows, $context->entryFactory());
            }
        }
    }
}
