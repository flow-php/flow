<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class TextExtractor implements Extractor
{
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBatch = 1000,
        private readonly string $rowEntryName = 'row'
    ) {
    }

    /**
     * @psalm-suppress ImpureFunctionCall
     * @psalm-suppress ImpureMethodCall
     */
    public function extract(FlowContext $context) : \Generator
    {
        /** @var array<Row> $rows */
        $rows = [];

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            $fileStream = $context->streams()->fs()->open($filePath, Mode::READ);

            $rowData = \fgets($fileStream->resource());

            if ($rowData === false) {
                return;
            }

            while ($rowData !== false) {
                if ($context->config->shouldPutInputIntoRows()) {
                    $rows[] = Row::create(
                        Entry::string($this->rowEntryName, \rtrim($rowData)),
                        Entry::string('input_file_uri', $filePath->uri())
                    );
                } else {
                    $rows[] = Row::create(Entry::string($this->rowEntryName, \rtrim($rowData)));
                }

                if (\count($rows) >= $this->rowsInBatch) {
                    yield new Rows(...$rows);

                    /** @var array<Row> $rows */
                    $rows = [];
                }

                $rowData = \fgets($fileStream->resource());
            }

            if (\count($rows)) {
                yield new Rows(...$rows);
            }
        }
    }
}
