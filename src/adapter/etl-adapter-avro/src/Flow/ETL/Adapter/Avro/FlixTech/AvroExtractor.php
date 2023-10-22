<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use function Flow\ETL\DSL\array_to_rows;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

final class AvroExtractor implements Extractor
{
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBach = 1000
    ) {
    }

    /**
     * @psalm-suppress MixedArgument
     * @psalm-suppress MixedAssignment
     */
    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        /** @var array<Row> $rows */
        $rows = [];

        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            $reader = new \AvroDataIOReader(
                new AvroResource(
                    $context->streams()->fs()->open(
                        $filePath,
                        Mode::READ_BINARY
                    )->resource()
                ),
                new \AvroIODatumReader(null, null),
            );

            /** @phpstan-ignore-next-line */
            $valueConverter = new ValueConverter(\json_decode($reader->metadata['avro.schema'], true));

            foreach ($reader->data() as $rowData) {
                $row = $valueConverter->convert($rowData);

                if ($shouldPutInputIntoRows) {
                    $row['_input_file_uri'] = $filePath->uri();
                }

                $rows[] = $row;

                if (\count($rows) >= $this->rowsInBach) {
                    yield array_to_rows($rows, $context->entryFactory());
                    /** @var array<Row> $rows */
                    $rows = [];
                }
            }
        }

        if ([] !== $rows) {
            yield array_to_rows($rows, $context->entryFactory());
        }
    }
}
