<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class AvroExtractor implements Extractor
{
    /**
     * @param Path $path
     * @param string $rowEntryName
     */
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBach = 1000,
        private readonly string $rowEntryName = 'row'
    ) {
    }

    /**
     * @psalm-suppress MixedArgument
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress MixedAssignment
     */
    public function extract(FlowContext $context) : \Generator
    {
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
                if ($context->config->shouldPutInputIntoRows()) {
                    $rows[] = Row::create(
                        new Row\Entry\ArrayEntry($this->rowEntryName, $valueConverter->convert($rowData)),
                        new Row\Entry\StringEntry('input_file_uri', $filePath->uri())
                    );
                } else {
                    $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $valueConverter->convert($rowData)));
                }

                if (\count($rows) >= $this->rowsInBach) {
                    yield new Rows(...$rows);
                    /** @var array<Row> $rows */
                    $rows = [];
                }
            }
        }

        if (\count($rows) > 0) {
            yield new Rows(...$rows);
        }
    }
}
