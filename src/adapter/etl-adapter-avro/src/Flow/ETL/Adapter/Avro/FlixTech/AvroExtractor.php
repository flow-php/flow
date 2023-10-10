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
    /**
     * @param Path $path
     */
    public function __construct(
        private readonly Path $path,
        private readonly int $rowsInBach = 1000,
        private readonly Row\EntryFactory $entryFactory = new Row\Factory\NativeEntryFactory()
    ) {
    }

    /**
     * @psalm-suppress MixedArgument
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
                    $rows[] = \array_merge($valueConverter->convert($rowData), ['_input_file_uri' => $filePath->uri()]);
                } else {
                    $rows[] = $valueConverter->convert($rowData);
                }

                if (\count($rows) >= $this->rowsInBach) {
                    yield array_to_rows($rows, $this->entryFactory);
                    /** @var array<Row> $rows */
                    $rows = [];
                }
            }
        }

        if ([] !== $rows) {
            yield array_to_rows($rows, $this->entryFactory);
        }
    }
}
