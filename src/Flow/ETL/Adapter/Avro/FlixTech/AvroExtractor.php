<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\Mode;

/**
 * @psalm-immutable
 */
final class AvroExtractor implements Extractor
{
    private ?\AvroDataIOReader $reader = null;

    /**
     * @param FileStream $stream
     * @param string $rowEntryName
     */
    public function __construct(
        private readonly FileStream $stream,
        private readonly int $rowsInBach = 1000,
        private readonly string $rowEntryName = 'row'
    ) {
    }

    /**
     * @psalm-suppress MixedArgument
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress MixedAssignment
     */
    public function extract() : \Generator
    {
        /** @var array<Row> $rows */
        $rows = [];
        /** @phpstan-ignore-next-line  */
        $valueConverter = new ValueConverter(\json_decode($this->reader()->metadata['avro.schema'], true));

        foreach ($this->reader()->data() as $rowData) {
            $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $valueConverter->convert($rowData)));

            if (\count($rows) >= $this->rowsInBach) {
                yield new Rows(...$rows);
                /** @var array<Row> $rows */
                $rows = [];
            }
        }

        if (\count($rows) > 0) {
            yield new Rows(...$rows);
        }
    }

    /**
     * @psalm-suppress InvalidReturnType
     * @psalm-suppress InvalidPropertyAssignmentValue
     * @psalm-suppress InaccessibleProperty
     * @psalm-suppress ImpureMethodCall
     * @psalm-suppress InvalidReturnStatement
     */
    private function reader() : \AvroDataIOReader
    {
        if ($this->reader !== null) {
            return $this->reader;
        }

        $this->reader = new \AvroDataIOReader(
            new AvroResource(Handler::file()->open($this->stream, Mode::READ_BINARY)),
            new \AvroIODatumReader(null, null),
        );

        return $this->reader;
    }
}
