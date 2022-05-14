<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @psalm-immutable
 */
final class AvroExtractor implements Extractor
{
    private ?\AvroDataIOReader $reader = null;

    /**
     * @param string $path
     * @param string $rowEntryName
     */
    public function __construct(
        private readonly string $path,
        private readonly int $rowsInBach = 1000,
        private readonly string $rowEntryName = 'row'
    ) {
        /** @psalm-suppress ImpureFunctionCall */
        if (!\file_exists($path)) {
            throw new InvalidArgumentException("Avro file not found in path: {$path}");
        }
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

        /** @phpstan-ignore-next-line */
        $this->reader = \AvroDataIO::open_file($this->path);

        /** @phpstan-ignore-next-line */
        return $this->reader;
    }
}
