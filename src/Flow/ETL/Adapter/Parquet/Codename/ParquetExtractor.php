<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Codename;

use codename\parquet\ParquetReader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @psalm-suppress MissingImmutableAnnotation
 */
final class ParquetExtractor implements Extractor
{
    private ?ParquetReader $reader = null;

    /**
     * @param string $path
     * @param string $rowEntryName
     * @param array<string> $fields
     */
    public function __construct(private readonly string $path, private readonly string $rowEntryName = 'row', private readonly array $fields = [])
    {
        if (!\file_exists($path)) {
            throw new InvalidArgumentException("Parquet file not found in path: {$path}");
        }
    }

    public function extract() : \Generator
    {
        $dataFields = $this->reader()->schema->getDataFields();

        for ($i = 0; $i < $this->reader()->getRowGroupCount(); $i++) {
            $groupReader = $this->reader()->OpenRowGroupReader($i);
            /** @var array<int, array<mixed>> $data */
            $data = [];

            foreach ($dataFields as $field) {
                if (\count($this->fields) && !\in_array($field->name, $this->fields, true)) {
                    continue;
                }

                $column = $groupReader->ReadColumn($field);
                $arrayRow = -1;
                /**
                 * @psalm-suppress MixedAssignment
                 * @psalm-suppress PossiblyNullArrayAccess
                 * @psalm-suppress MixedArrayAssignment
                 */
                foreach ($column->getData() as $row => $value) {
                    if ($field->isArray) {
                        /** @phpstan-ignore-next-line */
                        if ($column->repetitionLevels[$row] === 0) {
                            $arrayRow+= 1;
                        }

                        $data[$arrayRow][$field->name][] = $value;
                    } else {
                        $data[$row][$field->name] = $value;
                    }
                }
            }

            $rows = [];

            foreach ($data as $rowData) {
                $rows[] = Row::create(new Row\Entry\ArrayEntry($this->rowEntryName, $rowData));
            }

            yield new Rows(...$rows);
        }
    }

    /**
     * @psalm-suppress NullableReturnStatement
     */
    private function reader() : ParquetReader
    {
        if ($this->reader === null) {
            $this->reader = new ParquetReader(\fopen($this->path, 'r'));
        }

        return $this->reader;
    }
}
