<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Codename;

use codename\parquet\ParquetReader;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @psalm-suppress MissingImmutableAnnotation
 */
final class ParquetExtractor implements Extractor
{
    /**
     * @param Path $path
     * @param string $rowEntryName
     * @param array<string> $fields
     */
    public function __construct(
        private readonly Path $path,
        private readonly string $rowEntryName = 'row',
        private readonly array $fields = []
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        foreach ($this->readers($context) as $reader) {
            $dataFields = $reader->schema->getDataFields();

            for ($i = 0; $i < $reader->getRowGroupCount(); $i++) {
                $groupReader = $reader->OpenRowGroupReader($i);
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
                                $arrayRow += 1;
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
    }

    /**
     * @psalm-suppress NullableReturnStatement
     *
     * @return \Generator<int, ParquetReader>
     */
    private function readers(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            yield new ParquetReader($context->streams()->fs()->open($filePath, Mode::READ)->resource());
        }
    }
}
