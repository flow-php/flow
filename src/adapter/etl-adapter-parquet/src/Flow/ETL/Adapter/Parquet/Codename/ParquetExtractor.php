<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Codename;

use function Flow\ETL\DSL\array_to_rows;
use codename\parquet\ParquetReader;
use Flow\ETL\Extractor;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;

/**
 * @deprecated Use \Flow\ETL\Adapter\Parquet\ParquetExtractor instead
 */
final class ParquetExtractor implements Extractor
{
    /**
     * @param Path $path
     * @param array<string> $fields
     */
    public function __construct(
        private readonly Path $path,
        private readonly array $fields = [],
        private readonly Row\EntryFactory $entryFactory = new Row\Factory\NativeEntryFactory()
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($this->readers($context) as $readerData) {
            $dataFields = $readerData['reader']->schema->getDataFields();

            for ($i = 0; $i < $readerData['reader']->getRowGroupCount(); $i++) {
                $groupReader = $readerData['reader']->OpenRowGroupReader($i);
                /** @var array<int, array<mixed>> $data */
                $data = [];

                foreach ($dataFields as $field) {
                    if ([] !== $this->fields && !\in_array($field->name, $this->fields, true)) {
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
                    if ($shouldPutInputIntoRows) {
                        $rows[] = \array_merge($rowData, ['_input_file_uri' => $readerData['uri']]);
                    } else {
                        $rows[] = $rowData;
                    }
                }

                yield array_to_rows($rows, $this->entryFactory);
            }
        }
    }

    /**
     * @psalm-suppress NullableReturnStatement
     *
     * @return \Generator<int, array{reader: ParquetReader, uri: string}>
     */
    private function readers(FlowContext $context) : \Generator
    {
        foreach ($context->streams()->fs()->scan($this->path, $context->partitionFilter()) as $filePath) {
            yield ['reader' => new ParquetReader($context->streams()->fs()->open($filePath, Mode::READ)->resource()), 'uri' => $filePath->uri()];
        }
    }
}
