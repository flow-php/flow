<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Filter\Evaluation\RowEvaluator;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\ColumnReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Reader\ReadPlan;
use Flow\Parquet\Reader\SelectedPages;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\FileMetaData;
use Generator;

use function array_fill_keys;
use function array_map;
use function array_values;
use function is_array;
use function min;
use function sprintf;
use function unpack;

final class PhpParquetFileReader implements ParquetFileReader
{
    private ?PhpParquetFooter $footer = null;

    private bool $open = true;

    public function __construct(
        private readonly SourceStream $stream,
        private readonly ByteOrder $byteOrder,
        private readonly Options $options,
    ) {}

    public function close(): void
    {
        $this->open || throw new RuntimeException('Reader is not open');

        $this->open = false;
        $this->stream->close();
    }

    public function footer(): PhpParquetFooter
    {
        $this->open || throw new RuntimeException('Reader is not open');

        if ($this->footer !== null) {
            return $this->footer;
        }

        $fileTotalSize = $this->stream->size();

        if ($fileTotalSize === null) {
            throw new InvalidArgumentException('Cannot determine Parquet file size');
        }

        if ($this->stream->read(4, $fileTotalSize - 4) !== ParquetFile::PARQUET_MAGIC_NUMBER) {
            throw new InvalidArgumentException('Given file is not valid Parquet file');
        }

        $unpacked = unpack($this->byteOrder->value, $this->stream->read(4, $fileTotalSize - 8));

        if ($unpacked === false) {
            throw new InvalidArgumentException('Failed to read Parquet metadata length');
        }

        $metadataLength = $unpacked[1];

        if ($metadataLength <= 0) {
            throw new InvalidArgumentException('Parquet metadata length must be positive, got ' . $metadataLength);
        }

        $metadata = $this->stream->read($metadataLength, $fileTotalSize - ($metadataLength + 8));

        $thriftMetadata = new FileMetaData();
        $thriftMetadata->read(new CompactProtocol(new MemoryBuffer($metadata)));

        $this->footer = new PhpParquetFooter($thriftMetadata);

        return $this->footer;
    }

    /**
     * @param list<string> $columns
     * @param int $batchSize refused below 1, as the arrow engine refuses it
     *
     * @return Generator<int, array<string, list<mixed>>>
     */
    public function readColumns(array $columns, int $batchSize, ReadPlan $plan): Generator
    {
        $this->open || throw new RuntimeException('Reader is not open');

        if ($batchSize < 1) {
            throw new InvalidArgumentException('Parquet batch size must be greater than 0');
        }

        $metadata = $this->footer()->metadata();

        foreach ($columns as $columnName) {
            if (!$metadata->schema()->has($columnName)) {
                throw new InvalidArgumentException('Parquet file has no column "' . $columnName . '"');
            }
        }

        $columnReader = new ColumnReader(
            new SelectedPages(new ColumnChunkReader(new PageReader($this->byteOrder, $this->options), $this->options)),
            new DremelAssembler(DataConverter::initialize($this->options)),
        );

        if ($plan->selection->totalRows() !== $metadata->rowsNumber()) {
            throw new InvalidArgumentException(sprintf(
                'A read plan over %d rows cannot read a file of %d rows',
                $plan->selection->totalRows(),
                $metadata->rowsNumber(),
            ));
        }

        if ($columns === [] || !$plan->selection->selectsAny() || $plan->limit === 0) {
            return;
        }

        $rowGroups = array_values($metadata->rowGroups()->all());
        $rowCounts = array_map(static fn(RowGroup $rowGroup): int => $rowGroup->rowsCount(), $rowGroups);
        $selections = $plan->selection->split($rowCounts);
        $matches = $plan->matched->split($rowCounts);
        $remaining = $plan->limit;
        $batch = array_fill_keys($columns, []);
        $batched = 0;

        foreach ($rowGroups as $group => $rowGroup) {
            $selection = $selections[$group];

            if (!$selection->selectsAny()) {
                continue;
            }

            $decoded = [];

            if ($plan->rowFilter !== null) {
                $unmatched = $selection->difference($matches[$group]);

                if ($unmatched->selectsAny()) {
                    $physical = [];

                    foreach ($plan->rowFilter->columns() as $leaf) {
                        ['chunk' => $decoded[$leaf->flatPath()], 'values' => $physical[$leaf->flatPath()]] =
                            $columnReader->readPhysical(
                                $leaf,
                                $rowGroup,
                                $group,
                                $this->stream,
                                $unmatched,
                                $plan->indexes,
                            );
                    }

                    $selection = $matches[$group]->union($unmatched->masked(RowEvaluator::evaluate(
                        $plan->rowFilter,
                        $physical,
                        $unmatched->rowCount(),
                    )));
                }
            }

            $rows = $selection->rowCount();

            if ($remaining !== null && $rows > $remaining) {
                $selection = $selection->head($remaining);
                $rows = $remaining;
            }

            if ($rows === 0) {
                continue;
            }

            $generators = [];

            foreach ($columns as $columnName) {
                $generators[$columnName] = $columnReader->readRowGroup(
                    $metadata->schema()->get($columnName),
                    $rowGroup,
                    $group,
                    $this->stream,
                    $selection,
                    $plan->indexes,
                    $decoded,
                );
            }

            $left = $rows;

            while ($left > 0) {
                $take = min($batchSize - $batched, $left);

                foreach ($generators as $name => $generator) {
                    for ($taken = 0; $taken < $take && $generator->valid(); $taken++) {
                        // @mago-ignore analysis:mixed-assignment
                        $entry = $generator->current();

                        if (is_array($entry)) {
                            // @mago-ignore analysis:mixed-assignment
                            foreach ($entry as $value) {
                                $batch[$name][] = $value;
                            }
                        }

                        $generator->next();
                    }

                    if ($taken < $take) {
                        throw new RuntimeException(sprintf(
                            'Column "%s" decodes fewer rows of row group %d than its metadata declares',
                            $name,
                            $group,
                        ));
                    }
                }

                $batched += $take;
                $left -= $take;

                if ($batched === $batchSize) {
                    yield $batch;
                    $batch = array_fill_keys($columns, []);
                    $batched = 0;
                }
            }

            if ($remaining !== null) {
                $remaining -= $rows;

                if ($remaining === 0) {
                    break;
                }
            }
        }

        if ($batched > 0) {
            yield $batch;
        }
    }
}
