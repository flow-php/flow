<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\SourceStream;
use Flow\Parquet\Engine\PhpParquetEngine;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFileReader;
use Flow\Parquet\ParquetFileWriter;
use Flow\Parquet\Reader\ReadPlan;
use Iterator;

/**
 * Reads through PhpParquetEngine and records, per readColumns() call, the row groups its ReadPlan selects rows from.
 */
final class ReadPlanRecordingParquetEngine implements ParquetEngine
{
    /**
     * @var list<int> the row groups of a file one readColumns() call selected rows from
     */
    public array $selectedRowGroups = [];

    /**
     * @var int the row groups of the files the reads opened
     */
    public int $rowGroups = 0;

    public function __construct(
        private readonly ParquetEngine $engine = new PhpParquetEngine(),
    ) {}

    public static function fromArray(array $configuration): ParquetEngine
    {
        throw new InvalidArgumentException('A recording engine holds live state and is never rebuilt');
    }

    public function normalize(): ?array
    {
        return null;
    }

    public function openForRead(SourceStream $stream): ParquetFileReader
    {
        return new class($this->engine->openForRead($stream), $this) implements ParquetFileReader {
            public function __construct(
                private readonly ParquetFileReader $reader,
                private readonly ReadPlanRecordingParquetEngine $engine,
            ) {}

            public function close(): void
            {
                $this->reader->close();
            }

            public function footer(): Footer
            {
                return $this->reader->footer();
            }

            public function readColumns(array $columns, int $batchSize, ReadPlan $plan): Iterator
            {
                $first = 0;
                $selected = 0;

                foreach ($this->reader->footer()->metadata()->rowGroups()->all() as $group) {
                    $this->engine->rowGroups++;
                    $selected += $plan->selection->slice($first, $group->rowsCount())->rowCount() > 0 ? 1 : 0;
                    $first += $group->rowsCount();
                }

                $this->engine->selectedRowGroups[] = $selected;

                return $this->reader->readColumns($columns, $batchSize, $plan);
            }
        };
    }

    public function openForWrite(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
    ): ParquetFileWriter {
        return $this->engine->openForWrite($stream, $schema, $compression, $options);
    }

    public function writeRows(
        DestinationStream $stream,
        Schema $schema,
        Compressions $compression,
        Options $options,
        iterable $rows,
    ): void {
        $this->engine->writeRows($stream, $schema, $compression, $options, $rows);
    }
}
