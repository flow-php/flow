<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Partition;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;

final class CSVLoader implements Closure, Loader, Loader\FileLoader
{
    public function __construct(
        private readonly Path $path,
        private bool $header = true,
        private string $separator = ',',
        private string $enclosure = '"',
        private string $escape = '\\',
        private string $newLineSeparator = PHP_EOL
    ) {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("CSVLoader path can't be pattern, given: " . $this->path->path());
        }
    }

    public function closure(FlowContext $context) : void
    {
        $context->streams()->close($this->path);
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $headers = $rows->first()->entries()->map(fn (Entry $entry) => $entry->name());

        if ($rows->partitions()->count()) {
            $this->write($rows, $headers, $context, $rows->partitions()->toArray());
        } else {
            $this->write($rows, $headers, $context, []);
        }
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $headers, FlowContext $context, array $partitions) : void
    {
        if ($this->header && !$context->streams()->isOpen($this->path, $partitions)) {
            $this->writeCSV(
                $headers,
                $context->streams()->open($this->path, 'csv', $context->appendSafe(), $partitions)
            );
        }

        foreach ($nextRows as $row) {
            $this->writeCSV(
                $row->toArray(),
                $context->streams()->open($this->path, 'csv', $context->appendSafe(), $partitions)
            );
        }
    }

    private function writeCSV(array $row, FileStream $destination) : void
    {
        /**
         * @var string $entry
         * @var mixed $value
         */
        foreach ($row as $entry => $value) {
            if (\is_array($value)) {
                throw new RuntimeException("Entry \"{$entry}\" is an list|array, please cast to string before writing to CSV. Easiest way to cast arrays to string is to use Transform::to_json transformer.");
            }
        }

        \fputcsv(
            stream: $destination->resource(),
            fields: $row,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            eol: $this->newLineSeparator
        );
    }
}
