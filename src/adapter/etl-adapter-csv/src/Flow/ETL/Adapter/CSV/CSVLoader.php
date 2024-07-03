<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Row\Entry;
use Flow\ETL\{FlowContext, Loader, Rows};
use Flow\Filesystem\{DestinationStream, Partition, Path};

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
        $context->streams()->closeWriters($this->path);
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
                $context->streams()->writeTo($this->path, $partitions)
            );
        }

        foreach ($nextRows as $row) {
            $this->writeCSV(
                $row->toArray(),
                $context->streams()->writeTo($this->path, $partitions)
            );
        }
    }

    private function writeCSV(array $row, DestinationStream $stream) : void
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

        $tmpHandle = fopen('php://temp/maxmemory:' . (5 * 1024 * 1024), 'rb+');

        if ($tmpHandle === false) {
            throw new RuntimeException('Failed to open temporary stream for CSV row');
        }

        \fputcsv(
            stream: $tmpHandle,
            fields: $row,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            eol: $this->newLineSeparator
        );
        $csvRowData = \stream_get_contents($tmpHandle, offset: 0);
        \fclose($tmpHandle);

        if ($csvRowData === false) {
            throw new RuntimeException('Failed to read temporary stream for CSV row');
        }

        $stream->append($csvRowData);
    }
}
