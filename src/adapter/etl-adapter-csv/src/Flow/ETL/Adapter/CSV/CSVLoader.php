<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Row\Entry;
use Flow\ETL\{Adapter\CSV\RowsNormalizer\EntryNormalizer, FlowContext, Loader, Rows};
use Flow\Filesystem\{DestinationStream, Partition, Path};

final class CSVLoader implements Closure, Loader, Loader\FileLoader
{
    private string $dateTimeFormat = \DateTimeInterface::ATOM;

    private string $enclosure = '"';

    private string $escape = '\\';

    private bool $header = true;

    private string $newLineSeparator = PHP_EOL;

    private string $separator = ',';

    public function __construct(
        private readonly Path $path,
    ) {
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

        $normalizer = new RowsNormalizer(new EntryNormalizer($context->config->caster(), $this->dateTimeFormat));

        $headers = $rows->first()->entries()->map(fn (Entry $entry) => $entry->name());

        if ($rows->partitions()->count()) {
            $this->write($rows, $headers, $context, $rows->partitions()->toArray(), $normalizer);
        } else {
            $this->write($rows, $headers, $context, [], $normalizer);
        }
    }

    public function withDateTimeFormat(string $dateTimeFormat) : self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withEnclosure(string $enclosure) : self
    {
        $this->enclosure = $enclosure;

        return $this;
    }

    public function withEscape(string $escape) : self
    {
        $this->escape = $escape;

        return $this;
    }

    public function withHeader(bool $header) : self
    {
        $this->header = $header;

        return $this;
    }

    public function withNewLineSeparator(string $newLineSeparator) : self
    {
        $this->newLineSeparator = $newLineSeparator;

        return $this;
    }

    public function withSeparator(string $separator) : self
    {
        $this->separator = $separator;

        return $this;
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $headers, FlowContext $context, array $partitions, RowsNormalizer $normalizer) : void
    {
        if ($this->header && !$context->streams()->isOpen($this->path, $partitions)) {
            $this->writeCSV(
                $headers,
                $context->streams()->writeTo($this->path, $partitions)
            );
        }

        foreach ($normalizer->normalize($nextRows) as $normalizedRow) {
            $this->writeCSV($normalizedRow, $context->streams()->writeTo($this->path, $partitions));
        }
    }

    private function writeCSV(array $row, DestinationStream $stream) : void
    {
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
