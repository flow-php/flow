<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use DateTimeInterface;
use Flow\ETL\Adapter\CSV\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Row\Entry;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Throwable;

use function fclose;
use function fopen;
use function fputcsv;
use function ftruncate;
use function is_resource;
use function rewind;
use function stream_get_contents;

final class CSVLoader implements Closure, FileLoader, Loader
{
    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private string $enclosure = '"';

    private string $escape = '\\';

    private bool $header = true;

    private string $newLineSeparator = PHP_EOL;

    private readonly Path $path;

    /**
     * @var null|closed-resource|resource
     */
    private $rowBuffer = null;

    private string $separator = ',';

    public function __construct(Path $path)
    {
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::CSV);
    }

    public function closure(FlowContext $context): void
    {
        if (is_resource($this->rowBuffer)) {
            fclose($this->rowBuffer);
        }

        $this->rowBuffer = null;

        $context->streams()->closeStreams($this->path);
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this, [
            TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $this->path->uri(),
        ]);

        try {
            $normalizer = new RowsNormalizer(new EntryNormalizer($this->dateTimeFormat));

            $headers = $rows
                ->first()
                ->entries()
                ->map(static fn(Entry $entry) => $entry->name());

            if ($rows->partitions()->count()) {
                $this->write($rows, $headers, $context, $rows->partitions()->toArray(), $normalizer);
            } else {
                $this->write($rows, $headers, $context, [], $normalizer);
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withDateTimeFormat(string $dateTimeFormat): self
    {
        $this->dateTimeFormat = $dateTimeFormat;

        return $this;
    }

    public function withEnclosure(string $enclosure): self
    {
        $this->enclosure = $enclosure;

        return $this;
    }

    public function withEscape(string $escape): self
    {
        $this->escape = $escape;

        return $this;
    }

    public function withHeader(bool $header): self
    {
        $this->header = $header;

        return $this;
    }

    public function withNewLineSeparator(string $newLineSeparator): self
    {
        $this->newLineSeparator = $newLineSeparator;

        return $this;
    }

    public function withSeparator(string $separator): self
    {
        $this->separator = $separator;

        return $this;
    }

    /**
     * @param array<string> $headers
     * @param array<Partition> $partitions
     */
    public function write(
        Rows $nextRows,
        array $headers,
        FlowContext $context,
        array $partitions,
        RowsNormalizer $normalizer,
    ): void {
        $streams = $context->streams();

        if ($this->header && !$streams->isOpen($this->path, $partitions)) {
            $this->writeCSV([$headers], $streams->writeTo($this->path, $partitions));
        }

        $this->writeCSV($normalizer->normalize($nextRows), $streams->writeTo($this->path, $partitions));
    }

    /**
     * @param iterable<array<array-key, null|bool|float|int|string>> $rows
     */
    private function writeCSV(iterable $rows, DestinationStream $stream): void
    {
        $buffer = $this->rowBuffer();

        ftruncate($buffer, 0);
        rewind($buffer);

        foreach ($rows as $row) {
            fputcsv(
                stream: $buffer,
                fields: $row,
                separator: $this->separator,
                enclosure: $this->enclosure,
                escape: $this->escape,
                eol: $this->newLineSeparator,
            );
        }

        $csvData = stream_get_contents($buffer, offset: 0);

        if ($csvData === false) {
            throw new RuntimeException('Failed to read temporary stream for CSV rows');
        }

        $stream->append($csvData);
    }

    /**
     * @return resource
     */
    private function rowBuffer()
    {
        if (!is_resource($this->rowBuffer)) {
            $handle = fopen('php://temp/maxmemory:' . (5 * 1024 * 1024), 'rb+');

            if ($handle === false) {
                throw new RuntimeException('Failed to open temporary stream for CSV rows');
            }

            $this->rowBuffer = $handle;
        }

        return $this->rowBuffer;
    }
}
