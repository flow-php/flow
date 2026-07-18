<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use DateTimeInterface;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Throwable;

use function array_values;
use function implode;

final class CSVLoader implements Closure, FileLoader, Loader
{
    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private ?CSVEncoder $encoder = null;

    private string $enclosure = '"';

    private string $escape = '\\';

    private bool $header = true;

    private string $newLineSeparator = PHP_EOL;

    private readonly Path $path;

    private string $separator = ',';

    public function __construct(Path $path)
    {
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::CSV);
    }

    public function closure(FlowContext $context): void
    {
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
            $headers = array_values($rows->first()->entries()->names());

            if ($rows->partitions()->count()) {
                $this->write($rows, $headers, $context, $rows->partitions()->toArray());
            } else {
                $this->write($rows, $headers, $context, []);
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withDateFormat(string $dateFormat): self
    {
        $this->dateFormat = $dateFormat;

        return $this;
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
     * @param list<string> $headers
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $headers, FlowContext $context, array $partitions): void
    {
        $streams = $context->streams();

        $encoder = $this->encoder();

        $writeHeader = $this->header && !$streams->isOpen($this->path, $partitions);
        $stream = $streams->writeTo($this->path, $partitions);

        if ($writeHeader) {
            $stream->append($encoder->encodeHeader($headers));
        }

        $stream->append(implode('', $encoder->encode($context->hydrator()->dehydrate($nextRows))));
    }

    private function encoder(): CSVEncoder
    {
        return $this->encoder ??= new CSVEncoder(
            withHeader: $this->header,
            separator: $this->separator,
            enclosure: $this->enclosure,
            escape: $this->escape,
            dateTimeFormat: $this->dateTimeFormat,
            dateFormat: $this->dateFormat,
            newLineSeparator: $this->newLineSeparator,
        );
    }
}
