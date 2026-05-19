<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use DateTimeInterface;
use Flow\ETL\Adapter\JSON\RowsNormalizer\EntryNormalizer;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use JsonException;
use Throwable;

use function array_key_exists;
use function count;

final class JsonLoader implements Closure, FileLoader, Loader
{
    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private int $flags = JSON_THROW_ON_ERROR;

    private readonly Path $path;

    private bool $putRowsInNewLines = false;

    /**
     * @var array<string, int>
     */
    private array $writes = [];

    public function __construct(Path $path)
    {
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::JSON);
    }

    public function closure(FlowContext $context): void
    {
        foreach ($context->streams()->listOpenStreams($this->path) as $stream) {
            $stream->append($this->putRowsInNewLines ? "\n]" : ']');
        }

        $context->streams()->closeStreams($this->path);
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $context->telemetry()->loadingStarted($this, [
            TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $this->path->uri(),
        ]);

        try {
            if ($rows->partitions()->count()) {
                $this->write($rows, $rows->partitions()->toArray(), $context);
            } else {
                $this->write($rows, [], $context);
            }

            $context->telemetry()->loadingCompleted($this, [
                TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count(),
            ]);
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

    public function withFlags(int $flags): self
    {
        $this->flags = $flags;

        return $this;
    }

    public function withRowsInNewLines(bool $putRowsInNewLines): self
    {
        $this->putRowsInNewLines = $putRowsInNewLines;

        return $this;
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $partitions, FlowContext $context): void
    {
        $streams = $context->streams();
        $normalizer = new RowsNormalizer(new EntryNormalizer($this->dateTimeFormat));

        if (!$streams->isOpen($this->path, $partitions)) {
            $stream = $streams->writeTo($this->path, $partitions);

            if (!array_key_exists($stream->path()->path(), $this->writes)) {
                $this->writes[$stream->path()->path()] = 0;
            }

            $stream->append($this->putRowsInNewLines ? "[\n" : '[');
        } else {
            $stream = $streams->writeTo($this->path, $partitions);
        }

        $this->writeJSON($nextRows, $stream, $normalizer);
    }

    /**
     * @param Rows $rows
     * @param DestinationStream $stream
     *
     * @throws RuntimeException
     * @throws \JsonException
     */
    private function writeJSON(Rows $rows, DestinationStream $stream, RowsNormalizer $normalizer): void
    {
        if (!count($rows)) {
            return;
        }

        $separator = $this->putRowsInNewLines ? ",\n" : ',';

        foreach ($normalizer->normalize($rows) as $normalizedRow) {
            try {
                $json = json_encode($normalizedRow, $this->flags);

                if ($json === false) {
                    throw new RuntimeException('Failed to encode JSON: ' . json_last_error_msg());
                }
            } catch (JsonException $e) {
                throw new RuntimeException('Failed to encode JSON: ' . $e->getMessage(), 0, $e);
            }

            $json = $this->writes[$stream->path()->path()] > 0 ? $separator . $json : $json;

            $stream->append($json);

            $this->writes[$stream->path()->path()]++;
        }
    }
}
