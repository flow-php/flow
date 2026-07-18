<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use DateTimeInterface;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use JsonException;
use Throwable;

final class JsonLinesLoader implements Closure, FileLoader, Loader
{
    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private ?JSONEncoder $encoder = null;

    private int $flags = JSON_THROW_ON_ERROR;

    private readonly Path $path;

    public function __construct(Path $path)
    {
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE->value, ContentType::JSON);
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
        $context->telemetry()->loadingStarted($this, [
            TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $this->path->uri(),
        ]);

        try {
            if ($rows->partitions()->count()) {
                $this->write($rows, $rows->partitions()->toArray(), $context);
            } else {
                $this->write($rows, [], $context);
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

    public function withFlags(int $flags): self
    {
        $this->flags = $flags &= ~JSON_PRETTY_PRINT;

        return $this;
    }

    /**
     * @param array<Partition> $partitions
     */
    public function write(Rows $nextRows, array $partitions, FlowContext $context): void
    {
        $stream = $context->streams()->writeTo($this->path, $partitions);

        foreach ($this->encoder()->encode($context->hydrator()->dehydrate($nextRows)) as $normalizedRow) {
            try {
                $json = json_encode($normalizedRow, $this->flags);

                if ($json === false) {
                    throw new RuntimeException('Failed to encode JSON: ' . json_last_error_msg());
                }
            } catch (JsonException $e) {
                throw new RuntimeException('Failed to encode JSON: ' . $e->getMessage(), 0, $e);
            }

            $stream->append($json . "\n");
        }
    }

    private function encoder(): JSONEncoder
    {
        return $this->encoder ??= new JSONEncoder($this->dateTimeFormat, $this->dateFormat);
    }
}
