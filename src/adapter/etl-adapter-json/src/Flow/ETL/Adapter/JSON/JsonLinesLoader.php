<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use DateTimeInterface;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesSink;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use JsonException;
use Throwable;

use function sprintf;

final class JsonLinesLoader implements Closure, Discardable, FileLoader, Loader
{
    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?FilesSink $files = null;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private ?JSONEncoder $encoder = null;

    private int $flags = JSON_THROW_ON_ERROR;

    private readonly Path $path;

    public function __construct(Path $path, Filesystem $filesystem = new NativeLocalFilesystem())
    {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. to_json_lines($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE->value, ContentType::JSON);
    }

    public function closure(FlowContext $context): void
    {
        $this->files?->publish();
        $this->files = null;
    }

    public function discard(FlowContext $context): void
    {
        $this->files?->abandon();
        $this->files = null;
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

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
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
        $stream = ($this->files ??= new FilesSink($this->filesystem, $this->path, $this->saveMode))->writeTo(
            $partitions,
        );

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
