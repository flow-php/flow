<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use DateTimeInterface;
use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Filesystem\FilesSink;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Loader\Partitioning;
use Flow\ETL\Loader\PartitioningLoader;
use Flow\ETL\Loader\PartitionRouter;
use Flow\ETL\Rows;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Partition;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Throwable;

use function array_values;
use function implode;
use function sprintf;

final class CSVLoader implements Closure, Discardable, FileLoader, Loader, PartitioningLoader
{
    private PartitionRouter $router;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?FilesSink $files = null;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private ?CSVEncoder $encoder = null;

    private string $enclosure = '"';

    private string $escape = '\\';

    private bool $header = true;

    private string $newLineSeparator = PHP_EOL;

    private readonly Path $path;

    private string $separator = ',';

    public function __construct(Path $path, Filesystem $filesystem = new NativeLocalFilesystem())
    {
        if (!$filesystem->supports($path)) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem %s serves "%s://" paths, given: "%s". Pass the filesystem that handles '
                . 'this scheme, e.g. to_csv($path, filesystem: aws_s3_filesystem(...)).',
                $filesystem::class,
                $filesystem->mount()->protocol,
                $path->uri(),
            ));
        }

        $this->filesystem = $filesystem;
        $this->router = new PartitionRouter(Partitioning::none());
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::CSV);
    }

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->router = new PartitionRouter($partitioning);

        return $this;
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
        if (!$rows->count()) {
            return;
        }

        $context->telemetry()->loadingStarted($this, [
            TelemetryAttributes::ATTR_LOADER_DESTINATION_URI => $this->path->uri(),
        ]);

        try {
            foreach ($this->router->route($rows) as [$partitions, $group]) {
                $this->write(
                    $group,
                    array_values($group->schema()->references()->names()),
                    $context,
                    $partitions->toArray(),
                );
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
        $files = $this->files ??= new FilesSink($this->filesystem, $this->path, $this->saveMode);

        $encoder = $this->encoder();

        $writeHeader = $this->header && !$files->touched($partitions);
        $stream = $files->writeTo($partitions);

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
