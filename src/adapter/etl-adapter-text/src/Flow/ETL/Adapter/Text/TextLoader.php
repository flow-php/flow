<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;

final class TextLoader implements Closure, FileLoader, Loader
{
    private string $newLineSeparator = PHP_EOL;

    private readonly Path $path;

    public function __construct(Path $path)
    {
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE->value, ContentType::TEXT);
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
            if ($rows->partitions()->count()) {
                foreach ($rows as $row) {
                    if ($row->entries()->count() > 1) {
                        throw new RuntimeException(\sprintf(
                            'Text data loader supports only a single entry rows, and you have %d rows.',
                            $row->entries()->count(),
                        ));
                    }

                    $context
                        ->streams()
                        ->writeTo($this->path, $rows->partitions()->toArray())
                        ->append($row->entries()->all()[0]->toString() . $this->newLineSeparator);
                }
            } else {
                foreach ($rows as $row) {
                    if ($row->entries()->count() > 1) {
                        throw new RuntimeException(\sprintf(
                            'Text data loader supports only a single entry rows, and you have %d rows.',
                            $row->entries()->count(),
                        ));
                    }

                    $context
                        ->streams()
                        ->writeTo($this->path)
                        ->append($row->entries()->all()[0]->toString() . $this->newLineSeparator);
                }
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (\Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withNewLineSeparator(string $newLineSeparator): self
    {
        $this->newLineSeparator = $newLineSeparator;

        return $this;
    }
}
