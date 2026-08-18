<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\Path;
use Flow\Floe\Exception\FloeException;
use Throwable;

use function array_key_exists;

final class FloeLoader implements Closure, FileLoader, Loader
{
    /**
     * @var array<string, FloeWriter>
     */
    private array $writers = [];

    private ?Schema $schema = null;

    private ?Schema $inferredSchema = null;

    public function __construct(
        private readonly Path $path,
        private readonly ?Metadata $metadata = null,
        private readonly Options $options = new Options(),
        private readonly FloeEngine $engine = FloeEngine::adaptive,
    ) {}

    public function withSchema(Schema $schema): self
    {
        $this->schema = $schema;

        return $this;
    }

    public function closure(FlowContext $context): void
    {
        foreach ($this->writers as $writer) {
            $writer->close();
        }

        $context->streams()->closeStreams($this->path);
        $this->writers = [];
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
            if ($this->schema === null && $this->inferredSchema === null) {
                $this->inferredSchema = $rows->schema();
            }

            $stream = $rows->partitions()->count()
                ? $context->streams()->writeTo($this->path, $rows->partitions()->toArray())
                : $context->streams()->writeTo($this->path);

            $uri = $stream->path()->uri();

            if (!array_key_exists($uri, $this->writers)) {
                $writer = new FloeWriter(
                    $context->filesystem($this->path),
                    $this->schema ?? $this->inferredSchema ?? throw new FloeException(
                        'Floe loader has no schema to write with',
                    ),
                    $this->options,
                    hydrator: $context->hydrator(),
                    engine: $this->engine,
                );
                $writer->createForStream($stream, $this->metadata);
                $this->writers[$uri] = $writer;
            }

            $this->writers[$uri]->write($rows);

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }
}
