<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Config\Telemetry\TelemetryAttributes;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\FileLoader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\Writer;

final class ParquetLoader implements Closure, FileLoader, Loader
{
    private Compressions $compressions = Compressions::SNAPPY;

    private readonly SchemaConverter $converter;

    private ?Schema $inferredSchema = null;

    private readonly RowsNormalizer $normalizer;

    private Options $options;

    private readonly Path $path;

    private ?Schema $schema = null;

    /**
     * @var array<string, Writer>
     */
    private array $writers = [];

    public function __construct(Path $path)
    {
        $this->converter = new SchemaConverter();
        $this->normalizer = new RowsNormalizer();
        $this->options = Options::default();
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::PARQUET);
    }

    public function closure(FlowContext $context): void
    {
        if (\count($this->writers)) {
            foreach ($this->writers as $writer) {
                $writer->close();
            }
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
                $this->inferSchema($rows);
            }

            $streams = $context->streams();

            if ($rows->partitions()->count()) {
                $stream = $streams->writeTo($this->path, $rows->partitions()->toArray());

                if (!\array_key_exists($stream->path()->uri(), $this->writers)) {
                    $this->writers[$stream->path()->uri()] = new Writer(
                        compression: $this->compressions,
                        options: $this->options,
                    );

                    $this->writers[$stream->path()->uri()]->openForStream(
                        $stream,
                        $this->converter->toParquet($this->schema()),
                    );
                }

                $this->writers[$stream->path()->uri()]->writeBatch($this->normalizer->normalize(
                    $rows,
                    $this->schema(),
                ));
            } else {
                $stream = $streams->writeTo($this->path);

                if (!\array_key_exists($stream->path()->uri(), $this->writers)) {
                    $this->writers[$stream->path()->uri()] = new Writer(
                        compression: $this->compressions,
                        options: $this->options,
                    );

                    $this->writers[$stream->path()->uri()]->openForStream(
                        $stream,
                        $this->converter->toParquet($this->schema()),
                    );
                }

                $this->writers[$stream->path()->uri()]->writeBatch($this->normalizer->normalize(
                    $rows,
                    $this->schema(),
                ));
            }

            $context->telemetry()->loadingCompleted($this, [TelemetryAttributes::ATTR_LOADING_ROWS => $rows->count()]);
        } catch (\Throwable $e) {
            $context->telemetry()->loadingFailed($this, $e);

            throw $e;
        }
    }

    public function withCompressions(Compressions $compressions): self
    {
        $this->compressions = $compressions;

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;

        return $this;
    }

    public function withSchema(Schema $schema): self
    {
        $this->schema = $schema;

        return $this;
    }

    private function inferSchema(Rows $rows): void
    {
        if ($this->inferredSchema === null) {
            $this->inferredSchema = $rows->schema()->makeNullable();
        } else {
            $this->inferredSchema = $this->inferredSchema->merge($rows->schema())->makeNullable();
        }
    }

    private function schema(): Schema
    {
        /** @phpstan-ignore-next-line  */
        return $this->schema ?? $this->inferredSchema;
    }
}
