<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\IO\Loader;

use Flow\ETL\Adapter\Parquet\IO\ConfiguredParquetEngine;
use Flow\ETL\Adapter\Parquet\Values\Schema\SchemaConverter;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\IO\Loader\File\FileWrite;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\File\PartitionRouter;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use Flow\Parquet\Option as ParquetOption;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Compressions;
use SensitiveParameter;

use function extension_loaded;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_string;

final class ParquetLoader implements Closure, Discardable, FileLoader, Loader
{
    private ?FileWrite $frame = null;

    private ?Part $task = null;

    private PartitionRouter $router;

    private Partitioning $partitioning;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private Compressions $compressions = Compressions::SNAPPY;

    private readonly SchemaConverter $converter;

    private ?ParquetEngine $engine = null;

    private Options $options;

    private readonly Path $path;

    private ?Schema $schema = null;

    public function __construct(Path $path, Filesystem $filesystem = new NativeLocalFilesystem())
    {
        $filesystem->supports($path)
            || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'to_parquet');

        $this->filesystem = $filesystem;
        $this->partitioning = Partitioning::none();
        $this->router = new PartitionRouter($this->partitioning);
        $this->converter = new SchemaConverter();
        // every row reaching a loader was validated where it entered the pipeline, so the writer does not validate
        // each value against the column again - withOptions() can still turn it back on
        $this->options = Options::default()->set(ParquetOption::VALIDATE_DATA, false);
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::PARQUET);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $loader = (new self(
            Path::fromArray(type_array()->assert($data['path'] ?? null)),
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
        ))
            ->saveMode(type_enum(SaveMode::class)->assert(constant(
                SaveMode::class . '::' . type_string()->assert($data['save_mode'] ?? null),
            )))
            ->withCompressions(type_enum(Compressions::class)->assert(constant(
                Compressions::class . '::' . type_string()->assert($data['compressions'] ?? null),
            )))
            ->withOptions(Options::fromArray(type_array()->assert($data['options'] ?? null)))
            ->partitionBy(Partitioning::fromArray(type_array()->assert($data['partitioning'] ?? null)));

        if (($data['engine'] ?? null) !== null) {
            $loader->withEngine($rebuild->of($data['engine'], ConfiguredParquetEngine::class)->engine());
        }

        if (($data['schema'] ?? null) !== null) {
            $loader->withSchema(Schema::fromArray(type_array()->assert($data['schema'])));
        }

        if (($data['task'] ?? null) !== null) {
            $loader->task = Part::fromArray(type_array()->assert($data['task']));
        }

        return $loader;
    }

    public function describe(): ?Description
    {
        return Description::compose(
            self::class,
            [
                'path' => $this->path->normalize(),
                'filesystem' => new ConfiguredFilesystem($this->filesystem),
                'save_mode' => $this->saveMode->name,
                'compressions' => $this->compressions->name,
                'engine' => $this->engine === null ? null : new ConfiguredParquetEngine($this->engine),
                'options' => $this->options->normalize(),
                'schema' => $this->schema?->normalize(),
                'partitioning' => $this->partitioning->normalize(),
                'task' => $this->task?->normalize(),
            ],
            extensions: [
                ...ConfiguredParquetEngine::EXTENSIONS,
                ...match ($this->compressions) {
                    Compressions::BROTLI => ['brotli'],
                    Compressions::LZ4, Compressions::LZ4_RAW => ['lz4'],
                    Compressions::ZSTD => ['zstd'],
                    Compressions::SNAPPY => extension_loaded('snappy') ? ['snappy'] : [],
                    default => [],
                },
            ],
        );
    }

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->partitioning = $partitioning;
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    public function closure(): void
    {
        $this->frame()->closure();
        $this->frame = null;
    }

    public function discard(): void
    {
        $this->frame?->discard();
        $this->frame = null;
    }

    public function commit(string $run, int $tasks): ?FileCommit
    {
        return FileCommit::admitted(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->partitioning->by->count() > 0,
            $run,
            $tasks,
        );
    }

    public function destination(): Path
    {
        return $this->path;
    }

    public function partitioning(): Partitioning
    {
        return $this->partitioning;
    }

    public function inTask(Part $task): static
    {
        $loader = (new self($this->path, $this->filesystem))
            ->saveMode($this->saveMode)
            ->partitionBy($this->partitioning)
            ->withCompressions($this->compressions)
            ->withEngine($this->engine)
            ->withOptions($this->options);

        if ($this->schema !== null) {
            $loader->withSchema($this->schema);
        }

        $loader->task = $task;

        return $loader;
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->frame()->write($rows, $backend);
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
    }

    public function withCompressions(Compressions $compressions): self
    {
        $this->compressions = $compressions;

        return $this;
    }

    public function withEngine(?ParquetEngine $engine): self
    {
        $this->engine = $engine;

        return $this;
    }

    public function withOptions(Options $options): self
    {
        $this->options = $options;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }

    private function frame(): FileWrite
    {
        return $this->frame ??= new FileWrite(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->router,
            new ParquetFileWriterFactory(
                $this->schema?->gracefulRemove(...$this->router->pathOnlyColumns()),
                $this->converter,
                $this->compressions,
                $this->options,
                $this->engine,
            ),
            $this->task,
        );
    }
}
