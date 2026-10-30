<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Floe;

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
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\NoopCodec;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_union;

final class FloeLoader implements Closure, Discardable, FileLoader, Loader
{
    private ?FileWrite $frame = null;

    private ?Part $task = null;

    private PartitionRouter $router;

    private Partitioning $partitioning;

    public function partitionBy(Partitioning $partitioning): static
    {
        $this->partitioning = $partitioning;
        $this->router = new PartitionRouter($partitioning);

        return $this;
    }

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private ?Schema $schema = null;

    public function __construct(
        private readonly Path $path,
        private readonly ?Metadata $metadata = null,
        private readonly Options $options = new Options(),
        Filesystem $filesystem = new NativeLocalFilesystem(),
    ) {
        $filesystem->supports($path) || throw InvalidArgumentException::unsupportedPath($filesystem, $path, 'to_floe');

        $this->filesystem = $filesystem;
        $this->partitioning = Partitioning::none();
        $this->router = new PartitionRouter($this->partitioning);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $loader = (new self(
            Path::fromArray(type_array()->assert($data['path'] ?? null)),
            ($data['metadata'] ?? null) === null
                ? null
                : Metadata::fromArray(type_map(type_string(), type_union(
                    type_array(),
                    type_boolean(),
                    type_float(),
                    type_integer(),
                    type_string(),
                ))->assert($data['metadata'])),
            new Options(type_integer()->assert($data['buffer_size'] ?? null)),
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
        ))
            ->saveMode(type_enum(SaveMode::class)->assert(constant(
                SaveMode::class . '::' . type_string()->assert($data['save_mode'] ?? null),
            )))
            ->partitionBy(Partitioning::fromArray(type_array()->assert($data['partitioning'] ?? null)));

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
        if (!$this->options->codec instanceof NoopCodec) {
            return null;
        }

        return Description::compose(self::class, [
            'path' => $this->path->normalize(),
            'metadata' => $this->metadata?->normalize(),
            'schema' => $this->schema?->normalize(),
            'buffer_size' => $this->options->bufferSize,
            'save_mode' => $this->saveMode->name,
            'partitioning' => $this->partitioning->normalize(),
            'task' => $this->task?->normalize(),
            'filesystem' => new ConfiguredFilesystem($this->filesystem),
        ]);
    }

    public function saveMode(SaveMode $mode): static
    {
        $this->saveMode = $mode;

        return $this;
    }

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

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
        $loader = (new self($this->path, $this->metadata, $this->options, $this->filesystem))
            ->saveMode($this->saveMode)
            ->partitionBy($this->partitioning);

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

    private function frame(): FileWrite
    {
        // partitionBy() keeps the partition columns in the path rather than the body, so the file schema must not
        // declare them either. Every other declared column stays - a batch that does not carry one is the writer's
        // to refuse.
        return $this->frame ??= new FileWrite(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->router,
            new FloeFileWriterFactory(
                $this->schema?->gracefulRemove(...$this->router->droppedNames()),
                $this->metadata,
                $this->options,
            ),
            $this->task,
        );
    }
}
