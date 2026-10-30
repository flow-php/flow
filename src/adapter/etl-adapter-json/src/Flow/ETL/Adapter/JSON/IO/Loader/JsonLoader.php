<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Loader;

use DateTimeInterface;
use Flow\ETL\Adapter\JSON\IO\JsonFraming;
use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\FlowPhpExtension;
use Flow\ETL\IO\Filesystem\SaveMode;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Closure;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\IO\Loader\File\FileCommit;
use Flow\ETL\IO\Loader\File\FileLoader;
use Flow\ETL\IO\Loader\File\FileWrite;
use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\IO\Loader\File\FileWriterFactory;
use Flow\ETL\IO\Loader\File\Part;
use Flow\ETL\IO\Loader\File\PartitionRouter;
use Flow\ETL\IO\Loader\Partitioning;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Option;
use Flow\Filesystem\Path\Option\ContentType;
use SensitiveParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class JsonLoader implements Closure, Discardable, FileLoader, FileWriterFactory, Loader
{
    private ?FileWrite $frame = null;

    private ?Part $task = null;

    private PartitionRouter $router;

    private Partitioning $partitioning;

    private readonly Filesystem $filesystem;

    private SaveMode $saveMode = SaveMode::ExceptionIfExists;

    private string $dateFormat = 'Y-m-d';

    private string $dateTimeFormat = DateTimeInterface::ATOM;

    private int $flags = JSON_THROW_ON_ERROR;

    private readonly Path $path;

    public function __construct(
        Path $path,
        Filesystem $filesystem = new NativeLocalFilesystem(),
        private JsonFraming $framing = JsonFraming::Array,
        private readonly ?FlowPhpExtension $extension = null,
    ) {
        $filesystem->supports($path)
            || throw InvalidArgumentException::unsupportedPath(
                $filesystem,
                $path,
                $framing === JsonFraming::Lines ? 'to_json_lines' : 'to_json',
            );

        $this->filesystem = $filesystem;
        $this->partitioning = Partitioning::none();
        $this->router = new PartitionRouter($this->partitioning);
        $this->path = $path->setOptionWhenEmpty(Option::CONTENT_TYPE, ContentType::JSON);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        $loader = (new self(
            Path::fromArray(type_array()->assert($data['path'] ?? null)),
            $rebuild->of($data['filesystem'] ?? null, ConfiguredFilesystem::class)->filesystem(),
            type_enum(JsonFraming::class)->assert(constant(
                JsonFraming::class . '::' . type_string()->assert($data['framing'] ?? null),
            )),
        ))
            ->saveMode(type_enum(SaveMode::class)->assert(constant(
                SaveMode::class . '::' . type_string()->assert($data['save_mode'] ?? null),
            )))
            ->partitionBy(Partitioning::fromArray(type_array()->assert($data['partitioning'] ?? null)))
            ->withFlags(type_integer()->assert($data['flags'] ?? null))
            ->withDateFormat(type_string()->assert($data['date_format'] ?? null))
            ->withDateTimeFormat(type_string()->assert($data['date_time_format'] ?? null));

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
                'partitioning' => $this->partitioning->normalize(),
                'task' => $this->task?->normalize(),
                'framing' => $this->framing->name,
                'flags' => $this->flags,
                'date_format' => $this->dateFormat,
                'date_time_format' => $this->dateTimeFormat,
            ],
            extensions: ['json'],
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
        $loader = (new self($this->path, $this->filesystem, $this->framing, $this->extension))
            ->saveMode($this->saveMode)
            ->partitionBy($this->partitioning)
            ->withFlags($this->flags)
            ->withDateFormat($this->dateFormat)
            ->withDateTimeFormat($this->dateTimeFormat);

        $loader->task = $task;

        return $loader;
    }

    public function load(Rows $rows, Backend $backend): void
    {
        $this->frame()->write($rows, $backend);
    }

    public function writer(DestinationStream $stream, Schema $schema, Backend $backend): FileWriter
    {
        $php = new PhpJsonEncoder($this->flags, $this->dateTimeFormat, $this->dateFormat);

        return new JsonFileWriter(
            $stream,
            // RustJsonEncoder renders only these flags; any other one writes through PHP
            ($this->extension ?? FlowPhpExtension::detect())->available()
            && (
                $this->flags
                & ~(JSON_THROW_ON_ERROR | JSON_UNESCAPED_SLASHES | JSON_UNESCAPED_UNICODE | JSON_PRESERVE_ZERO_FRACTION)
            ) === 0
                ? new RustJsonEncoder($php)
                : $php,
            $this->framing,
        );
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
        $this->flags = $this->framing === JsonFraming::Lines ? $flags & ~JSON_PRETTY_PRINT : $flags;

        return $this;
    }

    /**
     * @throws InvalidArgumentException
     */
    public function withRowsInNewLines(bool $putRowsInNewLines): self
    {
        if ($this->framing === JsonFraming::Lines) {
            throw new InvalidArgumentException('withRowsInNewLines() applies to to_json(), not to_json_lines()');
        }

        $this->framing = $putRowsInNewLines ? JsonFraming::ArrayLines : JsonFraming::Array;

        return $this;
    }

    private function frame(): FileWrite
    {
        return $this->frame ??= new FileWrite(
            $this->filesystem,
            $this->path,
            $this->saveMode,
            $this->router,
            $this,
            $this->task,
        );
    }
}
