<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\FilesystemStreams;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\SaveMode;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\TypedCollection\ObjectType;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{path: Path, schema: ?Schema}>
 */
final class AvroLoader implements Closure, Loader
{
    private ?Schema $inferredSchema = null;

    private ?FilesystemStreams $streams = null;

    private ?\AvroDataIOWriter $writer = null;

    public function __construct(
        private readonly Path $path,
        private readonly ?Schema $schema = null
    ) {
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'schema' => $this->schema,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->schema = $data['schema'];
        $this->streams = null;
        $this->writer = null;
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        if ($this->writer !== null) {
            $this->writer($context)->close();
        }
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (\count($context->partitionEntries())) {
            throw new RuntimeException('Partitioning is not supported yet');
        }

        $streams = $this->streams($context);

        if ($context->mode() === SaveMode::ExceptionIfExists && $streams->exists($this->path) && !$streams->isOpen($this->path)) {
            throw new RuntimeException('Destination path "' . $this->path->uri() . '" already exists, please change path to different or set different SaveMode');
        }

        if ($context->mode() === SaveMode::Ignore && $streams->exists($this->path) && !$streams->isOpen($this->path)) {
            return;
        }

        if ($context->mode() === SaveMode::Overwrite && $streams->exists($this->path) && !$streams->isOpen($this->path)) {
            $streams->rm($this->path);
        }

        if ($context->mode() === SaveMode::Append && $streams->exists($this->path)) {
            throw new RuntimeException('Append SaveMode is not yet supported in AvroLoader');
        }

        if ($this->schema === null) {
            if ($this->inferredSchema === null) {
                $this->inferredSchema = $rows->schema();
            } else {
                $this->inferredSchema = $this->inferredSchema->merge($rows->schema());
            }
        }

        foreach ($rows as $row) {
            /** @var array<mixed> $rowData */
            $rowData = [];

            foreach ($row->entries()->all() as $entry) {
                /**
                 * @psalm-suppress MixedAssignment
                 */
                $rowData[$entry->name()] = match (\get_class($entry)) {
                    /** @phpstan-ignore-next-line */
                    Row\Entry\ListEntry::class => $this->listEntryToValues($entry),
                    /** @phpstan-ignore-next-line */
                    DateTimeEntry::class => (int) $entry->value()->format('Uu'),
                    /** @phpstan-ignore-next-line */
                    Row\Entry\EnumEntry::class => $entry->value()->name,
                    default => $entry->value(),
                };
            }

            $this->writer($context)->append($rowData);
        }
    }

    private function listEntryToValues(Row\Entry\ListEntry $entry) : array
    {
        $listType = $entry->definition()->metadata()->get(Schema\FlowMetadata::METADATA_LIST_ENTRY_TYPE);

        if ($listType instanceof ObjectType) {
            if (\is_a($listType->class, \DateTimeInterface::class, true)) {
                /** @var array<int> $data */
                $data = [];

                /** @psalm-suppress MixedAssignment */
                foreach ($entry->value() as $value) {
                    /**
                     * @psalm-suppress MixedMethodCall
                     *
                     * @phpstan-ignore-next-line
                     */
                    $data[] = (int) $value->format('Uu');
                }

                return $data;
            }
        }

        return $entry->value();
    }

    private function schema() : string
    {
        if ($this->schema !== null) {
            (new SchemaConverter())->toAvroJsonSchema($this->schema);
        }

        /**
         * @psalm-suppress PossiblyNullArgument
         *
         * @phpstan-ignore-next-line
         */
        return (new SchemaConverter())->toAvroJsonSchema($this->inferredSchema);
    }

    private function streams(FlowContext $context) : FilesystemStreams
    {
        if ($this->streams === null) {
            $this->streams = new FilesystemStreams($context->fs());
        }

        return $this->streams;
    }

    private function writer(FlowContext $context) : \AvroDataIOWriter
    {
        if ($this->writer !== null) {
            return $this->writer;
        }

        $schema = \AvroSchema::parse($this->schema());

        $this->writer =  new \AvroDataIOWriter(
            new AvroResource(
                $this->streams($context)->open(
                    $this->path,
                    'avro',
                    Mode::WRITE_BINARY,
                    $context->threadSafe()
                )->resource()
            ),
            new \AvroIODatumWriter($schema),
            $schema,
            'null'
        );

        return $this->writer;
    }
}
