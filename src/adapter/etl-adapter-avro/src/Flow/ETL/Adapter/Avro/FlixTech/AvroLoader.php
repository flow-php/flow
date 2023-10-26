<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
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
final class AvroLoader implements Closure, Loader, Loader\FileLoader
{
    private ?Schema $inferredSchema = null;

    private ?\AvroDataIOWriter $writer = null;

    public function __construct(
        private readonly Path $path,
        private readonly ?Schema $schema = null
    ) {
        if ($this->path->isPattern()) {
            throw new \InvalidArgumentException("AvroLoader path can't be pattern, given: " . $this->path->path());
        }
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
        $this->writer = null;
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        if ($this->writer !== null) {
            $this->writer($context)->close();
        }

        $context->streams()->close($this->path);
        $this->writer = null;
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if ($context->partitionEntries()->count()) {
            throw new RuntimeException('Partitioning is not supported yet');
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
                $rowData[$entry->name()] = match ($entry::class) {
                    Row\Entry\ListEntry::class => $this->listEntryToValues($entry),
                    DateTimeEntry::class => (int) $entry->value()->format('Uu'),
                    Row\Entry\UuidEntry::class => $entry->value()->toString(),
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
            if (\is_a($listType->class, Row\Entry\Type\Uuid::class, true)) {
                /** @var array<string> $data */
                $data = [];

                foreach ($entry->value() as $value) {
                    $data[] = $value->toString();
                }

                return $data;
            }

            if (\is_a($listType->class, \DateTimeInterface::class, true)) {
                /** @var array<int> $data */
                $data = [];

                foreach ($entry->value() as $value) {
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

    private function writer(FlowContext $context) : \AvroDataIOWriter
    {
        if ($this->writer !== null) {
            return $this->writer;
        }

        $schema = \AvroSchema::parse($this->schema());

        $this->writer =  new \AvroDataIOWriter(
            new AvroResource(
                $context->streams()->open(
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
