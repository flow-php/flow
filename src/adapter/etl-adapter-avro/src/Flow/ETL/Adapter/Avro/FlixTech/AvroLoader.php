<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Filesystem\Path;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\PHP\Type\Logical\DateTimeType;
use Flow\ETL\PHP\Type\Logical\ListType;
use Flow\ETL\PHP\Type\Logical\UuidType;
use Flow\ETL\Row;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

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

    public function closure(FlowContext $context) : void
    {
        if ($this->writer !== null) {
            $this->writer($context)->close();
        }

        $context->streams()->closeWriters($this->path);
        $this->writer = null;
    }

    public function destination() : Path
    {
        return $this->path;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
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
                    Row\Entry\DateTimeEntry::class => (int) $entry->value()->format('Uu'),
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
        /** @var ListType $listType */
        $listType = $entry->definition()->type();
        $listElement = $listType->element();

        if ($listElement->type() instanceof UuidType) {
            /** @var array<string> $data */
            $data = [];

            foreach ($entry->value() as $value) {
                $data[] = $value->toString();
            }

            return $data;
        }

        if ($listElement->type() instanceof DateTimeType) {
            /** @var array<int> $data */
            $data = [];

            foreach ($entry->value() as $value) {
                $data[] = (int) $value->format('Uu');
            }

            return $data;
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

        $this->writer = new \AvroDataIOWriter(
            new AvroResource($context->streams()->writeTo($this->path)->resource()),
            new \AvroIODatumWriter($schema),
            $schema,
            'null'
        );

        return $this->writer;
    }
}
