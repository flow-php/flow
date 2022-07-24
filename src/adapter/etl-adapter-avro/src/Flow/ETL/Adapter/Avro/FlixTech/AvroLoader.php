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
 * @implements Loader<array{path: Path, safe_mode: bool, schema: ?Schema}>
 */
final class AvroLoader implements Closure, Loader
{
    private ?Schema $inferredSchema = null;

    private ?\AvroDataIOWriter $writer = null;

    public function __construct(
        private readonly Path $path,
        private readonly bool $safeMode = true,
        private readonly ?Schema $schema = null
    ) {
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'safe_mode' => $this->safeMode,
            'schema' => $this->schema,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->safeMode = $data['safe_mode'];
        $this->schema = $data['schema'];
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        $this->writer($context)->close();
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (\count($context->partitionEntries())) {
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
                $context->fs()->open(
                    $this->safeMode ? $this->path->randomize() : $this->path,
                    Mode::WRITE_BINARY
                )->resource()
            ),
            new \AvroIODatumWriter($schema),
            $schema,
            'null'
        );

        return $this->writer;
    }
}
