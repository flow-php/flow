<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Avro\FlixTech;

use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\DateTimeEntry;
use Flow\ETL\Row\Entry\TypedCollection\ObjectType;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;
use Flow\ETL\Stream\FileStream;
use Flow\ETL\Stream\Handler;
use Flow\ETL\Stream\Mode;

/**
 * @implements Loader<array{stream: FileStream, safe_mode: bool, schema: ?Schema}>
 */
final class AvroLoader implements Closure, Loader
{
    private ?\AvroDataIOWriter $writer = null;

    private ?Schema $inferredSchema = null;

    private Handler $handler;

    public function __construct(
        private readonly FileStream $stream,
        private readonly bool $safeMode = true,
        private readonly ?Schema $schema = null
    ) {
        $this->handler = $this->safeMode ? Handler::directory('avro') : Handler::file();
    }

    public function __serialize() : array
    {
        return [
            'stream' => $this->stream,
            'safe_mode' => $this->safeMode,
            'schema' => $this->schema,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->stream = $data['stream'];
        $this->safeMode = $data['safe_mode'];
        $this->schema = $data['schema'];
        $this->safeMode ? Handler::directory('avro') : Handler::file();
    }

    public function load(Rows $rows) : void
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

            $this->writer()->append($rowData);
        }
    }

    public function closure(Rows $rows) : void
    {
        $this->writer()->close();
    }

    private function writer() : \AvroDataIOWriter
    {
        if ($this->writer !== null) {
            return $this->writer;
        }

        $schema = \AvroSchema::parse($this->schema());

        $this->writer =  new \AvroDataIOWriter(
            new AvroResource($this->handler->open($this->stream, Mode::WRITE_BINARY)),
            new \AvroIODatumWriter($schema),
            $schema,
            'null'
        );

        return $this->writer;
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
}
