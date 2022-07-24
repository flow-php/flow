<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Codename;

use codename\parquet\data\DataColumn;
use codename\parquet\data\DataField;
use codename\parquet\ParquetWriter;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Pipeline\Closure;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Schema;
use Flow\ETL\Rows;

/**
 * @implements Loader<array{
 *   path: Path,
 *   rows_per_group: int,
 *   safe_mode: bool
 * }>
 */
final class ParquetLoader implements Closure, Loader
{
    private ?ParquetWriter $writer = null;

    private readonly SchemaConverter $converter;

    private ?Schema $inferredSchema = null;

    /**
     * @var array<string, array<mixed>>
     */
    private array $dataBuffer = [];

    /**
     * @var array<string, array<int>>
     */
    private array $dataRepetitionsBuffer = [];

    private ?FileStream $fileStream;

    public function __construct(
        private readonly Path $path,
        private readonly int $rowsPerGroup = 1000,
        private readonly bool $safeMode = true,
        private readonly ?Schema $schema = null
    ) {
        $this->converter = new SchemaConverter();
        $this->fileStream = null;
    }

    public function __serialize() : array
    {
        return [
            'path' => $this->path,
            'rows_per_group' => $this->rowsPerGroup,
            'safe_mode' => $this->safeMode,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->path = $data['path'];
        $this->safeMode = $data['safe_mode'];
        $this->rowsPerGroup = $data['rows_per_group'];
        $this->fileStream = null;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (\count($context->partitionEntries())) {
            throw new RuntimeException('Partitioning is not supported yet');
        }

        if ($this->schema === null) {
            $this->inferSchema($rows);
        }

        $this->prepareDataBuffer();

        $write = false;

        foreach ($rows as $row) {
            foreach ($row->entries()->all() as $entry) {
                $this->dataBuffer[$entry->name()][] = $entry->value();

                if ($entry instanceof ListEntry) {
                    for ($repetition = 0; $repetition < \count($entry->value()); $repetition++) {
                        $this->dataRepetitionsBuffer[$entry->name()][] = $repetition === 0 ? 0 : 1;
                    }
                }

                if (\count($this->dataBuffer[$entry->name()]) >= $this->rowsPerGroup) {
                    $write = true;
                }
            }

            if ($write === true) {
                $this->writeRowGroup($context);
                $write = false;
            }
        }
    }

    public function closure(Rows $rows, FlowContext $context) : void
    {
        $this->writeRowGroup($context);

        if ($this->fileStream !== null && $this->fileStream->isOpen()) {
            $this->fileStream->close();
        }
    }

    private function writer(FlowContext $context) : ParquetWriter
    {
        if ($this->writer !== null) {
            return $this->writer;
        }

        if ($this->fileStream === null) {
            $this->fileStream = $context->fs()->open(
                $this->safeMode ? $this->path->randomize() : $this->path,
                Mode::WRITE
            );
        }

        $this->writer = new ParquetWriter($this->converter->toParquet($this->schema()), $this->fileStream->resource());

        return $this->writer;
    }

    /**
     * @throws \Exception
     */
    private function writeRowGroup(FlowContext $context) : void
    {
        foreach ($this->dataBuffer as $fieldData) {
            if (!\count($fieldData)) {
                return;
            }
        }

        $this->prepareDataBuffer();

        $rg = $this->writer($context)->CreateRowGroup();

        foreach ($this->writer($context)->schema->fields as $field) {
            if (!$field instanceof DataField) {
                throw new RuntimeException('Only DataFields are currently supported, given: ' . \get_class($field));
            }

            try {
                if ($field->isArray) {
                    /**
                     * @psalm-suppress MixedArgument
                     * @phpstan-ignore-next-line
                     */
                    $rg->WriteColumn(new DataColumn($field, \array_merge(...$this->dataBuffer[$field->name]), $this->dataRepetitionsBuffer[$field->name]));
                } else {
                    $rg->WriteColumn(new DataColumn($field, $this->dataBuffer[$field->name]));
                }
            } catch (\Throwable $e) {
                throw new RuntimeException(message: "Error occurred while writing {$field->name} of type {$field->phpType}", previous: $e);
            }
        }

        $rg->finish();
        $this->writer($context)->finish();

        if ($this->fileStream !== null && $this->fileStream->isOpen()) {
            $this->fileStream->close();
        }

        $this->fileStream = null;
        $this->writer = null;
        $this->dataBuffer = [];
        $this->dataRepetitionsBuffer = [];

        $this->prepareDataBuffer();
    }

    private function inferSchema(Rows $rows) : void
    {
        if ($this->inferredSchema === null) {
            $this->inferredSchema = $rows->schema();
        } else {
            $this->inferredSchema = $this->inferredSchema->merge($rows->schema());
        }
    }

    /**
     * @psalm-suppress PossiblyNullReference
     */
    private function prepareDataBuffer() : void
    {
        foreach ($this->schema()->entries() as $entry) {
            if (!\array_key_exists($entry, $this->dataBuffer)) {
                $this->dataBuffer[$entry] = [];
                $this->dataRepetitionsBuffer[$entry] = [];
            }
        }
    }

    /**
     * @psalm-suppress InvalidNullableReturnType
     * @psalm-suppress NullableReturnStatement
     */
    private function schema() : Schema
    {
        /** @phpstan-ignore-next-line  */
        return $this->schema ?? $this->inferredSchema;
    }
}
