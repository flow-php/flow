<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Column\Backend;
use Flow\ETL\Column\DefaultBackend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

use function array_key_exists;

final readonly class FileConstants
{
    /**
     * @param null|string $uri null when metadata columns are off
     * @param array<string, bool> $partitionNames
     * @param array<string, mixed> $partitionValues already the type the declared schema gives them
     */
    public function __construct(
        private PartitionColumns $partitionColumns,
        private ?string $uri,
        private array $partitionNames,
        private array $partitionValues,
    ) {}

    /**
     * @param array<array-key, mixed> $row
     *
     * @return array<array-key, mixed>
     */
    public function fill(array $row): array
    {
        // plain assignment, unlike the partition columns below: a body column named _input_file_uri
        // keeps its position and is overwritten in place
        if ($this->uri !== null) {
            $row['_input_file_uri'] = $this->uri;
        }

        return $this->partitionColumns->fill($row, $this->partitionNames, $this->partitionValues);
    }

    /**
     * fill() over a batch the reader already matched to the file's body schema, adopting $declared - the schema
     * FileColumns::declare() built over that body schema. A batch with nothing to add is returned as it is.
     */
    public function fillRows(Rows $rows, Schema $declared, Backend $backend = new DefaultBackend()): Rows
    {
        if ($this->uri === null && $this->partitionNames === []) {
            return $rows;
        }

        $added = [];

        if ($this->uri !== null) {
            $added['_input_file_uri'] = $backend->constant(
                $declared->get('_input_file_uri'),
                $this->uri,
                $rows->count(),
            );
        }

        foreach ($this->partitionNames as $name => $_) {
            $added[$name] = $backend->constant(
                $declared->get($name),
                array_key_exists($name, $this->partitionValues) ? $this->partitionValues[$name] : null,
                $rows->count(),
            );
        }

        return $rows->withColumns($declared, $added);
    }
}
