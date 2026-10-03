<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor\File;

use Flow\ETL\Column\Backend;
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
        private ?string $uri,
        private array $partitionNames,
        private array $partitionValues,
    ) {}

    /**
     * The file's value of every column this file adds - `_input_file_uri` when metadata columns are on, then each
     * partition column (null when the path does not carry it).
     *
     * @return array<string, mixed>
     */
    public function values(): array
    {
        $values = $this->uri === null ? [] : ['_input_file_uri' => $this->uri];

        foreach ($this->partitionNames as $name => $_) {
            $values[$name] = array_key_exists($name, $this->partitionValues) ? $this->partitionValues[$name] : null;
        }

        return $values;
    }

    /**
     * values() over a batch the reader already matched to the file's body schema, adopting $declared - the schema
     * FileColumns::declare() built over that body schema. A batch with nothing to add is returned as it is.
     */
    public function fillRows(Rows $rows, Schema $declared, Backend $backend): Rows
    {
        if ($this->uri === null && $this->partitionNames === []) {
            return $rows;
        }

        $added = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values() as $name => $value) {
            $added[$name] = $backend->constant($declared->get($name), $value, $rows->count());
        }

        return $rows->withColumns($declared, $added);
    }
}
