<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Extractor\File;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;

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

        $present = [];

        // a source may leave out a declared column no node above it reads
        foreach ($declared->names() as $name) {
            if (array_key_exists($name, $added) || $rows->schema()->findDefinition($name) !== null) {
                $present[] = $name;
            }
        }

        return $rows->withColumns($declared->keep(...$present), $added);
    }
}
