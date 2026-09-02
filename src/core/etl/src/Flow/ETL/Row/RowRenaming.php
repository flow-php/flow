<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Row;

final readonly class RowRenaming
{
    /**
     * @param array<string, string> $renames current_name => new_name
     */
    private function __construct(
        private array $renames,
    ) {}

    /**
     * @param array<string, string> $renames current_name => new_name
     */
    public static function of(array $renames): self
    {
        return new self($renames);
    }

    public function apply(Row $row): Row
    {
        if ($this->renames === []) {
            return $row;
        }

        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($row->values() as $name => $value) {
            $values[$this->renames[$name] ?? $name] = $value;
        }

        return new Row($values);
    }
}
