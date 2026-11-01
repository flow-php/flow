<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use WeakReference;

/**
 * Held weakly, so an aggregate never keeps the last batch it saw alive.
 */
final class LastBatchColumn
{
    private ?Column $column = null;

    /**
     * @var null|WeakReference<Rows>
     */
    private ?WeakReference $rows = null;

    public function __construct(
        private readonly string $name,
    ) {}

    public function of(Rows $rows): ?Column
    {
        if ($this->rows?->get() !== $rows) {
            $this->rows = WeakReference::create($rows);
            $this->column = $rows->schema()->findDefinition($this->name) === null ? null : $rows->column($this->name);
        }

        return $this->column;
    }
}
