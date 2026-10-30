<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

final class JoinSide
{
    private ?Rows $nullRow = null;

    /**
     * @param Generator<Rows> $rows
     * @param null|Schema $schema the whole side's shape; a bucket pair only streams a slice of it
     */
    private function __construct(
        public readonly Generator $rows,
        public readonly ?Schema $schema = null,
    ) {}

    /**
     * @param Generator<Rows> $rows
     */
    public static function of(Generator $rows, ?Schema $schema = null): self
    {
        return new self($rows, $schema);
    }

    /**
     * The one-row batch that pads a row of the other side that found no match: this side's schema, or $fallback when
     * it declares none, all null.
     */
    public function nullRow(Schema $fallback, Backend $backend): Rows
    {
        return $this->nullRow ??= Rows::nulls($this->schema ?? $fallback, $backend);
    }
}
