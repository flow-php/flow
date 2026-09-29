<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final readonly class JoinSide
{
    /**
     * @param Generator<Rows> $rows
     * @param null|Rows $nullRow the one-row batch that pads a row on the opposite side that found no match
     * @param null|Schema $schema the whole side's shape; a bucket pair only streams a slice of it
     */
    private function __construct(
        public Generator $rows,
        public ?Rows $nullRow = null,
        public ?Schema $schema = null,
    ) {}

    /**
     * @param Generator<Rows> $rows
     */
    public static function of(Generator $rows, ?Rows $nullRow = null, ?Schema $schema = null): self
    {
        return new self($rows, $nullRow, $schema);
    }
}
