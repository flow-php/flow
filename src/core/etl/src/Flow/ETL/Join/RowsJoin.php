<?php

declare(strict_types=1);

namespace Flow\ETL\Join;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Join\HashJoin\Joiner;
use Flow\ETL\Join\HashJoin\JoinSide;
use Flow\ETL\Join\HashJoin\RowMerger;
use Flow\ETL\Rows;
use Generator;

use function array_slice;

final readonly class RowsJoin
{
    public function __construct(
        private Backend $backend,
    ) {}

    public function cross(Rows $left, Rows $right, string $joinPrefix = 'joined_'): Rows
    {
        $schema = (new JoinSchema($joinPrefix))->cross($left->schema(), $right->schema());
        $leftIndices = [];
        $rightIndices = [];
        $rightCount = $right->count();

        for ($i = 0; $i < $left->count(); $i++) {
            for ($j = 0; $j < $rightCount; $j++) {
                $leftIndices[] = $i;
                $rightIndices[] = $j;
            }
        }

        return (new RowMerger($joinPrefix))
            ->merge($left->gather($leftIndices), $right->gather($rightIndices))
            ->project($schema, $this->backend);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function inner(Rows $left, Rows $right, Expression $expression): Rows
    {
        return $this->join($left, $right, $expression, Join::inner);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function left(Rows $left, Rows $right, Expression $expression): Rows
    {
        return $this->join($left, $right, $expression, Join::left);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function leftAnti(Rows $left, Rows $right, Expression $expression): Rows
    {
        return $this->join($left, $right, $expression, Join::left_anti);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function right(Rows $left, Rows $right, Expression $expression): Rows
    {
        return $this->join($left, $right, $expression, Join::right);
    }

    /**
     * @throws InvalidArgumentException
     */
    public function join(Rows $left, Rows $right, Expression $expression, Join $type): Rows
    {
        $single = static function (Rows $rows): Generator {
            yield $rows;
        };

        $joiner = new Joiner($expression, $type, $this->backend);
        $joined = [];

        foreach ($joiner->join(JoinSide::of($single($left)), JoinSide::of($single($right))) as $batch) {
            $joined[] = $batch;
        }

        return $joined === []
            ? Rows::empty($joiner->schema($left->schema(), $right->schema()), $this->backend)
            : $joined[0]->concat($this->backend, ...array_slice($joined, 1));
    }
}
