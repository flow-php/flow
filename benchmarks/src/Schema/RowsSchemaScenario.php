<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Schema;

use Flow\ETL\Rows;

use function array_key_exists;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

/**
 * Guards that Rows::schema() stays an O(1) getter rather than recomputing per row.
 *
 * The assertion is a SHAPE, not a level: the mode must be FLAT across batch size. No threshold
 * policy can express that, so it is checked by hand at the baseline - the modes must agree within
 * the noise band - and nothing automated evaluates it.
 *
 * One call makes CALLS invocations because a single one is far below timer resolution.
 */
final class RowsSchemaScenario
{
    private const CALLS = 1_000_000;

    /** @var array<int, Rows> */
    private static array $batches = [];

    public function __construct(
        private readonly int $batch,
    ) {}

    public static function clear(): void
    {
        self::$batches = [];
    }

    public function run(): void
    {
        $batch = $this->rows();

        for ($call = 0; $call < self::CALLS; $call++) {
            $batch->schema();
        }
    }

    public function warm(): void
    {
        $this->rows();
    }

    public function rows(): Rows
    {
        if (array_key_exists($this->batch, self::$batches)) {
            return self::$batches[$this->batch];
        }

        $list = [];

        for ($index = 0; $index < $this->batch; $index++) {
            $list[] = row(['a' => 'x' . $index]);
        }

        return self::$batches[$this->batch] = rows(schema(str_schema('a')), ...$list);
    }
}
