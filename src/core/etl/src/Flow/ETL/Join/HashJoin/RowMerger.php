<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;

use function array_key_exists;
use function array_keys;
use function implode;
use function sprintf;

final class RowMerger
{
    /**
     * @var array<array-key, true>
     */
    private readonly array $dropLeft;

    /**
     * @var array<array-key, true>
     */
    private readonly array $dropRight;

    /**
     * @var array<string, array{array<array-key, true>, array<array-key, true>, array<array-key, string>}>
     */
    private array $plans = [];

    /**
     * @param array<string> $dropLeft - left side entries skipped in the output (duplicated join columns)
     * @param array<string> $dropRight - right side entries skipped in the output (duplicated join columns)
     */
    public function __construct(
        private readonly string $prefix = '',
        array $dropLeft = [],
        array $dropRight = [],
    ) {
        $dropLeftSet = [];

        foreach ($dropLeft as $name) {
            $dropLeftSet[$name] = true;
        }

        $dropRightSet = [];

        foreach ($dropRight as $name) {
            $dropRightSet[$name] = true;
        }

        $this->dropLeft = $dropLeftSet;
        $this->dropRight = $dropRightSet;
    }

    /**
     * Pair-aligned: row i of the result is left row i merged with right row i.
     *
     * @throws DuplicatedEntriesException
     */
    public function merge(Rows $left, Rows $right): Rows
    {
        $leftNames = array_keys($left->schema()->definitions());
        $rightNames = array_keys($right->schema()->definitions());

        $planKey = implode("\x00", $leftNames) . "\x01" . implode("\x00", $rightNames);

        [$keepLeft, $keepRight, $renames] = $this->plans[$planKey] ??= $this->plan($leftNames, $rightNames);

        $definitions = [];
        $columns = [];

        foreach ($left->schema()->definitions() as $name => $definition) {
            if (array_key_exists($name, $keepLeft)) {
                $definitions[] = $definition;
                $columns[$name] = $left->column($name);
            }
        }

        foreach ($right->schema()->definitions() as $name => $definition) {
            if (!array_key_exists($name, $keepRight)) {
                continue;
            }

            if (array_key_exists($name, $renames)) {
                $definitions[] = $definition->rename($renames[$name]);
                $columns[$renames[$name]] = $right->column($name);

                continue;
            }

            $definitions[] = $definition;
            $columns[$name] = $right->column($name);
        }

        return Rows::fromColumns(new Schema(...$definitions), $columns, $left->count());
    }

    /**
     * @param list<array-key> $leftNames
     * @param list<array-key> $rightNames
     *
     * @throws DuplicatedEntriesException
     *
     * @return array{array<array-key, true>, array<array-key, true>, array<array-key, string>}
     */
    private function plan(array $leftNames, array $rightNames): array
    {
        $keepLeft = [];
        $outputNames = [];

        foreach ($leftNames as $name) {
            if (array_key_exists($name, $this->dropLeft)) {
                continue;
            }

            $keepLeft[$name] = true;
            $outputNames[$name] = true;
        }

        $keepRight = [];
        $renames = [];
        $rightOutputNames = [];
        $collision = false;

        foreach ($rightNames as $name) {
            if (array_key_exists($name, $this->dropRight)) {
                continue;
            }

            $keepRight[$name] = true;

            if ($this->prefix === '') {
                $outputName = $name;
            } else {
                $outputName = $this->prefix . $name;
                $renames[$name] = $outputName;
            }

            if (array_key_exists($outputName, $outputNames)) {
                $collision = true;
            }

            $outputNames[$outputName] = true;
            $rightOutputNames[] = $outputName;
        }

        if ($collision) {
            throw new DuplicatedEntriesException(sprintf(
                'Merged entries names must be unique, given: [%s] + [%s]',
                implode(', ', array_keys($keepLeft)),
                implode(', ', $rightOutputNames),
            ));
        }

        return [$keepLeft, $keepRight, $renames];
    }
}
