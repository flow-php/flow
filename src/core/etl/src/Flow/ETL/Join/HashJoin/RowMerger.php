<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Row;

use function array_key_exists;
use function array_keys;
use function implode;
use function sprintf;

final class RowMerger
{
    /**
     * @var array<string, true>
     */
    private readonly array $dropLeft;

    /**
     * @var array<string, true>
     */
    private readonly array $dropRight;

    /**
     * @var array<string, array{array<string, true>, array<string, true>, array<string, string>}>
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
     * @throws DuplicatedEntriesException
     */
    public function merge(Row $left, Row $right): Row
    {
        $leftValues = $left->values();
        $rightValues = $right->values();

        $planKey = implode("\x00", array_keys($leftValues)) . "\x01" . implode("\x00", array_keys($rightValues));

        [$keepLeft, $keepRight, $renames] =
            $this->plans[$planKey] ??= $this->plan(array_keys($leftValues), array_keys($rightValues));

        $values = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($leftValues as $name => $value) {
            if (array_key_exists($name, $keepLeft)) {
                $values[$name] = $value;
            }
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rightValues as $name => $value) {
            if (!array_key_exists($name, $keepRight)) {
                continue;
            }

            $values[$renames[$name] ?? $name] = $value;
        }

        return new Row($values);
    }

    /**
     * @param array<string> $leftNames
     * @param array<string> $rightNames
     *
     * @throws DuplicatedEntriesException
     *
     * @return array{array<string, true>, array<string, true>, array<string, string>}
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
            $outputName = $this->prefix === '' ? $name : $this->prefix . $name;

            if ($this->prefix !== '') {
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
