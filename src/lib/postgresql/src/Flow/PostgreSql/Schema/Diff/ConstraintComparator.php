<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Schema\Constraint\CheckConstraint;
use Flow\PostgreSql\Schema\Constraint\ExcludeConstraint;
use Flow\PostgreSql\Schema\Constraint\ForeignKey;
use Flow\PostgreSql\Schema\Constraint\UniqueConstraint;

final readonly class ConstraintComparator
{
    /**
     * @param list<CheckConstraint> $sourceCcs
     * @param list<CheckConstraint> $targetCcs
     *
     * @return ChangeSet<CheckConstraint, mixed>
     */
    public function diffCheckConstraints(array $sourceCcs, array $targetCcs): ChangeSet
    {
        return $this->diffConstraints(
            $sourceCcs,
            $targetCcs,
            static fn(CheckConstraint $cc): string => $cc->name ?? $cc->expression,
            static fn(CheckConstraint $a, CheckConstraint $b): bool => $a->isEqualStructure($b),
        );
    }

    /**
     * @param list<ExcludeConstraint> $sourceEcs
     * @param list<ExcludeConstraint> $targetEcs
     *
     * @return ChangeSet<ExcludeConstraint, mixed>
     */
    public function diffExcludeConstraints(array $sourceEcs, array $targetEcs): ChangeSet
    {
        return $this->diffConstraints(
            $sourceEcs,
            $targetEcs,
            static fn(ExcludeConstraint $ec): string => $ec->name ?? $ec->definition,
            static fn(ExcludeConstraint $a, ExcludeConstraint $b): bool => $a->isEqualStructure($b),
        );
    }

    /**
     * @param list<ForeignKey> $sourceFks
     * @param list<ForeignKey> $targetFks
     *
     * @return ChangeSet<ForeignKey, mixed>
     */
    public function diffForeignKeys(array $sourceFks, array $targetFks): ChangeSet
    {
        return $this->diffConstraints(
            $sourceFks,
            $targetFks,
            static fn(ForeignKey $fk): string => (
                $fk->name
                ?? \implode(',', $fk->columns)
                . '=>'
                . $fk->referenceSchema
                . '.'
                . $fk->referenceTable
                . '('
                . \implode(',', $fk->referenceColumns)
                . ')'
            ),
            static fn(ForeignKey $a, ForeignKey $b): bool => $a->isEqualStructure($b),
        );
    }

    /**
     * @param list<UniqueConstraint> $sourceUcs
     * @param list<UniqueConstraint> $targetUcs
     *
     * @return ChangeSet<UniqueConstraint, mixed>
     */
    public function diffUniqueConstraints(array $sourceUcs, array $targetUcs): ChangeSet
    {
        return $this->diffConstraints(
            $sourceUcs,
            $targetUcs,
            static function (UniqueConstraint $uc): string {
                if ($uc->name !== null) {
                    return $uc->name;
                }

                $cols = $uc->columns;
                \sort($cols);

                return \implode(',', $cols);
            },
            static fn(UniqueConstraint $a, UniqueConstraint $b): bool => $a->isEqualStructure($b),
        );
    }

    /**
     * @template T
     *
     * @param list<T> $sourceList
     * @param list<T> $targetList
     * @param callable(T): string $identityFn
     * @param callable(T, T): bool $equalsFn
     *
     * @return ChangeSet<T, mixed>
     */
    private function diffConstraints(
        array $sourceList,
        array $targetList,
        callable $identityFn,
        callable $equalsFn,
    ): ChangeSet {
        $sourceMap = [];

        foreach ($sourceList as $item) {
            $sourceMap[$identityFn($item)] = $item;
        }

        $targetMap = [];

        foreach ($targetList as $item) {
            $targetMap[$identityFn($item)] = $item;
        }

        $added = [];
        $removed = [];

        foreach ($targetMap as $identity => $item) {
            if (!array_key_exists($identity, $sourceMap)) {
                $added[] = $item;
            } elseif (!$equalsFn($sourceMap[$identity], $item)) {
                $removed[] = $sourceMap[$identity];
                $added[] = $item;
            }
        }

        foreach ($sourceMap as $identity => $item) {
            if (!array_key_exists($identity, $targetMap)) {
                $removed[] = $item;
            }
        }

        return new ChangeSet($added, $removed);
    }
}
