<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Bucketing\Hasher;
use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\SingleBucketHasher;
use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Rows;
use Generator;

final class Joiner
{
    private readonly Hasher $hasher;

    private readonly ?JoinKeys $keys;

    private readonly KeyValues $leftValues;

    private readonly RowMerger $merger;

    private readonly KeyValues $rightValues;

    /**
     * @param int<1, max> $batchSize - size of output batches emitted for unmatched right side rows
     */
    public function __construct(
        private readonly Expression $expression,
        private readonly Join $type,
        private readonly EntryFactory $entryFactory = new EntryFactory(),
        private readonly int $batchSize = 1000,
    ) {
        // @mago-ignore analysis:invalid-operand
        // @mago-ignore analysis:impossible-condition,redundant-comparison
        if ($this->batchSize < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, given: ' . $this->batchSize);
        }

        $this->keys = JoinKeys::fromComparison($expression->comparison());

        // non-equality joins have nothing to hash by, everything lands in a single candidate list
        $this->hasher = $this->keys !== null ? new NativeHasher() : new SingleBucketHasher();
        $this->leftValues = new KeyValues($this->keys?->leftRefs() ?? []);
        $this->rightValues = new KeyValues($this->keys?->rightRefs() ?? []);

        $duplicates = [];

        if ($expression->prefix() === '') {
            foreach ($expression->left() as $leftRef) {
                foreach ($expression->right() as $rightRef) {
                    if ($leftRef->name() === $rightRef->name()) {
                        $duplicates[] = $leftRef->name();

                        continue 2;
                    }
                }
            }
        }

        $this->merger = new RowMerger(
            $expression->prefix(),
            $this->type === Join::right ? $duplicates : [],
            $this->type === Join::right ? [] : $duplicates,
        );
    }

    /**
     * @param Generator<Rows> $left
     * @param Generator<Rows> $right
     * @param null|Row $nullLeftRow - pre-computed row used to null-pad unmatched right rows, collected from left rows when null
     * @param null|Row $nullRightRow - pre-computed row used to null-pad unmatched left rows, collected from right rows when null
     * @param bool $buildLeft - build the hash table from the left side and stream the right one, memory is then bounded by the left side
     *
     * @throws DuplicatedEntriesException
     *
     * @return Generator<Rows>
     */
    public function join(
        Generator $left,
        Generator $right,
        ?Row $nullLeftRow = null,
        ?Row $nullRightRow = null,
        bool $buildLeft = false,
    ): Generator {
        if ($buildLeft) {
            yield from $this->joinBuildingLeft($left, $right, $nullLeftRow, $nullRightRow);

            return;
        }

        $hashTable = new HashTable(trackUnmatched: $this->type === Join::right);

        $nullRightBuilder =
            $this->type === Join::left && $nullRightRow === null ? new NullRowBuilder($this->entryFactory) : null;

        foreach ($right as $batch) {
            $hashes = $this->hasher->hash($this->rightValues->of($batch));

            foreach ($batch as $i => $row) {
                $hashTable->add($hashes[$i], $row);
                $nullRightBuilder?->collect($row);
            }
        }

        $nullRightRow ??= $nullRightBuilder?->row();

        $nullLeftBuilder =
            $this->type === Join::right && $nullLeftRow === null ? new NullRowBuilder($this->entryFactory) : null;

        foreach ($left as $batch) {
            $hashes = $this->hasher->hash($this->leftValues->of($batch));
            $joined = [];

            foreach ($batch as $i => $leftRow) {
                $nullLeftBuilder?->collect($leftRow);

                $matched = false;

                foreach ($hashTable->candidatesFor($hashes[$i]) as $index => $rightRow) {
                    if (!$this->expression->meet($leftRow, $rightRow)) {
                        continue;
                    }

                    $matched = true;

                    if ($this->type === Join::left_anti) {
                        break;
                    }

                    if ($this->type === Join::right) {
                        $hashTable->matched($index);
                    }

                    $joined[] = $this->merge($leftRow, $rightRow);
                }

                if (!$matched && $this->type === Join::left) {
                    /** @var Row $nullRightRow */
                    $joined[] = $this->merge($leftRow, $nullRightRow);
                }

                if (!$matched && $this->type === Join::left_anti) {
                    $joined[] = $leftRow;
                }
            }

            if ($joined !== []) {
                yield new Rows(...$joined);
            }
        }

        if ($this->type === Join::right) {
            $nullLeftRow ??= $nullLeftBuilder?->row() ?? new Row(Entries::recreate([]));
            $buffer = new RowsBuffer($this->batchSize);

            foreach ($hashTable->unmatchedRows() as $rightRow) {
                if (null !== ($batch = $buffer->add($this->merge($nullLeftRow, $rightRow)))) {
                    yield $batch;
                }
            }

            if (null !== ($batch = $buffer->flush())) {
                yield $batch;
            }
        }
    }

    /**
     * @return null|JoinKeys null when the join expression is not a conjunction of equalities
     */
    public function keys(): ?JoinKeys
    {
        return $this->keys;
    }

    /**
     * Mirror of the default execution - roles swap, output rows do not: merge($left, $right)
     * argument order is preserved, unmatched-row tracking moves to whichever side sits in the table.
     *
     * @param Generator<Rows> $left
     * @param Generator<Rows> $right
     *
     * @return Generator<Rows>
     */
    private function joinBuildingLeft(
        Generator $left,
        Generator $right,
        ?Row $nullLeftRow,
        ?Row $nullRightRow,
    ): Generator {
        $hashTable = new HashTable(trackUnmatched: $this->type === Join::left || $this->type === Join::left_anti);

        $nullLeftBuilder =
            $this->type === Join::right && $nullLeftRow === null ? new NullRowBuilder($this->entryFactory) : null;

        foreach ($left as $batch) {
            $hashes = $this->hasher->hash($this->leftValues->of($batch));

            foreach ($batch as $i => $row) {
                $hashTable->add($hashes[$i], $row);
                $nullLeftBuilder?->collect($row);
            }
        }

        $nullLeftRow ??= $nullLeftBuilder?->row();

        $nullRightBuilder =
            $this->type === Join::left && $nullRightRow === null ? new NullRowBuilder($this->entryFactory) : null;

        foreach ($right as $batch) {
            $hashes = $this->hasher->hash($this->rightValues->of($batch));
            $joined = [];

            foreach ($batch as $i => $rightRow) {
                $nullRightBuilder?->collect($rightRow);

                $matched = false;

                foreach ($hashTable->candidatesFor($hashes[$i]) as $index => $leftRow) {
                    if (!$this->expression->meet($leftRow, $rightRow)) {
                        continue;
                    }

                    $matched = true;

                    if ($this->type === Join::left || $this->type === Join::left_anti) {
                        $hashTable->matched($index);
                    }

                    if ($this->type !== Join::left_anti) {
                        $joined[] = $this->merge($leftRow, $rightRow);
                    }
                }

                if (!$matched && $this->type === Join::right) {
                    $joined[] = $this->merge($nullLeftRow ?? new Row(Entries::recreate([])), $rightRow);
                }
            }

            if ($joined !== []) {
                yield new Rows(...$joined);
            }
        }

        if ($this->type === Join::left || $this->type === Join::left_anti) {
            $nullRightRow ??= $nullRightBuilder?->row() ?? new Row(Entries::recreate([]));
            $buffer = new RowsBuffer($this->batchSize);

            foreach ($hashTable->unmatchedRows() as $leftRow) {
                $joined = $this->type === Join::left ? $this->merge($leftRow, $nullRightRow) : $leftRow;

                if (null !== ($batch = $buffer->add($joined))) {
                    yield $batch;
                }
            }

            if (null !== ($batch = $buffer->flush())) {
                yield $batch;
            }
        }
    }

    private function merge(Row $left, Row $right): Row
    {
        try {
            return $this->merger->merge($left, $right);
        } catch (DuplicatedEntriesException $e) {
            throw new DuplicatedEntriesException(
                $e->getMessage() . ' try to use a different join prefix than: "' . $this->expression->prefix() . '"',
                (int) $e->getCode(),
                $e,
            );
        }
    }
}
