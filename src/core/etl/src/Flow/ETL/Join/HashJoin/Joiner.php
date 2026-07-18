<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Row;
use Flow\ETL\Row\Entries;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Rows;
use Generator;

use function count;

final class Joiner
{
    private readonly JoinKeys $keys;

    private readonly RowMerger $merger;

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

        $this->keys = EqualityJoinKeys::fromComparison($expression->comparison()) ?? new SingleBucketJoinKeys();

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
    ): Generator {
        $hashTable = new HashTable($this->keys, trackUnmatched: $this->type === Join::right);

        $nullRightBuilder =
            $this->type === Join::left && $nullRightRow === null ? new NullRowBuilder($this->entryFactory) : null;

        foreach ($right as $batch) {
            foreach ($batch as $row) {
                $hashTable->add($row);
                $nullRightBuilder?->collect($row);
            }
        }

        $nullRightRow ??= $nullRightBuilder?->row();

        $nullLeftBuilder =
            $this->type === Join::right && $nullLeftRow === null ? new NullRowBuilder($this->entryFactory) : null;

        foreach ($left as $batch) {
            $joined = [];

            foreach ($batch as $leftRow) {
                $nullLeftBuilder?->collect($leftRow);

                $matched = false;

                foreach ($hashTable->candidatesFor($leftRow) as $index => $rightRow) {
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
            $joined = [];

            foreach ($hashTable->unmatchedRows() as $rightRow) {
                $joined[] = $this->merge($nullLeftRow, $rightRow);

                if (count($joined) >= $this->batchSize) {
                    yield new Rows(...$joined);
                    $joined = [];
                }
            }

            if ($joined !== []) {
                yield new Rows(...$joined);
            }
        }
    }

    public function keys(): JoinKeys
    {
        return $this->keys;
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
