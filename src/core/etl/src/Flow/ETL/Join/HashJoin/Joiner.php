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
use Flow\ETL\Join\JoinSchema;
use Flow\ETL\Join\JoinShape;
use Flow\ETL\Row;
use Flow\ETL\Row\RowsBuffer;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

final class Joiner
{
    private readonly Hasher $hasher;

    private readonly JoinSchema $joinSchema;

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

        $shape = JoinShape::of($expression, $type);
        $this->merger = $shape->merger();
        $this->joinSchema = $shape->schema();
    }

    /**
     * @param bool $buildLeft - build the hash table from the left side and stream the right one, memory is then bounded by the left side
     *
     * @throws DuplicatedEntriesException
     *
     * @return Generator<Rows>
     */
    public function join(JoinSide $left, JoinSide $right, bool $buildLeft = false): Generator
    {
        if ($buildLeft) {
            yield from $this->joinBuildingLeft($left, $right);

            return;
        }

        $nullLeftRow = $left->nullRow;
        $nullRightRow = $right->nullRow;
        $leftSideSchema = $left->schema;
        $rightSideSchema = $right->schema;

        $hashTable = new HashTable(trackUnmatched: $this->type === Join::right);
        $rightSchema = $rightSideSchema;

        foreach ($right->rows as $batch) {
            $rightSchema ??= $batch->schema();
            $hashes = $this->hasher->hash($this->rightValues->of($batch));

            foreach ($batch as $i => $row) {
                $hashTable->add($hashes[$i], $row);
            }
        }

        $rightSchema ??= new Schema();

        if ($this->type === Join::left) {
            $nullRightRow ??= (new NullRowBuilder($rightSchema))->row();
        }

        $leftSchema = $leftSideSchema;
        $outputSchema = null;

        foreach ($left->rows as $batch) {
            $leftSchema ??= $batch->schema();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);
            $hashes = $this->hasher->hash($this->leftValues->of($batch));
            $joined = [];

            foreach ($batch as $i => $leftRow) {
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
                // a later batch can be wider than the side schema the output was derived from, and
                // every arm answers for the same declared output - so all of them project, not one
                $projected = [];

                foreach ($joined as $joinedRow) {
                    $projected[] = $joinedRow->project($outputSchema);
                }

                // every joined value passed the gate on its own side - conforming checks the joined shape and the
                // nulls the join introduced, not the values again
                yield Rows::conformed($outputSchema, $projected);
            }
        }

        if ($this->type === Join::right) {
            $leftSchema ??= new Schema();
            $nullLeftRow ??= (new NullRowBuilder($leftSchema))->row();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);
            $buffer = new RowsBuffer($outputSchema, $this->batchSize, Rows::conformed(...));

            foreach ($hashTable->unmatchedRows() as $rightRow) {
                if (null !== ($batch = $buffer->add($this->merge($nullLeftRow, $rightRow)->project($outputSchema)))) {
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

    public function schema(Schema $left, Schema $right): Schema
    {
        return $this->joinSchema->of($this->type, $left, $right);
    }

    /**
     * Mirror of the default execution - roles swap, output rows do not: merge($left, $right)
     * argument order is preserved, unmatched-row tracking moves to whichever side sits in the table.
     *
     *
     * @return Generator<Rows>
     */
    private function joinBuildingLeft(JoinSide $left, JoinSide $right): Generator
    {
        $nullLeftRow = $left->nullRow;
        $nullRightRow = $right->nullRow;
        $leftSideSchema = $left->schema;
        $rightSideSchema = $right->schema;

        $hashTable = new HashTable(trackUnmatched: $this->type === Join::left || $this->type === Join::left_anti);
        $leftSchema = $leftSideSchema;

        foreach ($left->rows as $batch) {
            $leftSchema ??= $batch->schema();
            $hashes = $this->hasher->hash($this->leftValues->of($batch));

            foreach ($batch as $i => $row) {
                $hashTable->add($hashes[$i], $row);
            }
        }

        $leftSchema ??= new Schema();
        $nullLeftRow ??= (new NullRowBuilder($leftSchema))->row();

        $rightSchema = $rightSideSchema;
        $outputSchema = null;

        foreach ($right->rows as $batch) {
            $rightSchema ??= $batch->schema();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);
            $hashes = $this->hasher->hash($this->rightValues->of($batch));
            $joined = [];

            foreach ($batch as $i => $rightRow) {
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
                    $joined[] = $this->merge($nullLeftRow, $rightRow);
                }
            }

            if ($joined !== []) {
                $projected = [];

                foreach ($joined as $joinedRow) {
                    $projected[] = $joinedRow->project($outputSchema);
                }

                yield Rows::conformed($outputSchema, $projected);
            }
        }

        $rightSchema ??= new Schema();

        if ($this->type === Join::left || $this->type === Join::left_anti) {
            $nullRightRow ??= (new NullRowBuilder($rightSchema))->row();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);
            $buffer = new RowsBuffer($outputSchema, $this->batchSize, Rows::conformed(...));

            foreach ($hashTable->unmatchedRows() as $leftRow) {
                $joined = $this->type === Join::left ? $this->merge($leftRow, $nullRightRow) : $leftRow;

                if (null !== ($batch = $buffer->add($joined->project($outputSchema)))) {
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
