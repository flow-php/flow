<?php

declare(strict_types=1);

namespace Flow\ETL\Join\HashJoin;

use Flow\ETL\Bucketing\Hasher;
use Flow\ETL\Bucketing\KeyValues;
use Flow\ETL\Bucketing\NativeHasher;
use Flow\ETL\Bucketing\SingleBucketHasher;
use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\DuplicatedEntriesException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Join\Expression;
use Flow\ETL\Join\Join;
use Flow\ETL\Join\JoinSchema;
use Flow\ETL\Join\JoinShape;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

use function array_chunk;
use function array_fill;
use function array_map;
use function array_slice;
use function array_unique;
use function array_values;
use function count;

final class Joiner
{
    private readonly Hasher $hasher;

    private readonly JoinSchema $joinSchema;

    private readonly ?JoinKeys $keys;

    private readonly KeyValues $leftValues;

    private readonly RowMerger $merger;

    private readonly KeyValues $rightValues;

    /**
     * @param int<1, max> $batchSize - size of output batches emitted for unmatched build side rows
     */
    public function __construct(
        private readonly Expression $expression,
        private readonly Join $type,
        private readonly Backend $backend,
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

        [$build, $rightSchema] = $this->build($right);
        $table = $this->table($build, $this->rightValues, $this->type === Join::right);

        // the null row sits at $build->count(), so a gather pads an unmatched left row with it
        $probe = $this->type === Join::left ? $this->padded($build, $right->nullRow, $rightSchema) : $build;
        $nullIndex = $build->count();

        $leftSchema = $left->schema;
        $outputSchema = null;

        foreach ($left->rows as $batch) {
            $leftSchema ??= $batch->schema();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);
            [$candidatesLeft, $candidatesRight] = $this->candidates($table, $this->leftValues->of($batch));
            $met = $this->meet($batch, $candidatesLeft, $build, $candidatesRight);

            $outputLeft = [];
            $outputRight = [];
            $pair = 0;
            $pairs = count($candidatesLeft);

            for ($i = 0, $rows = $batch->count(); $i < $rows; $i++) {
                $matched = false;

                for (; $pair < $pairs && $candidatesLeft[$pair] === $i; $pair++) {
                    if (!$met[$pair]) {
                        continue;
                    }

                    $matched = true;

                    if ($this->type === Join::left_anti) {
                        continue;
                    }

                    if ($this->type === Join::right) {
                        $table->matched($candidatesRight[$pair]);
                    }

                    $outputLeft[] = $i;
                    $outputRight[] = $candidatesRight[$pair];
                }

                if (!$matched && ($this->type === Join::left || $this->type === Join::left_anti)) {
                    $outputLeft[] = $i;
                    $outputRight[] = $nullIndex;
                }
            }

            if ($outputLeft === []) {
                continue;
            }

            // a later batch can be wider than the side schema the output was derived from, and every arm answers
            // for the same declared output - the projection drops the undeclared columns and pads the nulls the join
            // introduced
            yield $this->type === Join::left_anti
                ? $batch->gather($outputLeft)->project($outputSchema, $this->backend)
                : $this->merge($batch->gather($outputLeft), $probe->gather($outputRight))->project(
                    $outputSchema,
                    $this->backend,
                );
        }

        if ($this->type === Join::right) {
            $leftSchema ??= new Schema();
            $nullLeft = $left->nullRow ?? (new NullRowBuilder($leftSchema, $this->backend))->rows();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);

            foreach (array_chunk($table->unmatched(), $this->batchSize) as $unmatched) {
                yield $this->merge(
                    $nullLeft->gather(array_fill(0, count($unmatched), 0)),
                    $build->gather($unmatched),
                )->project($outputSchema, $this->backend);
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
     * @return array{Rows, Schema}
     */
    private function build(JoinSide $side): array
    {
        $schema = $side->schema;
        $parts = [];

        foreach ($side->rows as $batch) {
            $schema ??= $batch->schema();

            if (!$batch->isEmpty()) {
                $parts[] = $batch->project($schema, $this->backend);
            }
        }

        $schema ??= new Schema();

        return [
            $parts === []
                ? Rows::empty($schema, $this->backend)
                : $parts[0]->concat($this->backend, ...array_slice($parts, 1)),
            $schema,
        ];
    }

    /**
     * @param list<list<mixed>> $keys
     *
     * @return array{list<int>, list<int>}
     */
    private function candidates(HashTable $table, array $keys): array
    {
        $left = [];
        $right = [];

        foreach ($this->hasher->hash($keys) as $i => $hash) {
            foreach ($table->candidatesFor($hash) as $j) {
                $left[] = $i;
                $right[] = $j;
            }
        }

        return [$left, $right];
    }

    /**
     * Mirror of the default execution - roles swap, output rows do not: merge($left, $right)
     * argument order is preserved, unmatched-row tracking moves to whichever side sits in the table.
     *
     * @return Generator<Rows>
     */
    private function joinBuildingLeft(JoinSide $left, JoinSide $right): Generator
    {
        [$build, $leftSchema] = $this->build($left);
        $table = $this->table($build, $this->leftValues, $this->type === Join::left || $this->type === Join::left_anti);

        $probe = $this->type === Join::right ? $this->padded($build, $left->nullRow, $leftSchema) : $build;
        $nullIndex = $build->count();

        $rightSchema = $right->schema;
        $outputSchema = null;

        foreach ($right->rows as $batch) {
            $rightSchema ??= $batch->schema();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);
            [$candidatesRight, $candidatesLeft] = $this->candidates($table, $this->rightValues->of($batch));
            $met = $this->meet($build, $candidatesLeft, $batch, $candidatesRight);

            $outputLeft = [];
            $outputRight = [];
            $pair = 0;
            $pairs = count($candidatesRight);

            for ($i = 0, $rows = $batch->count(); $i < $rows; $i++) {
                $matched = false;

                for (; $pair < $pairs && $candidatesRight[$pair] === $i; $pair++) {
                    if (!$met[$pair]) {
                        continue;
                    }

                    $matched = true;

                    if ($this->type === Join::left || $this->type === Join::left_anti) {
                        $table->matched($candidatesLeft[$pair]);
                    }

                    if ($this->type !== Join::left_anti) {
                        $outputLeft[] = $candidatesLeft[$pair];
                        $outputRight[] = $i;
                    }
                }

                if (!$matched && $this->type === Join::right) {
                    $outputLeft[] = $nullIndex;
                    $outputRight[] = $i;
                }
            }

            if ($outputLeft !== []) {
                yield $this->merge($probe->gather($outputLeft), $batch->gather($outputRight))->project(
                    $outputSchema,
                    $this->backend,
                );
            }
        }

        $rightSchema ??= new Schema();

        if ($this->type === Join::left || $this->type === Join::left_anti) {
            $nullRight = $right->nullRow ?? (new NullRowBuilder($rightSchema, $this->backend))->rows();
            $outputSchema ??= $this->joinSchema->of($this->type, $leftSchema, $rightSchema);

            foreach (array_chunk($table->unmatched(), $this->batchSize) as $unmatched) {
                yield $this->type === Join::left
                    ? $this->merge(
                        $build->gather($unmatched),
                        $nullRight->gather(array_fill(0, count($unmatched), 0)),
                    )->project($outputSchema, $this->backend)
                    : $build->gather($unmatched)->project($outputSchema, $this->backend);
            }
        }
    }

    /**
     * @param list<int> $leftIndices
     * @param list<int> $rightIndices
     *
     * @return list<bool>
     */
    private function meet(Rows $left, array $leftIndices, Rows $right, array $rightIndices): array
    {
        if ($leftIndices === []) {
            return [];
        }

        $names = static fn(array $refs): array => array_values(array_unique(array_map(
            static fn(Reference $ref): string => $ref->base(),
            $refs,
        )));

        return $this->expression->meet(
            $left->project($left->schema()->keep(...$names($this->expression->left())), $this->backend)->gather(
                $leftIndices,
            ),
            $right
                ->project($right->schema()->keep(...$names($this->expression->right())), $this->backend)
                ->gather($rightIndices),
        );
    }

    /**
     * @throws DuplicatedEntriesException
     */
    private function merge(Rows $left, Rows $right): Rows
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

    private function padded(Rows $build, ?Rows $nullRow, Schema $schema): Rows
    {
        $null = $nullRow ?? (new NullRowBuilder($schema, $this->backend))->rows();

        return $build->withSchema($null->schema())->concat($this->backend, $null);
    }

    private function table(Rows $build, KeyValues $values, bool $trackUnmatched): HashTable
    {
        $table = new HashTable(trackUnmatched: $trackUnmatched);

        foreach ($this->hasher->hash($values->of($build)) as $index => $hash) {
            $table->add($hash, $index);
        }

        return $table;
    }
}
