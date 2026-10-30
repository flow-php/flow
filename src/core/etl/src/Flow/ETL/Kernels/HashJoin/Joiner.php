<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Kernels\Join\JoinSchema;
use Flow\ETL\Values\BatchSize;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Join\JoinType;
use Flow\ETL\Values\Join\Side;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function array_chunk;
use function array_fill;
use function count;

final readonly class Joiner
{
    /**
     * @param null|JoinKeys $keys null: no equality to key by, every build row is a candidate of every probe row
     * @param BatchSize $batchSize - size of output batches emitted for unmatched build side rows
     */
    public function __construct(
        private JoinType $type,
        private JoinSchema $joinSchema,
        private ?JoinKeys $keys,
        private PairPredicate $condition,
        private Backend $backend,
        private BatchSize $batchSize = new BatchSize(1000),
    ) {}

    /**
     * @param bool $buildLeft - build the hash table from the left side and stream the right one, memory is then bounded
     *                        by the left side
     *
     * @return Generator<Rows>
     */
    public function join(JoinSide $left, JoinSide $right, bool $buildLeft = false): Generator
    {
        $role = $buildLeft ? Role::buildLeft($this->type) : Role::buildRight($this->type);
        $builtSide = $buildLeft ? $left : $right;
        $probeSide = $buildLeft ? $right : $left;
        [$build, $buildSchema] = $this->build($builtSide);
        // the first probe batch names the probe types, when the side declares none
        $probeSchema = $probeSide->schema ?? ($probeSide->rows->valid() ? $probeSide->rows->current()->schema() : null);
        [$leftKeys, $rightKeys] = $this->keys?->encoders(
            $buildLeft ? $buildSchema : $probeSchema,
            $buildLeft ? $probeSchema : $buildSchema,
        ) ?? [new KeyEncoder([]), new KeyEncoder([])];
        $table = $this->table($build, $buildLeft ? $leftKeys : $rightKeys, $role->tracksUnmatched());
        $probeKeys = $buildLeft ? $rightKeys : $leftKeys;
        $padsProbe = $this->type->padsWithNullRow($buildLeft ? Side::right : Side::left);
        // a left anti join keeps the left rows alone, so only a left probe yields them while it streams
        $keepsProbeAlone = $this->type === JoinType::left_anti && !$buildLeft;

        // the null row sits at $build->count(), so a gather pads an unmatched probe row with it
        $padded = $padsProbe ? $this->padded($build, $builtSide->nullRow($buildSchema, $this->backend)) : $build;
        $nullIndex = $build->count();

        $output = null;
        $merger = null;

        // valid()/next(), not foreach: reading the probe schema above may have run an empty side to its end
        for ($probe = $probeSide->rows; $probe->valid(); $probe->next()) {
            $batch = $probe->current();
            $probeSchema ??= $batch->schema();
            [$leftSchema, $rightSchema] = $buildLeft ? [$buildSchema, $probeSchema] : [$probeSchema, $buildSchema];
            $output ??= $this->joinSchema->output($leftSchema, $rightSchema);
            $merger ??= $this->joinSchema->merger($leftSchema, $rightSchema);
            [$candidatesProbe, $candidatesBuild] = $this->candidates($table, $probeKeys->of($batch));
            $met = $buildLeft
                ? $this->condition->meet($build, $candidatesBuild, $batch, $candidatesProbe)
                : $this->condition->meet($batch, $candidatesProbe, $build, $candidatesBuild);

            $outputProbe = [];
            $outputBuild = [];
            $pair = 0;
            $pairs = count($candidatesProbe);

            for ($i = 0, $rows = $batch->count(); $i < $rows; $i++) {
                $matched = false;

                for (; $pair < $pairs && $candidatesProbe[$pair] === $i; $pair++) {
                    if (!$met[$pair]) {
                        continue;
                    }

                    $matched = true;

                    if ($role->tracksUnmatched()) {
                        $table->matched($candidatesBuild[$pair]);
                    }

                    if ($this->type !== JoinType::left_anti) {
                        $outputProbe[] = $i;
                        $outputBuild[] = $candidatesBuild[$pair];
                    }
                }

                if (!$matched && $padsProbe) {
                    $outputProbe[] = $i;
                    $outputBuild[] = $nullIndex;
                } elseif (!$matched && $keepsProbeAlone) {
                    $outputProbe[] = $i;
                }
            }

            if ($outputProbe === []) {
                continue;
            }

            // a later batch can be wider than the side schema the output was derived from, and every arm answers
            // for the same declared output - the projection drops the undeclared columns and pads the nulls the join
            // introduced
            yield $keepsProbeAlone
                ? $batch->gather($outputProbe)->project($output, $this->backend)
                : $role->merge($merger, $batch->gather($outputProbe), $padded->gather($outputBuild))->project(
                    $output,
                    $this->backend,
                );
        }

        if (!$role->tracksUnmatched()) {
            return;
        }

        $probeSchema ??= new Schema();
        [$leftSchema, $rightSchema] = $buildLeft ? [$buildSchema, $probeSchema] : [$probeSchema, $buildSchema];
        $output ??= $this->joinSchema->output($leftSchema, $rightSchema);
        $merger ??= $this->joinSchema->merger($leftSchema, $rightSchema);
        $nullProbe = $this->type->padsWithNullRow($role->built())
            ? $probeSide->nullRow($probeSchema, $this->backend)
            : null;

        foreach (array_chunk($table->unmatched(), $this->batchSize->size) as $unmatched) {
            yield $nullProbe === null
                ? $build->gather($unmatched)->project($output, $this->backend)
                : $role->merge(
                    $merger,
                    $nullProbe->gather(array_fill(0, count($unmatched), 0)),
                    $build->gather($unmatched),
                )->project($output, $this->backend);
        }
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
            Rows::concat($schema, $this->backend, ...$parts),
            $schema,
        ];
    }

    /**
     * @param list<string> $keys
     *
     * @return array{list<int>, list<int>}
     */
    private function candidates(HashTable $table, array $keys): array
    {
        $left = [];
        $right = [];

        foreach ($keys as $i => $key) {
            foreach ($table->candidatesFor($key) as $j) {
                $left[] = $i;
                $right[] = $j;
            }
        }

        return [$left, $right];
    }

    private function padded(Rows $build, Rows $nullRow): Rows
    {
        return Rows::concat($nullRow->schema(), $this->backend, $build->withSchema($nullRow->schema()), $nullRow);
    }

    private function table(Rows $build, KeyEncoder $keys, bool $trackUnmatched): HashTable
    {
        $table = new HashTable(trackUnmatched: $trackUnmatched);

        foreach ($keys->of($build) as $index => $key) {
            $table->add($key, $index);
        }

        return $table;
    }
}
