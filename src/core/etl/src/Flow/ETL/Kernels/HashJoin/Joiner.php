<?php

declare(strict_types=1);

namespace Flow\ETL\Kernels\HashJoin;

use Flow\ETL\Foundation\Progress;
use Flow\ETL\Foundation\VoidProgress;
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
use function is_int;

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
        private Progress $progress = new VoidProgress(),
    ) {}

    /**
     * @param bool $buildLeft - build the hash table from the left side and stream the right one, memory is then bounded
     *                        by the left side
     *
     * @return Generator<Rows>
     */
    public function join(JoinSide $left, JoinSide $right, bool $buildLeft = false): Generator
    {
        $role = $buildLeft ? BuildSide::buildLeft($this->type) : BuildSide::buildRight($this->type);
        $builtSide = $buildLeft ? $left : $right;
        $probeSide = $buildLeft ? $right : $left;
        [$build, $buildSchema] = $this->build($builtSide);
        // the first probe batch names the probe types, when the side declares none
        $probeSchema = $probeSide->schema ?? ($probeSide->rows->valid() ? $probeSide->rows->current()->schema() : null);
        $types = $this->keys?->types($buildLeft ? $buildSchema : $probeSchema, $buildLeft ? $probeSchema : $buildSchema)
        ?? [];
        $tracksUnmatched = $role->tracksUnmatched();
        $table = new HashTable(trackUnmatched: $tracksUnmatched);
        $groups = null;
        $buildDecides = false;

        if ($this->keys === null || $build->isEmpty()) {
            $this->fill($table, array_fill(0, count($build), 0));
        } else {
            $buildKeys = $buildLeft ? $this->keys->leftKeys($build, $types) : $this->keys->rightKeys($build, $types);
            $groups = $this->backend->groupTable($buildKeys->schema()->names(), $buildKeys->schema());
            $this->fill($table, $groups->assign($buildKeys)->ids->physicals());
            $buildDecides = $this->keys->decides($buildKeys);
        }

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
            $this->progress->advanced($batch->count());
            $probeSchema ??= $batch->schema();
            [$leftSchema, $rightSchema] = $buildLeft ? [$buildSchema, $probeSchema] : [$probeSchema, $buildSchema];
            $output ??= $this->joinSchema->output($leftSchema, $rightSchema);
            $merger ??= $this->joinSchema->merger($leftSchema, $rightSchema);
            $skipsPairCheck = false;

            if ($this->keys === null) {
                $probeGroups = array_fill(0, count($batch), 0);
            } elseif ($batch->isEmpty()) {
                $probeGroups = [];
            } else {
                $probeKeys = $buildLeft
                    ? $this->keys->rightKeys($batch, $types)
                    : $this->keys->leftKeys($batch, $types);
                $probeGroups = $groups?->lookup($probeKeys)->physicals() ?? [];
                $skipsPairCheck = $buildDecides && $this->keys->decides($probeKeys);
            }

            [$candidatesProbe, $candidatesBuild] = $this->candidates($table, $probeGroups);
            // keys that decide the condition make its pair check confirm every candidate
            $met = $skipsPairCheck
                ? null
                : (
                    $buildLeft
                        ? $this->condition->meet($build, $candidatesBuild, $batch, $candidatesProbe)
                        : $this->condition->meet($batch, $candidatesProbe, $build, $candidatesBuild)
                );

            $outputProbe = [];
            $outputBuild = [];
            $pair = 0;
            $pairs = count($candidatesProbe);

            for ($i = 0, $rows = $batch->count(); $i < $rows; $i++) {
                $matched = false;

                for (; $pair < $pairs && $candidatesProbe[$pair] === $i; $pair++) {
                    if ($met !== null && !$met[$pair]) {
                        continue;
                    }

                    $matched = true;

                    if ($tracksUnmatched) {
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

        if (!$tracksUnmatched) {
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
            $this->progress->busy();

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
            // the build side's rows stand on that side's read line; the join counts the stream it probes
            $this->progress->busy();

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
     * @param array<int, mixed> $groups the group of each probe row, null for a key the build side lacks
     *
     * @return array{list<int>, list<int>}
     */
    private function candidates(HashTable $table, array $groups): array
    {
        $left = [];
        $right = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($groups as $i => $group) {
            if (!is_int($group)) {
                continue;
            }

            foreach ($table->candidatesFor($group) as $j) {
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

    /**
     * @param array<int, mixed> $groups the group of each build row
     */
    private function fill(HashTable $table, array $groups): void
    {
        $unreported = 0;

        // @mago-ignore analysis:mixed-assignment
        foreach ($groups as $index => $group) {
            $table->add((int) $group, $index);

            if (++$unreported === Progress::ROWS_PER_CALL) {
                $this->progress->busy();
                $unreported = 0;
            }
        }

        $this->progress->busy();
    }
}
