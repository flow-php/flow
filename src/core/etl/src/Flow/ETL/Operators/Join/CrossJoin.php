<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\Join;

use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\Kernels\Join\JoinSchema;
use Flow\ETL\Operators\BoundStep;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

final readonly class CrossJoin
{
    public function __construct(
        public string $prefix = '',
    ) {}

    /**
     * @return BoundStep<self>
     */
    public function bind(Schema $left, Schema $right): BoundStep
    {
        return new BoundStep($this, (new JoinSchema($this->prefix))->cross($left, $right));
    }

    /**
     * Every left batch paired with every right row. The right side is read once, on the first left batch, so an
     * empty left never reads it.
     *
     * @param Generator<Rows> $left
     * @param Generator<Rows> $right
     *
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function join(Generator $left, Generator $right, Backend $backend): Generator
    {
        $schema = new JoinSchema($this->prefix);
        $collected = null;

        foreach ($left as $rows) {
            if ($collected === null) {
                $collected = [];

                foreach ($right as $batch) {
                    if (!$batch->isEmpty()) {
                        $collected[] = $batch;
                    }
                }

                $collected = $collected === [] ? [] : [Rows::concat($collected[0]->schema(), $backend, ...$collected)];
            }

            if ($collected === [] || $rows->isEmpty()) {
                continue;
            }

            $leftIndices = [];
            $rightIndices = [];
            $rightCount = $collected[0]->count();

            for ($i = 0, $leftCount = $rows->count(); $i < $leftCount; $i++) {
                for ($j = 0; $j < $rightCount; $j++) {
                    $leftIndices[] = $i;
                    $rightIndices[] = $j;
                }
            }

            $signal = yield $schema
                ->merger($rows->schema(), $collected[0]->schema())
                ->merge($rows->gather($leftIndices), $collected[0]->gather($rightIndices))
                ->project($schema->cross($rows->schema(), $collected[0]->schema()), $backend);

            if ($signal === Signal::STOP) {
                $left->send(Signal::STOP);

                return;
            }
        }
    }
}
