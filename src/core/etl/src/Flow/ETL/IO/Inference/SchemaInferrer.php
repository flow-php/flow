<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definitions;

use function array_values;
use function Flow\Types\DSL\type_null;
use function max;
use function min;

final readonly class SchemaInferrer
{
    public function __construct(
        private SchemaInference $inference,
    ) {}

    /**
     * everyRow() sniffs every unit whole, through the inference's Sniffs when it takes them, and merges the samples
     * in unit order; a bounded sample drains one row budget across the units in listing order, counting a unit toward
     * filesToSniff only when it yielded a row.
     */
    public function infer(Sniffer $sniffer): Inferred
    {
        $floor = new TypeFloor($this->inference->candidates());
        $samples = [];

        if ($this->inference->sampleSize === null) {
            $units = max(
                0,
                $this->inference->filesToSniff === null
                    ? $sniffer->units()
                    : min($this->inference->filesToSniff, $sniffer->units()),
            );
            $samples = $this->inference->sniffs?->sniff($sniffer, $units);

            if ($samples === null) {
                $samples = [];

                for ($unit = 0; $unit < $units; $unit++) {
                    $samples[] = $sniffer->sniff($unit, null);
                }
            }
        } else {
            $rows = 0;
            $sniffed = 0;

            for ($unit = 0; $unit < $sniffer->units(); $unit++) {
                if ($this->inference->filesToSniff !== null && $sniffed >= $this->inference->filesToSniff) {
                    break;
                }

                if ($rows >= $this->inference->sampleSize) {
                    break;
                }

                $sample = $sniffer->sniff($unit, max(0, $this->inference->sampleSize - $rows));
                $rows += $sample->columns->rows;

                if ($sample->columns->rows > 0) {
                    $sniffed++;
                }

                $samples[] = $sample;
            }
        }

        $merged = null;

        foreach ($samples as $sample) {
            $merged = $merged === null
                ? $sample->columns
                : $merged->merge($sample->columns, $merged->rows === 0 || $this->inference->unionByName);
        }

        if ($merged === null) {
            $definitions = [];

            foreach ($sniffer->names() as $name) {
                $definitions[] = Definitions::of($name, $floor->floor(type_null()), nullable: true);
            }

            return new Inferred(new Schema(...$definitions), new SampledUnits([]));
        }

        return new Inferred($merged->schema($floor), new SampledUnits(array_values($samples)));
    }
}
