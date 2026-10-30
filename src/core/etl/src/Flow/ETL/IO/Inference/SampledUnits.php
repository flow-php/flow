<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Inference;

use Flow\ETL\Values\Cardinality;

use function count;
use function round;

/**
 * The samples one inference read, in unit order; units that were never sniffed are not counted.
 */
final readonly class SampledUnits
{
    /**
     * @param list<Sample> $samples
     */
    public function __construct(
        public array $samples,
    ) {}

    /**
     * Listed bytes over the mean bytes of the sampled rows; exact when the samples read every row of every listed unit.
     */
    public function estimatedRows(int $listedUnits, Cardinality $listedBytes): Cardinality
    {
        $exact = $this->exactRows($listedUnits);

        if ($exact->exactly() !== null) {
            return $exact;
        }

        $rows = 0;
        $bytes = 0;

        foreach ($this->samples as $sample) {
            $rows += $sample->columns->rows;
            $bytes += $sample->bytes;
        }

        if ($listedBytes->estimate === null || $rows === 0 || $bytes === 0) {
            return Cardinality::unknown();
        }

        return Cardinality::approximately(
            (int) round($listedBytes->estimate / ($bytes / $rows)),
            Cardinality::DEFAULT_RELATIVE_ERROR,
        );
    }

    /**
     * Exact when the samples read every row of every listed unit, unknown otherwise.
     */
    public function exactRows(int $listedUnits): Cardinality
    {
        $rows = 0;

        foreach ($this->samples as $sample) {
            if (!$sample->complete) {
                return Cardinality::unknown();
            }

            $rows += $sample->columns->rows;
        }

        return count($this->samples) === $listedUnits ? Cardinality::exact($rows) : Cardinality::unknown();
    }
}
