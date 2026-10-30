<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Evaluation;

use Exception;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;

use function array_diff;
use function array_values;
use function range;

final readonly class TolerantEvaluation
{
    public const int RETRIES = 8;

    /**
     * @return array{failed: array<int, true>, values: array<int, mixed>} values of every row that did not fail
     */
    public function evaluate(ScalarFunction $function, Rows $rows, Backend $backend): array
    {
        $live = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);
        $failed = [];
        $values = [];
        $retries = 0;

        while (true) {
            try {
                // @mago-ignore analysis:mixed-assignment
                foreach ((new Selection($live))
                    ->evaluate($function, $rows, $backend)
                    ->values() as $k => $value) {
                    $values[$live[$k]] = $value;
                }

                break;
            } catch (EvaluationException $e) {
                $failed[$e->rowIndex] = true;
                $live = array_values(array_diff($live, [$e->rowIndex]));

                if (++$retries === self::RETRIES || $live === []) {
                    $lane = $this->rowByRow($function, $rows, $live, $backend);

                    return ['failed' => $failed + $lane['failed'], 'values' => $values + $lane['values']];
                }
            } catch (Exception) {
                // no row coordinate: nothing to exclude
                $lane = $this->rowByRow($function, $rows, $live, $backend);

                return ['failed' => $failed + $lane['failed'], 'values' => $values + $lane['values']];
            }
        }

        return ['failed' => $failed, 'values' => $values];
    }

    /**
     * @param list<int> $indices
     *
     * @return array{failed: array<int, true>, values: array<int, mixed>}
     */
    public function rowByRow(ScalarFunction $function, Rows $rows, array $indices, Backend $backend): array
    {
        $failed = [];
        $values = [];

        foreach ($indices as $i) {
            try {
                $values[$i] = $function->eval($rows->slice($i, 1), $backend)->values()[0];
            } catch (Exception) {
                $failed[$i] = true;
            }
        }

        return ['failed' => $failed, 'values' => $values];
    }
}
