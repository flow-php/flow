<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Evaluation;

use Exception;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Rows;

use function array_diff;
use function array_values;
use function range;

final readonly class TolerantEvaluation
{
    public const int RETRIES = 8;

    /**
     * @return array{failed: array<int, true>, values: array<int, mixed>} values of every row that did not fail
     */
    public function evaluate(ScalarFunction $function, Rows $rows, FlowContext $context): array
    {
        $live = $rows->isEmpty() ? [] : range(0, $rows->count() - 1);
        $failed = [];
        $values = [];
        $retries = 0;

        while (true) {
            try {
                // @mago-ignore analysis:mixed-assignment
                foreach ((new Selection($live))
                    ->evaluate($function, $rows, $context)
                    ->values() as $k => $value) {
                    $values[$live[$k]] = $value;
                }

                break;
            } catch (EvaluationException $e) {
                $failed[$e->rowIndex] = true;
                $live = array_values(array_diff($live, [$e->rowIndex]));

                if (++$retries === self::RETRIES || $live === []) {
                    $lane = $this->rowByRow($function, $rows, $live, $context);

                    return ['failed' => $failed + $lane['failed'], 'values' => $values + $lane['values']];
                }
            } catch (Exception) {
                // no row coordinate: nothing to exclude
                $lane = $this->rowByRow($function, $rows, $live, $context);

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
    public function rowByRow(ScalarFunction $function, Rows $rows, array $indices, FlowContext $context): array
    {
        $failed = [];
        $values = [];

        foreach ($indices as $i) {
            try {
                $values[$i] = $function->eval($rows->slice($i, 1), $context)->values()[0];
            } catch (Exception) {
                $failed[$i] = true;
            }
        }

        return ['failed' => $failed, 'values' => $values];
    }
}
