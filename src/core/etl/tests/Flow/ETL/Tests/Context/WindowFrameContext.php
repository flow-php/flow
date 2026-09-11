<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\DataFrame;
use Flow\ETL\Function\WindowFunction;

use function Flow\ETL\DSL\data_frame;
use function Flow\ETL\DSL\from_array;
use function is_float;
use function is_int;

final class WindowFrameContext
{
    /**
     * @return list<float|int|null>
     */
    public static function results(DataFrame $frame): array
    {
        $results = [];

        foreach ($frame->get() as $batch) {
            foreach ($batch as $row) {
                $value = $row->get('result');

                $results[] = is_int($value) || is_float($value) ? $value : null;
            }
        }

        return $results;
    }

    /**
     * Five ascending salaries in a single department, the dataset every frame expectation
     * in WindowFunctionsTest was derived from against PostgreSQL.
     *
     * @return list<float|int|null>
     */
    public static function salaries(WindowFunction $function): array
    {
        return self::results(
            data_frame()
                ->read(from_array([
                    ['id' => 1, 'department' => 'IT', 'date' => '2024-01-01', 'salary' => 100],
                    ['id' => 2, 'department' => 'IT', 'date' => '2024-01-02', 'salary' => 200],
                    ['id' => 3, 'department' => 'IT', 'date' => '2024-01-03', 'salary' => 300],
                    ['id' => 4, 'department' => 'IT', 'date' => '2024-01-04', 'salary' => 400],
                    ['id' => 5, 'department' => 'IT', 'date' => '2024-01-05', 'salary' => 500],
                ]))
                ->withEntry('result', $function),
        );
    }

    /**
     * Two rows tied on date, the peer-awareness fixture.
     *
     * @return list<float|int|null>
     */
    public static function tiedDates(WindowFunction $function): array
    {
        return self::results(
            data_frame()
                ->read(from_array([
                    ['id' => 1, 'department' => 'IT', 'date' => '2024-01-01', 'salary' => 100],
                    ['id' => 2, 'department' => 'IT', 'date' => '2024-01-01', 'salary' => 200],
                    ['id' => 3, 'department' => 'IT', 'date' => '2024-01-02', 'salary' => 300],
                    ['id' => 4, 'department' => 'IT', 'date' => '2024-01-03', 'salary' => 400],
                ]))
                ->withEntry('result', $function),
        );
    }
}
