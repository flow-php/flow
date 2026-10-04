<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Comparator;

use Flow\ETL\Rows;
use SebastianBergmann\Comparator\Comparator;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Exporter\Exporter;

use function assert;

final class RowsComparator extends Comparator
{
    public function accepts(mixed $expected, mixed $actual): bool
    {
        return $expected instanceof Rows && $actual instanceof Rows;
    }

    public function assertEquals(
        mixed $expected,
        mixed $actual,
        float $delta = 0.0,
        bool $canonicalize = false,
        bool $ignoreCase = false,
    ): void {
        assert($expected instanceof Rows && $actual instanceof Rows);

        $exporter = new Exporter();

        if (!$expected->schema()->isSame($actual->schema())) {
            throw new ComparisonFailure(
                $expected,
                $actual,
                $exporter->export($expected->schema()->normalize()),
                $exporter->export($actual->schema()->normalize()),
                'Failed asserting that two Rows have the same schema.',
            );
        }

        $expectedArray = $expected->toArray();
        $actualArray = $actual->toArray();

        try {
            $this
                ->factory()
                ->getComparatorFor($expectedArray, $actualArray)
                ->assertEquals($expectedArray, $actualArray, $delta, $canonicalize, $ignoreCase);
        } catch (ComparisonFailure) {
            throw new ComparisonFailure(
                $expected,
                $actual,
                $exporter->export($expectedArray),
                $exporter->export($actualArray),
                'Failed asserting that two Rows are equal.',
            );
        }
    }
}
