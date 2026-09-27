<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Comparator;

use Flow\ETL\Row;
use SebastianBergmann\Comparator\Comparator;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Exporter\Exporter;

use function assert;

final class RowComparator extends Comparator
{
    public function accepts(mixed $expected, mixed $actual): bool
    {
        return $expected instanceof Row && $actual instanceof Row;
    }

    public function assertEquals(
        mixed $expected,
        mixed $actual,
        float $delta = 0.0,
        bool $canonicalize = false,
        bool $ignoreCase = false,
    ): void {
        assert($expected instanceof Row && $actual instanceof Row);

        $exporter = new Exporter();

        if ($expected->names() !== $actual->names()) {
            throw new ComparisonFailure(
                $expected,
                $actual,
                $exporter->export($expected->names()),
                $exporter->export($actual->names()),
                'Failed asserting that two Row objects have the same names.',
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
                'Failed asserting that two Row objects are equal.',
            );
        }
    }
}
