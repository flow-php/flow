<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Comparator;

use Flow\ETL\Column\Column;
use SebastianBergmann\Comparator\Comparator;
use SebastianBergmann\Comparator\ComparisonFailure;
use SebastianBergmann\Exporter\Exporter;

use function assert;

/**
 * An extension object exports no properties, so two native columns are compared by type and physicals.
 */
final class ColumnComparator extends Comparator
{
    public function accepts(mixed $expected, mixed $actual): bool
    {
        return $expected instanceof Column && $actual instanceof Column;
    }

    public function assertEquals(
        mixed $expected,
        mixed $actual,
        float $delta = 0.0,
        bool $canonicalize = false,
        bool $ignoreCase = false,
    ): void {
        assert($expected instanceof Column && $actual instanceof Column);

        $exporter = new Exporter();

        if ($expected->type()->toString() !== $actual->type()->toString()) {
            throw new ComparisonFailure(
                $expected,
                $actual,
                $expected->type()->toString(),
                $actual->type()->toString(),
                'Failed asserting that two columns have the same type.',
            );
        }

        $expectedPhysicals = $expected->physicals();
        $actualPhysicals = $actual->physicals();

        try {
            $this
                ->factory()
                ->getComparatorFor($expectedPhysicals, $actualPhysicals)
                ->assertEquals($expectedPhysicals, $actualPhysicals, $delta, $canonicalize, $ignoreCase);
        } catch (ComparisonFailure) {
            throw new ComparisonFailure(
                $expected,
                $actual,
                $exporter->export($expectedPhysicals),
                $exporter->export($actualPhysicals),
                'Failed asserting that two columns are equal.',
            );
        }
    }
}
