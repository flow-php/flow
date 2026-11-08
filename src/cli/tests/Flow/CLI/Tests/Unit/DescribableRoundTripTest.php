<?php

declare(strict_types=1);

namespace Flow\CLI\Tests\Unit;

use Flow\CLI\Tests\Context\DescribableFixtures;
use Flow\ETL\Foundation\Describe\Describable;
use Flow\ETL\Tests\Context\ExtractorClasses;
use Flow\ETL\Tests\Context\ScalarFunctionClasses;
use Flow\ETL\Tests\FlowTestCase;

use function array_diff;
use function array_keys;

final class DescribableRoundTripTest extends FlowTestCase
{
    public function test_every_describable_has_a_fixture(): void
    {
        static::assertSame(
            [],
            array_diff(
                ScalarFunctionClasses::implementing(Describable::class, ExtractorClasses::root('cli')),
                array_keys(DescribableFixtures::all()),
            ),
        );
    }
}
