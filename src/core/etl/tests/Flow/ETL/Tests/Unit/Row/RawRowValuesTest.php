<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use Flow\ETL\Row\RawRowValues;
use Flow\ETL\Schema\Metadata;
use Flow\ETL\Tests\FlowTestCase;

final class RawRowValuesTest extends FlowTestCase
{
    public function test_metadata_defaults_to_empty(): void
    {
        $rowValues = new RawRowValues(['id' => 1]);

        static::assertSame(['id' => 1], $rowValues->values);
        static::assertSame([], $rowValues->metadata);
    }

    public function test_carries_per_value_metadata(): void
    {
        $metadata = Metadata::with('source', 'decoder');
        $rowValues = new RawRowValues(['id' => null], ['id' => $metadata]);

        static::assertSame(['id' => null], $rowValues->values);
        static::assertSame(['id' => $metadata], $rowValues->metadata);
    }
}
