<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema\Inference;

use Flow\ETL\Schema\Inference\SchemaInference;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_html;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_time_zone;
use function Flow\Types\DSL\type_xml;

final class SchemaInferenceTest extends FlowTestCase
{
    public function test_defaults(): void
    {
        $inference = new SchemaInference();

        static::assertSame(20_480, $inference->sampleSize);
        static::assertSame(10, $inference->filesToSniff);
        static::assertNull($inference->types);
        static::assertFalse($inference->unionByName);
    }

    public function test_the_default_candidate_set_is_every_rung_but_markup(): void
    {
        $candidates = (new SchemaInference())->candidates();

        static::assertTrue($candidates->allows(type_integer()));
        static::assertTrue($candidates->allows(type_date()));
        static::assertTrue($candidates->allows(type_time_zone()));
        static::assertFalse($candidates->allows(type_html()));
        static::assertFalse($candidates->allows(type_xml()));
    }
}
