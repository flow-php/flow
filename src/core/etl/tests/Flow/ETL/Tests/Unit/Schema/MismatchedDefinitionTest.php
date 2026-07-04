<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\Validator\MismatchedDefinition;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\string_schema;

final class MismatchedDefinitionTest extends FlowTestCase
{
    public function test_exposes_expected_and_given_definitions(): void
    {
        $expectedDefinition = integer_schema('id');
        $givenDefinition = string_schema('id');

        $mismatchedDefinition = new MismatchedDefinition($expectedDefinition, $givenDefinition);

        static::assertSame($expectedDefinition, $mismatchedDefinition->expected());
        static::assertSame($givenDefinition, $mismatchedDefinition->given());
    }
}
