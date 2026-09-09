<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit\Docs;

use Flow\Documentation\Docs\DefinedFunctions;
use PHPUnit\Framework\TestCase;

final class DefinedFunctionsTest extends TestCase
{
    public function test_a_dsl_function_is_known_by_its_short_name(): void
    {
        static::assertTrue((new DefinedFunctions())->has('from_csv'));
    }

    public function test_a_dsl_function_is_known_by_its_qualified_name(): void
    {
        static::assertTrue((new DefinedFunctions())->has('Flow\ETL\Adapter\CSV\from_csv'));
    }

    public function test_lookup_ignores_case(): void
    {
        static::assertTrue((new DefinedFunctions())->has('FROM_CSV'));
    }

    public function test_a_function_this_project_does_not_define_is_unknown(): void
    {
        static::assertFalse((new DefinedFunctions())->has('to_database'));
    }
}
