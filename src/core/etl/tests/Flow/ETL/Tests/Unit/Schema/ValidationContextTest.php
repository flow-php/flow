<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Schema;

use Flow\ETL\Schema\Validator\MismatchedDefinition;
use Flow\ETL\Schema\Validator\ValidationContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\integer_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final class ValidationContextTest extends FlowTestCase
{
    public function test_context_with_mismatched_definitions_is_not_valid(): void
    {
        $mismatchedDefinition = new MismatchedDefinition(integer_schema('id'), string_schema('id'));

        $context = new ValidationContext(mismatchedDefinitions: [$mismatchedDefinition]);

        static::assertFalse($context->isValid());
        static::assertSame([$mismatchedDefinition], $context->mismatchedDefinitions());
    }

    public function test_context_with_missing_definitions_is_not_valid(): void
    {
        $missingDefinition = integer_schema('id');

        $context = new ValidationContext(missingDefinitions: [$missingDefinition]);

        static::assertFalse($context->isValid());
        static::assertSame([$missingDefinition], $context->missingDefinitions());
    }

    public function test_context_with_unexpected_definitions_is_not_valid(): void
    {
        $unexpectedDefinition = bool_schema('active');

        $context = new ValidationContext(unexpectedDefinitions: [$unexpectedDefinition]);

        static::assertFalse($context->isValid());
        static::assertSame([$unexpectedDefinition], $context->unexpectedDefinitions());
    }

    public function test_empty_context_is_valid(): void
    {
        $context = new ValidationContext();

        static::assertTrue($context->isValid());
        static::assertSame([], $context->missingDefinitions());
        static::assertSame([], $context->mismatchedDefinitions());
        static::assertSame([], $context->unexpectedDefinitions());
        static::assertSame('', $context->toString());
    }

    public function test_to_string_renders_full_nested_type_of_deeply_mismatched_definition(): void
    {
        $context = new ValidationContext(mismatchedDefinitions: [
            new MismatchedDefinition(
                map_schema('inventory', type_map(type_string(), type_structure([
                    'sku' => type_string(),
                    'dimensions' => type_list(type_float()),
                ]))),
                map_schema('inventory', type_map(type_string(), type_structure([
                    'sku' => type_string(),
                    'dimensions' => type_list(type_integer()),
                ]))),
            ),
        ]);

        static::assertSame(
            "  Mismatched Definitions: \n"
            . '    |-- expected: inventory<map<string, structure{sku: string, dimensions: list<float>}>>'
            . ', given: inventory<map<string, structure{sku: string, dimensions: list<integer>}>>'
            . "\n",
            $context->toString(),
        );
    }

    public function test_to_string_renders_all_sections_with_nullable_markers(): void
    {
        $context = new ValidationContext(
            missingDefinitions: [datetime_schema('deleted_at', nullable: true)],
            mismatchedDefinitions: [new MismatchedDefinition(
                string_schema('id', nullable: true),
                integer_schema('id'),
            )],
            unexpectedDefinitions: [bool_schema('active')],
        );

        static::assertSame(<<<'DIFF'
              Missing Definitions: 
                |-- deleted_at<?datetime>
              Mismatched Definitions: 
                |-- expected: id<?string>, given: id<integer>
              Unexpected Definitions: 
                |-- active<boolean>

            DIFF, $context->toString());
    }
}
