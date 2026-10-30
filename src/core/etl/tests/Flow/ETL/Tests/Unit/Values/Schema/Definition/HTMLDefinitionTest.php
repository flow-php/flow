<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Schema\Definition;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\HTMLDefinition;
use Flow\ETL\Values\Schema\Metadata;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\RequiresPhp;

use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\html_schema;
use function Flow\ETL\DSL\null_schema;
use function Flow\ETL\DSL\string_schema;
use function Flow\Types\DSL\type_html;

final class HTMLDefinitionTest extends FlowTestCase
{
    public static function provideIsCompatibleCases(): Generator
    {
        yield 'same type and name' => [
            html_schema('content'),
            html_schema('content'),
            true,
        ];

        yield 'same type different name' => [
            html_schema('content'),
            html_schema('other'),
            false,
        ];

        yield 'not nullable with nullable' => [
            html_schema('content', false),
            html_schema('content', true),
            false,
        ];

        yield 'nullable with not nullable' => [
            html_schema('content', true),
            html_schema('content', false),
            true,
        ];
    }

    public static function provideMergeCases(): Generator
    {
        yield 'same type' => [
            html_schema('content'),
            html_schema('content'),
            html_schema('content'),
        ];

        yield 'makes nullable when other is nullable' => [
            html_schema('content', false),
            html_schema('content', true),
            html_schema('content', true),
        ];

        yield 'with string produces string' => [
            html_schema('col'),
            string_schema('col'),
            string_schema('col'),
        ];
    }

    public function test_add_metadata(): void
    {
        $def = html_schema('content');

        $withMeta = $def->addMetadata('key', 'value');

        static::assertTrue($withMeta->metadata()->has('key'));
        static::assertSame('value', $withMeta->metadata()->get('key'));
        static::assertFalse($def->metadata()->has('key'));
    }

    public function test_does_not_match_a_null_entry_when_not_nullable(): void
    {
        static::assertFalse(html_schema('col')->matches(null));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_does_not_match_entry_with_different_type(): void
    {
        static::assertFalse(html_schema('col')->matches(1));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     */
    #[DataProvider('provideIsCompatibleCases')]
    public function test_is_compatible(Definition $definition, Definition $other, bool $expected): void
    {
        static::assertSame($expected, $definition->isCompatible($other));
    }

    public function test_is_same_with_different_metadata(): void
    {
        static::assertFalse(html_schema('content', false, Metadata::with('key', 'value1'))->isSame(html_schema(
            'content',
            false,
            Metadata::with('key', 'value2'),
        )));
    }

    public function test_is_same_with_different_nullability(): void
    {
        static::assertFalse(html_schema('content', true)->isSame(html_schema('content', false)));
    }

    public function test_is_same_with_different_type(): void
    {
        static::assertFalse(html_schema('content')->isSame(string_schema('content')));
    }

    public function test_is_same_with_identical_definition(): void
    {
        static::assertTrue(html_schema('content', true, Metadata::with('key', 'value'))->isSame(html_schema(
            'content',
            true,
            Metadata::with('key', 'value'),
        )));
    }

    public function test_make_nullable(): void
    {
        $def = html_schema('content', false);

        static::assertTrue($def->makeNullable()->isNullable());
        static::assertFalse($def->isNullable());
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_matches_entry_with_same_name_and_type(): void
    {
        static::assertTrue(html_schema('content')->matches(type_html()->cast(
            '<!DOCTYPE html><html><head></head><body></body></html>',
        )));
    }

    /**
     * @param Definition<mixed> $definition
     * @param Definition<mixed> $other
     * @param Definition<mixed> $expected
     */
    #[DataProvider('provideMergeCases')]
    public function test_merge(Definition $definition, Definition $other, Definition $expected): void
    {
        static::assertEquals($expected, $definition->merge($other));
    }

    public function test_merge_with_null_definition_keeps_original_type(): void
    {
        $merged = html_schema('col', false)->merge(null_schema('col'));

        static::assertInstanceOf(HTMLDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_when_this_is_null_definition(): void
    {
        $merged = null_schema('col')->merge(html_schema('col', false));

        static::assertInstanceOf(HTMLDefinition::class, $merged);
        static::assertTrue($merged->isNullable());
    }

    public function test_merge_with_different_entry_name_throws_exception(): void
    {
        $def = html_schema('content');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Cannot merge different definitions, content and other');

        $def->merge(html_schema('other'));
    }

    public function test_merge_with_incompatible_type_falls_back_to_string(): void
    {
        static::assertSame('string', html_schema('col')->merge(new BooleanDefinition('col'))->type()->toString());
    }

    public function test_normalize(): void
    {
        $normalized = html_schema('content', true, Metadata::with('key', 'value'))->normalize();

        static::assertSame('content', $normalized['ref']);
        static::assertTrue($normalized['nullable']);
        static::assertArrayHasKey('type', $normalized);
        static::assertArrayHasKey('metadata', $normalized);
    }

    public function test_nullable_does_not_match_an_entry_of_a_different_type(): void
    {
        static::assertFalse(html_schema('col', true)->matches(1));
    }

    public function test_nullable_matches_null(): void
    {
        static::assertTrue(html_schema('col', true)->matches(null));
    }

    #[RequiresPhp('>= 8.4.0')]
    public function test_nullable_matches_an_entry_with_a_non_null_value_of_its_type(): void
    {
        static::assertTrue(html_schema('col', true)->matches(type_html()->cast(
            '<!DOCTYPE html><html><head></head><body></body></html>',
        )));
    }

    public function test_rename(): void
    {
        $def = html_schema('content');

        static::assertSame('new_content', $def->rename('new_content')->name());
        static::assertSame('content', $def->name());
    }

    public function test_set_metadata(): void
    {
        $def = html_schema('content');
        $metadata = Metadata::with('key', 'value');

        static::assertTrue($def->setMetadata($metadata)->metadata()->isEqual($metadata));
        static::assertTrue($def->metadata()->isEmpty());
    }

    public function test_type_returns_html_type(): void
    {
        static::assertSame('html', html_schema('content')->type()->toString());
    }

    public function test_merge_with_an_unrelated_type_falls_back_to_common_type(): void
    {
        static::assertSame('string', html_schema('col')->merge(bool_schema('col'))->type()->toString());
    }
}
