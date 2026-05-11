<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client;

use Flow\PostgreSql\Client\Context;
use Flow\PostgreSql\Client\Exception\ContextException;
use Flow\PostgreSql\Schema\Catalog;
use Flow\Types\Exception\InvalidTypeException;
use PHPUnit\Framework\TestCase;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;

final class ContextTest extends TestCase
{
    public function test_all_returns_user_data(): void
    {
        $context = new Context(data: ['tenant' => 42]);

        static::assertSame(['tenant' => 42], $context->all());
    }

    public function test_catalog_returns_null_when_unset(): void
    {
        static::assertNull((new Context())->catalog());
    }

    public function test_catalog_returns_provided_instance(): void
    {
        $catalog = new Catalog([]);

        static::assertSame($catalog, (new Context(catalog: $catalog))->catalog());
    }

    public function test_get_propagates_type_assertion_failure(): void
    {
        $this->expectException(InvalidTypeException::class);

        (new Context(data: ['count' => 'not-an-int']))->get('count', type_integer());
    }

    public function test_get_returns_asserted_value(): void
    {
        static::assertSame(42, (new Context(data: ['tenant' => 42]))->get('tenant', type_integer()));
    }

    public function test_get_throws_for_missing_key(): void
    {
        $this->expectException(ContextException::class);
        $this->expectExceptionMessage('Context has no value for key "missing".');

        (new Context())->get('missing', type_string());
    }

    public function test_has_returns_false_for_missing_key(): void
    {
        static::assertFalse((new Context())->has('tenant'));
    }

    public function test_has_returns_true_for_existing_key(): void
    {
        static::assertTrue((new Context(data: ['tenant' => 42]))->has('tenant'));
    }

    public function test_merge_catalog_prefers_other_when_set(): void
    {
        $selfCatalog = new Catalog([]);
        $otherCatalog = new Catalog([]);

        $result = (new Context(catalog: $selfCatalog))->merge(new Context(catalog: $otherCatalog));

        static::assertSame($otherCatalog, $result->catalog());
    }

    public function test_merge_catalog_preserves_self_when_other_null(): void
    {
        $catalog = new Catalog([]);

        $result = (new Context(catalog: $catalog))->merge(new Context());

        static::assertSame($catalog, $result->catalog());
    }

    public function test_merge_data_other_wins_on_key_conflict(): void
    {
        $result = (new Context(data: ['a' => 1, 'b' => 2]))->merge(new Context(data: ['b' => 20, 'c' => 30]));

        static::assertSame(['a' => 1, 'b' => 20, 'c' => 30], $result->all());
    }

    public function test_merge_returns_new_instance(): void
    {
        $original = new Context(data: ['a' => 1]);
        $merged = $original->merge(new Context(data: ['b' => 2]));

        static::assertNotSame($original, $merged);
        static::assertSame(['a' => 1], $original->all());
    }

    public function test_with_does_not_mutate_original(): void
    {
        $original = new Context();
        $modified = $original->with('key', 'value');

        static::assertFalse($original->has('key'));
        static::assertTrue($modified->has('key'));
    }

    public function test_with_overwrites_existing_key(): void
    {
        $context = (new Context(data: ['key' => 'old']))->with('key', 'new');

        static::assertSame('new', $context->get('key', type_string()));
    }

    public function test_with_preserves_catalog(): void
    {
        $catalog = new Catalog([]);

        $modified = (new Context($catalog))->with('extra', 'value');

        static::assertSame($catalog, $modified->catalog());
        static::assertSame('value', $modified->get('extra', type_string()));
    }

    public function test_with_returns_new_instance(): void
    {
        $original = new Context();
        $modified = $original->with('key', 'value');

        static::assertNotSame($original, $modified);
    }
}
