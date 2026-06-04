<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\MigrationContext;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use PHPUnit\Framework\TestCase;
use stdClass;

final class MigrationContextTest extends TestCase
{
    public function test_attribute_returns_stored_value(): void
    {
        $value = new stdClass();
        $context = new MigrationContext(new SpyClient(), ['service' => $value]);

        static::assertSame($value, $context->attribute('service'));
    }

    public function test_attribute_throws_when_missing(): void
    {
        $context = new MigrationContext(new SpyClient());

        $this->expectException(MigrationException::class);
        $this->expectExceptionMessage('Migration context attribute "missing" not found.');

        $context->attribute('missing');
    }

    public function test_attributes_default_to_empty_array(): void
    {
        $context = new MigrationContext(new SpyClient());

        static::assertSame([], $context->attributes);
    }

    public function test_has_attribute_returns_false_for_missing_key(): void
    {
        $context = new MigrationContext(new SpyClient());

        static::assertFalse($context->hasAttribute('missing'));
    }

    public function test_has_attribute_returns_true_for_existing_key(): void
    {
        $context = new MigrationContext(new SpyClient(), ['service' => new stdClass()]);

        static::assertTrue($context->hasAttribute('service'));
    }

    public function test_has_attribute_returns_true_for_null_value(): void
    {
        $context = new MigrationContext(new SpyClient(), ['nullable' => null]);

        static::assertTrue($context->hasAttribute('nullable'));
        static::assertNull($context->attribute('nullable'));
    }
}
