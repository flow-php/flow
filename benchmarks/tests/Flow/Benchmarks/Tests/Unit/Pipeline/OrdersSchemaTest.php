<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Tests\Unit\Pipeline;

use Flow\Benchmarks\Pipeline\OrdersSchema;
use Flow\Benchmarks\Pipeline\ServiceSource;
use Flow\Benchmarks\Pipeline\Source;
use PHPUnit\Framework\TestCase;

final class OrdersSchemaTest extends TestCase
{
    public function test_every_service_source_declares_eleven_columns(): void
    {
        foreach (ServiceSource::cases() as $source) {
            static::assertCount(11, OrdersSchema::ofService($source)->definitions(), $source->value);
        }
    }

    public function test_every_source_declares_eleven_columns(): void
    {
        foreach (Source::cases() as $source) {
            static::assertCount(11, OrdersSchema::of($source)->definitions(), $source->value);
        }
    }

    public function test_every_column_is_nullable_on_every_source(): void
    {
        foreach (Source::cases() as $source) {
            foreach (OrdersSchema::of($source)->definitions() as $definition) {
                static::assertTrue($definition->isNullable(), $source->value . '.' . $definition->entry()->name());
            }
        }
    }

    public function test_the_five_projected_columns_exist_on_every_source(): void
    {
        foreach (Source::cases() as $source) {
            $names = [];

            foreach (OrdersSchema::of($source)->definitions() as $definition) {
                $names[] = $definition->entry()->name();
            }

            foreach (['order_id', 'seller_id', 'created_at', 'customer', 'email'] as $projected) {
                static::assertContains($projected, $names, $source->value);
            }
        }
    }

    public function test_the_json_family_reads_its_scalars_as_strings(): void
    {
        foreach ([Source::json, Source::json_lines] as $source) {
            foreach (['order_id', 'seller_id', 'created_at'] as $column) {
                static::assertSame(
                    'string',
                    OrdersSchema::of($source)->definitions()[$column]->type()->toString(),
                    $source->value . '.' . $column,
                );
            }
        }
    }
}
