<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Bridge\Valinor\Tests\Unit;

use CuyZ\Valinor\MapperBuilder;
use Flow\PostgreSql\Bridge\Valinor\Tests\Unit\Fixture\SimpleDto;
use Flow\PostgreSql\Bridge\Valinor\ValinorBuilderMapper;
use Flow\PostgreSql\Bridge\Valinor\ValinorTreeMapper;
use Flow\PostgreSql\Tests\Mother\MapperContextMother;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\Bridge\Valinor\DSL\valinor_builder_mapper;
use function Flow\PostgreSql\Bridge\Valinor\DSL\valinor_tree_mapper;

final class DSLTest extends TestCase
{
    public function test_valinor_builder_mapper_delegates_to_class(): void
    {
        $mapper = valinor_builder_mapper(new MapperBuilder(), SimpleDto::class);

        static::assertInstanceOf(ValinorBuilderMapper::class, $mapper);
        static::assertSame(
            1,
            $mapper->map(['id' => 1, 'name' => 'Jane', 'email' => 'jane@example.com'], MapperContextMother::any())->id,
        );
    }

    public function test_valinor_tree_mapper_delegates_to_class(): void
    {
        $mapper = valinor_tree_mapper((new MapperBuilder())->mapper(), SimpleDto::class);

        static::assertInstanceOf(ValinorTreeMapper::class, $mapper);
        static::assertSame(
            1,
            $mapper->map(['id' => 1, 'name' => 'Jane', 'email' => 'jane@example.com'], MapperContextMother::any())->id,
        );
    }
}
