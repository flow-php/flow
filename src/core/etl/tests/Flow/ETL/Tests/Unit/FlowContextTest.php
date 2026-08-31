<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit;

use Flow\Calculator\Calculator;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\FilesystemTable;
use ReflectionClass;
use ReflectionMethod;
use ReflectionParameter;

use function array_filter;
use function array_map;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;

final class FlowContextTest extends FlowTestCase
{
    public function test_config_constructor_takes_no_filesystem_table(): void
    {
        $constructor = (new ReflectionClass(Config::class))->getConstructor();

        static::assertNotNull($constructor);
        static::assertCount(17, $constructor->getParameters());
        static::assertSame(
            [],
            array_filter(
                $constructor->getParameters(),
                static fn(ReflectionParameter $p): bool => (string) $p->getType() === FilesystemTable::class,
            ),
        );
    }

    public function test_flow_context_exposes_no_filesystem_and_no_streams(): void
    {
        // a new name here would mean a replacement member was added back
        static::assertSame(
            [
                '__construct',
                'cache',
                'calculator',
                'errorHandler',
                'hydrator',
                'setErrorHandler',
                'telemetry',
            ],
            array_map(
                static fn(ReflectionMethod $m): string => $m->getName(),
                (new ReflectionClass(FlowContext::class))->getMethods(ReflectionMethod::IS_PUBLIC),
            ),
        );
    }

    public function test_provides_shared_calculator_instance_from_config(): void
    {
        $context = flow_context(config());

        static::assertInstanceOf(Calculator::class, $context->calculator());
        static::assertSame($context->calculator(), $context->calculator());
    }
}
