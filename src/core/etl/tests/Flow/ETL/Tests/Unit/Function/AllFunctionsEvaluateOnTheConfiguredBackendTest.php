<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Column\AdaptiveBackend;
use Flow\ETL\Column\PhpBackend;
use Flow\ETL\Column\ValueColumn;
use Flow\ETL\Exception\RequiredPHPVersionException;
use Flow\ETL\Function\ReferenceResolver;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use Throwable;

use function array_keys;
use function extension_loaded;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\flow_context;
use function str_starts_with;

final class AllFunctionsEvaluateOnTheConfiguredBackendTest extends FlowTestCase
{
    /**
     * @return Generator<string, array{class-string<ScalarFunction>}>
     */
    public static function functions(): Generator
    {
        foreach (array_keys(ScalarFunctionFixtures::all()) as $class) {
            yield $class => [$class];
        }
    }

    /**
     * @param class-string<ScalarFunction> $class
     */
    #[DataProvider('functions')]
    public function test_a_function_gives_the_same_values_and_a_column_of_the_configured_backend(string $class): void
    {
        try {
            // a fresh function per run: a function may keep what it evaluated (Literal keeps its column)
            $resolve = static fn(): ScalarFunction => (new ReferenceResolver())->resolve(
                ScalarFunctionFixtures::instance($class),
                ScalarFunctionFixtures::schema(),
            );
            $resolve();
        } catch (RequiredPHPVersionException $e) {
            static::markTestSkipped($e->getMessage());
        }

        try {
            $php = $resolve()->eval(
                ScalarFunctionFixtures::rows(new PhpBackend()),
                flow_context(config_builder()->backend(new PhpBackend())->build()),
            );
        } catch (Throwable $phpFailure) {
            $this->expectException($phpFailure::class);
            $this->expectExceptionMessage($phpFailure->getMessage());

            $resolve()->eval(
                ScalarFunctionFixtures::rows(new AdaptiveBackend()),
                flow_context(config_builder()->backend(new AdaptiveBackend())->build()),
            );

            return;
        }

        $function = $resolve();
        $configured = $function->eval(
            ScalarFunctionFixtures::rows(new AdaptiveBackend()),
            flow_context(config_builder()->backend(new AdaptiveBackend())->build()),
        );
        // the batch's columns come from another backend than the configured one
        $adaptiveUnderPhp = $resolve()->eval(
            ScalarFunctionFixtures::rows(new AdaptiveBackend()),
            flow_context(config_builder()->backend(new PhpBackend())->build()),
        );

        if ($function->deterministic()) {
            static::assertEquals($php->values(), $configured->values());
            static::assertEquals($php->values(), $adaptiveUnderPhp->values());
        } else {
            static::assertSame($php->count(), $configured->count());
            static::assertSame($php->count(), $adaptiveUnderPhp->count());
        }

        static::assertStringStartsWith('Flow\ETL\Column\Php\\', $php::class, $class . ' under PhpBackend');

        // a reference hands back the batch's own column, whatever backend built it
        if (!$function instanceof Reference) {
            static::assertStringStartsWith(
                'Flow\ETL\Column\Php\\',
                $adaptiveUnderPhp::class,
                $class . ' over Adaptive columns under PhpBackend',
            );
        }

        if ($php instanceof ValueColumn) {
            return;
        }

        static::assertTrue(
            extension_loaded('flow_php')
                ? $configured::class === 'Flow\ETL\Column\RustColumn'
                : str_starts_with($configured::class, 'Flow\ETL\Column\Php\\'),
            $class . ' evaluated into a ' . $configured::class,
        );
    }
}
