<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use ErrorException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Values\Column\AdaptiveBackend;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Column\ValueColumn;
use Flow\Types\Type;
use PHPUnit\Framework\Assert;
use Throwable;

use function count;
use function extension_loaded;
use function Flow\ETL\DSL\config_builder;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\flow_context;
use function in_array;
use function restore_error_handler;
use function set_error_handler;
use function str_starts_with;

/**
 * The checks every ScalarFunction a package ships must pass, run over ScalarFunctionFixtures' rows and schema.
 */
final class ScalarFunctionContract
{
    public static function returnsItsDeclaredType(ScalarFunction $function, Type $expected): void
    {
        $returns = (new ReferenceResolver())
            ->resolve($function, ScalarFunctionFixtures::schema())
            ->returns();

        Assert::assertSame($expected->toString(), $returns->toString());
        Assert::assertNotNull(definition_from_type('c', $returns));
    }

    public static function withChildrenRoundTrips(ScalarFunction $function): void
    {
        $children = $function->children();

        Assert::assertSame($children, $function->withChildren($children)->children());

        if (count($children) === 0) {
            return;
        }

        set_error_handler(static fn(int $errno, string $errstr) => throw new ErrorException($errstr));

        try {
            $rebuilt = $function->withChildren([]);
        } catch (Throwable) {
            return;
        } finally {
            restore_error_handler();
        }

        // An arity change must never go unnoticed: an empty rebuild may be constructible for a
        // variadic node, but it must not still report the original arity.
        Assert::assertNotSame(count($children), count($rebuilt->children()));
    }

    /**
     * @param list<class-string<ScalarFunction>> $undescribable
     */
    public static function describesItself(ScalarFunction $function, array $undescribable): void
    {
        if ($function->describe() === null) {
            Assert::assertTrue(in_array($function::class, $undescribable, true), $function::class);

            return;
        }

        $rebuilt = DescribeRoundTrip::rebuilt($function);

        Assert::assertSame($function::class, $rebuilt::class);

        foreach ([new PhpBackend(), ...(extension_loaded('flow_php') ? [new AdaptiveBackend()] : [])] as $backend) {
            $context = new FunctionContext(flow_context(config_builder()->backend($backend)->build()));
            $rows = ScalarFunctionFixtures::rows($backend);
            $schema = ScalarFunctionFixtures::schema();

            $original = $context->exported($function, $rows, $schema);
            $copy = $context->exported($rebuilt, $rows, $schema);

            // a clock or a random generator gives other values per call: the column's shape is what must match
            if ($original !== $copy && !$function->deterministic()) {
                $original = $context->columnOf($function, $rows, $schema);
                $copy = $context->columnOf($rebuilt, $rows, $schema);

                Assert::assertSame($original->count(), $copy->count());
                Assert::assertSame($original->type()->toString(), $copy->type()->toString());

                continue;
            }

            Assert::assertSame($original, $copy, $backend::class);
        }
    }

    public static function declaresDeterminism(ScalarFunction $function, bool $deterministic): void
    {
        Assert::assertSame($deterministic, $function->deterministic());
    }

    /**
     * Three fresh instances of one function, one per run: a function may keep what it evaluated (Literal keeps its
     * column).
     */
    public static function evaluatesOnTheConfiguredBackend(
        ScalarFunction $onPhp,
        ScalarFunction $onAdaptive,
        ScalarFunction $adaptiveUnderPhp,
    ): void {
        $resolver = new ReferenceResolver();
        $schema = ScalarFunctionFixtures::schema();
        $class = $onPhp::class;

        try {
            $php = $resolver->resolve($onPhp, $schema)->eval(
                ScalarFunctionFixtures::rows(new PhpBackend()),
                new PhpBackend(),
            );
        } catch (Throwable $phpFailure) {
            try {
                $resolver->resolve($onAdaptive, $schema)->eval(
                    ScalarFunctionFixtures::rows(new AdaptiveBackend()),
                    new AdaptiveBackend(),
                );
            } catch (Throwable $adaptiveFailure) {
                Assert::assertSame($phpFailure::class, $adaptiveFailure::class);
                Assert::assertSame($phpFailure->getMessage(), $adaptiveFailure->getMessage());

                return;
            }

            Assert::fail($class . ' failed under PhpBackend only: ' . $phpFailure->getMessage());
        }

        $function = $resolver->resolve($onAdaptive, $schema);
        $configured = $function->eval(ScalarFunctionFixtures::rows(new AdaptiveBackend()), new AdaptiveBackend());
        // the batch's columns come from another backend than the configured one
        $underPhp = $resolver->resolve($adaptiveUnderPhp, $schema)->eval(
            ScalarFunctionFixtures::rows(new AdaptiveBackend()),
            new PhpBackend(),
        );

        if ($function->deterministic()) {
            Assert::assertEquals($php->values(), $configured->values());
            Assert::assertEquals($php->values(), $underPhp->values());
        } else {
            Assert::assertSame($php->count(), $configured->count());
            Assert::assertSame($php->count(), $underPhp->count());
        }

        Assert::assertStringStartsWith('Flow\ETL\Values\Column\Php\\', $php::class, $class . ' under PhpBackend');

        // a reference hands back the batch's own column, whatever backend built it
        if (!$function instanceof Reference) {
            Assert::assertStringStartsWith(
                'Flow\ETL\Values\Column\Php\\',
                $underPhp::class,
                $class . ' over Adaptive columns under PhpBackend',
            );
        }

        if ($php instanceof ValueColumn) {
            return;
        }

        Assert::assertTrue(
            extension_loaded('flow_php')
                ? $configured::class === 'Flow\ETL\Values\Column\RustColumn'
                : str_starts_with($configured::class, 'Flow\ETL\Values\Column\Php\\'),
            $class . ' evaluated into a ' . $configured::class,
        );
    }
}
