<?php

declare(strict_types=1);

namespace Flow\Documentation\Tests\Unit;

use DateTimeImmutable;
use Flow\Documentation\Tests\Context\ExtractedClassesContext;
use Flow\Documentation\Tests\Double\Api\BaseStep;
use Flow\Documentation\Tests\Double\Api\Chain;
use Flow\Documentation\Tests\Double\Api\Describable;
use Flow\Documentation\Tests\Double\Api\Mode;
use Flow\Documentation\Tests\Double\Api\Option;
use Flow\Documentation\Tests\Double\Api\Pipeline;
use Flow\Documentation\Tests\Double\Api\Step;
use PHPUnit\Framework\TestCase;

use function array_keys;

final class ClassesExtractorTest extends TestCase
{
    private ExtractedClassesContext $context;

    protected function setUp(): void
    {
        $this->context = new ExtractedClassesContext();
    }

    public function test_follows_return_parameter_and_supertypes_within_the_repository(): void
    {
        $classes = $this->context->extract([Pipeline::class]);

        static::assertSame(
            [
                BaseStep::class,
                Chain::class,
                Describable::class,
                Mode::class,
                Option::class,
                Pipeline::class,
                Step::class,
            ],
            array_keys($classes),
        );
    }

    public function test_lists_parent_interfaces_and_traits_as_supertypes(): void
    {
        $classes = $this->context->extract([Pipeline::class]);

        static::assertSame([Describable::class, Chain::class], $classes[Pipeline::class]->supertypes);
        static::assertSame([BaseStep::class], $classes[Step::class]->supertypes);
    }

    public function test_keeps_only_public_instance_methods_declared_in_the_type(): void
    {
        $classes = $this->context->extract([Pipeline::class]);

        static::assertSame(
            ['describe', 'step', 'untyped', 'configure'],
            $this->context->methodNames($classes[Pipeline::class]),
        );
        static::assertSame(['chained'], $this->context->methodNames($classes[Chain::class]));
        static::assertSame(['next'], $this->context->methodNames($classes[Step::class]));
        static::assertSame(['base'], $this->context->methodNames($classes[BaseStep::class]));
    }

    public function test_keeps_public_static_methods_apart_from_instance_methods(): void
    {
        $classes = $this->context->extract([Pipeline::class]);

        static::assertSame(['create', 'inMode'], $this->context->staticMethodNames($classes[Pipeline::class]));
    }

    public function test_keeps_public_constants_and_enum_cases(): void
    {
        $classes = $this->context->extract([Pipeline::class]);

        static::assertSame(['NAME'], $classes[Pipeline::class]->constants);
        static::assertSame(['FAST', 'SAFE'], $classes[Mode::class]->constants);
    }

    public function test_keeps_the_source_path_relative_to_the_repository(): void
    {
        $classes = $this->context->extract([Pipeline::class]);

        static::assertSame('Double/Api/Pipeline.php', $classes[Pipeline::class]->repositoryPath);
    }

    public function test_untyped_parameter_is_exported_without_types(): void
    {
        $classes = $this->context->extract([Pipeline::class]);

        static::assertSame([], $classes[Pipeline::class]->methods[2]->parameters->arguments[0]->type->types);
    }

    public function test_types_outside_the_repository_are_skipped(): void
    {
        static::assertSame(
            [],
            $this->context->extract([DateTimeImmutable::class, 'Flow\Documentation\Tests\Double\Api\Missing']),
        );
    }
}
