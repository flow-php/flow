<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Describe;

use Flow\ETL\Foundation\Describe\ConfiguredFilesystem;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\Described;
use Flow\ETL\Tests\Double\NotDescribed;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Filesystem\Local\NativeLocalFilesystem;

final class DescriptionTest extends FlowTestCase
{
    public function test_a_description_is_sensitive_when_any_nested_description_is(): void
    {
        $secret = new Description(Described::class, ['token' => 'x'], secret: true);

        static::assertFalse((new Description(Described::class, ['a' => 1]))->sensitive());
        static::assertTrue((new Description(Described::class, ['nested' => ['deep' => [$secret]]]))->sensitive());
    }

    public function test_normalize_nests_descriptions_and_reports_the_derived_secrecy(): void
    {
        $normalized = (new Description(Described::class, [
            'nested' => new Description(Described::class, ['v' => 1], secret: true),
        ]))->normalize();

        static::assertSame(
            [
                'class' => Described::class,
                'secret' => true,
                'data' => ['nested' => ['class' => Described::class, 'secret' => true, 'data' => ['v' => 1]]],
            ],
            $normalized,
        );
    }

    public function test_extensions_union_nested_descriptions_sorted_and_unique(): void
    {
        $description = Description::compose(
            Described::class,
            ['nested' => ['deep' => new Description(Described::class, [], extensions: ['snappy', 'zlib'])]],
            extensions: ['zlib'],
        );

        static::assertSame(['snappy', 'zlib'], $description?->extensions());
        static::assertSame([], (new Description(Described::class, ['a' => 1]))->extensions());
    }

    public function test_normalize_does_not_carry_extensions_or_settings(): void
    {
        static::assertSame(
            ['class' => Described::class, 'secret' => false, 'data' => ['a' => 1]],
            (new Description(
                Described::class,
                ['a' => 1],
                extensions: ['zlib'],
                settings: ['precision' => '17'],
            ))->normalize(),
        );
    }

    public function test_settings_union_nested_descriptions_naming_the_declaring_class(): void
    {
        $description = Description::compose(
            Described::class,
            ['nested' => [new Description(NotDescribed::class, [], settings: ['precision' => '17', 'a' => '1'])]],
            settings: ['precision' => '17'],
        );

        static::assertSame(
            [
                'precision' => ['value' => '17', 'class' => Described::class],
                'a' => ['value' => '1', 'class' => NotDescribed::class],
            ],
            $description?->settings(),
        );
    }

    public function test_two_descriptions_declaring_different_values_for_one_setting_are_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            Described::class
            . ' declares the worker setting "precision" as "17", '
            . NotDescribed::class
            . ' declares it as "14"',
        );

        (new Description(
            Described::class,
            ['nested' => new Description(NotDescribed::class, [], settings: ['precision' => '14'])],
            settings: ['precision' => '17'],
        ))->settings();
    }

    public function test_from_array_does_not_recurse_into_data(): void
    {
        $description = Description::fromArray([
            'class' => Described::class,
            'secret' => false,
            'data' => ['nested' => ['class' => Described::class, 'secret' => true, 'data' => []]],
        ]);

        static::assertFalse($description->sensitive());
        static::assertSame(['class' => Described::class, 'secret' => true, 'data' => []], $description->data['nested']);
    }

    public function test_from_array_refuses_a_missing_key(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Description is missing the "data" key');

        Description::fromArray(['class' => Described::class, 'secret' => false]);
    }

    public function test_from_array_refuses_a_class_that_is_not_a_string(): void
    {
        $this->expectException(InvalidArgumentException::class);

        Description::fromArray(['class' => 1, 'secret' => false, 'data' => []]);
    }

    public function test_debug_info_redacts_sensitive_data(): void
    {
        static::assertSame(
            ['class' => Described::class, 'secret' => true, 'data' => '[redacted]'],
            (new Description(Described::class, ['token' => 'x'], secret: true))->__debugInfo(),
        );
        static::assertSame(
            ['class' => Described::class, 'secret' => false, 'data' => ['a' => 1]],
            (new Description(Described::class, ['a' => 1]))->__debugInfo(),
        );
    }

    public function test_compose_replaces_nested_describables(): void
    {
        static::assertEquals(
            new Description(Described::class, [
                'a' => (new Described('x'))->describe(),
                'list' => [(new Described('y'))->describe()],
            ]),
            Description::compose(Described::class, ['a' => new Described('x'), 'list' => [new Described('y')]]),
        );
    }

    public function test_compose_is_null_when_one_nested_describable_is_null(): void
    {
        static::assertNull(Description::compose(Described::class, [
            'a' => new Described('x'),
            'deep' => [[new NotDescribed()]],
        ]));
    }

    public function test_compose_keeps_keys_and_scalars(): void
    {
        $composed = Description::compose(Described::class, [
            'n' => 1,
            'f' => 1.5,
            's' => 'x',
            'b' => false,
            'null' => null,
            'map' => ['k' => 'v'],
        ]);

        static::assertSame(
            ['n' => 1, 'f' => 1.5, 's' => 'x', 'b' => false, 'null' => null, 'map' => ['k' => 'v']],
            $composed?->data,
        );
    }

    public function test_compose_is_sensitive_when_a_nested_description_is(): void
    {
        $composed = Description::compose(Described::class, [
            'fs' => new ConfiguredFilesystem(new NativeLocalFilesystem()),
        ]);

        static::assertTrue($composed?->sensitive());
        static::assertFalse(Description::compose(Described::class, ['a' => new Described('x')])?->sensitive());
    }

    public function test_compose_refuses_an_object_that_is_not_describable(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('is neither Describable nor a Description');

        Description::compose(Described::class, ['fs' => new NativeLocalFilesystem()]);
    }
}
