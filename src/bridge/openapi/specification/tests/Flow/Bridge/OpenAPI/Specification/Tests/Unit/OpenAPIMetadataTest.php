<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use Flow\Bridge\OpenAPI\Specification\OpenAPIMetadata;
use Flow\ETL\Schema\Metadata;
use PHPUnit\Framework\TestCase;

final class OpenAPIMetadataTest extends TestCase
{
    public function test_all_metadata_types_return_metadata_instances(): void
    {
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::description('test'));
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::format('test'));
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::example('test'));
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::examples([]));
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::deprecated());
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::title('test'));
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::default('test'));
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::readOnly());
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::writeOnly());
        static::assertInstanceOf(Metadata::class, OpenAPIMetadata::nullable());
    }

    public function test_default_creates_metadata_with_correct_key_and_value(): void
    {
        $metadata = OpenAPIMetadata::default('default_value');

        static::assertTrue($metadata->has(OpenAPIMetadata::DEFAULT->value));
        static::assertSame('default_value', $metadata->get(OpenAPIMetadata::DEFAULT->value));
    }

    public function test_default_with_different_types(): void
    {
        $stringDefault = OpenAPIMetadata::default('test');
        $intDefault = OpenAPIMetadata::default(0);
        $boolDefault = OpenAPIMetadata::default(false);
        $arrayDefault = OpenAPIMetadata::default([]);

        static::assertSame('test', $stringDefault->get(OpenAPIMetadata::DEFAULT->value));
        static::assertSame(0, $intDefault->get(OpenAPIMetadata::DEFAULT->value));
        static::assertFalse($boolDefault->get(OpenAPIMetadata::DEFAULT->value));
        static::assertSame([], $arrayDefault->get(OpenAPIMetadata::DEFAULT->value));
    }

    public function test_deprecated_creates_metadata_with_correct_key_and_default_value(): void
    {
        $metadata = OpenAPIMetadata::deprecated();

        static::assertTrue($metadata->has(OpenAPIMetadata::DEPRECATED->value));
        static::assertTrue($metadata->get(OpenAPIMetadata::DEPRECATED->value));
    }

    public function test_deprecated_creates_metadata_with_custom_value(): void
    {
        $metadata = OpenAPIMetadata::deprecated(false);

        static::assertTrue($metadata->has(OpenAPIMetadata::DEPRECATED->value));
        static::assertFalse($metadata->get(OpenAPIMetadata::DEPRECATED->value));
    }

    public function test_description_creates_metadata_with_correct_key_and_value(): void
    {
        $metadata = OpenAPIMetadata::description('User name');

        static::assertTrue($metadata->has(OpenAPIMetadata::DESCRIPTION->value));
        static::assertSame('User name', $metadata->get(OpenAPIMetadata::DESCRIPTION->value));
    }

    public function test_enum_values_are_consistent(): void
    {
        static::assertSame('openapi_description', OpenAPIMetadata::DESCRIPTION->value);
        static::assertSame('openapi_format', OpenAPIMetadata::FORMAT->value);
        static::assertSame('openapi_example', OpenAPIMetadata::EXAMPLE->value);
        static::assertSame('openapi_examples', OpenAPIMetadata::EXAMPLES->value);
        static::assertSame('openapi_deprecated', OpenAPIMetadata::DEPRECATED->value);
        static::assertSame('openapi_title', OpenAPIMetadata::TITLE->value);
        static::assertSame('openapi_default', OpenAPIMetadata::DEFAULT->value);
        static::assertSame('openapi_read_only', OpenAPIMetadata::READ_ONLY->value);
        static::assertSame('openapi_write_only', OpenAPIMetadata::WRITE_ONLY->value);
        static::assertSame('openapi_nullable', OpenAPIMetadata::NULLABLE->value);
    }

    public function test_example_creates_metadata_with_correct_key_and_value(): void
    {
        $metadata = OpenAPIMetadata::example('john@example.com');

        static::assertTrue($metadata->has(OpenAPIMetadata::EXAMPLE->value));
        static::assertSame('john@example.com', $metadata->get(OpenAPIMetadata::EXAMPLE->value));
    }

    public function test_example_with_different_types(): void
    {
        $stringExample = OpenAPIMetadata::example('test');
        $intExample = OpenAPIMetadata::example(42);
        $arrayExample = OpenAPIMetadata::example(['a', 'b']);
        $boolExample = OpenAPIMetadata::example(true);

        static::assertSame('test', $stringExample->get(OpenAPIMetadata::EXAMPLE->value));
        static::assertSame(42, $intExample->get(OpenAPIMetadata::EXAMPLE->value));
        static::assertSame(['a', 'b'], $arrayExample->get(OpenAPIMetadata::EXAMPLE->value));
        static::assertTrue($boolExample->get(OpenAPIMetadata::EXAMPLE->value));
    }

    public function test_examples_creates_metadata_with_correct_key_and_value(): void
    {
        $examples = [
            'active' => 'active',
            'inactive' => 'inactive',
            'pending' => 'pending',
        ];
        $metadata = OpenAPIMetadata::examples($examples);

        static::assertTrue($metadata->has(OpenAPIMetadata::EXAMPLES->value));
        static::assertSame($examples, $metadata->get(OpenAPIMetadata::EXAMPLES->value));
    }

    public function test_format_creates_metadata_with_correct_key_and_value(): void
    {
        $metadata = OpenAPIMetadata::format('email');

        static::assertTrue($metadata->has(OpenAPIMetadata::FORMAT->value));
        static::assertSame('email', $metadata->get(OpenAPIMetadata::FORMAT->value));
    }

    public function test_metadata_can_be_combined(): void
    {
        $description = OpenAPIMetadata::description('User email');
        $format = OpenAPIMetadata::format('email');
        $example = OpenAPIMetadata::example('user@example.com');

        $combined = $description->merge($format)->merge($example);

        static::assertTrue($combined->has(OpenAPIMetadata::DESCRIPTION->value));
        static::assertTrue($combined->has(OpenAPIMetadata::FORMAT->value));
        static::assertTrue($combined->has(OpenAPIMetadata::EXAMPLE->value));
        static::assertSame('User email', $combined->get(OpenAPIMetadata::DESCRIPTION->value));
        static::assertSame('email', $combined->get(OpenAPIMetadata::FORMAT->value));
        static::assertSame('user@example.com', $combined->get(OpenAPIMetadata::EXAMPLE->value));
    }

    public function test_nullable_creates_metadata_with_correct_key_and_default_value(): void
    {
        $metadata = OpenAPIMetadata::nullable();

        static::assertTrue($metadata->has(OpenAPIMetadata::NULLABLE->value));
        static::assertTrue($metadata->get(OpenAPIMetadata::NULLABLE->value));
    }

    public function test_nullable_creates_metadata_with_custom_value(): void
    {
        $metadata = OpenAPIMetadata::nullable(false);

        static::assertTrue($metadata->has(OpenAPIMetadata::NULLABLE->value));
        static::assertFalse($metadata->get(OpenAPIMetadata::NULLABLE->value));
    }

    public function test_read_only_creates_metadata_with_correct_key_and_default_value(): void
    {
        $metadata = OpenAPIMetadata::readOnly();

        static::assertTrue($metadata->has(OpenAPIMetadata::READ_ONLY->value));
        static::assertTrue($metadata->get(OpenAPIMetadata::READ_ONLY->value));
    }

    public function test_read_only_creates_metadata_with_custom_value(): void
    {
        $metadata = OpenAPIMetadata::readOnly(false);

        static::assertTrue($metadata->has(OpenAPIMetadata::READ_ONLY->value));
        static::assertFalse($metadata->get(OpenAPIMetadata::READ_ONLY->value));
    }

    public function test_title_creates_metadata_with_correct_key_and_value(): void
    {
        $metadata = OpenAPIMetadata::title('User ID');

        static::assertTrue($metadata->has(OpenAPIMetadata::TITLE->value));
        static::assertSame('User ID', $metadata->get(OpenAPIMetadata::TITLE->value));
    }

    public function test_write_only_creates_metadata_with_correct_key_and_default_value(): void
    {
        $metadata = OpenAPIMetadata::writeOnly();

        static::assertTrue($metadata->has(OpenAPIMetadata::WRITE_ONLY->value));
        static::assertTrue($metadata->get(OpenAPIMetadata::WRITE_ONLY->value));
    }

    public function test_write_only_creates_metadata_with_custom_value(): void
    {
        $metadata = OpenAPIMetadata::writeOnly(false);

        static::assertTrue($metadata->has(OpenAPIMetadata::WRITE_ONLY->value));
        static::assertFalse($metadata->get(OpenAPIMetadata::WRITE_ONLY->value));
    }
}
