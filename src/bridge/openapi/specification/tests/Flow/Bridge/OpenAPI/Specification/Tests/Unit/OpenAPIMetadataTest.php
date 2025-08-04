<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use Flow\Bridge\OpenAPI\Specification\OpenAPIMetadata;
use Flow\ETL\Schema\Metadata;
use PHPUnit\Framework\TestCase;

final class OpenAPIMetadataTest extends TestCase
{
    public function test_all_metadata_types_return_metadata_instances() : void
    {
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::description('test'));
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::format('test'));
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::example('test'));
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::examples([]));
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::deprecated());
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::title('test'));
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::default('test'));
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::readOnly());
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::writeOnly());
        self::assertInstanceOf(Metadata::class, OpenAPIMetadata::nullable());
    }

    public function test_default_creates_metadata_with_correct_key_and_value() : void
    {
        $metadata = OpenAPIMetadata::default('default_value');

        self::assertTrue($metadata->has(OpenAPIMetadata::DEFAULT->value));
        self::assertSame('default_value', $metadata->get(OpenAPIMetadata::DEFAULT->value));
    }

    public function test_default_with_different_types() : void
    {
        $stringDefault = OpenAPIMetadata::default('test');
        $intDefault = OpenAPIMetadata::default(0);
        $boolDefault = OpenAPIMetadata::default(false);
        $arrayDefault = OpenAPIMetadata::default([]);

        self::assertSame('test', $stringDefault->get(OpenAPIMetadata::DEFAULT->value));
        self::assertSame(0, $intDefault->get(OpenAPIMetadata::DEFAULT->value));
        self::assertFalse($boolDefault->get(OpenAPIMetadata::DEFAULT->value));
        self::assertSame([], $arrayDefault->get(OpenAPIMetadata::DEFAULT->value));
    }

    public function test_deprecated_creates_metadata_with_correct_key_and_default_value() : void
    {
        $metadata = OpenAPIMetadata::deprecated();

        self::assertTrue($metadata->has(OpenAPIMetadata::DEPRECATED->value));
        self::assertTrue($metadata->get(OpenAPIMetadata::DEPRECATED->value));
    }

    public function test_deprecated_creates_metadata_with_custom_value() : void
    {
        $metadata = OpenAPIMetadata::deprecated(false);

        self::assertTrue($metadata->has(OpenAPIMetadata::DEPRECATED->value));
        self::assertFalse($metadata->get(OpenAPIMetadata::DEPRECATED->value));
    }

    public function test_description_creates_metadata_with_correct_key_and_value() : void
    {
        $metadata = OpenAPIMetadata::description('User name');

        self::assertTrue($metadata->has(OpenAPIMetadata::DESCRIPTION->value));
        self::assertSame('User name', $metadata->get(OpenAPIMetadata::DESCRIPTION->value));
    }

    public function test_enum_values_are_consistent() : void
    {
        self::assertSame('openapi_description', OpenAPIMetadata::DESCRIPTION->value);
        self::assertSame('openapi_format', OpenAPIMetadata::FORMAT->value);
        self::assertSame('openapi_example', OpenAPIMetadata::EXAMPLE->value);
        self::assertSame('openapi_examples', OpenAPIMetadata::EXAMPLES->value);
        self::assertSame('openapi_deprecated', OpenAPIMetadata::DEPRECATED->value);
        self::assertSame('openapi_title', OpenAPIMetadata::TITLE->value);
        self::assertSame('openapi_default', OpenAPIMetadata::DEFAULT->value);
        self::assertSame('openapi_read_only', OpenAPIMetadata::READ_ONLY->value);
        self::assertSame('openapi_write_only', OpenAPIMetadata::WRITE_ONLY->value);
        self::assertSame('openapi_nullable', OpenAPIMetadata::NULLABLE->value);
    }

    public function test_example_creates_metadata_with_correct_key_and_value() : void
    {
        $metadata = OpenAPIMetadata::example('john@example.com');

        self::assertTrue($metadata->has(OpenAPIMetadata::EXAMPLE->value));
        self::assertSame('john@example.com', $metadata->get(OpenAPIMetadata::EXAMPLE->value));
    }

    public function test_example_with_different_types() : void
    {
        $stringExample = OpenAPIMetadata::example('test');
        $intExample = OpenAPIMetadata::example(42);
        $arrayExample = OpenAPIMetadata::example(['a', 'b']);
        $boolExample = OpenAPIMetadata::example(true);

        self::assertSame('test', $stringExample->get(OpenAPIMetadata::EXAMPLE->value));
        self::assertSame(42, $intExample->get(OpenAPIMetadata::EXAMPLE->value));
        self::assertSame(['a', 'b'], $arrayExample->get(OpenAPIMetadata::EXAMPLE->value));
        self::assertTrue($boolExample->get(OpenAPIMetadata::EXAMPLE->value));
    }

    public function test_examples_creates_metadata_with_correct_key_and_value() : void
    {
        $examples = [
            'active' => 'active',
            'inactive' => 'inactive',
            'pending' => 'pending',
        ];
        $metadata = OpenAPIMetadata::examples($examples);

        self::assertTrue($metadata->has(OpenAPIMetadata::EXAMPLES->value));
        self::assertSame($examples, $metadata->get(OpenAPIMetadata::EXAMPLES->value));
    }

    public function test_format_creates_metadata_with_correct_key_and_value() : void
    {
        $metadata = OpenAPIMetadata::format('email');

        self::assertTrue($metadata->has(OpenAPIMetadata::FORMAT->value));
        self::assertSame('email', $metadata->get(OpenAPIMetadata::FORMAT->value));
    }

    public function test_metadata_can_be_combined() : void
    {
        $description = OpenAPIMetadata::description('User email');
        $format = OpenAPIMetadata::format('email');
        $example = OpenAPIMetadata::example('user@example.com');

        $combined = $description->merge($format)->merge($example);

        self::assertTrue($combined->has(OpenAPIMetadata::DESCRIPTION->value));
        self::assertTrue($combined->has(OpenAPIMetadata::FORMAT->value));
        self::assertTrue($combined->has(OpenAPIMetadata::EXAMPLE->value));
        self::assertSame('User email', $combined->get(OpenAPIMetadata::DESCRIPTION->value));
        self::assertSame('email', $combined->get(OpenAPIMetadata::FORMAT->value));
        self::assertSame('user@example.com', $combined->get(OpenAPIMetadata::EXAMPLE->value));
    }

    public function test_nullable_creates_metadata_with_correct_key_and_default_value() : void
    {
        $metadata = OpenAPIMetadata::nullable();

        self::assertTrue($metadata->has(OpenAPIMetadata::NULLABLE->value));
        self::assertTrue($metadata->get(OpenAPIMetadata::NULLABLE->value));
    }

    public function test_nullable_creates_metadata_with_custom_value() : void
    {
        $metadata = OpenAPIMetadata::nullable(false);

        self::assertTrue($metadata->has(OpenAPIMetadata::NULLABLE->value));
        self::assertFalse($metadata->get(OpenAPIMetadata::NULLABLE->value));
    }

    public function test_read_only_creates_metadata_with_correct_key_and_default_value() : void
    {
        $metadata = OpenAPIMetadata::readOnly();

        self::assertTrue($metadata->has(OpenAPIMetadata::READ_ONLY->value));
        self::assertTrue($metadata->get(OpenAPIMetadata::READ_ONLY->value));
    }

    public function test_read_only_creates_metadata_with_custom_value() : void
    {
        $metadata = OpenAPIMetadata::readOnly(false);

        self::assertTrue($metadata->has(OpenAPIMetadata::READ_ONLY->value));
        self::assertFalse($metadata->get(OpenAPIMetadata::READ_ONLY->value));
    }

    public function test_title_creates_metadata_with_correct_key_and_value() : void
    {
        $metadata = OpenAPIMetadata::title('User ID');

        self::assertTrue($metadata->has(OpenAPIMetadata::TITLE->value));
        self::assertSame('User ID', $metadata->get(OpenAPIMetadata::TITLE->value));
    }

    public function test_write_only_creates_metadata_with_correct_key_and_default_value() : void
    {
        $metadata = OpenAPIMetadata::writeOnly();

        self::assertTrue($metadata->has(OpenAPIMetadata::WRITE_ONLY->value));
        self::assertTrue($metadata->get(OpenAPIMetadata::WRITE_ONLY->value));
    }

    public function test_write_only_creates_metadata_with_custom_value() : void
    {
        $metadata = OpenAPIMetadata::writeOnly(false);

        self::assertTrue($metadata->has(OpenAPIMetadata::WRITE_ONLY->value));
        self::assertFalse($metadata->get(OpenAPIMetadata::WRITE_ONLY->value));
    }
}
