<?php

declare(strict_types=1);

namespace Flow\Bridge\OpenAPI\Specification\Tests\Unit;

use Flow\Bridge\OpenAPI\Specification\OpenAPIConverter;
use Flow\Bridge\OpenAPI\Specification\OpenAPIMetadata;
use Flow\ETL\Schema\Metadata;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class OpenAPIMetadataIntegrationTest extends TestCase
{
    public function test_complex_example_with_all_metadata_types(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(
            int_schema(
                'id',
                false,
                OpenAPIMetadata::description('Unique identifier')
                    ->merge(OpenAPIMetadata::title('ID'))
                    ->merge(OpenAPIMetadata::example(12345))
                    ->merge(OpenAPIMetadata::format('int64'))
                    ->merge(OpenAPIMetadata::readOnly()),
            ),
            str_schema(
                'email',
                true,
                OpenAPIMetadata::description('User email address')
                    ->merge(OpenAPIMetadata::format('email'))
                    ->merge(OpenAPIMetadata::example('user@example.com'))
                    ->merge(OpenAPIMetadata::title('Email Address')),
            ),
            str_schema(
                'password',
                false,
                OpenAPIMetadata::description('User password')
                    ->merge(OpenAPIMetadata::format('password'))
                    ->merge(OpenAPIMetadata::writeOnly())
                    ->merge(OpenAPIMetadata::title('Password')),
            ),
            str_schema(
                'status',
                false,
                OpenAPIMetadata::description('Account status')
                    ->merge(OpenAPIMetadata::deprecated(true))
                    ->merge(OpenAPIMetadata::default('active'))
                    ->merge(OpenAPIMetadata::examples([
                        'active' => 'active',
                        'inactive' => 'inactive',
                        'suspended' => 'suspended',
                    ]))
                    ->merge(OpenAPIMetadata::title('Status')),
            ),
            str_schema('optional_field', true, OpenAPIMetadata::nullable(false)), // Override schema nullability
        );

        $result = $converter->toOpenAPI($schema);

        static::assertSame(
            [
                'type' => 'object',
                'properties' => [
                    'id' => [
                        'type' => 'integer',
                        'nullable' => false,
                        'description' => 'Unique identifier',
                        'format' => 'int64',
                        'example' => 12345,
                        'title' => 'ID',
                        'readOnly' => true,
                    ],
                    'email' => [
                        'type' => 'string',
                        'nullable' => true,
                        'description' => 'User email address',
                        'format' => 'email',
                        'example' => 'user@example.com',
                        'title' => 'Email Address',
                    ],
                    'password' => [
                        'type' => 'string',
                        'nullable' => false,
                        'description' => 'User password',
                        'format' => 'password',
                        'title' => 'Password',
                        'writeOnly' => true,
                    ],
                    'status' => [
                        'type' => 'string',
                        'nullable' => false,
                        'description' => 'Account status',
                        'examples' => [
                            'active' => 'active',
                            'inactive' => 'inactive',
                            'suspended' => 'suspended',
                        ],
                        'deprecated' => true,
                        'title' => 'Status',
                        'default' => 'active',
                    ],
                    'optional_field' => [
                        'type' => 'string',
                        'nullable' => false,
                    ],
                ],
            ],
            $result,
        );
    }

    public function test_converter_falls_back_to_legacy_metadata_when_openapi_not_present(): void
    {
        $converter = new OpenAPIConverter();
        $metadata = Metadata::with('description', 'Legacy description')->merge(Metadata::with(
            'example',
            'Legacy example',
        ));

        $schema = schema(str_schema('name', false, $metadata));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $nameProperty */
        $nameProperty = $properties['name'];

        static::assertSame('Legacy description', $nameProperty['description']);
        static::assertSame('Legacy example', $nameProperty['example']);
    }

    public function test_converter_handles_mixed_legacy_and_openapi_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $metadata = Metadata::with('description', 'Legacy description')
            ->merge(OpenAPIMetadata::format('email')) // Only format is OpenAPI-specific
            ->merge(Metadata::with('example', 'Legacy example'));

        $schema = schema(str_schema('mixed', false, $metadata));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $mixedProperty */
        $mixedProperty = $properties['mixed'];

        static::assertSame('Legacy description', $mixedProperty['description']); // Fallback to legacy
        static::assertSame('email', $mixedProperty['format']); // OpenAPI specific
        static::assertSame('Legacy example', $mixedProperty['example']); // Fallback to legacy
    }

    public function test_converter_handles_multiple_openapi_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $metadata = OpenAPIMetadata::description('User email address')
            ->merge(OpenAPIMetadata::format('email'))
            ->merge(OpenAPIMetadata::example('user@example.com'))
            ->merge(OpenAPIMetadata::title('Email Address'));

        $schema = schema(str_schema('email', false, $metadata));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $emailProperty */
        $emailProperty = $properties['email'];

        static::assertSame('User email address', $emailProperty['description']);
        static::assertSame('email', $emailProperty['format']);
        static::assertSame('user@example.com', $emailProperty['example']);
        static::assertSame('Email Address', $emailProperty['title']);
    }

    public function test_converter_handles_openapi_default_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('status', false, OpenAPIMetadata::default('active')));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $statusProperty */
        $statusProperty = $properties['status'];
        static::assertSame('active', $statusProperty['default']);
    }

    public function test_converter_handles_openapi_deprecated_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('old_field', false, OpenAPIMetadata::deprecated()));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $oldFieldProperty */
        $oldFieldProperty = $properties['old_field'];
        static::assertTrue($oldFieldProperty['deprecated']);
    }

    public function test_converter_handles_openapi_description_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('name', false, OpenAPIMetadata::description('User full name')));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $nameProperty */
        $nameProperty = $properties['name'];
        static::assertSame('User full name', $nameProperty['description']);
    }

    public function test_converter_handles_openapi_example_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('email', false, OpenAPIMetadata::example('user@example.com')));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $emailProperty */
        $emailProperty = $properties['email'];
        static::assertSame('user@example.com', $emailProperty['example']);
    }

    public function test_converter_handles_openapi_examples_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $examples = [
            'active' => 'active',
            'inactive' => 'inactive',
            'pending' => 'pending',
        ];
        $schema = schema(str_schema('status', false, OpenAPIMetadata::examples($examples)));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $statusProperty */
        $statusProperty = $properties['status'];
        static::assertSame($examples, $statusProperty['examples']);
    }

    public function test_converter_handles_openapi_format_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('email', false, OpenAPIMetadata::format('email')));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $emailProperty */
        $emailProperty = $properties['email'];
        static::assertSame('email', $emailProperty['format']);
    }

    public function test_converter_handles_openapi_nullable_metadata_override(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('optional_field', false, OpenAPIMetadata::nullable(true))); // non-nullable schema definition but nullable in OpenAPI

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $optionalFieldProperty */
        $optionalFieldProperty = $properties['optional_field'];
        static::assertTrue($optionalFieldProperty['nullable']); // Should be overridden
    }

    public function test_converter_handles_openapi_read_only_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('created_at', false, OpenAPIMetadata::readOnly()));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $createdAtProperty */
        $createdAtProperty = $properties['created_at'];
        static::assertTrue($createdAtProperty['readOnly']);
    }

    public function test_converter_handles_openapi_title_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(int_schema('id', false, OpenAPIMetadata::title('User ID')));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $idProperty */
        $idProperty = $properties['id'];
        static::assertSame('User ID', $idProperty['title']);
    }

    public function test_converter_handles_openapi_write_only_metadata(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(str_schema('password', false, OpenAPIMetadata::writeOnly()));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $passwordProperty */
        $passwordProperty = $properties['password'];
        static::assertTrue($passwordProperty['writeOnly']);
    }

    public function test_converter_prioritizes_openapi_metadata_over_legacy(): void
    {
        $converter = new OpenAPIConverter();
        $metadata = Metadata::with('description', 'Legacy description') // Legacy metadata
            ->merge(OpenAPIMetadata::description('OpenAPI description')) // OpenAPI metadata should take priority
            ->merge(Metadata::with('example', 'Legacy example'))
            ->merge(OpenAPIMetadata::example('OpenAPI example'));

        $schema = schema(str_schema('name', false, $metadata));

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];
        /** @var array<string, mixed> $nameProperty */
        $nameProperty = $properties['name'];

        static::assertSame('OpenAPI description', $nameProperty['description']);
        static::assertSame('OpenAPI example', $nameProperty['example']);
    }

    public function test_openapi_metadata_usage_with_converter(): void
    {
        $converter = new OpenAPIConverter();
        $schema = schema(
            str_schema(
                'user_email',
                false,
                OpenAPIMetadata::description('User email address')
                    ->merge(OpenAPIMetadata::format('email'))
                    ->merge(OpenAPIMetadata::example('user@example.com'))
                    ->merge(OpenAPIMetadata::title('Email')),
            ),
            str_schema(
                'password',
                false,
                OpenAPIMetadata::description('User password')
                    ->merge(OpenAPIMetadata::format('password'))
                    ->merge(OpenAPIMetadata::writeOnly())
                    ->merge(OpenAPIMetadata::title('Password')),
            ),
            str_schema(
                'created_at',
                false,
                OpenAPIMetadata::description('Account creation timestamp')
                    ->merge(OpenAPIMetadata::format('date-time'))
                    ->merge(OpenAPIMetadata::readOnly())
                    ->merge(OpenAPIMetadata::example('2023-01-01T00:00:00Z')),
            ),
            str_schema(
                'status',
                false,
                OpenAPIMetadata::description('Account status')
                    ->merge(OpenAPIMetadata::deprecated())
                    ->merge(OpenAPIMetadata::default('active'))
                    ->merge(OpenAPIMetadata::examples([
                        'active' => 'active',
                        'inactive' => 'inactive',
                        'suspended' => 'suspended',
                    ])),
            ),
        );

        $result = $converter->toOpenAPI($schema);

        /** @var array<string, mixed> $properties */
        $properties = $result['properties'];

        /** @var array<string, mixed> $email */
        $email = $properties['user_email'];
        static::assertSame('User email address', $email['description']);
        static::assertSame('email', $email['format']);
        static::assertSame('user@example.com', $email['example']);
        static::assertSame('Email', $email['title']);

        /** @var array<string, mixed> $password */
        $password = $properties['password'];
        static::assertSame('User password', $password['description']);
        static::assertSame('password', $password['format']);
        static::assertTrue($password['writeOnly']);
        static::assertSame('Password', $password['title']);

        /** @var array<string, mixed> $createdAt */
        $createdAt = $properties['created_at'];
        static::assertSame('Account creation timestamp', $createdAt['description']);
        static::assertSame('date-time', $createdAt['format']);
        static::assertTrue($createdAt['readOnly']);
        static::assertSame('2023-01-01T00:00:00Z', $createdAt['example']);

        /** @var array<string, mixed> $status */
        $status = $properties['status'];
        static::assertSame('Account status', $status['description']);
        static::assertTrue($status['deprecated']);
        static::assertSame('active', $status['default']);
        static::assertSame(
            [
                'active' => 'active',
                'inactive' => 'inactive',
                'suspended' => 'suspended',
            ],
            $status['examples'],
        );
    }
}
