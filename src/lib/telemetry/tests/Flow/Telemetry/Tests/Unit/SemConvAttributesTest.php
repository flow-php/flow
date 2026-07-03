<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit;

use Flow\Telemetry\SemConvAttributes;
use PHPUnit\Framework\TestCase;

final class SemConvAttributesTest extends TestCase
{
    public function test_code_attributes_follow_otel_convention(): void
    {
        static::assertSame('code.function.name', SemConvAttributes::CODE_FUNCTION_NAME);
    }

    public function test_db_attributes_follow_otel_convention(): void
    {
        static::assertSame('db.collection.name', SemConvAttributes::DB_COLLECTION_NAME);
        static::assertSame('db.namespace', SemConvAttributes::DB_NAMESPACE);
        static::assertSame('db.operation.name', SemConvAttributes::DB_OPERATION_NAME);
        static::assertSame('db.query.parameter.', SemConvAttributes::DB_QUERY_PARAMETER_PREFIX);
        static::assertSame('db.query.text', SemConvAttributes::DB_QUERY_TEXT);
        static::assertSame('db.response.returned_rows', SemConvAttributes::DB_RESPONSE_RETURNED_ROWS);
        static::assertSame('db.response.status_code', SemConvAttributes::DB_RESPONSE_STATUS_CODE);
        static::assertSame('db.system.name', SemConvAttributes::DB_SYSTEM_NAME);
    }

    public function test_error_attributes_follow_otel_convention(): void
    {
        static::assertSame('error.type', SemConvAttributes::ERROR_TYPE);
    }

    public function test_http_attributes_follow_otel_convention(): void
    {
        static::assertSame('http.request.method', SemConvAttributes::HTTP_REQUEST_METHOD);
        static::assertSame('http.response.status_code', SemConvAttributes::HTTP_RESPONSE_STATUS_CODE);
        static::assertSame('http.route', SemConvAttributes::HTTP_ROUTE);
    }

    public function test_messaging_attributes_follow_otel_convention(): void
    {
        static::assertSame('messaging.consumer.group.name', SemConvAttributes::MESSAGING_CONSUMER_GROUP_NAME);
        static::assertSame('messaging.destination.name', SemConvAttributes::MESSAGING_DESTINATION_NAME);
        static::assertSame('messaging.message.id', SemConvAttributes::MESSAGING_MESSAGE_ID);
        static::assertSame('messaging.operation.name', SemConvAttributes::MESSAGING_OPERATION_NAME);
        static::assertSame('messaging.operation.type', SemConvAttributes::MESSAGING_OPERATION_TYPE);
        static::assertSame('messaging.system', SemConvAttributes::MESSAGING_SYSTEM);
    }

    public function test_process_attributes_follow_otel_convention(): void
    {
        static::assertSame('process.exit.code', SemConvAttributes::PROCESS_EXIT_CODE);
    }

    public function test_server_attributes_follow_otel_convention(): void
    {
        static::assertSame('server.address', SemConvAttributes::SERVER_ADDRESS);
        static::assertSame('server.port', SemConvAttributes::SERVER_PORT);
    }

    public function test_test_attributes_follow_otel_convention(): void
    {
        static::assertSame('test.case.name', SemConvAttributes::TEST_CASE_NAME);
        static::assertSame('test.case.result.status', SemConvAttributes::TEST_CASE_RESULT_STATUS);
        static::assertSame('test.suite.name', SemConvAttributes::TEST_SUITE_NAME);
        static::assertSame('test.suite.run.status', SemConvAttributes::TEST_SUITE_RUN_STATUS);
    }

    public function test_url_attributes_follow_otel_convention(): void
    {
        static::assertSame('url.full', SemConvAttributes::URL_FULL);
        static::assertSame('url.path', SemConvAttributes::URL_PATH);
        static::assertSame('url.query', SemConvAttributes::URL_QUERY);
        static::assertSame('url.scheme', SemConvAttributes::URL_SCHEME);
    }

    public function test_user_agent_attributes_follow_otel_convention(): void
    {
        static::assertSame('user_agent.original', SemConvAttributes::USER_AGENT_ORIGINAL);
    }

    public function test_user_attributes_follow_otel_convention(): void
    {
        static::assertSame('user.email', SemConvAttributes::USER_EMAIL);
        static::assertSame('user.id', SemConvAttributes::USER_ID);
        static::assertSame('user.roles', SemConvAttributes::USER_ROLES);
    }
}
