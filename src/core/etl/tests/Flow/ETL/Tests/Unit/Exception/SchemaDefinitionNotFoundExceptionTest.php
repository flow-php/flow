<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Exception;

use Flow\ETL\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\FlowTestCase;

final class SchemaDefinitionNotFoundExceptionTest extends FlowTestCase
{
    public function test_with_available_lists_the_columns(): void
    {
        $exception = SchemaDefinitionNotFoundException::withAvailable('scoree', 'id', 'score', 'name');

        static::assertSame(
            'Schema definition for entry "scoree" not found. Available columns: [id, score, name].',
            $exception->getMessage(),
        );
        static::assertSame('scoree', $exception->entry());
        static::assertSame(['id', 'score', 'name'], $exception->available());
    }

    public function test_the_single_argument_form_keeps_its_message(): void
    {
        static::assertSame(
            'Schema definition for entry "scoree" not found',
            (new SchemaDefinitionNotFoundException('scoree'))->getMessage(),
        );
    }
}
