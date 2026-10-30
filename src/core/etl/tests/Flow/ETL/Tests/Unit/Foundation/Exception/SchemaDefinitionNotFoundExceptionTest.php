<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Foundation\Exception;

use Flow\ETL\Foundation\Exception\SchemaDefinitionNotFoundException;
use Flow\ETL\Tests\FlowTestCase;

final class SchemaDefinitionNotFoundExceptionTest extends FlowTestCase
{
    public function test_with_available_lists_the_columns(): void
    {
        $exception = SchemaDefinitionNotFoundException::withAvailable('scoree', 'id', 'score', 'name');

        static::assertSame(
            'Schema definition for column "scoree" not found. Did you mean one of: [score]?',
            $exception->getMessage(),
        );
        static::assertSame('scoree', $exception->column());
        static::assertSame(['score'], $exception->suggestions());
    }

    public function test_the_single_argument_form_keeps_its_message(): void
    {
        static::assertSame(
            'Schema definition for column "scoree" not found.',
            (new SchemaDefinitionNotFoundException('scoree'))->getMessage(),
        );
    }
}
