<?php

declare(strict_types=1);

namespace Flow\Floe\Tests\Context;

use Flow\ETL\Schema;

use function json_encode;

use const JSON_THROW_ON_ERROR;

final class FloeSchemaContext
{
    public static function schemaBody(Schema $schema): string
    {
        return json_encode($schema->normalize(), JSON_THROW_ON_ERROR);
    }
}
