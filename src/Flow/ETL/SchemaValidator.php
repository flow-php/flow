<?php declare(strict_types=1);

namespace Flow\ETL;

use Flow\ETL\Row\Schema;
use Flow\Serializer\Serializable;

/**
 * @template T
 * @extends Serializable<T>
 */
interface SchemaValidator extends Serializable
{
    public function isValid(Rows $rows, Schema $schema) : bool;
}
