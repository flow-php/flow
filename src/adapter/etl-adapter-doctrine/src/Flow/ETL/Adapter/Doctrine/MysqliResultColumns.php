<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine;

use Flow\ETL\Exception\SchemaNotDerivableException;
use mysqli;
use mysqli_sql_exception;

use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function sprintf;

final readonly class MysqliResultColumns
{
    /**
     * @param class-string $extractor
     *
     * @throws SchemaNotDerivableException
     *
     * @return list<ResultColumn>
     */
    public function of(mysqli $connection, string $sql, string $extractor): array
    {
        try {
            $statement = $connection->prepare((new NativePlaceholders())->toMysqli($sql)->sql)
            ?: throw new mysqli_sql_exception($connection->error);
        } catch (mysqli_sql_exception $e) {
            throw SchemaNotDerivableException::extractor($extractor, sprintf(
                'MySQL refused to prepare this query (%s)',
                $e->getMessage(),
            ));
        }

        try {
            $metadata = $statement->result_metadata();

            if ($metadata === false) {
                throw SchemaNotDerivableException::extractor(
                    $extractor,
                    'MySQL reports no result columns for this query, so it cannot be typed',
                );
            }

            $columns = [];

            // fetch_fields() reports MYSQLI_NOT_NULL_FLAG correctly and it is deliberately ignored,
            // so a derived schema reads the same on every driver.
            foreach ($metadata->fetch_fields() as $field) {
                $columns[] = new ResultColumn(
                    // @mago-expect analysis:ambiguous-object-property-access
                    type_string()->assert($field->name),
                    // @mago-expect analysis:ambiguous-object-property-access
                    type_integer()->assert($field->type),
                );
            }

            $metadata->free();

            return $columns;
        } finally {
            $statement->close();
        }
    }
}
