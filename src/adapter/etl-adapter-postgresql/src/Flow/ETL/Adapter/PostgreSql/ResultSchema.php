<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Exception\QueryException;

use function array_values;
use function Flow\ETL\DSL\definition_from_type;
use function in_array;
use function sprintf;

final readonly class ResultSchema
{
    /**
     * The zero-row wrapper names no table, column, function, type or privilege of its own: these states can only come
     * from the query, and the read itself fails with the same one.
     */
    private const array QUERY_OWN_STATES = ['42P01', '42703', '42883', '42704', '42501'];

    public function __construct(
        private EntryTypesMap $typesMap = new EntryTypesMap(),
    ) {}

    /**
     * @param list<mixed> $parameters
     * @param class-string $extractor
     *
     * @throws QueryException
     * @throws SchemaNotDerivableException
     */
    public function of(Client $client, ReadQuery $query, array $parameters, string $extractor): Schema
    {
        $nested = $client->getTransactionNestingLevel() > 0;

        if ($nested) {
            $client->beginTransaction();
        }

        try {
            $columns = $client->describe($query->sql(), $parameters);
        } catch (QueryException $e) {
            if (in_array($e->error()->sqlState, self::QUERY_OWN_STATES, true)) {
                throw $e;
            }

            throw SchemaNotDerivableException::probeRefused(
                $extractor,
                sprintf('PostgreSQL refused the zero-row probe of this query (%s)', $e->getMessage()),
                $e,
            );
        } finally {
            if ($nested) {
                $client->rollBack();
            }
        }

        $definitions = [];

        foreach ($columns as $column) {
            try {
                $type = $this->typesMap->toFlowTypeWithTextFloor($column['type']);
            } catch (TypeMappingException) {
                throw SchemaNotDerivableException::extractor($extractor, sprintf(
                    'column "%s" has PostgreSQL type "%s", which Flow has no type for',
                    $column['name'],
                    $column['type']->normalize()['name'],
                ));
            }

            $definitions[$column['name']] = definition_from_type($column['name'], $type, nullable: true);
        }

        return new Schema(...array_values($definitions));
    }
}
