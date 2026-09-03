<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql;

use Flow\ETL\Adapter\PostgreSql\Exception\TypeMappingException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Schema;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\Exception\QueryException;
use Flow\PostgreSql\QueryBuilder\Sql;

use function array_values;
use function Flow\ETL\DSL\definition_from_type;
use function sprintf;

final readonly class ResultSchema
{
    public function __construct(
        private EntryTypesMap $typesMap = new EntryTypesMap(),
    ) {}

    /**
     * @param list<mixed> $parameters
     * @param class-string $extractor
     *
     * @throws SchemaNotDerivableException
     */
    public function of(Client $client, Sql|string $query, array $parameters, string $extractor): Schema
    {
        $nested = $client->getTransactionNestingLevel() > 0;

        if ($nested) {
            $client->beginTransaction();
        }

        try {
            $columns = $client->describe($query, $parameters);
        } catch (QueryException $e) {
            throw SchemaNotDerivableException::extractor($extractor, sprintf(
                'PostgreSQL refused the zero-row probe of this query (%s)',
                $e->getMessage(),
            ));
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
