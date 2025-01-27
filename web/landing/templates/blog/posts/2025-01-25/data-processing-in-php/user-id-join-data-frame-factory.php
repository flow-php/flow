<?php

use Doctrine\DBAL\ArrayParameterType;
use Doctrine\DBAL\Connection;
use Flow\ETL\DataFrame;
use Flow\ETL\DataFrameFactory;
use Flow\ETL\Rows;
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\df;

final readonly class UserIdJoinDataFrameFactory implements DataFrameFactory
{

    public function __construct(private Connection $connection)
    {
    }

    public function from(Rows $rows): DataFrame
    {
        return df()->read(
            from_dbal_query(
                $this->connection,
                "SELECT id as user_id, email as user_email FROM users WHERE email IN (:emails)",
                ['emails' => $rows->reduceToArray(ref('user_email'))],
                ['emails' => ArrayParameterType::STRING]
            )
        );
    }
}