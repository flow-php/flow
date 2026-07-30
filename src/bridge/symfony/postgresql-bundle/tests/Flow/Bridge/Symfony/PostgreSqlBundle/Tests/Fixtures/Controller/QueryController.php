<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures\Controller;

use Flow\PostgreSql\Client\Client;
use Symfony\Component\HttpFoundation\JsonResponse;
use Symfony\Component\HttpFoundation\Response;

use function count;

final class QueryController
{
    public function __construct(
        private readonly Client $client,
    ) {}

    public function run(): Response
    {
        $rows = $this->client->fetchAll('SELECT n FROM (VALUES (1), (2), (3)) AS t(n) WHERE n >= $1', [2]);

        return new JsonResponse(['count' => count($rows)]);
    }

    public function runFailing(): Response
    {
        $this->client->execute('SELECT * FROM table_that_does_not_exist');

        return new JsonResponse(['unreachable' => true]);
    }
}
