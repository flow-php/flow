<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Profiler;

use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\Client\Client;
use Psr\Container\ContainerInterface;
use SensitiveParameter;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\HttpKernel\Profiler\Profiler;
use Throwable;
use Twig\Environment;

use function array_key_exists;
use function array_values;

/**
 * Renders the "Explain query" fragment for the Flow PostgreSQL profiler panel. Reached via the
 * standard `_profiler` route with `page=explain` and a controller sub-request (no custom route),
 * mirroring how the Doctrine bundle exposes EXPLAIN. Dev-only: this service is registered solely
 * when the profiler panel is enabled.
 *
 * @internal
 */
final readonly class ProfilerController
{
    public function __construct(
        private Environment $twig,
        private ContainerInterface $clients,
        private Profiler $profiler,
    ) {}

    public function explainAction(#[SensitiveParameter] string $token, string $connection, int $query): Response
    {
        $this->profiler->disable();

        $profile = $this->profiler->loadProfile($token);

        if ($profile === null || !$profile->hasCollector('flow_postgresql')) {
            return new Response('This query does not exist.');
        }

        $collector = $profile->getCollector('flow_postgresql');

        if (!$collector instanceof FlowPostgreSqlDataCollector) {
            return new Response('This query does not exist.');
        }

        $queries = $collector->getQueries();

        if (!array_key_exists($connection, $queries) || !array_key_exists($query, $queries[$connection])) {
            return new Response('This query does not exist.');
        }

        $row = $queries[$connection][$query];

        if (!$row['explainable']) {
            return new Response('This query cannot be explained.');
        }

        if (!$this->clients->has($connection)) {
            return new Response('This query cannot be explained.');
        }

        // @mago-expect analysis:mixed-assignment
        $client = $this->clients->get($connection);

        if (!$client instanceof Client) {
            return new Response('This query cannot be explained.');
        }

        try {
            $plan = $client->explain($row['statement'], array_values($row['parameters']), ExplainConfig::forEstimate());
        } catch (Throwable $e) {
            return new Response('An error occurred while explaining the query: ' . $e->getMessage());
        }

        return new Response($this->twig->render('@FlowPostgreSql/Collector/explain.html.twig', [
            'plan' => $plan,
        ]));
    }
}
