<?php declare(strict_types=1);

namespace Flow\Website\Controller;

use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\to_memory;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Memory\ArrayMemory;
use Flow\Website\Factory\Github\ContributorsUrlFactory;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class DefaultController extends AbstractController
{
    public function __construct(
        private readonly ContributorsUrlFactory $contributorsUrlFactory
    ) {
    }

    #[Route('/', name: 'main')]
    public function main() : Response
    {
        $factory = new Psr17Factory();
        $client = new Client($factory, $factory);

        df()
            ->read(
                new PsrHttpClientDynamicExtractor($client, $this->contributorsUrlFactory)
            )
            // Extract response
            ->withEntry('unpacked', ref('response_body')->jsonDecode())
            ->select('unpacked')
            // Extract data as rows & columns
            ->withEntry('data', ref('unpacked')->expand())
            ->withEntry('data', ref('data')->unpack())
            ->renameAll('data.', '')
            ->drop('unpacked', 'data')
            // Filter out bots & limit the end list
            ->filter(not(ref('login')->endsWith(lit('[bot]'))))
            ->filter(not(ref('login')->equals(lit('aeon-automation'))))
            ->limit(24)
            // Store result in memory
            ->write(to_memory($memory = new ArrayMemory()))
            // Execute
            ->run();

        return $this->render('main/index.html.twig', [
            'contributors' => $memory->data,
        ]);
    }
}
