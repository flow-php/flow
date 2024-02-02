<?php declare(strict_types=1);

namespace Flow\Website\Controller;

use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Adapter\Http\PsrHttpClientDynamicExtractor;
use Flow\ETL\Filesystem\SaveMode;
use Flow\Website\Factory\Github\ContributorsUrlFactory;
use Http\Client\Curl\Client;
use Nyholm\Psr7\Factory\Psr17Factory;
use Symfony\Bundle\FrameworkBundle\Controller\AbstractController;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;
use Symfony\Component\HttpFoundation\Response;
use Symfony\Component\Routing\Attribute\Route;

final class DefaultController extends AbstractController
{
    private readonly string $path;

    public function __construct(
        ContainerBagInterface $parameters,
        private readonly ContributorsUrlFactory $contributorsUrlFactory
    ) {
        $this->path = $parameters->get('kernel.cache_dir') . '/data/latest_contributors.json';
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
            // Save with overwrite
            ->mode(SaveMode::Overwrite)
            ->write(to_json($this->path))
            // Execute
            ->run();

        return $this->render('main/index.html.twig', [
            'contributors' => \json_decode(\file_get_contents($this->path)),
        ]);
    }
}
