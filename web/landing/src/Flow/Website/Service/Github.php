<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use Flow\Bridge\Psr18\Telemetry\PSR18TraceableClient;
use Flow\ETL\Memory\ArrayMemory;
use Flow\Website\Factory\Github\ContributorsRequestFactory;
use Http\Client\Curl\Client;
use Http\Discovery\Psr17Factory;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;
use Throwable;

use function Flow\ETL\Adapter\Http\from_dynamic_http_requests;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\from_cache;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\not;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\rename_replace;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\to_memory;

final readonly class Github
{
    public function __construct(
        private ContributorsRequestFactory $requestFactory,
        private ContainerBagInterface $parameters,
        private FlowConfigFactory $configFactory,
    ) {}

    public function contributors(): array
    {
        if (in_array($this->parameters->get('kernel.environment'), ['test', 'dev'], true)) {
            return [
                [
                    'login' => 'norberttech',
                    'avatar_url' => 'https://avatars.githubusercontent.com/u/1921950?v=4&s=128',
                    'html_url' => 'https://github.com/norberttech',
                ],
            ];
        }

        $factory = new Psr17Factory();
        $client = new PSR18TraceableClient(new Client($factory, $factory), $this->configFactory->telemetry());

        $from_github = from_dynamic_http_requests($client, $this->requestFactory);

        try {
            df($this->configFactory->configBuilderWithCache('github_contributors'))
                ->read(from_cache('flow_github_contributors', $from_github))
                ->cache('flow_github_contributors')
                ->withEntry('unpacked', ref('response_body')->jsonDecode())
                ->select('unpacked')
                ->withEntry('data', ref('unpacked')->expand())
                ->withEntry(
                    'data',
                    ref('data')->unpack(schema(str_schema('login'), str_schema('avatar_url'), str_schema('html_url'))),
                )
                ->renameEach(rename_replace('data.', ''))
                ->drop('unpacked', 'data')
                ->filter(not(ref('login')->endsWith(lit('[bot]'))))
                ->filter(not(ref('login')->equals(lit('aeon-automation'))))
                ->filter(not(ref('login')->equals(lit('norbertmwk'))))
                ->withEntry('avatar_url', ref('avatar_url')->concat(lit('&s=128')))
                ->limit(24)
                ->write(to_memory($memory = new ArrayMemory()))
                ->run();

            return $memory->dump();
        } catch (Throwable) {
            return [];
        }
    }
}
