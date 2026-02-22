<?php

declare(strict_types=1);

namespace Flow\Website\Service;

use function Flow\ETL\Adapter\Http\from_dynamic_http_requests;
use function Flow\ETL\DSL\{df, from_cache, lit, not, ref, rename_replace, to_memory};
use Flow\Bridge\Psr18\Telemetry\PSR18TraceableClient;
use Flow\ETL\Memory\ArrayMemory;
use Flow\Website\Factory\Github\ContributorsRequestFactory;
use Http\Client\Curl\Client;
use Http\Discovery\Psr17Factory;
use Symfony\Component\DependencyInjection\ParameterBag\ContainerBagInterface;

final readonly class Github
{
    public function __construct(
        private ContributorsRequestFactory $requestFactory,
        private ContainerBagInterface $parameters,
        private FlowConfigFactory $configFactory,
    ) {
    }

    public function contributors() : array
    {
        if ($this->parameters->get('kernel.environment') === 'test') {
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
                ->read(
                    from_cache(
                        'flow_github_contributors',
                        $from_github
                    )
                )
                ->cache('flow_github_contributors')
                ->withEntry('unpacked', ref('response_body')->jsonDecode())
                ->select('unpacked')
                ->withEntry('data', ref('unpacked')->expand())
                ->withEntry('data', ref('data')->unpack())
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
        } catch (\Exception) {
            return [];
        }
    }
}
