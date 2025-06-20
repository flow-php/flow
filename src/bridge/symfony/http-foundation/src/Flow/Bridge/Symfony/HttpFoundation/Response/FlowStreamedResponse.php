<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Response;

use function Flow\ETL\DSL\df;
use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\{Config, Extractor, Transformation};
use Flow\ETL\{Transformations};
use Symfony\Component\HttpFoundation\StreamedResponse;

class FlowStreamedResponse extends StreamedResponse
{
    private readonly Config|ConfigBuilder $config;

    /**
     * @param array<string, mixed> $headers
     */
    public function __construct(
        private readonly Extractor $extractor,
        private readonly Output $output,
        private readonly Transformation $transformations = new Transformations(),
        int $status = 200,
        array $headers = [],
        Config|ConfigBuilder|null $config = null,
    ) {
        $this->config = $config ?? Config::default();

        parent::__construct($this->stream(...), $status, $headers);

        if (!$this->headers->get('Content-Type')) {
            $this->headers->set('Content-Type', $this->output->type()->toContentTypeHeader());
        }
    }

    private function stream() : void
    {
        df($this->config)
            ->read($this->extractor)
            ->with($this->transformations)
            ->dropPartitions()
            ->write($this->output->stdoutLoader())
            ->run();
    }
}
