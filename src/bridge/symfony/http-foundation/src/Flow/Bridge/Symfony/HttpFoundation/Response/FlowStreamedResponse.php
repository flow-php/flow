<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Response;

use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\Bridge\Symfony\HttpFoundation\StreamClosure;
use Flow\ETL\Config;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\Extractor;
use Flow\ETL\Transformation;
use Flow\ETL\Transformations;
use Symfony\Component\HttpFoundation\StreamedResponse;

use function Flow\ETL\DSL\df;
use function Flow\Filesystem\DSL\path;

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
        private readonly ?StreamClosure $streamClosure = null,
        private readonly string $filesystem = 'stdout',
    ) {
        $this->config = $config ?? Config::default();

        parent::__construct($this->stream(...), $status, $headers);

        if (!$this->headers->get('Content-Type')) {
            $this->headers->set('Content-Type', $this->output->type()->toContentTypeHeader());
        }
    }

    private function stream(): void
    {
        $stdoutPath = path($this->filesystem . '://' . \bin2hex(\random_bytes(16)) . '.stdout', ['stream' => 'output']);

        $report = df($this->config)
            ->read($this->extractor)
            ->with($this->transformations)
            ->dropPartitions()
            ->write($this->output->loader($stdoutPath))
            ->run();

        if ($this->streamClosure !== null) {
            $this->streamClosure->onComplete($report);
        }
    }
}
