<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Response;

use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\Bridge\Symfony\HttpFoundation\StreamClosure;
use Flow\ETL\API\Config\ConfigBuilder;
use Flow\ETL\API\DataFrame;
use Flow\ETL\API\Transformation;
use Flow\ETL\API\Transformations;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor;
use Flow\ETL\Runtime\Config;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\StdOutFilesystem;
use Symfony\Component\HttpFoundation\StreamedResponse;

use function bin2hex;
use function Flow\ETL\DSL\df;
use function Flow\Filesystem\DSL\path;
use function random_bytes;

class FlowStreamedResponse extends StreamedResponse
{
    private readonly Config|ConfigBuilder $config;

    /**
     * @param array<string, mixed> $headers
     *
     * @throws InvalidArgumentException when a config is given for a DataFrame source, which carries its own
     */
    public function __construct(
        private readonly Extractor|DataFrame $source,
        private readonly Output $output,
        private readonly Transformation $transformations = new Transformations(),
        int $status = 200,
        array $headers = [],
        Config|ConfigBuilder|null $config = null,
        private readonly ?StreamClosure $streamClosure = null,
        private readonly Filesystem $filesystem = new StdOutFilesystem(),
    ) {
        if ($source instanceof DataFrame && $config !== null) {
            throw new InvalidArgumentException('A DataFrame source carries its own config');
        }

        $this->config = $config ?? (new ConfigBuilder())->build();

        parent::__construct($this->stream(...), $status, $headers);

        if (!$this->headers->get('Content-Type')) {
            $this->headers->set('Content-Type', $this->output->type()->toContentTypeHeader());
        }
    }

    private function stream(): void
    {
        $stdoutPath = path($this->filesystem->mount()->protocol . '://' . bin2hex(random_bytes(16)) . '.stdout', [
            'stream' => 'output',
        ]);

        $report = ($this->source instanceof DataFrame ? $this->source : df($this->config)->read($this->source))
            ->with($this->transformations)
            ->write($this->output->loader($stdoutPath, $this->filesystem))
            ->run();

        if ($this->streamClosure !== null) {
            $this->streamClosure->onComplete($report);
        }
    }
}
