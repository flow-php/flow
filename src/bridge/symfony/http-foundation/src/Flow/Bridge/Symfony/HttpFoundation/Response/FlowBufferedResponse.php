<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundation\Response;

use function Flow\ETL\DSL\df;
use function Flow\Filesystem\DSL\path;
use Flow\Bridge\Symfony\HttpFoundation\Output;
use Flow\ETL\Config\ConfigBuilder;
use Flow\ETL\{Config, Extractor, Transformation, Transformations};
use Symfony\Component\HttpFoundation\Response;

final class FlowBufferedResponse extends Response
{
    private bool $buffered = false;

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
        private readonly string $filesystem = 'memory',
    ) {
        $this->config = $config ?? Config::default();

        parent::__construct(null, $status, $headers);
    }

    #[\Override]
    public function getContent() : string
    {
        $this->evaluate();

        return $this->content;
    }

    #[\Override]
    public function sendContent() : static
    {
        $this->evaluate();

        print $this->content;

        return $this;
    }

    private function evaluate() : void
    {
        if ($this->buffered) {
            return;
        }

        $config = $this->config instanceof ConfigBuilder ? $this->config->build() : $this->config;

        $id = \bin2hex(\random_bytes(16)) . '.memory';
        $bufferPath = path($this->filesystem . '://' . $id, ['stream' => 'temp']);

        df($config)
            ->read($this->extractor)
            ->with($this->transformations)
            ->dropPartitions()
            ->write($this->output->loader($bufferPath))
            ->run();

        $fs = $config->fstab()->for($this->filesystem);

        if ($fs->status($bufferPath) === null) {
            $this->buffered = true;
            $this->content = '';
            $this->statusCode = self::HTTP_NO_CONTENT;

            return;
        }

        $this->content = $fs->readFrom($bufferPath)->content();
        $this->buffered = true;
    }
}
