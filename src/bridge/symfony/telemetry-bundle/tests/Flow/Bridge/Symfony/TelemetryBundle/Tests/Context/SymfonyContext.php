<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Context;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use Symfony\Component\DependencyInjection\ContainerInterface;
use Symfony\Component\Filesystem\Filesystem;

final class SymfonyContext
{
    private ?TestKernel $kernel = null;

    /**
     * @param array{config?: callable(TestKernel): void} $options
     */
    public function bootKernel(array $options = []) : TestKernel
    {
        if ($this->kernel !== null) {
            $this->shutdown();
        }

        $this->kernel = new TestKernel('test', true);

        if (isset($options['config']) && \is_callable($options['config'])) {
            $options['config']($this->kernel);
        }

        $this->kernel->boot();

        return $this->kernel;
    }

    public function getContainer() : ContainerInterface
    {
        if ($this->kernel === null) {
            throw new \LogicException('Kernel has not been booted. Call bootKernel() first.');
        }

        return $this->kernel->getContainer();
    }

    public function getKernel() : TestKernel
    {
        if ($this->kernel === null) {
            throw new \LogicException('Kernel has not been booted. Call bootKernel() first.');
        }

        return $this->kernel;
    }

    public function shutdown() : void
    {
        if ($this->kernel === null) {
            return;
        }

        $cacheDir = $this->kernel->getCacheDir();
        $logDir = $this->kernel->getLogDir();

        $this->kernel->shutdown();
        $this->kernel = null;

        $filesystem = new Filesystem();

        if ($filesystem->exists($cacheDir)) {
            $filesystem->remove($cacheDir);
        }

        if ($filesystem->exists($logDir)) {
            $filesystem->remove($logDir);
        }
    }
}
