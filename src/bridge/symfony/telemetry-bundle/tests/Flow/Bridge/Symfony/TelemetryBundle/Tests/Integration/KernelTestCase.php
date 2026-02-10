<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Integration;

use Flow\Bridge\Symfony\TelemetryBundle\Tests\Context\SymfonyContext;
use Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\TestKernel;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\ContainerInterface;

abstract class KernelTestCase extends TestCase
{
    private SymfonyContext $context;

    protected function setUp() : void
    {
        $this->context = new SymfonyContext();
    }

    protected function tearDown() : void
    {
        $this->context->shutdown();
    }

    /**
     * @param array{config?: callable(TestKernel): void} $options
     */
    protected function bootKernel(array $options = []) : TestKernel
    {
        return $this->context->bootKernel($options);
    }

    protected function getContainer() : ContainerInterface
    {
        return $this->context->getContainer();
    }

    protected function getKernel() : TestKernel
    {
        return $this->context->getKernel();
    }

    protected function symfonyContext() : SymfonyContext
    {
        return $this->context;
    }
}
