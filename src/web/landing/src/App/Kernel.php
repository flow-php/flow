<?php declare(strict_types=1);

namespace App;

use Symfony\Bundle\FrameworkBundle\Kernel\MicroKernelTrait;
use Symfony\Component\DependencyInjection\Loader\Configurator\ContainerConfigurator;
use Symfony\Component\HttpKernel\Kernel as BaseKernel;
use Symfony\Component\Routing\Loader\Configurator\RoutingConfigurator;

class Kernel extends BaseKernel
{
    use MicroKernelTrait;

    public function getCacheDir() : string
    {
        return __DIR__ . '/../../var/cache/' . $this->getEnvironment();
    }

    public function getLogDir() : string
    {
        return __DIR__ . '/../../var/log';
    }

    public function registerBundles() : array
    {
        $bundles = [
            new \Symfony\Bundle\FrameworkBundle\FrameworkBundle(),
            new \Symfony\Bundle\TwigBundle\TwigBundle(),
        ];

        if ('dev' === $this->getEnvironment()) {
            $bundles[] = new \Symfony\Bundle\WebProfilerBundle\WebProfilerBundle();
            $bundles[] = new \Symfonycasts\TailwindBundle\SymfonycastsTailwindBundle();
            $bundles[] = new \NorbertTech\StaticContentGeneratorBundle\StaticContentGeneratorBundle();
        }

        return $bundles;
    }

    protected function configureContainer(ContainerConfigurator $container) : void
    {
        $container->extension('framework', [
            'secret' => 'S0ME_SECRET',
            'profiler' => [
                'only_exceptions' => false,
            ],
            'asset_mapper' => [
                'paths' => [
                    '%kernel.project_dir%/assets/',
                ],
            ],
        ]);

        $container->extension('static_content_generator', [
            'output_directory' => '%kernel.project_dir%/public/',
        ]);

        $container->services()
            ->load('App\\', __DIR__ . '/*')
            ->autowire()
            ->autoconfigure();

        if (isset($this->bundles['WebProfilerBundle'])) {
            $container->extension('web_profiler', [
                'toolbar' => true,
                'intercept_redirects' => false,
            ]);
        }
    }

    protected function configureRoutes(RoutingConfigurator $routes) : void
    {
        if (isset($this->bundles['WebProfilerBundle'])) {
            $routes->import('@WebProfilerBundle/Resources/config/routing/wdt.xml')->prefix('/_wdt');
            $routes->import('@WebProfilerBundle/Resources/config/routing/profiler.xml')->prefix('/_profiler');
        }

        $routes->add('main', '/')->methods(['GET'])->controller('App\\Controller\\DefaultController::main');
    }
}
