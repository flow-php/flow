<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

use function Flow\Filesystem\DSL\path_real;

require __DIR__ . '/vendor/autoload.php';

putenv('FLOW_MONOREPO_PROJECT_ROOT=' . __DIR__);
$_ENV['FLOW_MONOREPO_PROJECT_ROOT'] = $_SERVER['FLOW_MONOREPO_PROJECT_ROOT'] = __DIR__;

$dotenv = (new Dotenv())->usePutenv(true);

if (is_file($distEnv = __DIR__ . '/.env.dist')) {
    $dotenv->load($distEnv);
}

if (is_file($localEnv = __DIR__ . '/.env')) {
    $dotenv->overload($localEnv);
}

$cacheDir = (string) getenv('FLOW_LOCAL_FILESYSTEM_CACHE_DIR');

if ($cacheDir === '') {
    throw new RuntimeException('FLOW_LOCAL_FILESYSTEM_CACHE_DIR unset');
}

$resolvedCacheDir = path_real($cacheDir)->path();

if (in_array($resolvedCacheDir, ['', '/', __DIR__, getcwd()], true)) {
    throw new RuntimeException(sprintf(
        'FLOW_LOCAL_FILESYSTEM_CACHE_DIR resolves to "%s" (project root, cwd or filesystem root) — refusing to run.',
        $resolvedCacheDir,
    ));
}
