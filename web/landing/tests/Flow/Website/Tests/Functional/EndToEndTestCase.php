<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Functional;

use Flow\Website\Kernel;
use Symfony\Component\Panther\{Client, PantherTestCase};

abstract class EndToEndTestCase extends PantherTestCase
{
    private array $tempFiles = [];

    protected function tearDown() : void
    {
        foreach ($this->tempFiles as $file) {
            @\unlink($file);
        }

        parent::tearDown();
    }

    protected function clearLocalStorage(Client $client) : void
    {
        $client->executeScript('window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-storage").clearStorage();');
    }

    protected function createTempFile(string $filename, string $content) : string
    {
        $path = \sys_get_temp_dir() . '/' . $filename;
        \file_put_contents($path, $content);
        $this->tempFiles[] = $path;

        return $path;
    }

    protected function dismissAlert(Client $client) : void
    {
        try {
            $client->switchTo()->alert()->accept();
        } catch (\Exception) {
        }
    }

    protected function getFromLocalStorage(Client $client, string $key) : ?string
    {
        return $client->executeScript(\sprintf('return localStorage.getItem(%s);', \json_encode($key)));
    }

    protected function getPlaygroundCode(Client $client) : string
    {
        return $client->executeScript(
            'const textarea = document.getElementById("code-editor");
             const controller = window.Stimulus.getControllerForElementAndIdentifier(textarea, "code-mirror-editor");
             return controller.getCode();'
        );
    }

    protected function saveToLocalStorage(Client $client) : void
    {
        $client->executeScript('window.Stimulus.getControllerForElementAndIdentifier(document.getElementById("playground"), "playground-storage").saveCode();');
    }

    protected function setPlaygroundCode(Client $client, string $code) : void
    {
        $client->executeScript(\sprintf(
            'const textarea = document.getElementById("code-editor");
             const controller = window.Stimulus.getControllerForElementAndIdentifier(textarea, "code-mirror-editor");
             controller.setValue(%s);',
            \json_encode($code)
        ));
    }

    protected static function createE2EClient(array $options = []) : Client
    {
        return static::createPantherClient(\array_merge([
            'env' => ['APP_ENV' => 'test'],
        ], $options));
    }

    protected static function getKernelClass() : string
    {
        return Kernel::class;
    }
}
