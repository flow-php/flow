# Nix - Development Environment

Nix is probably the easiest way of setting up the development environment.

Before you start please make sure you have Nix installed. 
If you don't have it installed, you can install it by following official documentation.

[Nix installation instructions](https://nixos.org/download/)

Once you have Nix installed, you can start your development environment
by going to the project folder and running following command:

```bash
nix-shell
```

> If you are using Nix for the first time, it might take a while to download all the dependencies.

> To achieve full isolation, use `nix-shell --pure` command. This way nix will isolate your development environment from the system.

That's all, after running this command you will have all the necessary tools and dependencies.
Nix will create a new shell with all the necessary tools and dependencies for the project.

By default, we’re using [Starship](https://starship.rs/) to provide a nice bash prompt.
You can override it by creating `/.nix/shell/starship.toml` based on `/.nix/shell/starship.toml.dist` 
file. 

Once you apply your modification you can run `nix-shell` again to apply changes.

To use the php version from nix inside your IDE please start a nix shell `nix-shell`
and type: 

```shell
type php
```

This should return you path to your php version that is used inside of the nix shell.
It will look like this: 

```shell
php is /nix/store/p2m5bamh01ncpwjxscdl11p2m9xy8aq6-php-with-extensions-8.2.27/bin/php
```

## php.ini

Nix shell comes with predefined php.ini, but if for any reason
it wouldn't be enough for you, you can create your own php.ini file in path:

`./.nix/php/lib/php.ini`

If that file is not present, the default php.ini.dist from the same location will be used.

## Pcov

- `pcov` - required for code coverage

To skip installing pcov extension, you can run nix shell with `--arg with-pco false` flag:

```shell

nix-shell --arg with-pcov false
```

To configure pcov, you can create a file `./.nix/php/lib/pcov.ini` with your xdebug configuration.

## Xdebug

- `xdebug` - required for debugging

To install xdebug extension, you can run nix shell with `--arg with-xdebug true` flag:

```shell
nix-shell --arg with-xdebug true
```

To configure xdebug, you can create a file `./.nix/php/lib/xdebug.ini` with your xdebug configuration.

## Blackfire

- `blackfire` - required for profiling

To install blackfire extension, you can run nix shell with `--arg with-blackfire true` flag:

```shell
nix-shell --arg with-blackfire true
```

To configure blackfire, you can create a file `./.nix/php/lib/blackfire.ini` with your blackfire configuration.

## Changing PHP Versions

To change the PHP version, you can run nix shell with `--arg php-version 8.3` flag:

```shell
nix-shell --arg php-version 8.3
```

> In general, it's not recommended to change the PHP version, as development should always
> be done on the lowest supported PHP version. 
>
> This feature is mostly for testing new integrations
> or lowest/highest versions of dependencies.

## Local Webserver

To run the local webserver for Flow Website development, please use Symfony CLI app
that is also available in nix shell.

```shell
cd web/landing
symfony proxy:start
symfony server:start -d 
```

You can read more about it here: 

- [How to use .wip domain for development](https://symfony.com/doc/current/setup/symfony_server.html#setting-up-the-local-proxy)
