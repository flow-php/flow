# Nix - Development Environment

Nix is probably the easiest way of setting up the development environment.

Before you start please make sure you have Nix installed. 
If you don't have it installed, you can install it by following official documentation.

[Nix installation instructions](https://nixos.org/download/)

> Since this project is using Nix Flakes, which are still 
> considered experimental, you need to enable them first globally.

[Enable Flakes](https://nixos.wiki/wiki/Flakes#Other_Distros,_without_Home-Manager)

Once you have Nix installed and flakes enabled, you can start your development environment
by going to the project folder and running following command:

```bash
nix develop
```

That's all, after running this command you will have all the necessary tools and dependencies.
Nix will create a new shell with all the necessary tools and dependencies for the project.

By default we are using [Starship](https://starship.rs/) to provide a nice bash prompt.
You can override it by creating `/.nix/shell/starship.toml` based on `/.nix/shell/starship.toml.dist` 
file. 

Once you apply your modification you can run `nix develop` again to apply changes.

To use the php version from nix inside your IDE please start a nix shell `nix develop`
and type: 

```shell
type php
```

This should return you path to your php version that is used inside of the nix shell.
It will look like this: 

```shell
php is /nix/store/p2m5bamh01ncpwjxscdl11p2m9xy8aq6-php-with-extensions-8.2.27/bin/php
```