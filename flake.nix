{
	inputs = {
		# Required for Nix 2.27.0+ when using git submodules
		self.submodules = true;

		biobricks-script-lib.url = "path:./vendor/biobricks-script-lib";
		qendpoint-manage.url = "path:./vendor/biobricks-script-lib/component/qendpoint-manage";

		nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";
		flake-utils.url = "github:numtide/flake-utils";
		dev-shell.url = "github:biobricks-ai/dev-shell";
	};

	outputs = { self, nixpkgs, flake-utils, biobricks-script-lib, dev-shell, qendpoint-manage }:
		flake-utils.lib.eachDefaultSystem (system:
			with import nixpkgs { inherit system; }; {
				devShells.default = dev-shell.devShells.${system}.default.overrideAttrs
        (oldAttrs: {
					buildInputs = [
					] ++ biobricks-script-lib.packages.${system}.buildInputs;

					shellHook = ''
						# Activate biobricks-script-lib environment
						eval $(${biobricks-script-lib.packages.${system}.activateScript})

            source .venv/bin/activate
					'';
				});
			});
}